package postgres

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math/rand/v2"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
	timestamppb "google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/openclimatefix/data-platform/internal/gen/ocf/dp"
)

func ptr[T any](v T) *T {
	return &v
}

func createTestMetadata(t *testing.T, m map[string]any) *structpb.Struct {
	t.Helper()
	md, err := structpb.NewStruct(m)
	require.NoError(t, err)
	return md
}

func createTestLocation(
	t *testing.T,
	name, wkt string,
	capacity uint64,
	ts time.Time,
	metadata *structpb.Struct,
) *pb.CreateLocationResponse {
	t.Helper()
	resp, err := dc.CreateLocation(t.Context(), &pb.CreateLocationRequest{
		LocationName:           name,
		GeometryWkt:            wkt,
		EffectiveCapacityWatts: capacity,
		Metadata:               metadata,
		EnergySource:           pb.EnergySource_ENERGY_SOURCE_SOLAR,
		LocationType:           pb.LocationType_LOCATION_TYPE_SITE,
		ValidFromUtc:           timestamppb.New(ts),
	})
	require.NoError(t, err)

	return resp
}

func createTestForecaster(t *testing.T, name, version string) *pb.Forecaster {
	t.Helper()
	resp, err := dc.CreateForecaster(t.Context(), &pb.CreateForecasterRequest{
		Name:    name,
		Version: version,
	})
	require.NoError(t, err)

	return resp.Forecaster
}

// generateTestForecastValues creates a slice of ForecastValues with predictable plevels.
func generateTestForecastValues(
	numHorizons int,
	intervalMins uint32,
) []*pb.CreateForecastRequest_ForecastValue {
	values := make([]*pb.CreateForecastRequest_ForecastValue, numHorizons)
	for i := range values {
		values[i] = &pb.CreateForecastRequest_ForecastValue{
			HorizonMins: uint32(i) * intervalMins,
			P50Fraction: float32(0.5 + float64(i)*0.001),
			OtherStatisticsFractions: map[string]float32{
				"p98": float32(0.98 + float32(i)*0.001),
				"p90": float32(0.9 + float32(i)*0.001),
				"p75": float32(0.75 + float32(i)*0.001),
				"p25": float32(0.25 + float32(i)*0.001),
				"p10": float32(0.1 + float32(i)*0.001),
				"p02": float32(0.02 + float32(i)*0.001),
			},
		}
	}

	return values
}

func createTestForecast(
	t *testing.T,
	locationUuid string,
	forecaster *pb.Forecaster,
	initTime time.Time,
	values []*pb.CreateForecastRequest_ForecastValue,
	createdTime time.Time,
) {
	t.Helper()
	req := &pb.CreateForecastRequest{
		LocationUuid:        locationUuid,
		Forecaster:          forecaster,
		EnergySource:        pb.EnergySource_ENERGY_SOURCE_SOLAR,
		InitTimeUtc:         timestamppb.New(initTime),
		Values:              values,
		Metadata:            createTestMetadata(t, map[string]any{"source": "test"}),
		CreatedTimestampUtc: timestamppb.New(createdTime),
	}
	_, err := dc.CreateForecast(t.Context(), req)
	require.NoError(t, err)
}

func TestCreateLocation(t *testing.T) {
	metadata := createTestMetadata(t, map[string]any{"source": "test"})

	testcases := []struct {
		name           string
		req            *pb.CreateLocationRequest
		expectedLatLng *pb.LatLng
		shouldErr      bool
	}{
		{
			name: "Should create solar location",
			req: &pb.CreateLocationRequest{
				LocationName:           "greenwich_observatory",
				EnergySource:           pb.EnergySource_ENERGY_SOURCE_SOLAR,
				GeometryWkt:            "POINT(0.0 51.5)",
				EffectiveCapacityWatts: 1230,
				LocationType:           pb.LocationType_LOCATION_TYPE_SITE,
				Metadata:               metadata,
			},
			expectedLatLng: &pb.LatLng{
				Latitude:  51.5,
				Longitude: 0.0,
			},
			shouldErr: false,
		},
		{
			name: "Shouldn't create unknown energy source",
			req: &pb.CreateLocationRequest{
				LocationName:           "unknown_energy_source",
				EnergySource:           pb.EnergySource_ENERGY_SOURCE_UNSPECIFIED,
				GeometryWkt:            "POINT(0.0 51.5)",
				EffectiveCapacityWatts: 1230,
				LocationType:           pb.LocationType_LOCATION_TYPE_SITE,
				Metadata:               metadata,
			},
			shouldErr: true,
		},
		{
			name: "Should create wind location",
			req: &pb.CreateLocationRequest{
				LocationName:           "london_eye",
				EnergySource:           pb.EnergySource_ENERGY_SOURCE_WIND,
				GeometryWkt:            "POINT(0.0 51.5)",
				EffectiveCapacityWatts: 4560,
				LocationType:           pb.LocationType_LOCATION_TYPE_SITE,
				Metadata:               metadata,
			},
			expectedLatLng: &pb.LatLng{
				Latitude:  51.5,
				Longitude: 0.0,
			},
			shouldErr: false,
		},
		{
			name: "Shouldn't create unknown location type",
			req: &pb.CreateLocationRequest{
				LocationName:           "unknown_location_type",
				EnergySource:           pb.EnergySource_ENERGY_SOURCE_SOLAR,
				GeometryWkt:            "POINT(0.0 51.5)",
				EffectiveCapacityWatts: 1230,
				LocationType:           pb.LocationType_LOCATION_TYPE_UNSPECIFIED,
				Metadata:               metadata,
			},
			shouldErr: true,
		},
		{
			name: "Shouldn't create location with empty name",
			req: &pb.CreateLocationRequest{
				LocationName:           "",
				EnergySource:           pb.EnergySource_ENERGY_SOURCE_SOLAR,
				GeometryWkt:            "POINT(0.0 51.5)",
				EffectiveCapacityWatts: 1230,
				LocationType:           pb.LocationType_LOCATION_TYPE_SITE,
				Metadata:               metadata,
			},
			shouldErr: true,
		},
		{
			name: "Should create location with pipe in the name",
			req: &pb.CreateLocationRequest{
				LocationName:           "location|with|pipe",
				EnergySource:           pb.EnergySource_ENERGY_SOURCE_SOLAR,
				GeometryWkt:            "POINT(0.0 51.5)",
				EffectiveCapacityWatts: 1230,
				LocationType:           pb.LocationType_LOCATION_TYPE_SITE,
				Metadata:               metadata,
			},
			expectedLatLng: &pb.LatLng{
				Latitude:  51.5,
				Longitude: 0.0,
			},
			shouldErr: false,
		},
		{
			name: "Should create location with large capacity",
			req: &pb.CreateLocationRequest{
				LocationName:           "oxfordshire",
				EnergySource:           pb.EnergySource_ENERGY_SOURCE_SOLAR,
				GeometryWkt:            "POLYGON((0.0 51.5, 1.0 51.5, 1.0 52.0, 0.0 52.0, 0.0 51.5))",
				EffectiveCapacityWatts: 1000e9,
				LocationType:           pb.LocationType_LOCATION_TYPE_GSP,
				Metadata:               metadata,
			},
			expectedLatLng: &pb.LatLng{
				Latitude:  51.75,
				Longitude: 0.5,
			},
			shouldErr: false,
		},
		{
			name: "Shouldn't create location with non-closed POLYGON geometry",
			req: &pb.CreateLocationRequest{
				LocationName:           "unclosed_polygon",
				EnergySource:           pb.EnergySource_ENERGY_SOURCE_SOLAR,
				GeometryWkt:            "POLYGON((0.0 51.5, 1.0 51.5, 1.0 52.0, 0.0 52.0))",
				EffectiveCapacityWatts: 14e6,
				LocationType:           pb.LocationType_LOCATION_TYPE_DNO,
				Metadata:               metadata,
			},
			shouldErr: true,
		},
		{
			name: "Should create location with closed MULTIPOLYGON geometry",
			req: &pb.CreateLocationRequest{
				LocationName:           "closed_multipolygon",
				EnergySource:           pb.EnergySource_ENERGY_SOURCE_WIND,
				GeometryWkt:            "MULTIPOLYGON(((0.0 51.5, 1.0 51.5, 1.0 52.0, 0.0 52.0, 0.0 51.5)),((2.0 51.5, 3.0 51.5, 3.0 52.0, 2.0 52.0, 2.0 51.5)))",
				EffectiveCapacityWatts: 1100e6,
				LocationType:           pb.LocationType_LOCATION_TYPE_DNO,
				Metadata:               metadata,
			},
			expectedLatLng: &pb.LatLng{
				Latitude:  51.75,
				Longitude: 1.5,
			},
			shouldErr: false,
		},
		{
			name: "Shouldn't create location with non-closed MULTIPOLYGON geometry",
			req: &pb.CreateLocationRequest{
				LocationName:           "unclosed_multipolygon",
				EnergySource:           pb.EnergySource_ENERGY_SOURCE_WIND,
				GeometryWkt:            "MULTIPOLYGON(((0.0 51.5, 1.0 51.5, 1.0 52.0, 0.0 52.0)),((2.0 51.5, 3.0 51.5, 3.0 52.0, 2.0 52.0)))",
				EffectiveCapacityWatts: 14e6,
				LocationType:           pb.LocationType_LOCATION_TYPE_DNO,
				Metadata:               metadata,
			},
			shouldErr: true,
		},
		{
			name: "Shouldn't create location with non WSG84 geometry",
			req: &pb.CreateLocationRequest{
				LocationName:           "non_wgs84",
				EnergySource:           pb.EnergySource_ENERGY_SOURCE_SOLAR,
				GeometryWkt:            "POINT(1000000 1000000)",
				EffectiveCapacityWatts: 10289e3,
				LocationType:           pb.LocationType_LOCATION_TYPE_SITE,
				Metadata:               metadata,
			},
			shouldErr: true,
		},
		{
			name: "Should create location with associated lat long",
			req: &pb.CreateLocationRequest{
				LocationName:           "closed_multipolygon_with_latlng",
				EnergySource:           pb.EnergySource_ENERGY_SOURCE_WIND,
				GeometryWkt:            "MULTIPOLYGON(((0.0 51.5, 1.0 51.5, 1.0 52.0, 0.0 52.0, 0.0 51.5)),((2.0 51.5, 3.0 51.5, 3.0 52.0, 2.0 52.0, 2.0 51.5)))",
				EffectiveCapacityWatts: 14e6,
				LocationType:           pb.LocationType_LOCATION_TYPE_DNO,
				Metadata:               metadata,
				AssociatedLatlng: &pb.LatLng{
					Latitude:  51.5074,
					Longitude: -0.1278,
				},
			},
			expectedLatLng: &pb.LatLng{
				Latitude:  51.5074,
				Longitude: -0.1278,
			},
			shouldErr: false,
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := dc.CreateLocation(t.Context(), tc.req)

			if tc.shouldErr {
				require.Error(t, err, "Expected not to be able to create the location")
			} else {
				// Try fetching the created location and check it's the same
				require.NoError(t, err, "Expected to be able to create the location")

				resp2, err := dc.GetLocation(
					t.Context(),
					&pb.GetLocationRequest{
						LocationUuid:    resp.LocationUuid,
						EnergySource:    tc.req.EnergySource,
						IncludeGeometry: false,
					},
				)
				require.NoError(t, err, "Expected to be able to see created location")
				require.Equal(t, tc.req.LocationName, resp2.LocationName)
				// require.Equal(t, tc.req.GeometryWkt, string(resp2.GeometryWkb))
				require.Equal(t, tc.req.EffectiveCapacityWatts, resp2.EffectiveCapacityWatts)
				require.Equal(t, tc.req.Metadata.AsMap(), resp2.Metadata.AsMap())
				require.Equal(t, tc.expectedLatLng, resp2.Latlng)
			}
		})
	}

	t.Run("Shouldn't get non-existent location", func(t *testing.T) {
		_, err := dc.GetLocation(
			t.Context(),
			&pb.GetLocationRequest{LocationUuid: uuid.New().String()},
		)
		require.Error(t, err)
	})
}

func TestUpdateLocation(t *testing.T) {
	metadata := createTestMetadata(t, map[string]any{"source": "test"})

	pivotTime := time.Date(2019, 5, 6, 6, 0, 0, 0, time.UTC)

	createResp := createTestLocation(
		t,
		"test_update_location_site",
		"POINT(-0.1 51.5)",
		1234e6,
		pivotTime.Add(-10*time.Hour),
		metadata,
	)

	newMetadata := createTestMetadata(t, map[string]any{"source": "test", "updated": true})

	testcases := []struct {
		name                  string
		req                   *pb.UpdateLocationRequest
		expectedName          string
		expectedCapacityWatts uint64
		expectedMetadata      map[string]any
		shouldErr             bool
	}{
		{
			name: "Should return the same when the update doesn't change anything",
			req: &pb.UpdateLocationRequest{
				LocationUuid:              createResp.LocationUuid,
				NewEffectiveCapacityWatts: ptr(uint64(1234e6)),
				EnergySource:              pb.EnergySource_ENERGY_SOURCE_SOLAR,
				ValidFromUtc:              timestamppb.New(pivotTime.Add(-4 * time.Hour)),
			},
			expectedName:          "test_update_location_site",
			expectedCapacityWatts: 1234e6,
			expectedMetadata:      map[string]any{"source": "test"},
			shouldErr:             false,
		},
		{
			name: "Should update capacity to higher value",
			req: &pb.UpdateLocationRequest{
				LocationUuid:              createResp.LocationUuid,
				EnergySource:              pb.EnergySource_ENERGY_SOURCE_SOLAR,
				NewEffectiveCapacityWatts: ptr(uint64(1235e6)),
				ValidFromUtc:              timestamppb.New(pivotTime.Add(-5 * time.Hour)),
			},
			expectedName:          "test_update_location_site",
			expectedCapacityWatts: 1235e6,
			expectedMetadata:      map[string]any{"source": "test"},
			shouldErr:             false,
		},
		{
			name: "Shouldn't update anything when nothing is set",
			req: &pb.UpdateLocationRequest{
				LocationUuid: "invalid-uuid",
				EnergySource: pb.EnergySource_ENERGY_SOURCE_SOLAR,
				ValidFromUtc: timestamppb.New(pivotTime.Add(-4 * time.Hour)),
			},
			shouldErr: true,
		},
		{
			name: "Should update name and metadata",
			req: &pb.UpdateLocationRequest{
				LocationUuid:    createResp.LocationUuid,
				EnergySource:    pb.EnergySource_ENERGY_SOURCE_SOLAR,
				NewLocationName: ptr("test_updated_location_site"),
				NewMetadata:     newMetadata,
				ValidFromUtc:    timestamppb.New(pivotTime.Add(-3 * time.Hour)),
			},
			expectedName:          "test_updated_location_site",
			expectedCapacityWatts: 1235e6,
			expectedMetadata:      map[string]any{"source": "test", "updated": true},
			shouldErr:             false,
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := dc.UpdateLocation(t.Context(), tc.req)

			if tc.shouldErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)

				newGetResp, err := dc.GetLocation(t.Context(), &pb.GetLocationRequest{
					LocationUuid:    resp.LocationUuid,
					EnergySource:    pb.EnergySource_ENERGY_SOURCE_SOLAR,
					IncludeGeometry: false,
					PivotTimestampUtc: timestamppb.New(
						tc.req.ValidFromUtc.AsTime().Add(time.Minute),
					),
				})
				require.NoError(t, err)
				require.Equal(t, tc.expectedName, newGetResp.LocationName)
				require.Equal(
					t,
					int(tc.expectedCapacityWatts),
					int(newGetResp.EffectiveCapacityWatts),
				)
				require.Equal(t, tc.expectedMetadata, newGetResp.Metadata.AsMap())
			}
		})
	}
}

func TestUpdateLocationOwner(t *testing.T) {
	metadata := createTestMetadata(t, map[string]any{"source": "test"})
	createResp := createTestLocation(
		t,
		"test_update_location_owner_site",
		"POINT(-0.1 51.5)",
		1234e6,
		time.Date(2019, 5, 6, 6, 0, 0, 0, time.UTC),
		metadata,
	)

	createResp = createTestLocation(
		t,
		"test_update_location_owner_site_2",
		"POINT(-0.2 51.6)",
		1000e6,
		time.Date(2019, 5, 6, 6, 0, 0, 0, time.UTC),
		metadata,
	)

	testcases := []struct {
		name      string
		newOwner  string
		shouldErr bool
	}{
		{
			name:      "Should update owner to a new value",
			newOwner:  "first_owner",
			shouldErr: false,
		},
		{
			name:      "Should update owner to the same value",
			newOwner:  "first_owner",
			shouldErr: false,
		},
		{
			name:      "Should update owner to a different value",
			newOwner:  "second_owner",
			shouldErr: false,
		},
		{
			name:      "Should remove owner with empty string",
			newOwner:  "",
			shouldErr: false,
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := dc.UpdateLocationOwner(t.Context(), &pb.UpdateLocationOwnerRequest{
				LocationUuid:      createResp.LocationUuid,
				NewOrganisationId: tc.newOwner,
			})
			if tc.shouldErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				listResp, err := dc.ListLocations(t.Context(), &pb.ListLocationsRequest{
					LocationUuidsFilter:  []string{createResp.LocationUuid},
					OrganisationIdFilter: &tc.newOwner,
				})
				require.NoError(t, err)

				if tc.newOwner == "" {
					require.Len(t, listResp.Locations, 0)
				} else {
					require.Len(t, listResp.Locations, 1)
				}
			}
		})
	}
}

func TestCreateUpdateForecaster(t *testing.T) {
	testcases := []struct {
		name      string
		createReq *pb.CreateForecasterRequest
		updateReq *pb.UpdateForecasterRequest
		shouldErr bool
	}{
		{
			name: "Should create forecaster",
			createReq: &pb.CreateForecasterRequest{
				Name:    "test_forecaster_1",
				Version: "v1",
			},
			shouldErr: false,
		},
		{
			name: "Should update existing forecaster",
			updateReq: &pb.UpdateForecasterRequest{
				Name:       "test_forecaster_1",
				NewVersion: "v2",
			},
			shouldErr: false,
		},
		{
			name: "Shouldn't update with non-unique version",
			updateReq: &pb.UpdateForecasterRequest{
				Name:       "test_forecaster_1",
				NewVersion: "v2",
			},
			shouldErr: true,
		},
		{
			name: "Shouldn't update non-existent forecaster",
			updateReq: &pb.UpdateForecasterRequest{
				Name:       "non_existent_forecaster",
				NewVersion: "v1",
			},
			shouldErr: true,
		},
		{
			name: "Shouldn't create existing forecaster",
			createReq: &pb.CreateForecasterRequest{
				Name:    "test_forecaster_1",
				Version: "v2",
			},
			shouldErr: true,
		},
		{
			name: "Shouldn't create forecaster with invalid name",
			createReq: &pb.CreateForecasterRequest{
				Name:    "",
				Version: "v1",
			},
			shouldErr: true,
		},
	}

	for _, tc := range testcases {
		var err error
		if tc.createReq != nil {
			_, err = dc.CreateForecaster(t.Context(), tc.createReq)
		}

		if tc.updateReq != nil {
			_, err = dc.UpdateForecaster(t.Context(), tc.updateReq)
		}

		if tc.shouldErr {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
		}
	}
}

func TestListForecasters(t *testing.T) {
	for _, name := range []string{
		"test_list_forecaster_1",
		"test_list_forecaster_2",
	} {
		_ = createTestForecaster(t, name, "v0")

		for i := range 4 {
			_, err := dc.UpdateForecaster(t.Context(), &pb.UpdateForecasterRequest{
				Name:       name,
				NewVersion: fmt.Sprintf("v%d", i+1),
			})
			require.NoError(t, err)
		}
	}

	testcases := []struct {
		name          string
		req           *pb.ListForecastersRequest
		expectedCount int
		shouldErr     bool
	}{
		{
			name: "Should return all forecasters",
			req: &pb.ListForecastersRequest{
				ForecasterNamesFilter: []string{
					"test_list_forecaster_1",
					"test_list_forecaster_2",
				},
			},
			expectedCount: 2 * 5,
			shouldErr:     false,
		},
		{
			name: "Should list only forecasters with filtered names",
			req: &pb.ListForecastersRequest{
				ForecasterNamesFilter: []string{"test_list_forecaster_1"},
			},
			expectedCount: 5,
			shouldErr:     false,
		},
		{
			name: "Should list only the latest versions when asked",
			req: &pb.ListForecastersRequest{
				ForecasterNamesFilter: []string{
					"test_list_forecaster_1",
					"test_list_forecaster_2",
				},
				LatestVersionsOnly: true,
			},
			expectedCount: 2,
			shouldErr:     false,
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := dc.ListForecasters(t.Context(), tc.req)
			if tc.shouldErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.expectedCount, len(resp.Forecasters))
			}
		})
	}
}

func TestGetForecastAtTimestamp(t *testing.T) {
	pivotTime := time.Date(2025, 4, 5, 12, 0, 0, 0, time.UTC)
	// --- Create a forecast --- //
	// Create two sites to attach the forecasts to
	metadata := createTestMetadata(t, map[string]any{"source": "test"})
	siteResp := createTestLocation(
		t,
		"test_get_forecast_at_timestamp_site",
		"POINT(-0.6 51.8)",
		1000000,
		pivotTime.Add(-time.Hour*1),
		metadata,
	)
	siteResp2 := createTestLocation(
		t,
		"test_get_forecast_at_timestamp_site_2",
		"POINT(-0.5 58.6)",
		2000000,
		pivotTime.Add(-time.Hour*1),
		metadata,
	)

	// Create a forecaster
	fc := createTestForecaster(t, "test_get_forecast_at_timestamp_forecaster", "v1")

	yields := generateTestForecastValues(10, 30)
	createTestForecast(t, siteResp.LocationUuid, fc, pivotTime, yields, pivotTime)
	createTestForecast(t, siteResp2.LocationUuid, fc, pivotTime, yields, pivotTime)

	testcases := []struct {
		name          string
		timestamp     time.Time
		expectedStats []map[string]float32
		shouldErr     bool
	}{
		{
			name:      "Should get forecast at init time",
			timestamp: pivotTime,
			expectedStats: []map[string]float32{
				{
					"p50": 0.5,
					"p98": 0.98,
					"p90": 0.9,
					"p75": 0.75,
					"p25": 0.25,
					"p10": 0.1,
					"p02": 0.02,
				},
				{
					"p50": 0.5,
					"p98": 0.98,
					"p90": 0.9,
					"p75": 0.75,
					"p25": 0.25,
					"p10": 0.1,
					"p02": 0.02,
				},
			},
			shouldErr: false,
		},
		{
			name:      "Should get forecast at first horizon",
			timestamp: pivotTime.Add(30 * time.Minute),
			expectedStats: []map[string]float32{
				{
					"p50": 0.501,
					"p98": 0.981,
					"p90": 0.901,
					"p75": 0.751,
					"p25": 0.251,
					"p10": 0.101,
					"p02": 0.021,
				},
				{
					"p50": 0.501,
					"p98": 0.981,
					"p90": 0.901,
					"p75": 0.751,
					"p25": 0.251,
					"p10": 0.101,
					"p02": 0.021,
				},
			},
			shouldErr: false,
		},
		{
			name:          "Should return no values where no predicted values exist",
			timestamp:     pivotTime.Add(45 * time.Minute),
			expectedStats: []map[string]float32{},
			shouldErr:     false,
		},
		{
			name:      "Should get forecast at last horizon",
			timestamp: pivotTime.Add(270 * time.Minute),
			expectedStats: []map[string]float32{
				{
					"p50": 0.509,
					"p98": 0.989,
					"p90": 0.909,
					"p75": 0.759,
					"p25": 0.259,
					"p10": 0.109,
					"p02": 0.029,
				},
				{
					"p50": 0.509,
					"p98": 0.989,
					"p90": 0.909,
					"p75": 0.759,
					"p25": 0.259,
					"p10": 0.109,
					"p02": 0.029,
				},
			},
			shouldErr: false,
		},
		{
			name:          "Should return no values before init time",
			timestamp:     pivotTime.Add(-15 * time.Minute),
			expectedStats: []map[string]float32{},
			shouldErr:     false,
		},
		{
			name:          "Should return no values after last horizon",
			timestamp:     pivotTime.Add(300 * time.Minute),
			expectedStats: []map[string]float32{},
			shouldErr:     false,
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := dc.GetForecastAtTimestamp(t.Context(), &pb.GetForecastAtTimestampRequest{
				LocationUuids: []string{
					siteResp.LocationUuid,
					siteResp2.LocationUuid,
				},
				Forecaster:   fc,
				EnergySource: pb.EnergySource_ENERGY_SOURCE_SOLAR,
				TimestampUtc: timestamppb.New(tc.timestamp),
			})
			if tc.shouldErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.NotNil(t, resp)
				require.Len(t, resp.Values, len(tc.expectedStats))

				for i, forecast := range resp.Values {
					for stat, expected := range tc.expectedStats[i] {
						if stat == "p50" {
							require.InDelta(t, expected, forecast.ValueFraction, 0.0001)
						} else {
							require.InDelta(
								t,
								expected,
								forecast.OtherStatisticsFractions[stat],
								0.0001,
							)
						}
					}

					require.NotNil(t, forecast.InitializationTimestampUtc)
					require.NotNil(t, forecast.CreatedTimestampUtc)
				}
			}
		})
	}
}

func TestGetObservationsAtTimestamp(t *testing.T) {
	pivotTime := time.Date(2025, 2, 26, 12, 0, 0, 0, time.UTC)
	metadata := createTestMetadata(t, map[string]any{"source": "test"})
	observerResp, err := dc.CreateObserver(t.Context(), &pb.CreateObserverRequest{
		Name: "test_get_observations_at_timestamp_observer",
	})
	require.NoError(t, err)

	siteUuids := make([]string, 3)
	for i := range siteUuids {
		capacity := uint64(1000000 + i*100000)
		siteResp := createTestLocation(
			t,
			fmt.Sprintf("test_get_observations_at_timestamp_site_%d", i),
			fmt.Sprintf("POINT(%f %f)", -0.1+float32(i)*0.01, 51.5+float32(i)*0.01),
			capacity,
			pivotTime.Add(-time.Hour*1),
			metadata,
		)
		siteUuids[i] = siteResp.LocationUuid

		req := &pb.CreateObservationsRequest{
			LocationUuid: siteResp.LocationUuid,
			EnergySource: pb.EnergySource_ENERGY_SOURCE_SOLAR,
			ObserverName: observerResp.ObserverName,
			Values: []*pb.CreateObservationsRequest_Value{
				{
					ValueWatts:   uint64(capacity / 10),
					TimestampUtc: timestamppb.New(pivotTime),
				},
			},
		}
		_, err = dc.CreateObservations(t.Context(), req)
		require.NoError(t, err)
	}

	testcases := []struct {
		name              string
		req               *pb.GetObservationsAtTimestampRequest
		expectedFractions []float32
		shouldErr         bool
	}{
		{
			name: "Should get observation at exact timestamp for single location",
			req: &pb.GetObservationsAtTimestampRequest{
				LocationUuids: []string{siteUuids[0]},
				ObserverName:  observerResp.ObserverName,
				EnergySource:  pb.EnergySource_ENERGY_SOURCE_SOLAR,
				TimestampUtc:  timestamppb.New(pivotTime),
			},
			expectedFractions: []float32{0.1},
			shouldErr:         false,
		},
		{
			name: "Should get observation at exact timestamp for multiple locations",
			req: &pb.GetObservationsAtTimestampRequest{
				LocationUuids: siteUuids,
				ObserverName:  observerResp.ObserverName,
				EnergySource:  pb.EnergySource_ENERGY_SOURCE_SOLAR,
				TimestampUtc:  timestamppb.New(pivotTime),
			},
			expectedFractions: []float32{0.1, 0.1, 0.1},
			shouldErr:         false,
		},
		{
			name: "Should return no observations where no values exist at timestamp",
			req: &pb.GetObservationsAtTimestampRequest{
				LocationUuids: siteUuids,
				ObserverName:  observerResp.ObserverName,
				EnergySource:  pb.EnergySource_ENERGY_SOURCE_SOLAR,
				TimestampUtc:  timestamppb.New(pivotTime.Add(5 * time.Minute)),
			},
			shouldErr: false,
		},
		{
			name: "Should return no observations for non-existent location",
			req: &pb.GetObservationsAtTimestampRequest{
				LocationUuids: []string{uuid.New().String()},
				ObserverName:  observerResp.ObserverName,
				EnergySource:  pb.EnergySource_ENERGY_SOURCE_SOLAR,
				TimestampUtc:  timestamppb.New(pivotTime),
			},
			shouldErr: false,
		},
		{
			name: "Shouldn't return observations for non-existent observer",
			req: &pb.GetObservationsAtTimestampRequest{
				LocationUuids: siteUuids,
				ObserverName:  "non_existent_observer",
				EnergySource:  pb.EnergySource_ENERGY_SOURCE_SOLAR,
				TimestampUtc:  timestamppb.New(pivotTime),
			},
			shouldErr: true,
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := dc.GetObservationsAtTimestamp(t.Context(), tc.req)
			if tc.shouldErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.NotNil(t, resp)
				require.Len(t, resp.Values, len(tc.expectedFractions))

				for i, obs := range resp.Values {
					require.Equal(t, tc.expectedFractions[i], obs.ValueFraction)
				}
			}
		})
	}
}

func TestGetLocation(t *testing.T) {
	metadata := createTestMetadata(t, map[string]any{"source": "test"})
	createResp := createTestLocation(
		t,
		"test_get_location_site",
		"POLYGON((0 0,0 1,1 1,1 0,0 0))",
		12e6,
		time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC),
		metadata,
	)

	testCases := []struct {
		name      string
		req       *pb.GetLocationRequest
		shouldErr bool
	}{
		{
			name: "Should get location without geometry",
			req: &pb.GetLocationRequest{
				LocationUuid:    createResp.LocationUuid,
				EnergySource:    pb.EnergySource_ENERGY_SOURCE_SOLAR,
				IncludeGeometry: false,
			},
			shouldErr: false,
		},
		{
			name: "Should get location with geometry",
			req: &pb.GetLocationRequest{
				LocationUuid:    createResp.LocationUuid,
				EnergySource:    pb.EnergySource_ENERGY_SOURCE_SOLAR,
				IncludeGeometry: true,
			},
			shouldErr: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := dc.GetLocation(t.Context(), tc.req)
			if tc.shouldErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, createResp.LocationUuid, resp.LocationUuid)
				require.Equal(t, "test_get_location_site", resp.LocationName)
				require.Equal(t, uint64(12e6), resp.EffectiveCapacityWatts)

				if tc.req.IncludeGeometry {
					expected, err := hex.DecodeString(
						"01030000000100000005000000000000000000000000000000000000000000000000000000000000000000f03f000000000000f03f000000000000f03f000000000000f03f000000000000000000000000000000000000000000000000",
					)
					require.NoError(t, err)
					require.Equal(t, expected, resp.GeometryWkb)
				}
			}
		})
	}
}

func TestGetLocationAsTimeseries(t *testing.T) {
	pivotTime := time.Date(2026, 1, 4, 12, 0, 0, 0, time.UTC)

	metadata := createTestMetadata(t, map[string]any{"source": "test_initial"})

	// Site is valid from 48 hours before pivot
	siteResp := createTestLocation(
		t,
		"test_get_location_as_timeseries_site",
		"POINT(-60.25 57.5)",
		1000000,
		pivotTime.Add(-time.Hour*48),
		metadata,
	)

	// Update the metadata and capacity at 24 hours before pivot
	updatedMetadata := createTestMetadata(t, map[string]any{"source": "test_update_1"})
	_, err := dc.UpdateLocation(t.Context(), &pb.UpdateLocationRequest{
		LocationUuid:              siteResp.LocationUuid,
		EnergySource:              pb.EnergySource_ENERGY_SOURCE_SOLAR,
		NewEffectiveCapacityWatts: ptr(uint64(1500000)),
		NewMetadata:               updatedMetadata,
		ValidFromUtc:              timestamppb.New(pivotTime.Add(-time.Hour * 24)),
	})
	require.NoError(t, err)

	// Update the metadata and capacity again at pivot
	finalMetadata := createTestMetadata(t, map[string]any{"source": "test_update_2"})
	_, err = dc.UpdateLocation(t.Context(), &pb.UpdateLocationRequest{
		LocationUuid:              siteResp.LocationUuid,
		EnergySource:              pb.EnergySource_ENERGY_SOURCE_SOLAR,
		NewEffectiveCapacityWatts: ptr(uint64(2000000)),
		NewMetadata:               finalMetadata,
		ValidFromUtc:              timestamppb.New(pivotTime),
	})
	require.NoError(t, err)

	defaultTimeWindow := &pb.TimeWindow{
		StartTimestampUtc: timestamppb.New(pivotTime.Add(-time.Hour * 50)),
		EndTimestampUtc:   timestamppb.New(pivotTime.Add(time.Hour * 10)),
	}

	testcases := []struct {
		name               string
		req                *pb.GetLocationAsTimeseriesRequest
		expectedCapacities []uint64
		expectedSources    []string
		shouldErr          bool
	}{
		{
			name: "Should return full history of capacity changes when window covers everything",
			req: &pb.GetLocationAsTimeseriesRequest{
				LocationUuid: siteResp.LocationUuid,
				EnergySource: pb.EnergySource_ENERGY_SOURCE_SOLAR,
				TimeWindow:   defaultTimeWindow,
			},
			expectedCapacities: []uint64{1000000, 1500000, 2000000},
			expectedSources:    []string{"test_initial", "test_update_1", "test_update_2"},
			shouldErr:          false,
		},
		{
			name: "Should return only changes covered by time window and exclude others",
			req: &pb.GetLocationAsTimeseriesRequest{
				LocationUuid: siteResp.LocationUuid,
				EnergySource: pb.EnergySource_ENERGY_SOURCE_SOLAR,
				TimeWindow: &pb.TimeWindow{
					StartTimestampUtc: timestamppb.New(pivotTime.Add(-time.Hour * 24)),
					EndTimestampUtc:   timestamppb.New(pivotTime.Add(time.Hour * 10)),
				},
			},
			expectedCapacities: []uint64{1500000, 2000000},
			expectedSources:    []string{"test_update_1", "test_update_2"},
			shouldErr:          false,
		},
		{
			name: "Should return no values if time window is before the location existed",
			req: &pb.GetLocationAsTimeseriesRequest{
				LocationUuid: siteResp.LocationUuid,
				EnergySource: pb.EnergySource_ENERGY_SOURCE_SOLAR,
				TimeWindow: &pb.TimeWindow{
					StartTimestampUtc: timestamppb.New(pivotTime.Add(-time.Hour * 100)),
					EndTimestampUtc:   timestamppb.New(pivotTime.Add(-time.Hour * 50)),
				},
			},
			expectedCapacities: []uint64{},
			expectedSources:    []string{},
			shouldErr:          false,
		},
		{
			name: "Shouldn't work with an invalid UUID",
			req: &pb.GetLocationAsTimeseriesRequest{
				LocationUuid: "not-a-valid-uuid",
				EnergySource: pb.EnergySource_ENERGY_SOURCE_SOLAR,
				TimeWindow:   defaultTimeWindow,
			},
			shouldErr: true,
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := dc.GetLocationAsTimeseries(t.Context(), tc.req)

			if tc.shouldErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, resp)

			actualCapacities := make([]uint64, len(resp.Values))
			actualSources := make([]string, len(resp.Values))
			targetTimes := make([]int64, len(resp.Values))

			for i, v := range resp.Values {
				actualCapacities[i] = v.EffectiveCapacityWatts
				targetTimes[i] = v.TimestampUtc.AsTime().Unix()

				if v.Metadata != nil && v.Metadata.Fields["source"] != nil {
					actualSources[i] = v.Metadata.Fields["source"].GetStringValue()
				} else {
					actualSources[i] = ""
				}
			}

			require.IsIncreasing(t, targetTimes)
			require.Equal(t, tc.expectedCapacities, actualCapacities)
			require.Equal(t, tc.expectedSources, actualSources)
		})
	}
}

func TestGetLocationsAsGeoJSON(t *testing.T) {
	// Create some locations
	siteUuids := make([]string, 3)
	for i := range siteUuids {
		resp := createTestLocation(
			t,
			fmt.Sprintf("testsite%02d", i),
			fmt.Sprintf("POINT(%f %f)", -0.1+float32(i)*0.01, 51.5+float32(i)*0.01),
			uint64(1000000+i*100),
			time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC),
			&structpb.Struct{},
		)
		siteUuids[i] = resp.LocationUuid
	}

	geojson, err := dc.GetLocationsAsGeoJSON(t.Context(), &pb.GetLocationsAsGeoJSONRequest{
		LocationUuids: siteUuids,
	})
	require.NoError(t, err)

	var result map[string]any

	err = json.Unmarshal([]byte(geojson.Geojson), &result)
	require.NoError(t, err)

	features := result["features"].([]any)
	require.Equal(t, len(siteUuids), len(features))
}

func TestGetForecastAsTimeseries(t *testing.T) {
	pivotTime := time.Date(2025, 2, 5, 12, 0, 0, 0, time.UTC)

	// Create a site to attach the forecasts to
	metadata := createTestMetadata(t, map[string]any{"source": "test"})
	siteResp := createTestLocation(
		t,
		"test_get_forecast_as_timeseries_site",
		"POINT(-60.25 57.5)",
		1000000,
		pivotTime.Add(-time.Hour*49),
		metadata,
	)

	// Update the capacity of the site to check it is reflected in the values
	_, err := dc.UpdateLocation(t.Context(), &pb.UpdateLocationRequest{
		LocationUuid:              siteResp.LocationUuid,
		EnergySource:              pb.EnergySource_ENERGY_SOURCE_SOLAR,
		NewEffectiveCapacityWatts: ptr(uint64(1500000)),
		ValidFromUtc:              timestamppb.New(pivotTime.Add(-time.Hour * 1)),
	})
	require.NoError(t, err)

	// Create a forecaster to make the forecasts
	fc := createTestForecaster(t, "test_get_forecast_as_timeseries_forecaster", "v1")

	// Create 4, hour-long forecasts, each 30 minutes apart, with a resolution of 5 minutes.
	// The last forecast begins at the pivot time.
	yields := generateTestForecastValues(12, 5)

	for i := 3; i >= 0; i-- {
		createTestForecast(
			t,
			siteResp.LocationUuid,
			fc,
			pivotTime.Add(time.Duration(-i*30)*time.Minute),
			yields,
			pivotTime,
		)
	}

	defaultTimeWindow := &pb.TimeWindow{
		StartTimestampUtc: timestamppb.New(pivotTime.Add(-time.Hour * 48)),
		EndTimestampUtc:   timestamppb.New(pivotTime.Add(time.Hour * 36)),
	}

	testcases := []struct {
		name           string
		req            *pb.GetForecastAsTimeseriesRequest
		expectedValues []float32
		shouldErr      bool
	}{
		{
			name: "Should return expected values for horizon 0 mins",
			req: &pb.GetForecastAsTimeseriesRequest{
				LocationUuid: siteResp.LocationUuid,
				Forecaster:   fc,
				EnergySource: pb.EnergySource_ENERGY_SOURCE_SOLAR,
				HorizonMins:  0,
				TimeWindow:   defaultTimeWindow,
			},
			// For horizon 0, we should get all the values from the latest forecast,
			// plus the values from the previous forecasts that have the lowest horizon
			// for each target time.
			// Since the predicted values are every 5 minutes, and the forecasts are every 30,
			// we should get 6 values from each forecast, until the latest where we get all 12.
			// The forecast values are equal to the plevel fraction + (0.001*step),
			// where step is the index of the value in the forecast.
			// This means the values we are fetching should be:
			// - horizons 0 to 25 minutes from forecast 3
			// - horizons 0 to 25 minutes from forecast 2
			// - horizons 0 to 25 minutes from forecast 1
			// - all horizons for forecast 0
			// - horizons 0 to 55 minutes from forecast 0
			expectedValues: []float32{
				0.5, 0.501, 0.502, 0.503, 0.504, 0.505,
				0.5, 0.501, 0.502, 0.503, 0.504, 0.505,
				0.5, 0.501, 0.502, 0.503, 0.504, 0.505,
				0.5, 0.501, 0.502, 0.503, 0.504, 0.505, 0.506, 0.507, 0.508, 0.509, 0.510, 0.511,
			},
			shouldErr: false,
		},
		{
			name: "Should return expected values for horizon 14 mins",
			req: &pb.GetForecastAsTimeseriesRequest{
				LocationUuid: siteResp.LocationUuid,
				Forecaster:   fc,
				EnergySource: pb.EnergySource_ENERGY_SOURCE_SOLAR,
				HorizonMins:  14,
				TimeWindow:   defaultTimeWindow,
			},
			// For horizon of 14 minutes, anything with a lesser horizon should not be included.
			// So the value for 0, 5, and 10 minutes should not be included.
			expectedValues: []float32{
				0.503, 0.504, 0.505, 0.506, 0.507, 0.508,
				0.503, 0.504, 0.505, 0.506, 0.507, 0.508,
				0.503, 0.504, 0.505, 0.506, 0.507, 0.508,
				0.503, 0.504, 0.505, 0.506, 0.507, 0.508, 0.509, 0.510, 0.511,
			},
			shouldErr: false,
		},
		{
			name: "Should return expected values for horizon 30 mins",
			req: &pb.GetForecastAsTimeseriesRequest{
				LocationUuid: siteResp.LocationUuid,
				Forecaster:   fc,
				EnergySource: pb.EnergySource_ENERGY_SOURCE_SOLAR,
				HorizonMins:  30,
				TimeWindow:   defaultTimeWindow,
			},
			// For horizon of 30 minutes, the values for 0, 5, 10, 15, 20,
			// and 25 minutes should not be included.
			expectedValues: []float32{
				0.506, 0.507, 0.508, 0.509, 0.510, 0.511,
				0.506, 0.507, 0.508, 0.509, 0.510, 0.511,
				0.506, 0.507, 0.508, 0.509, 0.510, 0.511,
				0.506, 0.507, 0.508, 0.509, 0.510, 0.511,
			},
			shouldErr: false,
		},
		{
			name: "Should return no values for horizon 60 mins",
			req: &pb.GetForecastAsTimeseriesRequest{
				LocationUuid: siteResp.LocationUuid,
				Forecaster:   fc,
				EnergySource: pb.EnergySource_ENERGY_SOURCE_SOLAR,
				HorizonMins:  60,
				TimeWindow:   defaultTimeWindow,
			},
			expectedValues: []float32{},
			shouldErr:      false,
		},
		{
			name: "Should return no values for a time window outside of the forecasted values",
			req: &pb.GetForecastAsTimeseriesRequest{
				LocationUuid: siteResp.LocationUuid,
				Forecaster:   fc,
				EnergySource: pb.EnergySource_ENERGY_SOURCE_SOLAR,
				HorizonMins:  0,
				TimeWindow: &pb.TimeWindow{
					StartTimestampUtc: timestamppb.New(pivotTime.Add(-time.Hour * 48)),
					EndTimestampUtc:   timestamppb.New(pivotTime.Add(-time.Hour * 42)),
				},
			},
			expectedValues: []float32{},
			shouldErr:      false,
		},
		{
			name: "Should return all predictions for a specific initialization time",
			req: &pb.GetForecastAsTimeseriesRequest{
				LocationUuid:               siteResp.LocationUuid,
				Forecaster:                 fc,
				EnergySource:               pb.EnergySource_ENERGY_SOURCE_SOLAR,
				HorizonMins:                0,
				TimeWindow:                 defaultTimeWindow,
				InitializationTimestampUtc: timestamppb.New(pivotTime.Add(-30 * time.Minute)),
			},
			expectedValues: []float32{
				0.5, 0.501, 0.502, 0.503, 0.504, 0.505, 0.506, 0.507, 0.508, 0.509, 0.510, 0.511,
			},
			shouldErr: false,
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := dc.GetForecastAsTimeseries(t.Context(), tc.req)

			if tc.shouldErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, resp)

			targetTimes := make([]int64, len(resp.Values))
			actualValues := make([]float32, len(resp.Values))

			for i, v := range resp.Values {
				targetTimes[i] = v.TargetTimestampUtc.AsTime().Unix()
				actualValues[i] = v.P50ValueFraction
				require.NotEmpty(t, v.OtherStatisticsFractions)

				// Assert that the capacity change has been picked up
				if v.TargetTimestampUtc.AsTime().
					After(pivotTime.Add(-1 * time.Hour).Add(-1 * time.Second)) {
					require.Equal(t, 1500000, int(v.EffectiveCapacityWatts))
				} else {
					require.Equal(t, 1000000, int(v.EffectiveCapacityWatts))
				}
			}

			require.IsIncreasing(t, targetTimes)
			require.Equal(t, len(tc.expectedValues), len(actualValues))
			require.InDeltaSlice(t, tc.expectedValues, actualValues, 0.0001)
		})
	}
}

func TestListLocationsLocationFilters(t *testing.T) {
	pivotTime := time.Now().Truncate(time.Minute)
	metadata := createTestMetadata(t, map[string]any{"source": "test"})

	// Create a bunch of locations
	var locationUuids []string
	for i := range 5 {
		for _, energySource := range []pb.EnergySource{pb.EnergySource_ENERGY_SOURCE_SOLAR, pb.EnergySource_ENERGY_SOURCE_WIND} {
			for _, locType := range []pb.LocationType{pb.LocationType_LOCATION_TYPE_SITE, pb.LocationType_LOCATION_TYPE_GSP} {
				resp, err := dc.CreateLocation(t.Context(), &pb.CreateLocationRequest{
					LocationName: fmt.Sprintf(
						"test_list_locations_site_%02d_%d_%d",
						i,
						energySource,
						locType,
					),
					GeometryWkt:            fmt.Sprintf("POINT(-5.%d 51.%d)", i, i),
					EffectiveCapacityWatts: uint64(1000000 + i*100),
					EnergySource:           energySource,
					LocationType:           locType,
					ValidFromUtc:           timestamppb.New(pivotTime.Add(-time.Hour * 4)),
					Metadata:               metadata,
				})
				require.NoError(t, err)

				locationUuids = append(locationUuids, resp.LocationUuid)
			}
		}
	}

	sourceFilter := new(pb.EnergySource)
	*sourceFilter = pb.EnergySource_ENERGY_SOURCE_SOLAR
	typeFilter := new(pb.LocationType)
	*typeFilter = pb.LocationType_LOCATION_TYPE_SITE

	// All tests in the table need to filter by the location uuids just created as the postgres
	// container the tests are run against is reused across the tests for speed of unit testing.
	// As such, it may contain more than just the locations created here, depending on the number of
	// tests being run.
	// TODO: This is a fairly minimal test suite, and I imagine there are plenty of edge cases that
	// are not covered here. This purely covers the basic filtering functionality, and should by
	// improved upon in future.
	testcases := []struct {
		name          string
		req           *pb.ListLocationsRequest
		expectedCount int
	}{
		{
			name: "Should return everything without filters",
			req: &pb.ListLocationsRequest{
				LocationUuidsFilter: locationUuids,
			},
			expectedCount: 5 * 4,
		},
		{
			name: "Should filter by energy source",
			req: &pb.ListLocationsRequest{
				EnergySourceFilter:  sourceFilter,
				LocationUuidsFilter: locationUuids,
			},
			expectedCount: 5 * 2,
		},
		{
			name: "Should filter by location type",
			req: &pb.ListLocationsRequest{
				LocationTypeFilter:  typeFilter,
				LocationUuidsFilter: locationUuids,
			},
			expectedCount: 5 * 2,
		},
		{
			name: "Should filter by location uuids",
			req: &pb.ListLocationsRequest{
				LocationUuidsFilter: locationUuids[:3],
			},
			expectedCount: 3,
		},
		{
			name: "Should filter by location names",
			req: &pb.ListLocationsRequest{
				LocationNamesFilter: []string{
					fmt.Sprintf(
						"test_list_locations_site_%02d_%d_%d",
						0,
						*sourceFilter,
						*typeFilter,
					),
					fmt.Sprintf(
						"test_list_locations_site_%02d_%d_%d",
						1,
						*sourceFilter,
						*typeFilter,
					),
				},
			},
			expectedCount: 2,
		},
		{
			name: "Should filter by energy source and location type",
			req: &pb.ListLocationsRequest{
				EnergySourceFilter:  sourceFilter,
				LocationTypeFilter:  typeFilter,
				LocationUuidsFilter: locationUuids,
			},
			expectedCount: 5,
		},
		{
			name: "Should filter by energy source and location uuids",
			req: &pb.ListLocationsRequest{
				EnergySourceFilter:  sourceFilter,
				LocationUuidsFilter: locationUuids[3:8],
			},
			expectedCount: 2,
		},
		{
			name: "Should return nothing for non-matching filters",
			req: &pb.ListLocationsRequest{
				EnergySourceFilter:  sourceFilter,
				LocationTypeFilter:  ptr(pb.LocationType_LOCATION_TYPE_DNO),
				LocationUuidsFilter: locationUuids,
			},
		},
		{
			name: "Should filter by enclosing geometry",
			req: &pb.ListLocationsRequest{
				EnclosingLocationUuidFilter: &locationUuids[0],
				LocationUuidsFilter:         locationUuids,
			},
			expectedCount: 3,
		},
		{
			name: "Should filter by enclosing geometry and energy source",
			req: &pb.ListLocationsRequest{
				EnclosingLocationUuidFilter: &locationUuids[0],
				EnergySourceFilter:          sourceFilter,
				LocationUuidsFilter:         locationUuids,
			},
			expectedCount: 1,
		},
		{
			name: "Should filter by enclosing geometry and location type",
			req: &pb.ListLocationsRequest{
				EnclosingLocationUuidFilter: &locationUuids[0],
				LocationTypeFilter:          typeFilter,
				LocationUuidsFilter:         locationUuids,
			},
			expectedCount: 1,
		},
		{
			name: "Should filter by enclosed geometry",
			req: &pb.ListLocationsRequest{
				EnclosedLocationUuidFilter: &locationUuids[0],
				LocationUuidsFilter:        locationUuids,
			},
			expectedCount: 3,
		},
		{
			name: "Should filter by enclosed geometry and energy source",
			req: &pb.ListLocationsRequest{
				EnclosedLocationUuidFilter: &locationUuids[0],
				EnergySourceFilter:         sourceFilter,
				LocationUuidsFilter:        locationUuids,
			},
			expectedCount: 1,
		},
		{
			name: "Should filter by enclosed geometry and location type",
			req: &pb.ListLocationsRequest{
				EnclosedLocationUuidFilter: &locationUuids[0],
				LocationTypeFilter:         typeFilter,
				LocationUuidsFilter:        locationUuids,
			},
			expectedCount: 1,
		},
		{
			name: "Should return nothing for enclosing geometry containing nothing",
			req: &pb.ListLocationsRequest{
				EnclosingLocationUuidFilter: ptr(uuid.New().String()),
				LocationUuidsFilter:         locationUuids,
			},
			expectedCount: 0,
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := dc.ListLocations(t.Context(), tc.req)
			if strings.Contains(tc.name, "Shouldn't") {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.expectedCount, len(resp.Locations))

				for _, loc := range resp.Locations {
					require.Equal(t, metadata.AsMap(), loc.Metadata.AsMap())
				}
			}
		})
	}
}

func TestGetObservationsAsTimeseries(t *testing.T) {
	pivotTime := time.Now().Truncate(time.Minute)

	// Create a site to attach the observations to
	siteResp := createTestLocation(
		t,
		"test_get_observations_as_timeseries_site",
		"POINT(-20.25 57.5)",
		1000000,
		pivotTime.Add(-time.Hour*64),
		&structpb.Struct{},
	)

	// Create an observer to make the observations
	obsResp, err := dc.CreateObserver(t.Context(), &pb.CreateObserverRequest{
		Name: "test_get_observations_as_timeseries_observer",
	})
	require.NoError(t, err)

	// Seed some 5-minutely observations for the site.
	// The observations cover the 24-hour period ending at the pivot time,
	// and are all equal in value to half the capacity of the site.
	values := make([]*pb.CreateObservationsRequest_Value, 24*60/5)
	for i := range values {
		values[i] = &pb.CreateObservationsRequest_Value{
			TimestampUtc: timestamppb.New(
				pivotTime.Add(time.Duration(i*5*-1) * time.Minute),
			),
			ValueWatts: uint64(rand.Float64() * float64(siteResp.EffectiveCapacityWatts)),
		}
	}

	_, err = dc.CreateObservations(t.Context(), &pb.CreateObservationsRequest{
		LocationUuid: siteResp.LocationUuid,
		EnergySource: pb.EnergySource_ENERGY_SOURCE_SOLAR,
		ObserverName: obsResp.ObserverName,
		Values:       values,
	})
	require.NoError(t, err)

	testcases := []struct {
		startTime    time.Time
		endTime      time.Time
		expectedSize int
	}{
		{
			startTime:    pivotTime.Add(-time.Hour * 24),
			endTime:      pivotTime,
			expectedSize: len(values),
		},
	}

	for _, tc := range testcases {
		t.Run(fmt.Sprintf("Size %d", tc.expectedSize), func(t *testing.T) {
			resp, err := dc.GetObservationsAsTimeseries(
				t.Context(),
				&pb.GetObservationsAsTimeseriesRequest{
					LocationUuid: siteResp.LocationUuid,
					EnergySource: pb.EnergySource_ENERGY_SOURCE_SOLAR,
					TimeWindow: &pb.TimeWindow{
						StartTimestampUtc: timestamppb.New(tc.startTime),
						EndTimestampUtc:   timestamppb.New(tc.endTime),
					},
					ObserverName: obsResp.ObserverName,
				},
			)
			require.NoError(t, err)
			require.Equal(t, tc.expectedSize, len(resp.Values))
		})
	}
}

func TestGetLatestObservations(t *testing.T) {
	pivotTime := time.Now().Truncate(time.Minute)

	// Create a site to attach the observations to
	siteResp := createTestLocation(
		t,
		"test_get_latest_observations_site",
		"POINT(-20.25 57.5)",
		1000000,
		pivotTime.Add(-time.Hour*4),
		&structpb.Struct{},
	)

	// Create an observer to make the observations
	obsResp, err := dc.CreateObserver(t.Context(), &pb.CreateObserverRequest{
		Name: "test_get_latest_observations_observer",
	})
	require.NoError(t, err)

	// Seed some observations for the site.
	values := []*pb.CreateObservationsRequest_Value{
		{
			TimestampUtc: timestamppb.New(pivotTime.Add(-time.Hour * 2)),
			ValueWatts:   uint64(0.3 * float64(siteResp.EffectiveCapacityWatts)),
		},
		{
			TimestampUtc: timestamppb.New(pivotTime.Add(-time.Hour * 1)),
			ValueWatts:   uint64(0.5 * float64(siteResp.EffectiveCapacityWatts)),
		},
	}
	_, err = dc.CreateObservations(t.Context(), &pb.CreateObservationsRequest{
		LocationUuid: siteResp.LocationUuid,
		EnergySource: pb.EnergySource_ENERGY_SOURCE_SOLAR,
		ObserverName: obsResp.ObserverName,
		Values:       values,
	})
	require.NoError(t, err)

	testcases := []struct {
		name              string
		req               *pb.GetLatestObservationsRequest
		expectedFractions []float32
	}{
		{
			name: "Should get latest observations",
			req: &pb.GetLatestObservationsRequest{
				LocationUuids: []string{siteResp.LocationUuid},
				EnergySource:  pb.EnergySource_ENERGY_SOURCE_SOLAR,
				ObserverName:  obsResp.ObserverName,
			},
			expectedFractions: []float32{0.5},
		},
		{
			name: "Should get earlier observation before cutoff",
			req: &pb.GetLatestObservationsRequest{
				LocationUuids:     []string{siteResp.LocationUuid},
				EnergySource:      pb.EnergySource_ENERGY_SOURCE_SOLAR,
				ObserverName:      obsResp.ObserverName,
				PivotTimestampUtc: timestamppb.New(pivotTime.Add(-time.Hour * 1).Add(-time.Second)),
			},
			expectedFractions: []float32{0.3},
		},
		{
			name: "Should fetch no rows for non-existent observer",
			req: &pb.GetLatestObservationsRequest{
				LocationUuids: []string{siteResp.LocationUuid},
				EnergySource:  pb.EnergySource_ENERGY_SOURCE_SOLAR,
				ObserverName:  "non_existent_observer",
			},
		},
		{
			name: "Should fetch no rows for non-existent location",
			req: &pb.GetLatestObservationsRequest{
				LocationUuids: []string{uuid.New().String()},
				EnergySource:  pb.EnergySource_ENERGY_SOURCE_SOLAR,
				ObserverName:  obsResp.ObserverName,
			},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := dc.GetLatestObservations(t.Context(), tc.req)
			if strings.Contains(tc.name, "Shouldn't") {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.NotNil(t, resp)

				for i, obs := range resp.Observations {
					t.Log(obs)
					require.Equal(t, tc.expectedFractions[i], obs.ValueFraction)
				}
			}
		})
	}
}

func TestCreateListObservers(t *testing.T) {
	testcases := []struct {
		name          string
		createReq     *pb.CreateObserverRequest
		listReq       *pb.ListObserversRequest
		expectedNames []string
	}{
		{
			name: "Should create observer",
			createReq: &pb.CreateObserverRequest{
				Name: "test_create_list_observers_observer_1",
			},
			expectedNames: []string{"test_create_list_observers_observer_1"},
		},
		{
			name: "Should create another observer",
			createReq: &pb.CreateObserverRequest{
				Name: "test_create_list_observers_observer_2",
			},
			expectedNames: []string{
				"test_create_list_observers_observer_1",
				"test_create_list_observers_observer_2",
			},
		},
		{
			name: "Shouldn't create duplicate observer",
			createReq: &pb.CreateObserverRequest{
				Name: "test_create_list_observers_observer_1",
			},
		},
		{
			name: "Shouldn't create observer with empty name",
			createReq: &pb.CreateObserverRequest{
				Name: "",
			},
		},
		{
			name: "Shouldn't create observer with invalid name",
			createReq: &pb.CreateObserverRequest{
				Name: "invalid name with spaces and *special_characters*",
			},
		},
		{
			name: "Should list observers with name filter",
			listReq: &pb.ListObserversRequest{
				ObserverNamesFilter: []string{
					"test_create_list_observers_observer_2",
				},
			},
			expectedNames: []string{"test_create_list_observers_observer_2"},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.createReq != nil {
				_, err := dc.CreateObserver(t.Context(), tc.createReq)
				if strings.Contains(tc.name, "Shouldn't") {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
				}
			}

			if tc.listReq != nil {
				resp, err := dc.ListObservers(t.Context(), tc.listReq)
				if strings.Contains(tc.name, "Shouldn't") {
					require.Error(t, err)
				} else {
					require.NoError(t, err)

					var observerNames []string
					for _, observer := range resp.Observers {
						observerNames = append(observerNames, observer.ObserverName)
					}

					require.ElementsMatch(t, tc.expectedNames, observerNames)
				}
			}
		})
	}
}

func TestCreateObservations(t *testing.T) {
	pivotTime := time.Date(2020, 7, 7, 12, 0, 0, 0, time.UTC)
	// Create a site to attach the observations to
	siteResp := createTestLocation(
		t,
		"test_create_observations_site",
		"POINT(-0.1 51.5)",
		1000000,
		pivotTime.Add(-time.Hour*4),
		&structpb.Struct{},
	)

	// Update the capacity
	updateResp, err := dc.UpdateLocation(t.Context(), &pb.UpdateLocationRequest{
		LocationUuid:              siteResp.LocationUuid,
		EnergySource:              pb.EnergySource_ENERGY_SOURCE_SOLAR,
		NewEffectiveCapacityWatts: ptr(uint64(2000000)),
		ValidFromUtc:              timestamppb.New(pivotTime.Add(time.Hour * 1)),
	})
	require.NoError(t, err)

	// Create an observer to make the observations
	obsResp, err := dc.CreateObserver(t.Context(), &pb.CreateObserverRequest{
		Name: "test_create_observations_observer",
	})
	require.NoError(t, err)

	validObservations := make([]*pb.CreateObservationsRequest_Value, 10)
	for i := range validObservations {
		value := 0.5 * float64(siteResp.EffectiveCapacityWatts)
		if i >= 2 {
			value = 0.5 * float64(updateResp.EffectiveCapacityWatts)
		}

		validObservations[i] = &pb.CreateObservationsRequest_Value{
			TimestampUtc: timestamppb.New(pivotTime.Add(time.Duration(i*30) * time.Minute)),
			ValueWatts:   uint64(value),
		}
	}

	invalidObservations := make([]*pb.CreateObservationsRequest_Value, 10)
	for i := range invalidObservations {
		value := 0.5 * float64(siteResp.EffectiveCapacityWatts)
		if i >= 2 {
			value = 1.2 * float64(updateResp.EffectiveCapacityWatts)
		}

		invalidObservations[i] = &pb.CreateObservationsRequest_Value{
			TimestampUtc: timestamppb.New(pivotTime.Add(time.Duration(i*30) * time.Minute)),
			ValueWatts:   uint64(value),
		}
	}

	testcases := []struct {
		name string
		req  *pb.CreateObservationsRequest
	}{
		{
			name: "Should create valid observations",
			req: &pb.CreateObservationsRequest{
				LocationUuid: siteResp.LocationUuid,
				EnergySource: pb.EnergySource_ENERGY_SOURCE_SOLAR,
				ObserverName: obsResp.ObserverName,
				Values:       validObservations,
			},
		},
		{
			name: "Shouldn't create invalid observations",
			req: &pb.CreateObservationsRequest{
				LocationUuid: siteResp.LocationUuid,
				EnergySource: pb.EnergySource_ENERGY_SOURCE_SOLAR,
				ObserverName: obsResp.ObserverName,
				Values:       invalidObservations,
			},
		},
		{
			name: "Shouldn't create observations for non-existent location",
			req: &pb.CreateObservationsRequest{
				LocationUuid: "non_existent_location",
				EnergySource: pb.EnergySource_ENERGY_SOURCE_SOLAR,
				ObserverName: obsResp.ObserverName,
				Values:       validObservations,
			},
		},
		{
			name: "Shouldn't create observations for non-existent observer",
			req: &pb.CreateObservationsRequest{
				LocationUuid: siteResp.LocationUuid,
				EnergySource: pb.EnergySource_ENERGY_SOURCE_SOLAR,
				ObserverName: "non_existent_observer",
				Values:       validObservations,
			},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := dc.CreateObservations(t.Context(), tc.req)
			if strings.Contains(tc.name, "Shouldn't") {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestGetWeekAverageDeltas(t *testing.T) {
	pivotTime := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)

	siteResp := createTestLocation(
		t,
		"test_get_week_average_deltas_site",
		"POINT(-20.25 59.5)",
		1000000,
		pivotTime.Add(-time.Hour*12*24),
		&structpb.Struct{},
	)

	// Create an observer to make the observations
	obsResp, err := dc.CreateObserver(t.Context(), &pb.CreateObserverRequest{
		Name: "test_get_week_average_deltas_observer",
	})
	require.NoError(t, err)

	// Seed some 30-minutely observations for the site.
	// The observations cover a 7-day period ending at the pivot time,
	// and are all equal in value to half the capacity of the site.
	values := make([]*pb.CreateObservationsRequest_Value, 7*24*60/30)
	for i := range values {
		values[i] = &pb.CreateObservationsRequest_Value{
			TimestampUtc: timestamppb.New(
				pivotTime.Add(time.Duration(i*5*-1) * time.Minute),
			),
			ValueWatts: uint64(0.5 * float64(siteResp.EffectiveCapacityWatts)),
		}
	}

	_, err = dc.CreateObservations(t.Context(), &pb.CreateObservationsRequest{
		LocationUuid: siteResp.LocationUuid,
		EnergySource: pb.EnergySource_ENERGY_SOURCE_SOLAR,
		ObserverName: obsResp.ObserverName,
		Values:       values,
	})
	require.NoError(t, err)

	// Create a forecaster to make the forecasts
	fc := createTestForecaster(t, "test_get_week_average_deltas_forecaster", "v1")

	// Create 8, 8 hour-long forecasts, each one day apart, with a resolution of 30 minutes.
	// The forecast values increase linearly from 0% to 100% of capacity over each forecast.
	// The last forecast begins at the pivot time.
	yields := generateTestForecastValues(16, 30)

	for i := 7; i >= 0; i-- {
		createTestForecast(
			t,
			siteResp.LocationUuid,
			fc,
			pivotTime.Add(time.Duration(-i*24)*time.Hour),
			yields,
			pivotTime,
		)
	}

	deltaResp, err := dc.GetWeekAverageDeltas(t.Context(), &pb.GetWeekAverageDeltasRequest{
		LocationUuid:      siteResp.LocationUuid,
		EnergySource:      pb.EnergySource_ENERGY_SOURCE_SOLAR,
		Forecaster:        fc,
		ObserverName:      obsResp.ObserverName,
		PivotTimestampUtc: timestamppb.New(pivotTime),
	})
	require.NoError(t, err)
	require.NotNil(t, deltaResp)
	require.Len(t, deltaResp.Deltas, 8*60/30) // One per horizon
}

func TestCreateForecast(t *testing.T) {
	pivotTime := time.Date(2024, 5, 5, 0, 30, 0, 0, time.UTC)
	metadata := createTestMetadata(t, map[string]any{"source": "test"})
	metadata2 := createTestMetadata(t, map[string]any{"source": "test", "extra": "value"})

	// Create a site to attach the forecast to
	siteResp := createTestLocation(
		t,
		"test_create_forecast_site",
		"POINT(-0.1 51.5)",
		1000000,
		pivotTime.Add(-time.Hour*24),
		metadata,
	)

	// Create a forecaster
	fc := createTestForecaster(t, "test_create_forecast_forecaster", "v1")

	yields := generateTestForecastValues(10, 30)

	yieldsZeros := make([]*pb.CreateForecastRequest_ForecastValue, 10)
	for i := range yieldsZeros {
		yieldsZeros[i] = &pb.CreateForecastRequest_ForecastValue{
			HorizonMins:              uint32(i * 30),
			P50Fraction:              0.0,
			OtherStatisticsFractions: map[string]float32{},
		}
	}

	yieldsInvalid := make([]*pb.CreateForecastRequest_ForecastValue, 10)
	for i := range yieldsInvalid {
		yieldsInvalid[i] = &pb.CreateForecastRequest_ForecastValue{
			HorizonMins: uint32(i * 30),
			P50Fraction: 1.2,
			OtherStatisticsFractions: map[string]float32{
				"p02": 1.5,
				"p10": 1.3,
				"p25": 1.1,
				"p75": 0.9,
				"p90": -0.2,
				"p98": -0.1,
			},
		}
	}

	testcases := []struct {
		name      string
		req       *pb.CreateForecastRequest
		shouldErr bool
	}{
		{
			name: "Should create forecast with populated values",
			req: &pb.CreateForecastRequest{
				LocationUuid: siteResp.LocationUuid,
				Forecaster:   fc,
				EnergySource: pb.EnergySource_ENERGY_SOURCE_SOLAR,
				InitTimeUtc:  timestamppb.New(pivotTime),
				Values:       yields,
			},
			shouldErr: false,
		},
		{
			name: "Should create forecast with zeroed values and metadata",
			req: &pb.CreateForecastRequest{
				LocationUuid: siteResp.LocationUuid,
				Forecaster:   fc,
				EnergySource: pb.EnergySource_ENERGY_SOURCE_SOLAR,
				InitTimeUtc:  timestamppb.New(pivotTime),
				Values:       yieldsZeros,
			},
			shouldErr: false,
		},
		{
			name: "Shouldn't create forecast with no values",
			req: &pb.CreateForecastRequest{
				LocationUuid: siteResp.LocationUuid,
				Forecaster:   fc,
				EnergySource: pb.EnergySource_ENERGY_SOURCE_SOLAR,
				InitTimeUtc:  timestamppb.New(pivotTime),
				Values:       []*pb.CreateForecastRequest_ForecastValue{},
			},
			shouldErr: true,
		},
		{
			name: "Shouldn't create forecast for non-existent location source",
			req: &pb.CreateForecastRequest{
				LocationUuid: siteResp.LocationUuid,
				Forecaster:   fc,
				EnergySource: pb.EnergySource_ENERGY_SOURCE_WIND,
				InitTimeUtc:  timestamppb.New(pivotTime),
				Values:       yields,
			},
			shouldErr: true,
		},
		{
			name: "Shouldn't create forecast for non-existent forecaster",
			req: &pb.CreateForecastRequest{
				LocationUuid: siteResp.LocationUuid,
				Forecaster: &pb.Forecaster{
					ForecasterName:    "non_existent_forecaster",
					ForecasterVersion: "v1",
				},
				EnergySource: pb.EnergySource_ENERGY_SOURCE_SOLAR,
				InitTimeUtc:  timestamppb.New(pivotTime),
				Values:       yields,
			},
			shouldErr: true,
		},
		{
			name: "Shouldn't create forecast for non-existent forecaster version",
			req: &pb.CreateForecastRequest{
				LocationUuid: siteResp.LocationUuid,
				Forecaster: &pb.Forecaster{
					ForecasterName:    "test_create_forecast_forecaster",
					ForecasterVersion: "v999",
				},
				EnergySource: pb.EnergySource_ENERGY_SOURCE_SOLAR,
				InitTimeUtc:  timestamppb.New(pivotTime),
				Values:       yields,
			},
			shouldErr: true,
		},
		{
			name: "Shouldn't create forecast with invalid value fractions",
			req: &pb.CreateForecastRequest{
				LocationUuid: siteResp.LocationUuid,
				Forecaster:   fc,
				EnergySource: pb.EnergySource_ENERGY_SOURCE_SOLAR,
				InitTimeUtc:  timestamppb.New(pivotTime),
				Values:       yieldsInvalid,
			},
			shouldErr: true,
		},
		{
			name: "Should create forecast with extra metadata",
			req: &pb.CreateForecastRequest{
				LocationUuid: siteResp.LocationUuid,
				Forecaster:   fc,
				EnergySource: pb.EnergySource_ENERGY_SOURCE_SOLAR,
				InitTimeUtc:  timestamppb.New(pivotTime),
				Values:       yields,
				Metadata:     metadata2,
			},
			shouldErr: false,
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := dc.CreateForecast(t.Context(), tc.req)
			if tc.shouldErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)

				fResp, err := dc.GetForecastAsTimeseries(
					t.Context(),
					&pb.GetForecastAsTimeseriesRequest{
						LocationUuid: siteResp.LocationUuid,
						EnergySource: pb.EnergySource_ENERGY_SOURCE_SOLAR,
						HorizonMins:  0,
						TimeWindow: &pb.TimeWindow{
							StartTimestampUtc: tc.req.InitTimeUtc,
							EndTimestampUtc: timestamppb.New(
								tc.req.InitTimeUtc.AsTime().Add(5 * time.Hour),
							),
						},
						Forecaster: fc,
					},
				)
				require.NoError(t, err)
				require.Equal(t, len(tc.req.Values), len(fResp.Values))
				_, err = dc.DeleteForecast(t.Context(), &pb.DeleteForecastRequest{
					Forecaster:   fc,
					LocationUuid: siteResp.LocationUuid,
					EnergySource: pb.EnergySource_ENERGY_SOURCE_SOLAR,
					InitTimeUtc:  tc.req.InitTimeUtc,
				})
				require.NoError(t, err)

				for _, val := range fResp.Values {
					require.Equal(t, tc.req.Metadata.AsMap(), val.Metadata.AsMap())
				}
			}
		})
	}
}

func TestGetLatestForecasts(t *testing.T) {
	pivotTime := time.Date(2022, 1, 5, 12, 0, 0, 0, time.UTC)
	metadata := createTestMetadata(t, map[string]any{"source": "test"})

	// Create a site to attach the forecasts to
	siteResp := createTestLocation(
		t,
		"test_get_latest_forecasts_site",
		"POINT(-0.6 51.8)",
		1000000,
		pivotTime.Add(-time.Hour*24),
		metadata,
	)
	fc := createTestForecaster(t, "test_get_latest_forecasts_forecaster", "v1")

	// Update the forecaster a couple of times
	for i := range 2 {
		_, err := dc.UpdateForecaster(t.Context(), &pb.UpdateForecasterRequest{
			Name:       fc.ForecasterName,
			NewVersion: fmt.Sprintf("v%d", i+2),
		})
		require.NoError(t, err)
	}

	yields := generateTestForecastValues(10, 30)

	// Make a forecast at diferring times for each forecaster
	for i := range 3 {
		createTestForecast(
			t,
			siteResp.LocationUuid,
			fc,
			pivotTime.Add(time.Duration(-i)*time.Hour),
			yields,
			pivotTime.Add(time.Duration(-i)*time.Hour+time.Minute),
		)
	}

	testcases := []struct {
		name              string
		req               *pb.GetLatestForecastsRequest
		expectedInitTimes []time.Time
		shouldErr         bool
	}{
		{
			name: "Should return latest forecasts for each forecaster name",
			req: &pb.GetLatestForecastsRequest{
				LocationUuid:      siteResp.LocationUuid,
				EnergySource:      pb.EnergySource_ENERGY_SOURCE_SOLAR,
				PivotTimestampUtc: timestamppb.New(pivotTime.Add(time.Minute)),
			},
			expectedInitTimes: []time.Time{
				pivotTime,
			},
			shouldErr: false,
		},
		{
			name: "Should return older forecasts for earlier pivot time",
			req: &pb.GetLatestForecastsRequest{
				LocationUuid:      siteResp.LocationUuid,
				EnergySource:      pb.EnergySource_ENERGY_SOURCE_SOLAR,
				PivotTimestampUtc: timestamppb.New(pivotTime.Add(-time.Minute * 55)),
			},
			expectedInitTimes: []time.Time{
				pivotTime.Add(-time.Hour * 1),
			},
			shouldErr: false,
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := dc.GetLatestForecasts(t.Context(), tc.req)
			if tc.shouldErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.NotNil(t, resp)
				require.Equal(t, len(tc.expectedInitTimes), len(resp.Forecasts))

				actualTimes := make([]time.Time, len(resp.Forecasts))
				for i, forecast := range resp.Forecasts {
					actualTimes[i] = forecast.InitializationTimestampUtc.AsTime()
				}

				require.Equal(t, tc.expectedInitTimes, actualTimes)

				// check that created utc is not null
				for _, forecast := range resp.Forecasts {
					require.NotNil(t, forecast.CreatedTimestampUtc)
				}
			}
		})
	}
}

func TestStreamForecastData(t *testing.T) {
	pivotTime := time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)

	// Create a site to attach the forecasts to
	metadata := createTestMetadata(t, map[string]any{"stream_test": "true"})
	siteResp := createTestLocation(
		t,
		"test_stream_forecast_data_site",
		"POINT(-60.25 57.5)",
		1000000,
		pivotTime.Add(-time.Hour*48),
		metadata,
	)

	// Create two forecasters
	fc1 := createTestForecaster(t, "stream_forecaster_alpha", "v1")
	fc2 := createTestForecaster(t, "stream_forecaster_beta", "v2")

	// Generate 3 forecasts for each forecaster, each with 4 horizon values (0, 5, 10, 15)
	yields := generateTestForecastValues(4, 5)

	// Seed forecasts for both forecasters spanning backwards from pivot time
	for i := 2; i >= 0; i-- {
		initTime := timestamppb.New(pivotTime.Add(time.Duration(-i*60) * time.Minute))
		createTestForecast(
			t,
			siteResp.LocationUuid,
			fc1,
			initTime.AsTime(),
			yields,
			initTime.AsTime(),
		)
		createTestForecast(
			t,
			siteResp.LocationUuid,
			fc2,
			initTime.AsTime(),
			yields,
			initTime.AsTime(),
		)
	}

	defaultTimeWindow := &pb.TimeWindow{
		StartTimestampUtc: timestamppb.New(pivotTime.Add(-time.Hour * 10)),
		EndTimestampUtc:   timestamppb.New(pivotTime.Add(time.Hour * 10)),
	}

	testcases := []struct {
		name              string
		req               *pb.StreamForecastDataRequest
		expectedRowsCount int
		shouldErr         bool
		checkMetadata     bool
	}{
		{
			name: "Should successfully stream forecasts for a single forecaster",
			req: &pb.StreamForecastDataRequest{
				LocationUuids:   []string{siteResp.LocationUuid},
				EnergySource:    pb.EnergySource_ENERGY_SOURCE_SOLAR,
				Forecasters:     []*pb.Forecaster{fc1},
				TimeWindow:      defaultTimeWindow,
				IncludeMetadata: false,
			},
			// 3 forecasts * 4 values = 12 rows
			expectedRowsCount: 12,
			shouldErr:         false,
		},
		{
			name: "Should successfully stream forecasts for multiple forecasters",
			req: &pb.StreamForecastDataRequest{
				LocationUuids:   []string{siteResp.LocationUuid},
				EnergySource:    pb.EnergySource_ENERGY_SOURCE_SOLAR,
				Forecasters:     []*pb.Forecaster{fc1, fc2},
				TimeWindow:      defaultTimeWindow,
				IncludeMetadata: false,
			},
			// (3 forecasts * 4 values) * 2 forecasters = 24 rows
			expectedRowsCount: 24,
			shouldErr:         false,
		},
		{
			name: "Should only return forecasts whos init times respect the time window boundaries",
			req: &pb.StreamForecastDataRequest{
				LocationUuids: []string{siteResp.LocationUuid},
				EnergySource:  pb.EnergySource_ENERGY_SOURCE_SOLAR,
				Forecasters:   []*pb.Forecaster{fc1},
				TimeWindow: &pb.TimeWindow{
					// Constrain the window to only capture the most recent forecast
					StartTimestampUtc: timestamppb.New(pivotTime.Add(-time.Minute * 10)),
					EndTimestampUtc:   timestamppb.New(pivotTime.Add(time.Minute * 10)),
				},
				IncludeMetadata: false,
			},
			expectedRowsCount: 4,
			shouldErr:         false,
		},
		{
			name: "Should include metadata when asked",
			req: &pb.StreamForecastDataRequest{
				LocationUuids:   []string{siteResp.LocationUuid},
				EnergySource:    pb.EnergySource_ENERGY_SOURCE_SOLAR,
				Forecasters:     []*pb.Forecaster{fc1},
				TimeWindow:      defaultTimeWindow,
				IncludeMetadata: true,
			},
			expectedRowsCount: 12,
			shouldErr:         false,
			checkMetadata:     true,
		},
		{
			name: "Should fail gracefully with an invalid UUID",
			req: &pb.StreamForecastDataRequest{
				LocationUuids: []string{"not-a-valid-uuid"},
				EnergySource:  pb.EnergySource_ENERGY_SOURCE_SOLAR,
				Forecasters:   []*pb.Forecaster{fc1},
				TimeWindow:    defaultTimeWindow,
			},
			shouldErr: true,
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			stream, err := dc.StreamForecastData(t.Context(), tc.req)
			if err != nil {
				if tc.shouldErr {
					return
				}

				require.NoError(t, err)
			}

			var actualRowsCount int
			for {
				batchResp, err := stream.Recv()
				if err == io.EOF {
					break
				}

				if tc.shouldErr {
					require.Error(t, err)
					return
				}

				require.NoError(t, err)
				require.NotNil(t, batchResp)

				for _, pt := range batchResp.Values {
					actualRowsCount++

					if tc.checkMetadata {
						require.NotNil(t, pt.Metadata)
						require.Equal(t, "test", pt.Metadata["source"])
					} else {
						require.Empty(t, pt.Metadata)
					}

					require.NotZero(t, pt.EffectiveCapacityWatts)
					require.NotNil(t, pt.OtherStatisticsFractions)
					require.Contains(t, pt.OtherStatisticsFractions, "p90", "p02")
					require.NotContains(t, pt.OtherStatisticsFractions, "p50", "p25", "p75", "p98")
				}
			}

			if !tc.shouldErr {
				require.Equal(t, tc.expectedRowsCount, actualRowsCount)
			}
		})
	}
}

func TestStreamCreateForecasts(t *testing.T) {
	pivotTime := time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC)

	// Create a site
	siteResp := createTestLocation(
		t,
		"test_stream_create_forecasts_site",
		"POINT(-0.1 51.5)",
		1000000,
		pivotTime.Add(-time.Hour*24),
		nil,
	)

	// Create a forecaster
	fc := createTestForecaster(t, "test_stream_create_forecasts_forecaster", "v1")

	yields := generateTestForecastValues(10, 30)

	testcases := []struct {
		name               string
		setupStream        func(ctx context.Context) (pb.DataPlatformDataService_StreamCreateForecastsClient, error)
		sendCount          int
		getReq             func(i int) *pb.CreateForecastRequest
		shouldErr          bool
		expectedErrCode    codes.Code
		expectedUuidsCount int
	}{
		{
			name: "Valid stream under limit",
			setupStream: func(ctx context.Context) (pb.DataPlatformDataService_StreamCreateForecastsClient, error) {
				return dc.StreamCreateForecasts(ctx)
			},
			sendCount: 10,
			getReq: func(i int) *pb.CreateForecastRequest {
				return &pb.CreateForecastRequest{
					LocationUuid: siteResp.LocationUuid,
					Forecaster: &pb.Forecaster{
						ForecasterName:    fc.ForecasterName,
						ForecasterVersion: fc.ForecasterVersion,
					},
					EnergySource: pb.EnergySource_ENERGY_SOURCE_SOLAR,
					InitTimeUtc:  timestamppb.New(pivotTime.Add(time.Duration(i) * time.Hour)),
					Values:       yields,
				}
			},
			shouldErr:          false,
			expectedUuidsCount: 10,
		},
		{
			name: "Atomicity on Failure (rollback after valid batch)",
			setupStream: func(ctx context.Context) (pb.DataPlatformDataService_StreamCreateForecastsClient, error) {
				return dc.StreamCreateForecasts(ctx)
			},
			sendCount: 600, // Should exceed batch size of 500
			getReq: func(i int) *pb.CreateForecastRequest {
				// Inject error at the end
				if i == 599 {
					return &pb.CreateForecastRequest{
						LocationUuid: siteResp.LocationUuid,
						Forecaster: &pb.Forecaster{
							ForecasterName:    "non_existent",
							ForecasterVersion: "v1",
						},
						EnergySource: pb.EnergySource_ENERGY_SOURCE_SOLAR,
						InitTimeUtc:  timestamppb.New(pivotTime),
						Values:       yields,
					}
				}

				return &pb.CreateForecastRequest{
					LocationUuid: siteResp.LocationUuid,
					Forecaster: &pb.Forecaster{
						ForecasterName:    fc.ForecasterName,
						ForecasterVersion: fc.ForecasterVersion,
					},
					EnergySource: pb.EnergySource_ENERGY_SOURCE_SOLAR,
					InitTimeUtc:  timestamppb.New(pivotTime.Add(time.Duration(i) * time.Hour)),
					Values:       yields,
				}
			},
			shouldErr: true,
		},
		{
			name: "Limit Exceeded",
			setupStream: func(ctx context.Context) (pb.DataPlatformDataService_StreamCreateForecastsClient, error) {
				return dc.StreamCreateForecasts(ctx)
			},
			sendCount: 5001,
			getReq: func(i int) *pb.CreateForecastRequest {
				return &pb.CreateForecastRequest{
					LocationUuid: siteResp.LocationUuid,
					Forecaster: &pb.Forecaster{
						ForecasterName:    fc.ForecasterName,
						ForecasterVersion: fc.ForecasterVersion,
					},
					EnergySource: pb.EnergySource_ENERGY_SOURCE_SOLAR,
					InitTimeUtc:  timestamppb.New(pivotTime.Add(time.Duration(i) * time.Hour)),
					Values:       yields,
				}
			},
			shouldErr:       true,
			expectedErrCode: codes.InvalidArgument,
		},
	}

	for tcIdx, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()

			// We use a fresh site and forecaster per test case to avoid pollution
			// when checking atomicity
			siteRespTC := createTestLocation(
				t,
				fmt.Sprintf("test_stream_site_%d", tcIdx),
				"POINT(-0.1 51.5)",
				1000000,
				pivotTime.Add(-time.Hour*24),
				nil,
			)

			fcTC := createTestForecaster(t, fmt.Sprintf("test_stream_fc_%d", tcIdx), "v1")

			stream, err := tc.setupStream(ctx)
			require.NoError(t, err)

			var sendErr error
			for i := 0; i < tc.sendCount; i++ {
				req := tc.getReq(i)
				// Overwrite the location and forecaster with the testcase-specific ones
				// unless it's the deliberately broken one
				if req.Forecaster.ForecasterName != "non_existent" {
					req.LocationUuid = siteRespTC.LocationUuid
					req.Forecaster.ForecasterName = fcTC.ForecasterName
					req.Forecaster.ForecasterVersion = fcTC.ForecasterVersion
				}

				if err := stream.Send(req); err != nil && err != io.EOF {
					sendErr = err
					break
				}
			}

			var (
				closeErr error
				resp     *pb.StreamCreateForecastsResponse
			)

			if sendErr == nil {
				resp, closeErr = stream.CloseAndRecv()
			} else {
				closeErr = sendErr
			}

			if tc.shouldErr {
				require.Error(t, closeErr)

				if tc.expectedErrCode != codes.OK {
					require.Equal(t, tc.expectedErrCode, status.Code(closeErr))
				}

				// Assert atomicity: No new forecasts should have been saved
				postResp, err := dc.GetLatestForecasts(ctx, &pb.GetLatestForecastsRequest{
					LocationUuid: siteRespTC.LocationUuid,
					EnergySource: pb.EnergySource_ENERGY_SOURCE_SOLAR,
					PivotTimestampUtc: timestamppb.New(
						pivotTime.Add(time.Duration(100000) * time.Hour),
					),
				})

				var postCount int
				if err == nil && postResp != nil {
					postCount = len(postResp.Forecasts)
				}

				require.Equal(t, 0, postCount, "Atomicity failed: Forecasts were partially saved")
			} else {
				require.NoError(t, closeErr)
				require.NotNil(t, resp)
				require.Len(t, resp.ForecastUuids, tc.expectedUuidsCount)
			}
		})
	}
}

func TestPrepareForecastParams(t *testing.T) {
	geomID := uuid.MustParse("018e6a12-8854-7123-b123-123456789abc")
	sourceID := int16(2)
	forecasterID := int32(42)

	testcases := []struct {
		name                string
		req                 *pb.CreateForecastRequest
		expectedInitTime    time.Time
		expectedCreatedTime time.Time
		expectDynamicCreate bool
		expectedTargetLower time.Time
		expectedTargetUpper time.Time
		expectedResolution  int16
		shouldErr           bool
	}{
		{
			name: "Valid request with CreatedTimestampUtc",
			req: &pb.CreateForecastRequest{
				InitTimeUtc: timestamppb.New(
					time.Date(2024, 5, 5, 12, 30, 45, 0, time.UTC),
				),
				CreatedTimestampUtc: timestamppb.New(time.Date(2024, 5, 5, 12, 0, 0, 0, time.UTC)),
				Values: []*pb.CreateForecastRequest_ForecastValue{
					{HorizonMins: 30},
					{HorizonMins: 60},
					{HorizonMins: 90},
				},
			},
			expectedInitTime: time.Date(
				2024,
				5,
				5,
				12,
				30,
				0,
				0,
				time.UTC,
			), // Truncated to minute
			expectedCreatedTime: time.Date(2024, 5, 5, 12, 0, 0, 0, time.UTC),
			expectDynamicCreate: false,
			expectedTargetLower: time.Date(2024, 5, 5, 13, 0, 0, 0, time.UTC), // 12:30 + 30m
			expectedTargetUpper: time.Date(2024, 5, 5, 14, 0, 0, 0, time.UTC), // 12:30 + 90m
			expectedResolution:  30,
		},
		{
			name: "Valid request without CreatedTimestampUtc",
			req: &pb.CreateForecastRequest{
				InitTimeUtc: timestamppb.New(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)),
				Values: []*pb.CreateForecastRequest_ForecastValue{
					{HorizonMins: 0},
					{HorizonMins: 15},
				},
			},
			expectedInitTime:    time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			expectDynamicCreate: true, // Will default to current time
			expectedTargetLower: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			expectedTargetUpper: time.Date(2024, 1, 1, 0, 15, 0, 0, time.UTC),
			expectedResolution:  15,
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			params, err := prepareForecastParams(tc.req, geomID, sourceID, forecasterID)
			if tc.shouldErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)

			// Assert straightforward assignments
			require.Equal(t, geomID, params.GeometryUuid)
			require.Equal(t, sourceID, params.SourceTypeID)
			require.Equal(t, forecasterID, params.ForecasterID)
			require.Equal(t, tc.expectedResolution, params.ValueResolutionMins)

			// Assert InitTime (should be truncated to the minute)
			require.Equal(t, tc.expectedInitTime, params.InitTimeUtc.Time.UTC())

			// Assert TargetPeriod boundaries
			require.True(t, params.TargetPeriod.Valid)
			require.Equal(t, tc.expectedTargetLower, params.TargetPeriod.Lower.Time.UTC())
			require.Equal(t, tc.expectedTargetUpper, params.TargetPeriod.Upper.Time.UTC())

			// Assert CreatedAt logic (either explicitly set or fallback to current time)
			if tc.expectDynamicCreate {
				now := time.Now().UTC().Truncate(time.Minute)
				require.Equal(t, now, params.CreatedAtUtc.Time.UTC())
			} else {
				require.Equal(t, tc.expectedCreatedTime, params.CreatedAtUtc.Time.UTC())
			}

			// Assert UUIDv7 timestamp encoding
			uuidBytes := params.ForecastUuid
			ms := uint64(uuidBytes[0])<<40 |
				uint64(uuidBytes[1])<<32 |
				uint64(uuidBytes[2])<<24 |
				uint64(uuidBytes[3])<<16 |
				uint64(uuidBytes[4])<<8 |
				uint64(uuidBytes[5])

			extractedTime := time.UnixMilli(int64(ms)).UTC()
			require.Equal(
				t,
				tc.expectedInitTime,
				extractedTime,
				"UUID prefix should encode the truncated InitTimeUtc",
			)
		})
	}
}
