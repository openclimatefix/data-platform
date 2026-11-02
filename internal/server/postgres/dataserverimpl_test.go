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
	countryCode string,
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
		CountryCode:            ptr(countryCode),
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
				CountryCode:            ptr("GB"),
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
				CountryCode:            ptr("GB"),
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
				CountryCode:            ptr("GB"),
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
				CountryCode:            ptr("GB"),
				Metadata:               metadata,
			},
			shouldErr: true,
		},
		{
			name: "Shouldn't create non-site location with POINT geometry",
			req: &pb.CreateLocationRequest{
				LocationName:           "point_gsp",
				EnergySource:           pb.EnergySource_ENERGY_SOURCE_SOLAR,
				GeometryWkt:            "POINT(0.0 51.5)",
				EffectiveCapacityWatts: 1230,
				LocationType:           pb.LocationType_LOCATION_TYPE_GSP,
				CountryCode:            ptr("GB"),
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
				CountryCode:            ptr("GB"),
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
				CountryCode:            ptr("GB"),
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
				CountryCode:            ptr("GB"),
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
				CountryCode:            ptr("GB"),
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
				CountryCode:            ptr("GB"),
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
				CountryCode:            ptr("GB"),
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
				CountryCode:            ptr("GB"),
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
				CountryCode:            ptr("GB"),
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

func TestCreateLocationEnergySource(t *testing.T) {
	metadata := createTestMetadata(t, map[string]any{"source": "test_energy_source"})

	// Create a location with solar valid from today
	createResp := createTestLocation(
		t,
		"scotland_region",
		"POINT(-0.127 51.507)",
		1230,
		time.Now().UTC().Truncate(time.Minute),
		metadata,
		"GB",
	)

	testcases := []struct {
		name         string
		req          *pb.CreateLocationEnergySourceRequest
		shouldErr    bool
		errCodeCheck codes.Code
	}{
		{
			name: "Should add wind energy source to existing location",
			req: &pb.CreateLocationEnergySourceRequest{
				LocationUuid:           createResp.LocationUuid,
				EnergySource:           pb.EnergySource_ENERGY_SOURCE_WIND,
				EffectiveCapacityWatts: 5000,
				Metadata:               metadata,
			},
			shouldErr: false,
		},
		{
			name: "Should fail to add already existing energy source",
			req: &pb.CreateLocationEnergySourceRequest{
				LocationUuid:           createResp.LocationUuid,
				EnergySource:           pb.EnergySource_ENERGY_SOURCE_SOLAR, // Already exists
				EffectiveCapacityWatts: 2000,
				Metadata:               metadata,
			},
			shouldErr:    true,
			errCodeCheck: codes.AlreadyExists,
		},
		{
			name: "Should fail if the energy source already exists but we query a past valid_from",
			req: &pb.CreateLocationEnergySourceRequest{
				LocationUuid:           createResp.LocationUuid,
				EnergySource:           pb.EnergySource_ENERGY_SOURCE_SOLAR, // Created today
				EffectiveCapacityWatts: 2000,
				ValidFromUtc: timestamppb.New(
					time.Now().UTC().Add(-24 * time.Hour).Truncate(time.Minute),
				),
				Metadata: metadata,
			},
			shouldErr:    true,
			errCodeCheck: codes.AlreadyExists,
		},
		{
			name: "Should fail for non-existent location",
			req: &pb.CreateLocationEnergySourceRequest{
				LocationUuid:           uuid.New().String(),
				EnergySource:           pb.EnergySource_ENERGY_SOURCE_WIND,
				EffectiveCapacityWatts: 5000,
				Metadata:               metadata,
			},
			shouldErr: true,
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := dc.CreateLocationEnergySource(t.Context(), tc.req)

			if tc.shouldErr {
				require.Error(t, err, "Expected an error")

				if tc.errCodeCheck != codes.OK {
					require.Equal(t, tc.errCodeCheck, status.Code(err))
				}
			} else {
				require.NoError(t, err, "Expected to be able to create the location energy source")
				require.Equal(t, tc.req.LocationUuid, resp.LocationUuid)
				require.Equal(t, tc.req.EnergySource, resp.EnergySource)
				require.Equal(t, tc.req.EffectiveCapacityWatts, resp.EffectiveCapacityWatts)

				// Verify it can be fetched
				resp2, err := dc.GetLocation(
					t.Context(),
					&pb.GetLocationRequest{
						LocationUuid:    tc.req.LocationUuid,
						EnergySource:    tc.req.EnergySource,
						IncludeGeometry: false,
					},
				)
				require.NoError(
					t,
					err,
					"Expected to be able to fetch the newly created energy source",
				)
				require.Equal(t, tc.req.EffectiveCapacityWatts, resp2.EffectiveCapacityWatts)
			}
		})
	}
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
		"GB",
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
		"GB",
	)

	createResp = createTestLocation(
		t,
		"test_update_location_owner_site_2",
		"POINT(-0.2 51.6)",
		1000e6,
		time.Date(2019, 5, 6, 6, 0, 0, 0, time.UTC),
		metadata,
		"GB",
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

func TestBatchUpdateLocationCapacity(t *testing.T) {
	initialMetadata := createTestMetadata(t, map[string]any{"source": "test"})
	newMetadata := createTestMetadata(t, map[string]any{"source": "updated", "extra": 1})
	pivotTime := time.Date(2019, 5, 6, 6, 0, 0, 0, time.UTC)

	type expectedState struct {
		capacityWatts uint64
		metadata      map[string]any
	}

	// Each case operates on its own pair of locations, A and B, both created with 1000MW/2000MW
	// capacity and the initial metadata. The updates are built from the created location UUIDs.
	testcases := []struct {
		name                   string
		updates                func(a, b string) []*pb.BatchUpdateLocationCapacityRequest_Update
		expectedUpdatedCount   uint32
		expectedUnchangedCount uint32
		expectedA              expectedState
		expectedB              expectedState
		expectedErrCode        codes.Code
	}{
		{
			name: "Should update only the location whose capacity changed, keeping metadata",
			updates: func(a, b string) []*pb.BatchUpdateLocationCapacityRequest_Update {
				return []*pb.BatchUpdateLocationCapacityRequest_Update{
					{LocationUuid: a, NewEffectiveCapacityWatts: ptr(uint64(1500e6))},
					{LocationUuid: b, NewEffectiveCapacityWatts: ptr(uint64(2000e6))},
				}
			},
			expectedUpdatedCount:   1,
			expectedUnchangedCount: 1,
			expectedA:              expectedState{1500e6, initialMetadata.AsMap()},
			expectedB:              expectedState{2000e6, initialMetadata.AsMap()},
		},
		{
			name: "Should update metadata only, keeping capacity",
			updates: func(a, b string) []*pb.BatchUpdateLocationCapacityRequest_Update {
				return []*pb.BatchUpdateLocationCapacityRequest_Update{
					{LocationUuid: a, NewMetadata: newMetadata},
				}
			},
			expectedUpdatedCount:   1,
			expectedUnchangedCount: 0,
			expectedA:              expectedState{1000e6, newMetadata.AsMap()},
			expectedB:              expectedState{2000e6, initialMetadata.AsMap()},
		},
		{
			name: "Should update capacity and metadata together",
			updates: func(a, b string) []*pb.BatchUpdateLocationCapacityRequest_Update {
				return []*pb.BatchUpdateLocationCapacityRequest_Update{
					{
						LocationUuid:              a,
						NewEffectiveCapacityWatts: ptr(uint64(1100e6)),
						NewMetadata:               newMetadata,
					},
				}
			},
			expectedUpdatedCount:   1,
			expectedUnchangedCount: 0,
			expectedA:              expectedState{1100e6, newMetadata.AsMap()},
			expectedB:              expectedState{2000e6, initialMetadata.AsMap()},
		},
		{
			name: "Should apply a mix of capacity-only and metadata-only updates to the right rows",
			updates: func(a, b string) []*pb.BatchUpdateLocationCapacityRequest_Update {
				return []*pb.BatchUpdateLocationCapacityRequest_Update{
					{LocationUuid: a, NewMetadata: newMetadata},
					{LocationUuid: b, NewEffectiveCapacityWatts: ptr(uint64(2500e6))},
				}
			},
			expectedUpdatedCount:   2,
			expectedUnchangedCount: 0,
			expectedA:              expectedState{1000e6, newMetadata.AsMap()},
			expectedB:              expectedState{2500e6, initialMetadata.AsMap()},
		},
		{
			name: "Should clear metadata when given an empty object",
			updates: func(a, b string) []*pb.BatchUpdateLocationCapacityRequest_Update {
				return []*pb.BatchUpdateLocationCapacityRequest_Update{
					{LocationUuid: a, NewMetadata: &structpb.Struct{}},
				}
			},
			expectedUpdatedCount:   1,
			expectedUnchangedCount: 0,
			expectedA:              expectedState{1000e6, map[string]any{}},
			expectedB:              expectedState{2000e6, initialMetadata.AsMap()},
		},
		{
			name: "Should treat identical capacity and metadata as unchanged",
			updates: func(a, b string) []*pb.BatchUpdateLocationCapacityRequest_Update {
				return []*pb.BatchUpdateLocationCapacityRequest_Update{
					{
						LocationUuid:              a,
						NewEffectiveCapacityWatts: ptr(uint64(1000e6)),
						NewMetadata:               initialMetadata,
					},
					{LocationUuid: b, NewMetadata: initialMetadata},
				}
			},
			expectedUpdatedCount:   0,
			expectedUnchangedCount: 2,
			expectedA:              expectedState{1000e6, initialMetadata.AsMap()},
			expectedB:              expectedState{2000e6, initialMetadata.AsMap()},
		},
		{
			name: "Should fail the whole batch for an unknown location uuid",
			updates: func(a, b string) []*pb.BatchUpdateLocationCapacityRequest_Update {
				return []*pb.BatchUpdateLocationCapacityRequest_Update{
					{LocationUuid: a, NewEffectiveCapacityWatts: ptr(uint64(1600e6))},
					{LocationUuid: uuid.NewString(), NewEffectiveCapacityWatts: ptr(uint64(1e6))},
				}
			},
			expectedErrCode: codes.FailedPrecondition,
		},
		{
			name: "Should reject an update with neither capacity nor metadata",
			updates: func(a, b string) []*pb.BatchUpdateLocationCapacityRequest_Update {
				return []*pb.BatchUpdateLocationCapacityRequest_Update{
					{LocationUuid: a},
				}
			},
			expectedErrCode: codes.InvalidArgument,
		},
		{
			name: "Should reject duplicate location uuids in a batch",
			updates: func(a, b string) []*pb.BatchUpdateLocationCapacityRequest_Update {
				return []*pb.BatchUpdateLocationCapacityRequest_Update{
					{LocationUuid: a, NewEffectiveCapacityWatts: ptr(uint64(1100e6))},
					{LocationUuid: a, NewMetadata: newMetadata},
				}
			},
			expectedErrCode: codes.InvalidArgument,
		},
	}

	for i, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			locA := createTestLocation(
				t, fmt.Sprintf("test_batch_capacity_site_a_%d", i), "POINT(-0.1 51.5)",
				1000e6, pivotTime, initialMetadata, "GB",
			)
			locB := createTestLocation(
				t, fmt.Sprintf("test_batch_capacity_site_b_%d", i), "POINT(-0.2 51.6)",
				2000e6, pivotTime, initialMetadata, "GB",
			)

			req := &pb.BatchUpdateLocationCapacityRequest{
				EnergySource: pb.EnergySource_ENERGY_SOURCE_SOLAR,
				ValidFromUtc: timestamppb.New(pivotTime.Add(time.Hour)),
				Updates:      tc.updates(locA.LocationUuid, locB.LocationUuid),
			}

			resp, err := dc.BatchUpdateLocationCapacity(t.Context(), req)

			if tc.expectedErrCode != codes.OK {
				require.Error(t, err)
				require.Equal(t, tc.expectedErrCode, status.Code(err), err.Error())
				return
			}

			require.NoError(t, err)
			require.Equal(t, tc.expectedUpdatedCount, resp.UpdatedCount)
			require.Equal(t, tc.expectedUnchangedCount, resp.UnchangedCount)

			expected := map[string]expectedState{
				locA.LocationUuid: tc.expectedA,
				locB.LocationUuid: tc.expectedB,
			}
			for locationUuid, want := range expected {
				getResp, err := dc.GetLocation(t.Context(), &pb.GetLocationRequest{
					LocationUuid:      locationUuid,
					EnergySource:      pb.EnergySource_ENERGY_SOURCE_SOLAR,
					PivotTimestampUtc: timestamppb.New(req.ValidFromUtc.AsTime().Add(time.Minute)),
				})
				require.NoError(t, err)
				require.Equal(t, want.capacityWatts, getResp.EffectiveCapacityWatts)
				require.Equal(t, want.metadata, getResp.Metadata.AsMap())
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
		"GB",
	)
	siteResp2 := createTestLocation(
		t,
		"test_get_forecast_at_timestamp_site_2",
		"POINT(-0.5 58.6)",
		2000000,
		pivotTime.Add(-time.Hour*1),
		metadata,
		"GB",
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

	expectedCreatedTime := time.Date(2025, 2, 26, 12, 5, 0, 0, time.UTC)

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
			"GB",
		)
		siteUuids[i] = siteResp.LocationUuid

		req := &pb.CreateObservationsRequest{
			LocationUuid:        siteResp.LocationUuid,
			EnergySource:        pb.EnergySource_ENERGY_SOURCE_SOLAR,
			ObserverName:        observerResp.ObserverName,
			CreatedTimestampUtc: timestamppb.New(expectedCreatedTime),
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

					require.NotNil(t, obs.CreatedTimestampUtc)
					created := obs.CreatedTimestampUtc.AsTime()
					require.True(t, created.Equal(expectedCreatedTime),
						"created time %s should be equal to %s", created, expectedCreatedTime)
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
		"GB",
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
				require.Equal(t, "GB", resp.CountryCode)

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
		"GB",
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
			"GB",
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
		pivotTime.Add(-time.Hour*24*32),
		metadata,
		"GB",
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
		{
			name: "Should allow a time window of exactly 31 days",
			req: &pb.GetForecastAsTimeseriesRequest{
				LocationUuid: siteResp.LocationUuid,
				Forecaster:   fc,
				EnergySource: pb.EnergySource_ENERGY_SOURCE_SOLAR,
				HorizonMins:  0,
				TimeWindow: &pb.TimeWindow{
					StartTimestampUtc: timestamppb.New(
						pivotTime.Add(time.Hour * 36).Add(-time.Hour * 24 * 31),
					),
					EndTimestampUtc: timestamppb.New(pivotTime.Add(time.Hour * 36)),
				},
				InitializationTimestampUtc: timestamppb.New(pivotTime.Add(-30 * time.Minute)),
			},
			expectedValues: []float32{
				0.5, 0.501, 0.502, 0.503, 0.504, 0.505, 0.506, 0.507, 0.508, 0.509, 0.510, 0.511,
			},
			shouldErr: false,
		},
		{
			name: "Should reject a time window longer than 31 days",
			req: &pb.GetForecastAsTimeseriesRequest{
				LocationUuid: siteResp.LocationUuid,
				Forecaster:   fc,
				EnergySource: pb.EnergySource_ENERGY_SOURCE_SOLAR,
				HorizonMins:  0,
				TimeWindow: &pb.TimeWindow{
					StartTimestampUtc: timestamppb.New(
						pivotTime.Add(time.Hour * 36).Add(-time.Hour*24*31 - time.Minute),
					),
					EndTimestampUtc: timestamppb.New(pivotTime.Add(time.Hour * 36)),
				},
			},
			shouldErr: true,
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
		cc := "GB"
		if i == 1 {
			cc = "NL"
		}

		for _, energySource := range []pb.EnergySource{pb.EnergySource_ENERGY_SOURCE_SOLAR, pb.EnergySource_ENERGY_SOURCE_WIND} {
			for _, locType := range []pb.LocationType{pb.LocationType_LOCATION_TYPE_SITE, pb.LocationType_LOCATION_TYPE_GSP} {
				geometryWkt := fmt.Sprintf(
					"POLYGON((-5.%[1]d 51.%[1]d, -5.%[1]d 51.%[1]d1, -5.%[1]d1 51.%[1]d1, -5.%[1]d1 51.%[1]d, -5.%[1]d 51.%[1]d))",
					i,
				)

				resp, err := dc.CreateLocation(t.Context(), &pb.CreateLocationRequest{
					LocationName: fmt.Sprintf(
						"test_list_locations_site_%02d_%d_%d",
						i,
						energySource,
						locType,
					),
					GeometryWkt:            geometryWkt,
					EffectiveCapacityWatts: uint64(1000000 + i*100),
					EnergySource:           energySource,
					LocationType:           locType,
					ValidFromUtc:           timestamppb.New(pivotTime.Add(-time.Hour * 4)),
					Metadata:               metadata,
					CountryCode:            ptr(cc),
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
			name: "Should filter by country code",
			req: &pb.ListLocationsRequest{
				CountryCodeFilter:   ptr("GB"),
				LocationUuidsFilter: locationUuids,
			},
			expectedCount: 4 * 4, // 0, 2, 3, 4
		},
		{
			name: "Should filter by country code and energy source",
			req: &pb.ListLocationsRequest{
				CountryCodeFilter:   ptr("NL"),
				EnergySourceFilter:  sourceFilter,
				LocationUuidsFilter: locationUuids,
			},
			expectedCount: 2, // Only 01, which is 1 * 2 locations matching solar
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
		"GB",
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

	expectedCreatedTime := time.Date(2025, 2, 26, 12, 5, 0, 0, time.UTC)

	_, err = dc.CreateObservations(t.Context(), &pb.CreateObservationsRequest{
		LocationUuid:        siteResp.LocationUuid,
		EnergySource:        pb.EnergySource_ENERGY_SOURCE_SOLAR,
		ObserverName:        obsResp.ObserverName,
		CreatedTimestampUtc: timestamppb.New(expectedCreatedTime),
		Values:              values,
	})
	require.NoError(t, err)

	testcases := []struct {
		name            string
		startTime       time.Time
		endTime         time.Time
		expectedSize    int
		expectedErrCode codes.Code
	}{
		{
			name:         "Should return all values in a day-long window",
			startTime:    pivotTime.Add(-time.Hour * 24),
			endTime:      pivotTime,
			expectedSize: len(values),
		},
		{
			name:         "Should allow windows longer than a week",
			startTime:    pivotTime.Add(-time.Hour * 24 * 8),
			endTime:      pivotTime,
			expectedSize: len(values),
		},
		{
			name:         "Should allow windows of exactly 31 days",
			startTime:    pivotTime.Add(-time.Hour * 24 * 31),
			endTime:      pivotTime,
			expectedSize: len(values),
		},
		{
			name:            "Should reject windows longer than 31 days",
			startTime:       pivotTime.Add(-time.Hour*24*31 - time.Minute),
			endTime:         pivotTime,
			expectedErrCode: codes.InvalidArgument,
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
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
			if tc.expectedErrCode != codes.OK {
				require.Error(t, err)
				require.Equal(t, tc.expectedErrCode, status.Code(err))
				return
			}

			require.NoError(t, err)
			require.Equal(t, tc.expectedSize, len(resp.Values))

			for _, v := range resp.Values {
				require.NotNil(t, v.CreatedTimestampUtc)
				created := v.CreatedTimestampUtc.AsTime()
				require.True(t, created.Equal(expectedCreatedTime),
					"created time %s should be equal to %s", created, expectedCreatedTime)
			}
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
		"GB",
	)

	// Create an observer to make the observations
	obsResp, err := dc.CreateObserver(t.Context(), &pb.CreateObserverRequest{
		Name: "test_get_latest_observations_observer",
	})
	require.NoError(t, err)

	// Seed some observations for the site.
	// Create the older observation
	_, err = dc.CreateObservations(t.Context(), &pb.CreateObservationsRequest{
		LocationUuid:        siteResp.LocationUuid,
		EnergySource:        pb.EnergySource_ENERGY_SOURCE_SOLAR,
		ObserverName:        obsResp.ObserverName,
		CreatedTimestampUtc: timestamppb.New(pivotTime.Add(-time.Hour * 2)),
		Values: []*pb.CreateObservationsRequest_Value{
			{
				TimestampUtc: timestamppb.New(pivotTime.Add(-time.Hour * 2)),
				ValueWatts:   uint64(0.3 * float64(siteResp.EffectiveCapacityWatts)),
			},
		},
	})
	require.NoError(t, err)

	// Create the newer observation
	_, err = dc.CreateObservations(t.Context(), &pb.CreateObservationsRequest{
		LocationUuid:        siteResp.LocationUuid,
		EnergySource:        pb.EnergySource_ENERGY_SOURCE_SOLAR,
		ObserverName:        obsResp.ObserverName,
		CreatedTimestampUtc: timestamppb.New(pivotTime.Add(-time.Hour * 1)),
		Values: []*pb.CreateObservationsRequest_Value{
			{
				TimestampUtc: timestamppb.New(pivotTime.Add(-time.Hour * 1)),
				ValueWatts:   uint64(0.5 * float64(siteResp.EffectiveCapacityWatts)),
			},
		},
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
		"GB",
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
			name: "Should create valid observations with explicitly provided created timestamp",
			req: &pb.CreateObservationsRequest{
				LocationUuid:        siteResp.LocationUuid,
				EnergySource:        pb.EnergySource_ENERGY_SOURCE_SOLAR,
				ObserverName:        obsResp.ObserverName,
				CreatedTimestampUtc: timestamppb.New(time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC)),
				Values:              validObservations,
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
		"GB",
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
		"GB",
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

	yieldsPartial := make([]*pb.CreateForecastRequest_ForecastValue, 10)
	for i := range yieldsPartial {
		statFractions := map[string]float32{"p10": 0.1, "p90": 0.9}
		// p25 is only populated on some values, making it partial
		if i%2 == 0 {
			statFractions["p25"] = 0.25
		}

		yieldsPartial[i] = &pb.CreateForecastRequest_ForecastValue{
			HorizonMins:              uint32(i * 30),
			P50Fraction:              0.5,
			OtherStatisticsFractions: statFractions,
		}
	}

	testcases := []struct {
		name      string
		req       *pb.CreateForecastRequest
		shouldErr bool
	}{
		{
			name: "Shouldn't create forecast with partially populated statistic",
			req: &pb.CreateForecastRequest{
				LocationUuid: siteResp.LocationUuid,
				Forecaster:   fc,
				EnergySource: pb.EnergySource_ENERGY_SOURCE_SOLAR,
				InitTimeUtc:  timestamppb.New(pivotTime),
				Values:       yieldsPartial,
			},
			shouldErr: true,
		},
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
		"GB",
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
		"GB",
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
		"GB",
	)

	// Create a forecaster
	fc := createTestForecaster(t, "test_stream_create_forecasts_forecaster", "v1")

	yields := generateTestForecastValues(10, 30)

	yieldsPartial := make([]*pb.CreateForecastRequest_ForecastValue, 10)
	for i := range yieldsPartial {
		statFractions := map[string]float32{"p10": 0.1, "p90": 0.9}
		if i%2 == 0 {
			statFractions["p25"] = 0.25
		}

		yieldsPartial[i] = &pb.CreateForecastRequest_ForecastValue{
			HorizonMins:              uint32(i * 30),
			P50Fraction:              0.5,
			OtherStatisticsFractions: statFractions,
		}
	}

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
			name: "Shouldn't create forecast stream with partially populated statistic",
			setupStream: func(ctx context.Context) (pb.DataPlatformDataService_StreamCreateForecastsClient, error) {
				return dc.StreamCreateForecasts(ctx)
			},
			sendCount: 1,
			getReq: func(i int) *pb.CreateForecastRequest {
				return &pb.CreateForecastRequest{
					LocationUuid: siteResp.LocationUuid,
					Forecaster:   fc,
					EnergySource: pb.EnergySource_ENERGY_SOURCE_SOLAR,
					InitTimeUtc:  timestamppb.New(pivotTime.Add(time.Duration(i) * time.Hour)),
					Values:       yieldsPartial,
				}
			},
			shouldErr:          true,
			expectedErrCode:    codes.InvalidArgument,
			expectedUuidsCount: 0,
		},
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
				"GB",
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
			params, err := mapCreateForecast(tc.req, geomID, sourceID, forecasterID)
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

func TestCreateListMarkets(t *testing.T) {
	testcases := []struct {
		name          string
		createReq     *pb.CreateMarketRequest
		listReq       *pb.ListMarketsRequest
		expectedNames []string
	}{
		{
			name: "Should create market",
			createReq: &pb.CreateMarketRequest{
				Name:         "test_create_list_markets_market_1",
				CurrencyCode: "EUR",
				CountryCode:  "NL",
			},
			expectedNames: []string{"test_create_list_markets_market_1"},
		},
		{
			name: "Should create another market with a different currency",
			createReq: &pb.CreateMarketRequest{
				Name:         "test_create_list_markets_market_2",
				CurrencyCode: "GBP",
				CountryCode:  "GB",
			},
			expectedNames: []string{
				"test_create_list_markets_market_1",
				"test_create_list_markets_market_2",
			},
		},
		{
			name: "Shouldn't create duplicate market",
			createReq: &pb.CreateMarketRequest{
				Name:         "test_create_list_markets_market_1",
				CurrencyCode: "EUR",
				CountryCode:  "NL",
			},
		},
		{
			name: "Shouldn't create market with empty name",
			createReq: &pb.CreateMarketRequest{
				Name:         "",
				CurrencyCode: "EUR",
				CountryCode:  "NL",
			},
		},
		{
			name: "Shouldn't create market with invalid name",
			createReq: &pb.CreateMarketRequest{
				Name:         "invalid name with spaces and *special_characters*",
				CurrencyCode: "EUR",
				CountryCode:  "NL",
			},
		},
		{
			name: "Shouldn't create market with lowercase currency code",
			createReq: &pb.CreateMarketRequest{
				Name:         "test_create_list_markets_market_3",
				CurrencyCode: "eur",
				CountryCode:  "NL",
			},
		},
		{
			name: "Shouldn't create market without currency code",
			createReq: &pb.CreateMarketRequest{
				Name:        "test_create_list_markets_market_3",
				CountryCode: "NL",
			},
		},
		{
			name: "Shouldn't create market without country code",
			createReq: &pb.CreateMarketRequest{
				Name:         "test_create_list_markets_market_3",
				CurrencyCode: "EUR",
			},
		},
		{
			name: "Should list markets with name filter",
			listReq: &pb.ListMarketsRequest{
				MarketNamesFilter: []string{
					"test_create_list_markets_market_2",
				},
			},
			expectedNames: []string{"test_create_list_markets_market_2"},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.createReq != nil {
				_, err := dc.CreateMarket(t.Context(), tc.createReq)
				if strings.Contains(tc.name, "Shouldn't") {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
				}
			}

			if tc.listReq != nil {
				resp, err := dc.ListMarkets(t.Context(), tc.listReq)
				require.NoError(t, err)

				var marketNames []string
				for _, market := range resp.Markets {
					marketNames = append(marketNames, market.MarketName)
				}

				require.ElementsMatch(t, tc.expectedNames, marketNames)
			}
		})
	}
}

func TestCreateGetEnergyPrices(t *testing.T) {
	pivotTime := time.Date(2020, 7, 7, 0, 0, 0, 0, time.UTC)

	marketResp, err := dc.CreateMarket(t.Context(), &pb.CreateMarketRequest{
		Name:         "test_create_get_energy_prices_market",
		CurrencyCode: "EUR",
		CountryCode:  "NL",
	})
	require.NoError(t, err)

	otherMarketResp, err := dc.CreateMarket(t.Context(), &pb.CreateMarketRequest{
		Name:         "test_create_get_energy_prices_other_market",
		CurrencyCode: "EUR",
		CountryCode:  "NL",
	})
	require.NoError(t, err)

	hourlyValues := func(prices ...float64) []*pb.CreateEnergyPricesRequest_Value {
		values := make([]*pb.CreateEnergyPricesRequest_Value, len(prices))
		for i, p := range prices {
			values[i] = &pb.CreateEnergyPricesRequest_Value{
				TimestampUtc: timestamppb.New(pivotTime.Add(time.Duration(i) * time.Hour)),
				PricePerMwh:  p,
			}
		}

		return values
	}

	getPrices := func(t *testing.T, marketName string) *pb.GetEnergyPricesAsTimeseriesResponse {
		t.Helper()
		resp, err := dc.GetEnergyPricesAsTimeseries(
			t.Context(),
			&pb.GetEnergyPricesAsTimeseriesRequest{
				MarketName: marketName,
				TimeWindow: &pb.TimeWindow{
					StartTimestampUtc: timestamppb.New(pivotTime),
					EndTimestampUtc:   timestamppb.New(pivotTime.Add(time.Hour * 23)),
				},
			},
		)
		require.NoError(t, err)

		return resp
	}

	firstCreated := time.Date(2020, 7, 6, 12, 0, 0, 0, time.UTC)
	secondCreated := time.Date(2020, 7, 6, 18, 0, 0, 0, time.UTC)

	t.Run("Should return no prices before any are created", func(t *testing.T) {
		resp := getPrices(t, marketResp.MarketName)
		require.Empty(t, resp.Values)
		require.Equal(t, "EUR", resp.CurrencyCode)
	})

	t.Run("Should create prices, including negative ones", func(t *testing.T) {
		_, err := dc.CreateEnergyPrices(t.Context(), &pb.CreateEnergyPricesRequest{
			MarketName:          marketResp.MarketName,
			ResolutionMins:      60,
			CreatedTimestampUtc: timestamppb.New(firstCreated),
			Values:              hourlyValues(45.67, -0.01, -250, 0, 3999.99),
		})
		require.NoError(t, err)

		resp := getPrices(t, marketResp.MarketName)
		require.Len(t, resp.Values, 5)
		require.Equal(t, marketResp.MarketName, resp.MarketName)

		expected := []float64{45.67, -0.01, -250, 0, 3999.99}
		for i, v := range resp.Values {
			require.Equal(t, pivotTime.Add(time.Duration(i)*time.Hour), v.TimestampUtc.AsTime())
			require.Equal(t, uint32(60), v.ResolutionMins)
			require.InDelta(t, expected[i], v.PricePerMwh, 1e-9)
			require.Equal(t, firstCreated, v.CreatedTimestampUtc.AsTime())
		}
	})

	t.Run("Should not return prices of other markets", func(t *testing.T) {
		resp := getPrices(t, otherMarketResp.MarketName)
		require.Empty(t, resp.Values)
	})

	t.Run("Should replace changed prices and leave unchanged prices alone", func(t *testing.T) {
		// Republish with the second price changed, and a sixth added
		_, err := dc.CreateEnergyPrices(t.Context(), &pb.CreateEnergyPricesRequest{
			MarketName:          marketResp.MarketName,
			ResolutionMins:      60,
			CreatedTimestampUtc: timestamppb.New(secondCreated),
			Values:              hourlyValues(45.67, 12.34, -250, 0, 3999.99, 7),
		})
		require.NoError(t, err)

		resp := getPrices(t, marketResp.MarketName)
		require.Len(t, resp.Values, 6)

		expected := []float64{45.67, 12.34, -250, 0, 3999.99, 7}
		expectedCreated := []time.Time{
			firstCreated, secondCreated, firstCreated, firstCreated, firstCreated, secondCreated,
		}

		for i, v := range resp.Values {
			require.InDelta(t, expected[i], v.PricePerMwh, 1e-9)
			require.Equal(t, expectedCreated[i], v.CreatedTimestampUtc.AsTime())
		}
	})

	t.Run("Should replace the resolution of existing prices", func(t *testing.T) {
		_, err := dc.CreateEnergyPrices(t.Context(), &pb.CreateEnergyPricesRequest{
			MarketName:          marketResp.MarketName,
			ResolutionMins:      15,
			CreatedTimestampUtc: timestamppb.New(secondCreated),
			Values:              hourlyValues(45.67),
		})
		require.NoError(t, err)

		resp := getPrices(t, marketResp.MarketName)
		require.Equal(t, uint32(15), resp.Values[0].ResolutionMins)
		require.Equal(t, uint32(60), resp.Values[1].ResolutionMins)
	})

	t.Run("Should create prices in the future", func(t *testing.T) {
		tomorrow := time.Now().UTC().Truncate(24 * time.Hour).Add(24 * time.Hour)

		_, err := dc.CreateEnergyPrices(t.Context(), &pb.CreateEnergyPricesRequest{
			MarketName:     otherMarketResp.MarketName,
			ResolutionMins: 60,
			Values: []*pb.CreateEnergyPricesRequest_Value{
				{TimestampUtc: timestamppb.New(tomorrow), PricePerMwh: -5},
			},
		})
		require.NoError(t, err)

		resp, err := dc.GetEnergyPricesAsTimeseries(
			t.Context(),
			&pb.GetEnergyPricesAsTimeseriesRequest{
				MarketName: otherMarketResp.MarketName,
				TimeWindow: &pb.TimeWindow{
					StartTimestampUtc: timestamppb.New(tomorrow.Add(-time.Hour)),
					EndTimestampUtc:   timestamppb.New(tomorrow.Add(time.Hour)),
				},
			},
		)
		require.NoError(t, err)
		require.Len(t, resp.Values, 1)
		require.InDelta(t, -5.0, resp.Values[0].PricePerMwh, 1e-9)
	})

	invalidTestcases := []struct {
		name string
		req  *pb.CreateEnergyPricesRequest
		code codes.Code
	}{
		{
			name: "Shouldn't create prices for non-existent market",
			code: codes.NotFound,
			req: &pb.CreateEnergyPricesRequest{
				MarketName:     "non_existent_market",
				ResolutionMins: 60,
				Values:         hourlyValues(1),
			},
		},
		{
			name: "Shouldn't create prices with zero resolution",
			code: codes.InvalidArgument,
			req: &pb.CreateEnergyPricesRequest{
				MarketName: marketResp.MarketName,
				Values:     hourlyValues(1),
			},
		},
		{
			name: "Shouldn't create prices with a duplicated timestamp",
			code: codes.InvalidArgument,
			req: &pb.CreateEnergyPricesRequest{
				MarketName:     marketResp.MarketName,
				ResolutionMins: 60,
				Values: []*pb.CreateEnergyPricesRequest_Value{
					{TimestampUtc: timestamppb.New(pivotTime), PricePerMwh: 1},
					{TimestampUtc: timestamppb.New(pivotTime), PricePerMwh: 2},
				},
			},
		},
		{
			name: "Shouldn't create prices too far in the future",
			code: codes.InvalidArgument,
			req: &pb.CreateEnergyPricesRequest{
				MarketName:     marketResp.MarketName,
				ResolutionMins: 60,
				Values: []*pb.CreateEnergyPricesRequest_Value{
					{
						TimestampUtc: timestamppb.New(time.Now().UTC().AddDate(0, 0, 45)),
						PricePerMwh:  1,
					},
				},
			},
		},
		{
			name: "Shouldn't create prices outside of the allowed range",
			code: codes.InvalidArgument,
			req: &pb.CreateEnergyPricesRequest{
				MarketName:     marketResp.MarketName,
				ResolutionMins: 60,
				Values:         hourlyValues(-10001),
			},
		},
		{
			name: "Shouldn't create empty prices",
			code: codes.InvalidArgument,
			req: &pb.CreateEnergyPricesRequest{
				MarketName:     marketResp.MarketName,
				ResolutionMins: 60,
			},
		},
	}

	for _, tc := range invalidTestcases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := dc.CreateEnergyPrices(t.Context(), tc.req)
			require.Error(t, err)
			require.Equal(t, tc.code, status.Code(err), err.Error())
		})
	}

	t.Run("Shouldn't get prices for non-existent market", func(t *testing.T) {
		_, err := dc.GetEnergyPricesAsTimeseries(
			t.Context(),
			&pb.GetEnergyPricesAsTimeseriesRequest{
				MarketName: "non_existent_market",
				TimeWindow: &pb.TimeWindow{
					StartTimestampUtc: timestamppb.New(pivotTime),
					EndTimestampUtc:   timestamppb.New(pivotTime.Add(time.Hour)),
				},
			},
		)
		require.Error(t, err)
	})
}

func TestCompositeLocationCRUD(t *testing.T) {
	pivotTime := time.Date(2025, 3, 1, 12, 0, 0, 0, time.UTC)

	locA := createTestLocation(
		t,
		"test_cloc_crud_a",
		"POINT(-1.5 52.0)",
		100_000_000,
		pivotTime,
		&structpb.Struct{},
		"GB",
	)
	locB := createTestLocation(
		t,
		"test_cloc_crud_b",
		"POINT(-1.6 52.2)",
		200_000_000,
		pivotTime,
		&structpb.Struct{},
		"GB",
	)

	meta, err := structpb.NewStruct(map[string]any{"gsp_id": 99})
	require.NoError(t, err)

	compLoc := createTestLocation(
		t,
		"test_cloc_crud_main",
		"POINT(-1.55 52.1)",
		50_000_000,
		pivotTime,
		meta,
		"GB",
	)

	setResp, err := dc.SetLocationComponents(t.Context(), &pb.SetLocationComponentsRequest{
		LocationUuid: compLoc.LocationUuid,
		Components: []*pb.LocationComponent{
			{SourceLocationUuid: locA.LocationUuid, Weight: 0.5},
			{SourceLocationUuid: locB.LocationUuid, Weight: 1.0},
		},
	})
	require.NoError(t, err)
	require.Equal(t, compLoc.LocationUuid, setResp.LocationUuid)

	getResp, err := dc.GetLocation(t.Context(), &pb.GetLocationRequest{
		LocationUuid:      compLoc.LocationUuid,
		EnergySource:      pb.EnergySource_ENERGY_SOURCE_SOLAR,
		PivotTimestampUtc: timestamppb.New(pivotTime.Add(time.Hour)),
	})
	require.NoError(t, err)
	require.Equal(t, compLoc.LocationUuid, getResp.LocationUuid)
	require.Equal(t, "test_cloc_crud_main", getResp.LocationName)
	require.Equal(t, "GB", getResp.CountryCode)
	require.Equal(t, uint64(250_000_000), getResp.EffectiveCapacityWatts)
	require.InDelta(t, 52.1, getResp.Latlng.Latitude, 1e-4)
	require.InDelta(t, -1.55, getResp.Latlng.Longitude, 1e-4)
	require.Equal(t, float64(99), getResp.Metadata.Fields["gsp_id"].GetNumberValue())
	require.True(t, getResp.IsComposite)

	otherLoc := createTestLocation(
		t,
		"test_cloc_crud_other",
		"POINT(-1.7 52.3)",
		10_000_000,
		pivotTime,
		&structpb.Struct{},
		"GB",
	)

	invalidSetCases := []struct {
		name string
		req  *pb.SetLocationComponentsRequest
		code codes.Code
	}{
		{
			name: "Should reject unknown target location_uuid",
			code: codes.NotFound,
			req: &pb.SetLocationComponentsRequest{
				LocationUuid: uuid.New().String(),
				Components: []*pb.LocationComponent{
					{SourceLocationUuid: locA.LocationUuid, Weight: 1.0},
				},
			},
		},
		{
			name: "Should reject unknown source_location_uuid",
			code: codes.FailedPrecondition,
			req: &pb.SetLocationComponentsRequest{
				LocationUuid: otherLoc.LocationUuid,
				Components: []*pb.LocationComponent{
					{SourceLocationUuid: uuid.New().String(), Weight: 1.0},
				},
			},
		},
		{
			name: "Should reject recursive composite location as component",
			code: codes.FailedPrecondition,
			req: &pb.SetLocationComponentsRequest{
				LocationUuid: otherLoc.LocationUuid,
				Components: []*pb.LocationComponent{
					{SourceLocationUuid: compLoc.LocationUuid, Weight: 1.0},
				},
			},
		},
		{
			name: "Should reject duplicate source_location_uuids",
			code: codes.InvalidArgument,
			req: &pb.SetLocationComponentsRequest{
				LocationUuid: otherLoc.LocationUuid,
				Components: []*pb.LocationComponent{
					{SourceLocationUuid: locA.LocationUuid, Weight: 0.5},
					{SourceLocationUuid: locA.LocationUuid, Weight: 0.5},
				},
			},
		},
		{
			name: "Should reject non-positive weight",
			code: codes.InvalidArgument,
			req: &pb.SetLocationComponentsRequest{
				LocationUuid: otherLoc.LocationUuid,
				Components: []*pb.LocationComponent{
					{SourceLocationUuid: locA.LocationUuid, Weight: -0.5},
				},
			},
		},
	}

	for _, tc := range invalidSetCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := dc.SetLocationComponents(t.Context(), tc.req)
			require.Error(t, err)
			require.Equal(t, tc.code, status.Code(err), err.Error())
		})
	}

	t.Run(
		"Should update location components and reflect in GetLocation",
		func(t *testing.T) {
			_, err := dc.SetLocationComponents(t.Context(), &pb.SetLocationComponentsRequest{
				LocationUuid: compLoc.LocationUuid,
				Components: []*pb.LocationComponent{
					{SourceLocationUuid: locA.LocationUuid, Weight: 0.25},
				},
			})
			require.NoError(t, err)

			locSnap, err := dc.GetLocation(t.Context(), &pb.GetLocationRequest{
				LocationUuid:      compLoc.LocationUuid,
				EnergySource:      pb.EnergySource_ENERGY_SOURCE_SOLAR,
				IncludeGeometry:   true,
				PivotTimestampUtc: timestamppb.New(pivotTime.Add(time.Hour)),
			})
			require.NoError(t, err)
			require.Equal(t, uint64(25_000_000), locSnap.EffectiveCapacityWatts)
			require.True(t, locSnap.IsComposite)
			require.NotEmpty(t, locSnap.GeometryWkb)
		},
	)

	t.Run(
		"Should clear location components when given empty list and revert to non-composite",
		func(t *testing.T) {
			_, err := dc.SetLocationComponents(t.Context(), &pb.SetLocationComponentsRequest{
				LocationUuid: compLoc.LocationUuid,
				Components:   nil,
			})
			require.NoError(t, err)

			locSnap, err := dc.GetLocation(t.Context(), &pb.GetLocationRequest{
				LocationUuid:      compLoc.LocationUuid,
				EnergySource:      pb.EnergySource_ENERGY_SOURCE_SOLAR,
				PivotTimestampUtc: timestamppb.New(pivotTime.Add(time.Hour)),
			})
			require.NoError(t, err)
			require.Equal(t, uint64(50_000_000), locSnap.EffectiveCapacityWatts)
			require.False(t, locSnap.IsComposite)
		},
	)

	t.Run("Should delete location and remove from GetLocation", func(t *testing.T) {
		_, err := dc.DeleteLocation(t.Context(), &pb.DeleteLocationRequest{
			LocationUuid: compLoc.LocationUuid,
		})
		require.NoError(t, err)

		_, err = dc.GetLocation(t.Context(), &pb.GetLocationRequest{
			LocationUuid:      compLoc.LocationUuid,
			EnergySource:      pb.EnergySource_ENERGY_SOURCE_SOLAR,
			PivotTimestampUtc: timestamppb.New(pivotTime.Add(time.Hour)),
		})
		require.Error(t, err)
		require.Equal(t, codes.NotFound, status.Code(err))
	})
}

func TestCompositeLocationDiscoveryAndTemporal(t *testing.T) {
	t0 := time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC)
	t1 := t0.Add(2 * time.Hour)
	t2 := t0.Add(4 * time.Hour)

	outerNation, err := dc.CreateLocation(t.Context(), &pb.CreateLocationRequest{
		LocationName:           "test_cloc_outer_nation",
		EnergySource:           pb.EnergySource_ENERGY_SOURCE_SOLAR,
		GeometryWkt:            "POLYGON((-5.0 50.0, 2.0 50.0, 2.0 56.0, -5.0 56.0, -5.0 50.0))",
		LocationType:           pb.LocationType_LOCATION_TYPE_NATION,
		EffectiveCapacityWatts: 1_000_000_000,
		ValidFromUtc:           timestamppb.New(t0),
		CountryCode:            ptr("GB"),
	})
	require.NoError(t, err)

	locA := createTestLocation(
		t,
		"test_cloc_disc_a",
		"POINT(-1.0 51.5)",
		100_000_000,
		t0,
		&structpb.Struct{},
		"GB",
	)
	locB := createTestLocation(
		t,
		"test_cloc_disc_b",
		"POINT(-2.0 52.5)",
		200_000_000,
		t0,
		&structpb.Struct{},
		"GB",
	)

	// Add WIND only to locA initially (locB does not have WIND yet)
	_, err = dc.CreateLocationEnergySource(t.Context(), &pb.CreateLocationEnergySourceRequest{
		LocationUuid:           locA.LocationUuid,
		EnergySource:           pb.EnergySource_ENERGY_SOURCE_WIND,
		EffectiveCapacityWatts: 50_000_000,
		ValidFromUtc:           timestamppb.New(t0),
	})
	require.NoError(t, err)

	// Update locA SOLAR capacity at t1 (100 MW -> 150 MW) and locB SOLAR capacity at t2 (200 MW -> 300 MW)
	newCapA := uint64(150_000_000)
	_, err = dc.UpdateLocation(t.Context(), &pb.UpdateLocationRequest{
		LocationUuid:              locA.LocationUuid,
		EnergySource:              pb.EnergySource_ENERGY_SOURCE_SOLAR,
		NewEffectiveCapacityWatts: &newCapA,
		ValidFromUtc:              timestamppb.New(t1),
	})
	require.NoError(t, err)

	newCapB := uint64(300_000_000)
	_, err = dc.UpdateLocation(t.Context(), &pb.UpdateLocationRequest{
		LocationUuid:              locB.LocationUuid,
		EnergySource:              pb.EnergySource_ENERGY_SOURCE_SOLAR,
		NewEffectiveCapacityWatts: &newCapB,
		ValidFromUtc:              timestamppb.New(t2),
	})
	require.NoError(t, err)

	vloc, err := dc.CreateLocation(t.Context(), &pb.CreateLocationRequest{
		LocationName:           "test_cloc_disc_composite",
		EnergySource:           pb.EnergySource_ENERGY_SOURCE_SOLAR,
		GeometryWkt:            "POLYGON((-1.6 51.9, -1.4 51.9, -1.4 52.1, -1.6 52.1, -1.6 51.9))",
		LocationType:           pb.LocationType_LOCATION_TYPE_GSP,
		EffectiveCapacityWatts: 1,
		ValidFromUtc:           timestamppb.New(t0),
		CountryCode:            ptr("GB"),
	})
	require.NoError(t, err)

	_, err = dc.SetLocationComponents(t.Context(), &pb.SetLocationComponentsRequest{
		LocationUuid: vloc.LocationUuid,
		Components: []*pb.LocationComponent{
			{SourceLocationUuid: locA.LocationUuid, Weight: 1.0},
			{SourceLocationUuid: locB.LocationUuid, Weight: 0.5},
		},
	})
	require.NoError(t, err)

	t.Run("Strict all-or-nothing multi-energy-source inheritance", func(t *testing.T) {
		// SOLAR exists on both A and B -> composite location exists for SOLAR
		solarLoc, err := dc.GetLocation(t.Context(), &pb.GetLocationRequest{
			LocationUuid:      vloc.LocationUuid,
			EnergySource:      pb.EnergySource_ENERGY_SOURCE_SOLAR,
			PivotTimestampUtc: timestamppb.New(t0.Add(30 * time.Minute)),
		})
		require.NoError(t, err)
		require.Equal(t, uint64(200_000_000), solarLoc.EffectiveCapacityWatts)
		require.True(t, solarLoc.IsComposite)

		// WIND only exists on A -> composite location must NOT exist for WIND yet
		_, err = dc.GetLocation(t.Context(), &pb.GetLocationRequest{
			LocationUuid:      vloc.LocationUuid,
			EnergySource:      pb.EnergySource_ENERGY_SOURCE_WIND,
			PivotTimestampUtc: timestamppb.New(t0.Add(30 * time.Minute)),
		})
		require.Error(t, err)
		require.Equal(t, codes.NotFound, status.Code(err))

		// Now add WIND to locB -> composite location now appears for WIND too
		_, err = dc.CreateLocationEnergySource(t.Context(), &pb.CreateLocationEnergySourceRequest{
			LocationUuid:           locB.LocationUuid,
			EnergySource:           pb.EnergySource_ENERGY_SOURCE_WIND,
			EffectiveCapacityWatts: 80_000_000,
			ValidFromUtc:           timestamppb.New(t0),
		})
		require.NoError(t, err)

		windLoc, err := dc.GetLocation(t.Context(), &pb.GetLocationRequest{
			LocationUuid:      vloc.LocationUuid,
			EnergySource:      pb.EnergySource_ENERGY_SOURCE_WIND,
			PivotTimestampUtc: timestamppb.New(t0.Add(30 * time.Minute)),
		})
		require.NoError(t, err)
		require.Equal(t, uint64(90_000_000), windLoc.EffectiveCapacityWatts)
	})

	t.Run(
		"Non-overlapping temporal capacity slicing and GetLocationAsTimeseries",
		func(t *testing.T) {
			histResp, err := dc.GetLocationAsTimeseries(
				t.Context(),
				&pb.GetLocationAsTimeseriesRequest{
					LocationUuid: vloc.LocationUuid,
					EnergySource: pb.EnergySource_ENERGY_SOURCE_SOLAR,
					TimeWindow: &pb.TimeWindow{
						StartTimestampUtc: timestamppb.New(t0),
						EndTimestampUtc:   timestamppb.New(t2.Add(time.Hour)),
					},
				},
			)
			require.NoError(t, err)
			require.Len(t, histResp.Values, 3)
			// At t0: 1.0 * 100 MW + 0.5 * 200 MW = 200 MW
			require.Equal(t, uint64(200_000_000), histResp.Values[0].EffectiveCapacityWatts)
			require.Equal(t, t0, histResp.Values[0].TimestampUtc.AsTime())
			// At t1: 1.0 * 150 MW + 0.5 * 200 MW = 250 MW
			require.Equal(t, uint64(250_000_000), histResp.Values[1].EffectiveCapacityWatts)
			require.Equal(t, t1, histResp.Values[1].TimestampUtc.AsTime())
			// At t2: 1.0 * 150 MW + 0.5 * 300 MW = 300 MW
			require.Equal(t, uint64(300_000_000), histResp.Values[2].EffectiveCapacityWatts)
			require.Equal(t, t2, histResp.Values[2].TimestampUtc.AsTime())
		},
	)

	t.Run("Appears in ListLocations with spatial and attribute filters", func(t *testing.T) {
		gspType := pb.LocationType_LOCATION_TYPE_GSP
		solar := pb.EnergySource_ENERGY_SOURCE_SOLAR

		// Filter by enclosing_location_uuid_filter (outerNation encloses vloc)
		withinResp, err := dc.ListLocations(t.Context(), &pb.ListLocationsRequest{
			EnclosingLocationUuidFilter: &outerNation.LocationUuid,
			LocationUuidsFilter:         []string{vloc.LocationUuid, locA.LocationUuid},
			EnergySourceFilter:          &solar,
		})
		require.NoError(t, err)
		require.Len(t, withinResp.Locations, 2)

		var foundComp, foundPhys bool
		for _, loc := range withinResp.Locations {
			if loc.LocationUuid == vloc.LocationUuid {
				foundComp = true

				require.True(t, loc.IsComposite)
				require.Equal(t, uint64(300_000_000), loc.EffectiveCapacityWatts)
			}

			if loc.LocationUuid == locA.LocationUuid {
				foundPhys = true

				require.False(t, loc.IsComposite)
			}
		}

		require.True(t, foundComp)
		require.True(t, foundPhys)

		// Filter by enclosed_location_uuid_filter with composite location as inner location
		withoutResp, err := dc.ListLocations(t.Context(), &pb.ListLocationsRequest{
			EnclosedLocationUuidFilter: &vloc.LocationUuid,
			LocationUuidsFilter:        []string{outerNation.LocationUuid},
			EnergySourceFilter:         &solar,
		})
		require.NoError(t, err)
		require.Len(t, withoutResp.Locations, 1)
		require.Equal(t, outerNation.LocationUuid, withoutResp.Locations[0].LocationUuid)

		// Filter by location_names_filter and location_type_filter
		byNameResp, err := dc.ListLocations(t.Context(), &pb.ListLocationsRequest{
			LocationNamesFilter: []string{vloc.LocationName},
			LocationTypeFilter:  &gspType,
			EnergySourceFilter:  &solar,
		})
		require.NoError(t, err)
		require.Len(t, byNameResp.Locations, 1)
		require.Equal(t, vloc.LocationUuid, byNameResp.Locations[0].LocationUuid)
		require.True(t, byNameResp.Locations[0].IsComposite)
	})
}

func TestCompositeLocationPVLiveTransitionLifecycle(t *testing.T) {
	baseTime := time.Now().UTC().Truncate(time.Hour).Add(-6 * time.Hour)
	t0 := baseTime
	t1 := baseTime.Add(30 * time.Minute)
	t2 := baseTime.Add(60 * time.Minute)
	t3 := baseTime.Add(90 * time.Minute)

	fc := createTestForecaster(t, "test_cloc_pvlive_fc", "v1")
	obsResp, err := dc.CreateObserver(t.Context(), &pb.CreateObserverRequest{
		Name: "test_cloc_pvlive_obs",
	})
	require.NoError(t, err)

	// =========================================================================
	// Setup all 3 YAML scenarios at Step 0 (Pre-transition physical GSPs):
	//   Scenario A (1-to-1 alias): GSP 4 (80 MW) -> GSP 350 (80 MW, weight 1.0)
	//   Scenario B (2-to-1 merge): GSP 56 (27 MW) & GSP 122 (73 MW) -> GSP 351 (100 MW, weights 0.27 & 0.73)
	//   Scenario C (1-to-2 split): GSP 41 (250 MW) -> GSP 343 (100 MW, 1.0) + GSP 344 (150 MW, 1.0)
	// =========================================================================
	gsp4 := createTestLocation(
		t,
		"test_cloc_gsp_4",
		"POLYGON((-3.0 52.0, -2.9 52.0, -2.9 52.1, -3.0 52.1, -3.0 52.0))",
		80_000_000,
		t0.Add(-24*time.Hour),
		&structpb.Struct{},
		"GB",
	)
	gsp56 := createTestLocation(
		t,
		"test_cloc_carr_1",
		"POLYGON((-2.0 53.0, -1.9 53.0, -1.9 53.1, -2.0 53.1, -2.0 53.0))",
		27_000_000,
		t0.Add(-24*time.Hour),
		&structpb.Struct{},
		"GB",
	)
	gsp122 := createTestLocation(
		t,
		"test_cloc_fidf_1",
		"POLYGON((-1.9 53.0, -1.8 53.0, -1.8 53.1, -1.9 53.1, -1.9 53.0))",
		73_000_000,
		t0.Add(-24*time.Hour),
		&structpb.Struct{},
		"GB",
	)
	gsp41 := createTestLocation(
		t,
		"test_cloc_brle_1_flee_1",
		"POLYGON((-1.5 51.0, -1.3 51.0, -1.3 51.2, -1.5 51.2, -1.5 51.0))",
		250_000_000,
		t0.Add(-24*time.Hour),
		&structpb.Struct{},
		"GB",
	)

	// Step 0: Seed direct observations (50%) and forecasts (55%) at t0 on all 4 legacy GSPs
	for _, legacy := range []struct {
		uuid string
		cap  uint64
	}{
		{gsp4.LocationUuid, 80_000_000},
		{gsp56.LocationUuid, 27_000_000},
		{gsp122.LocationUuid, 73_000_000},
		{gsp41.LocationUuid, 250_000_000},
	} {
		_, err = dc.CreateObservations(t.Context(), &pb.CreateObservationsRequest{
			LocationUuid:        legacy.uuid,
			EnergySource:        pb.EnergySource_ENERGY_SOURCE_SOLAR,
			ObserverName:        obsResp.ObserverName,
			CreatedTimestampUtc: timestamppb.New(t0),
			Values: []*pb.CreateObservationsRequest_Value{
				{TimestampUtc: timestamppb.New(t0), ValueWatts: legacy.cap / 2}, // 50%
			},
		})
		require.NoError(t, err)

		_, err = dc.CreateForecast(t.Context(), &pb.CreateForecastRequest{
			Forecaster:   fc,
			LocationUuid: legacy.uuid,
			EnergySource: pb.EnergySource_ENERGY_SOURCE_SOLAR,
			InitTimeUtc:  timestamppb.New(t0),
			Values: []*pb.CreateForecastRequest_ForecastValue{
				{HorizonMins: 0, P50Fraction: 0.55},
				{HorizonMins: 30, P50Fraction: 0.55},
			},
		})
		require.NoError(t, err)
	}

	// =========================================================================
	// Step 1: PVLive introduces new GSPs (350, 351, 343, 344) at t1.
	// Legacy GSPs (4, 56, 122, 41) get component mappings via SetLocationComponents.
	// PVLive writes observations at t1 ONLY to the new GSPs.
	// Old ML model still writes forecasts at t1 (predicting t1=60% and t2=65%) ONLY to legacy GSPs.
	// =========================================================================
	gsp350 := createTestLocation(
		t,
		"test_cloc_gsp_350",
		"POINT(-2.95 52.05)",
		80_000_000,
		t1,
		&structpb.Struct{},
		"GB",
	)
	gsp351 := createTestLocation(
		t,
		"test_cloc_carr_1_fidf_1",
		"POINT(-1.95 53.05)",
		100_000_000,
		t1,
		&structpb.Struct{},
		"GB",
	)
	gsp343 := createTestLocation(
		t,
		"test_cloc_brle_1",
		"POINT(-1.45 51.05)",
		100_000_000,
		t1,
		&structpb.Struct{},
		"GB",
	)
	gsp344 := createTestLocation(
		t,
		"test_cloc_flee_1",
		"POINT(-1.35 51.15)",
		150_000_000,
		t1,
		&structpb.Struct{},
		"GB",
	)

	// Link Scenario A: 4 -> 1.0 * 350
	_, err = dc.SetLocationComponents(t.Context(), &pb.SetLocationComponentsRequest{
		LocationUuid: gsp4.LocationUuid,
		Components: []*pb.LocationComponent{
			{SourceLocationUuid: gsp350.LocationUuid, Weight: 1.0},
		},
	})
	require.NoError(t, err)

	// Link Scenario B: 56 -> 0.27 * 351, 122 -> 0.73 * 351
	_, err = dc.SetLocationComponents(t.Context(), &pb.SetLocationComponentsRequest{
		LocationUuid: gsp56.LocationUuid,
		Components: []*pb.LocationComponent{
			{SourceLocationUuid: gsp351.LocationUuid, Weight: 0.27},
		},
	})
	require.NoError(t, err)

	_, err = dc.SetLocationComponents(t.Context(), &pb.SetLocationComponentsRequest{
		LocationUuid: gsp122.LocationUuid,
		Components: []*pb.LocationComponent{
			{SourceLocationUuid: gsp351.LocationUuid, Weight: 0.73},
		},
	})
	require.NoError(t, err)

	// Link Scenario C: 41 -> 1.0 * 343 + 1.0 * 344
	_, err = dc.SetLocationComponents(t.Context(), &pb.SetLocationComponentsRequest{
		LocationUuid: gsp41.LocationUuid,
		Components: []*pb.LocationComponent{
			{SourceLocationUuid: gsp343.LocationUuid, Weight: 1.0},
			{SourceLocationUuid: gsp344.LocationUuid, Weight: 1.0},
		},
	})
	require.NoError(t, err)

	// PVLive writes observations at t1 ONLY to new GSPs:
	// - 350: 32 MW / 80 MW = 40%
	// - 351: 40 MW / 100 MW = 40% (so both 56 and 122 should read 40% with capacities 27 MW and 73 MW)
	// - 343: 50 MW (50% of 100 MW) and 344: 25 MW (16.67% of 150 MW) -> 41 should read (50+25)/250 = 30%!
	for _, obs := range []struct {
		uuid  string
		watts uint64
	}{
		{gsp350.LocationUuid, 32_000_000},
		{gsp351.LocationUuid, 40_000_000},
		{gsp343.LocationUuid, 50_000_000},
		{gsp344.LocationUuid, 25_000_000},
	} {
		_, err = dc.CreateObservations(t.Context(), &pb.CreateObservationsRequest{
			LocationUuid:        obs.uuid,
			EnergySource:        pb.EnergySource_ENERGY_SOURCE_SOLAR,
			ObserverName:        obsResp.ObserverName,
			CreatedTimestampUtc: timestamppb.New(t1),
			Values: []*pb.CreateObservationsRequest_Value{
				{TimestampUtc: timestamppb.New(t1), ValueWatts: obs.watts},
			},
		})
		require.NoError(t, err)
	}

	// Old ML model still writes forecast at t1 directly to legacy GSPs (60% at t1, stale 65% tail at t2)
	for _, legacyUuid := range []string{
		gsp4.LocationUuid,
		gsp56.LocationUuid,
		gsp122.LocationUuid,
		gsp41.LocationUuid,
	} {
		_, err = dc.CreateForecast(t.Context(), &pb.CreateForecastRequest{
			Forecaster:   fc,
			LocationUuid: legacyUuid,
			EnergySource: pb.EnergySource_ENERGY_SOURCE_SOLAR,
			InitTimeUtc:  timestamppb.New(t1),
			Values: []*pb.CreateForecastRequest_ForecastValue{
				{HorizonMins: 0, P50Fraction: 0.60},
				{HorizonMins: 30, P50Fraction: 0.65},
			},
		})
		require.NoError(t, err)
	}

	t.Run(
		"Step 1: all 3 YAML scenarios serve direct forecasts and composite observations",
		func(t *testing.T) {
			cases := []struct {
				name        string
				uuid        string
				expectedCap uint64
				expectedObs float64
			}{
				{"Scenario A (1-to-1 alias GSP 4)", gsp4.LocationUuid, 80_000_000, 0.40},
				{"Scenario B1 (split 0.27 GSP 56)", gsp56.LocationUuid, 27_000_000, 0.40},
				{"Scenario B2 (split 0.73 GSP 122)", gsp122.LocationUuid, 73_000_000, 0.40},
				{"Scenario C (sum 343+344 GSP 41)", gsp41.LocationUuid, 250_000_000, 0.30},
			}

			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					locSnap, err := dc.GetLocation(t.Context(), &pb.GetLocationRequest{
						LocationUuid:      tc.uuid,
						EnergySource:      pb.EnergySource_ENERGY_SOURCE_SOLAR,
						IncludeGeometry:   true,
						PivotTimestampUtc: timestamppb.New(t1),
					})
					require.NoError(t, err)
					require.True(t, locSnap.IsComposite)
					require.NotEmpty(t, locSnap.GeometryWkb)
					require.Equal(t, tc.expectedCap, locSnap.EffectiveCapacityWatts)

					fcResp, err := dc.GetForecastAsTimeseries(
						t.Context(),
						&pb.GetForecastAsTimeseriesRequest{
							LocationUuid: tc.uuid,
							EnergySource: pb.EnergySource_ENERGY_SOURCE_SOLAR,
							Forecaster:   fc,
							TimeWindow: &pb.TimeWindow{
								StartTimestampUtc: timestamppb.New(t0),
								EndTimestampUtc:   timestamppb.New(t1),
							},
						},
					)
					require.NoError(t, err)
					require.Len(t, fcResp.Values, 2)
					require.InDelta(t, 0.55, fcResp.Values[0].P50ValueFraction, 1e-4)
					require.InDelta(t, 0.60, fcResp.Values[1].P50ValueFraction, 1e-4)

					obsRespTs, err := dc.GetObservationsAsTimeseries(
						t.Context(),
						&pb.GetObservationsAsTimeseriesRequest{
							LocationUuid: tc.uuid,
							EnergySource: pb.EnergySource_ENERGY_SOURCE_SOLAR,
							ObserverName: obsResp.ObserverName,
							TimeWindow: &pb.TimeWindow{
								StartTimestampUtc: timestamppb.New(t0),
								EndTimestampUtc:   timestamppb.New(t1),
							},
						},
					)
					require.NoError(t, err)
					require.Len(t, obsRespTs.Values, 2)
					require.InDelta(t, 0.50, obsRespTs.Values[0].ValueFraction, 1e-4)
					require.InDelta(t, tc.expectedObs, obsRespTs.Values[1].ValueFraction, 1e-4)
				})
			}

			latestObs, err := dc.GetLatestObservations(
				t.Context(),
				&pb.GetLatestObservationsRequest{
					LocationUuids: []string{
						gsp4.LocationUuid,
						gsp56.LocationUuid,
						gsp122.LocationUuid,
						gsp41.LocationUuid,
					},
					EnergySource: pb.EnergySource_ENERGY_SOURCE_SOLAR,
					ObserverName: obsResp.ObserverName,
				},
			)
			require.NoError(t, err)
			require.Len(t, latestObs.Observations, 4)

			for _, o := range latestObs.Observations {
				require.Equal(t, t1, o.TimestampUtc.AsTime())
			}
		},
	)

	// =========================================================================
	// Step 2: New ML model deployed at t2 -> writes forecasts to new GSPs (350, 351, 343, 344)
	// and stops writing to legacy GSPs (4, 56, 122, 41).
	// Notice that at t2, legacy GSPs still have the 30-min horizon tail (65%) from init_time=t1,
	// which must be beaten by the fresh 0-min horizon forecast from init_time=t2 on the new GSPs!
	// =========================================================================
	for _, newFc := range []struct {
		uuid  string
		p50T2 float32
		p50T3 float32
	}{
		{gsp350.LocationUuid, 0.70, 0.75},
		{gsp351.LocationUuid, 0.80, 0.85},
		{gsp343.LocationUuid, 0.50, 0.60}, // 50 MW at t2, 60 MW at t3 (of 100 MW)
		{gsp344.LocationUuid, 0.50, 0.60}, // 75 MW at t2, 90 MW at t3 (of 150 MW) -> combined 41 = 50% at t2, 60% at t3
	} {
		_, err = dc.CreateForecast(t.Context(), &pb.CreateForecastRequest{
			Forecaster:   fc,
			LocationUuid: newFc.uuid,
			EnergySource: pb.EnergySource_ENERGY_SOURCE_SOLAR,
			InitTimeUtc:  timestamppb.New(t2),
			Values: []*pb.CreateForecastRequest_ForecastValue{
				{HorizonMins: 0, P50Fraction: newFc.p50T2},
				{HorizonMins: 30, P50Fraction: newFc.p50T3},
			},
		})
		require.NoError(t, err)
	}

	t.Run(
		"Step 2: all 3 YAML scenarios switch cleanly at t2 from direct forecasts to composite forecasts",
		func(t *testing.T) {
			cases := []struct {
				name  string
				uuid  string
				expT2 float64
				expT3 float64
			}{
				{"Scenario A (1-to-1 alias GSP 4)", gsp4.LocationUuid, 0.70, 0.75},
				{"Scenario B1 (split 0.27 GSP 56)", gsp56.LocationUuid, 0.80, 0.85},
				{"Scenario B2 (split 0.73 GSP 122)", gsp122.LocationUuid, 0.80, 0.85},
				{"Scenario C (sum 343+344 GSP 41)", gsp41.LocationUuid, 0.50, 0.60},
			}

			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					fcResp, err := dc.GetForecastAsTimeseries(
						t.Context(),
						&pb.GetForecastAsTimeseriesRequest{
							LocationUuid: tc.uuid,
							EnergySource: pb.EnergySource_ENERGY_SOURCE_SOLAR,
							Forecaster:   fc,
							TimeWindow: &pb.TimeWindow{
								StartTimestampUtc: timestamppb.New(t0),
								EndTimestampUtc:   timestamppb.New(t3),
							},
						},
					)
					require.NoError(t, err)
					require.Len(t, fcResp.Values, 4)
					// t0 (0.55) and t1 (0.60) from direct legacy GSP;
					// t2 and t3 from fresh init_time=t2 composite (beating stale init_time=t1 65% tail!)
					require.InDelta(t, 0.55, fcResp.Values[0].P50ValueFraction, 1e-4)
					require.InDelta(t, 0.60, fcResp.Values[1].P50ValueFraction, 1e-4)
					require.InDelta(t, tc.expT2, fcResp.Values[2].P50ValueFraction, 1e-4)
					require.InDelta(t, tc.expT3, fcResp.Values[3].P50ValueFraction, 1e-4)
				})
			}

			fcSnap, err := dc.GetForecastAtTimestamp(t.Context(), &pb.GetForecastAtTimestampRequest{
				LocationUuids: []string{
					gsp4.LocationUuid,
					gsp56.LocationUuid,
					gsp122.LocationUuid,
					gsp41.LocationUuid,
					gsp351.LocationUuid,
				},
				EnergySource: pb.EnergySource_ENERGY_SOURCE_SOLAR,
				Forecaster:   fc,
				TimestampUtc: timestamppb.New(t2),
			})
			require.NoError(t, err)
			require.Len(t, fcSnap.Values, 5)
		},
	)

	t.Run("Step 4: retired legacy GSPs can be deleted while new GSPs remain", func(t *testing.T) {
		for _, legacyUuid := range []string{
			gsp4.LocationUuid,
			gsp56.LocationUuid,
			gsp122.LocationUuid,
			gsp41.LocationUuid,
		} {
			_, err := dc.DeleteLocation(t.Context(), &pb.DeleteLocationRequest{
				LocationUuid: legacyUuid,
			})
			require.NoError(t, err)

			_, err = dc.GetLocation(t.Context(), &pb.GetLocationRequest{
				LocationUuid:      legacyUuid,
				EnergySource:      pb.EnergySource_ENERGY_SOURCE_SOLAR,
				PivotTimestampUtc: timestamppb.New(t2),
			})
			require.Error(t, err)
			require.Equal(t, codes.NotFound, status.Code(err))
		}

		loc351, err := dc.GetLocation(t.Context(), &pb.GetLocationRequest{
			LocationUuid:      gsp351.LocationUuid,
			EnergySource:      pb.EnergySource_ENERGY_SOURCE_SOLAR,
			PivotTimestampUtc: timestamppb.New(t2),
		})
		require.NoError(t, err)
		require.Equal(t, uint64(100_000_000), loc351.EffectiveCapacityWatts)
	})
}

func TestCompositeLocationForecastAndObservationMath(t *testing.T) {
	baseTime := time.Now().UTC().Truncate(time.Hour).Add(-4 * time.Hour)
	t0 := baseTime
	t1 := baseTime.Add(30 * time.Minute)
	t2 := baseTime.Add(60 * time.Minute)

	// locA: 100 MW, locB: 300 MW, locZero: 0 MW
	locA := createTestLocation(
		t,
		"test_cloc_math_a",
		"POINT(-1.1 51.1)",
		100_000_000,
		baseTime.Add(-24*time.Hour),
		&structpb.Struct{},
		"GB",
	)
	locB := createTestLocation(
		t,
		"test_cloc_math_b",
		"POINT(-1.2 51.2)",
		300_000_000,
		baseTime.Add(-24*time.Hour),
		&structpb.Struct{},
		"GB",
	)
	locZero := createTestLocation(
		t,
		"test_cloc_math_zero",
		"POINT(-1.3 51.3)",
		1000,
		baseTime.Add(-48*time.Hour),
		&structpb.Struct{},
		"GB",
	)
	zeroCap := uint64(0)
	_, err := dc.UpdateLocation(t.Context(), &pb.UpdateLocationRequest{
		LocationUuid:              locZero.LocationUuid,
		EnergySource:              pb.EnergySource_ENERGY_SOURCE_SOLAR,
		NewEffectiveCapacityWatts: &zeroCap,
		ValidFromUtc:              timestamppb.New(baseTime.Add(-24 * time.Hour)),
	})
	require.NoError(t, err)

	// 1. Single-source fractional split (weight = 0.27)
	vlocSplit := createTestLocation(
		t,
		"test_cloc_math_split",
		"POINT(-1.15 51.15)",
		1,
		baseTime.Add(-24*time.Hour),
		&structpb.Struct{},
		"GB",
	)
	_, err = dc.SetLocationComponents(t.Context(), &pb.SetLocationComponentsRequest{
		LocationUuid: vlocSplit.LocationUuid,
		Components: []*pb.LocationComponent{
			{SourceLocationUuid: locA.LocationUuid, Weight: 0.27},
		},
	})
	require.NoError(t, err)

	// 2. Multi-geometry weighted sum (A + B with weights [1.0, 1.0])
	vlocSum := createTestLocation(
		t,
		"test_cloc_math_sum",
		"POINT(-1.25 51.25)",
		1,
		baseTime.Add(-24*time.Hour),
		&structpb.Struct{},
		"GB",
	)
	_, err = dc.SetLocationComponents(t.Context(), &pb.SetLocationComponentsRequest{
		LocationUuid: vlocSum.LocationUuid,
		Components: []*pb.LocationComponent{
			{SourceLocationUuid: locA.LocationUuid, Weight: 1.0},
			{SourceLocationUuid: locB.LocationUuid, Weight: 1.0},
		},
	})
	require.NoError(t, err)

	// 3. Zero-capacity composite geometry
	vlocZero := createTestLocation(
		t,
		"test_cloc_math_zero_comp",
		"POINT(-1.35 51.35)",
		1,
		baseTime.Add(-24*time.Hour),
		&structpb.Struct{},
		"GB",
	)
	_, err = dc.SetLocationComponents(t.Context(), &pb.SetLocationComponentsRequest{
		LocationUuid: vlocZero.LocationUuid,
		Components: []*pb.LocationComponent{
			{SourceLocationUuid: locZero.LocationUuid, Weight: 1.0},
		},
	})
	require.NoError(t, err)

	fc := createTestForecaster(t, "test_cloc_math_fc", "v1")
	obsResp, err := dc.CreateObserver(t.Context(), &pb.CreateObserverRequest{
		Name: "test_cloc_math_obs",
	})
	require.NoError(t, err)

	// Seed forecasts:
	// locA has horizons [0, 30, 60] mins (t0, t1, t2) at 50% (50 MW), with p10=0.40 and p90=0.60
	_, err = dc.CreateForecast(t.Context(), &pb.CreateForecastRequest{
		Forecaster:   fc,
		LocationUuid: locA.LocationUuid,
		EnergySource: pb.EnergySource_ENERGY_SOURCE_SOLAR,
		InitTimeUtc:  timestamppb.New(t0),
		Values: []*pb.CreateForecastRequest_ForecastValue{
			{
				HorizonMins:              0,
				P50Fraction:              0.50,
				OtherStatisticsFractions: map[string]float32{"p10": 0.40, "p90": 0.60},
			},
			{
				HorizonMins:              30,
				P50Fraction:              0.50,
				OtherStatisticsFractions: map[string]float32{"p10": 0.40, "p90": 0.60},
			},
			{
				HorizonMins:              60,
				P50Fraction:              0.50,
				OtherStatisticsFractions: map[string]float32{"p10": 0.40, "p90": 0.60},
			},
		},
	})
	require.NoError(t, err)

	// locB ONLY has horizons [0, 30] mins (t0, t1) at 10% (30 MW), with p10=0.05 and NO p90!
	// (t2 is missing on locB!)
	_, err = dc.CreateForecast(t.Context(), &pb.CreateForecastRequest{
		Forecaster:   fc,
		LocationUuid: locB.LocationUuid,
		EnergySource: pb.EnergySource_ENERGY_SOURCE_SOLAR,
		InitTimeUtc:  timestamppb.New(t0),
		Values: []*pb.CreateForecastRequest_ForecastValue{
			{
				HorizonMins:              0,
				P50Fraction:              0.10,
				OtherStatisticsFractions: map[string]float32{"p10": 0.05},
			},
			{
				HorizonMins:              30,
				P50Fraction:              0.10,
				OtherStatisticsFractions: map[string]float32{"p10": 0.05},
			},
		},
	})
	require.NoError(t, err)

	// locZero has horizons [0, 30] mins at 0%
	_, err = dc.CreateForecast(t.Context(), &pb.CreateForecastRequest{
		Forecaster:   fc,
		LocationUuid: locZero.LocationUuid,
		EnergySource: pb.EnergySource_ENERGY_SOURCE_SOLAR,
		InitTimeUtc:  timestamppb.New(t0),
		Values: []*pb.CreateForecastRequest_ForecastValue{
			{HorizonMins: 0, P50Fraction: 0.0},
			{HorizonMins: 30, P50Fraction: 0.0},
		},
	})
	require.NoError(t, err)

	// Seed observations:
	// locA has observations at [t0, t1, t2] at 50 MW (50%)
	_, err = dc.CreateObservations(t.Context(), &pb.CreateObservationsRequest{
		LocationUuid:        locA.LocationUuid,
		EnergySource:        pb.EnergySource_ENERGY_SOURCE_SOLAR,
		ObserverName:        obsResp.ObserverName,
		CreatedTimestampUtc: timestamppb.New(t2),
		Values: []*pb.CreateObservationsRequest_Value{
			{TimestampUtc: timestamppb.New(t0), ValueWatts: 50_000_000},
			{TimestampUtc: timestamppb.New(t1), ValueWatts: 50_000_000},
			{TimestampUtc: timestamppb.New(t2), ValueWatts: 50_000_000},
		},
	})
	require.NoError(t, err)

	// locB only has observations at [t0, t1] at 30 MW (10%); t2 is missing!
	_, err = dc.CreateObservations(t.Context(), &pb.CreateObservationsRequest{
		LocationUuid:        locB.LocationUuid,
		EnergySource:        pb.EnergySource_ENERGY_SOURCE_SOLAR,
		ObserverName:        obsResp.ObserverName,
		CreatedTimestampUtc: timestamppb.New(t2),
		Values: []*pb.CreateObservationsRequest_Value{
			{TimestampUtc: timestamppb.New(t0), ValueWatts: 30_000_000},
			{TimestampUtc: timestamppb.New(t1), ValueWatts: 30_000_000},
		},
	})
	require.NoError(t, err)

	t.Run(
		"Single-source fractional split (0.27) preserves fraction and exact capacity",
		func(t *testing.T) {
			fcResp, err := dc.GetForecastAsTimeseries(
				t.Context(),
				&pb.GetForecastAsTimeseriesRequest{
					LocationUuid: vlocSplit.LocationUuid,
					EnergySource: pb.EnergySource_ENERGY_SOURCE_SOLAR,
					Forecaster:   fc,
					TimeWindow: &pb.TimeWindow{
						StartTimestampUtc: timestamppb.New(t0),
						EndTimestampUtc:   timestamppb.New(t2),
					},
				},
			)
			require.NoError(t, err)
			require.Len(t, fcResp.Values, 3)

			for _, v := range fcResp.Values {
				require.Equal(t, uint64(27_000_000), v.EffectiveCapacityWatts)
				require.InDelta(t, 0.50, v.P50ValueFraction, 1e-4)
			}

			obsTsResp, err := dc.GetObservationsAsTimeseries(
				t.Context(),
				&pb.GetObservationsAsTimeseriesRequest{
					LocationUuid: vlocSplit.LocationUuid,
					EnergySource: pb.EnergySource_ENERGY_SOURCE_SOLAR,
					ObserverName: obsResp.ObserverName,
					TimeWindow: &pb.TimeWindow{
						StartTimestampUtc: timestamppb.New(t0),
						EndTimestampUtc:   timestamppb.New(t2),
					},
				},
			)
			require.NoError(t, err)
			require.Len(t, obsTsResp.Values, 3)

			for _, v := range obsTsResp.Values {
				require.Equal(t, uint64(27_000_000), v.EffectiveCapacityWatts)
				require.InDelta(t, 0.50, v.ValueFraction, 1e-4)
			}
		},
	)

	t.Run(
		"Multi-geometry weighted sum, strict missing component omission, and quantile null-safety",
		func(t *testing.T) {
			// Expected at t0 and t1:
			// Capacity = 100 MW + 300 MW = 400 MW
			// p50 = (50 MW + 30 MW) / 400 MW = 0.20
			// p10 = (40 MW + 15 MW) / 400 MW = 55 / 400 = 0.1375
			// p90 = omitted (since locB has no p90)
			// t2 is omitted because locB has no forecast at t2!
			fcResp, err := dc.GetForecastAsTimeseries(
				t.Context(),
				&pb.GetForecastAsTimeseriesRequest{
					LocationUuid: vlocSum.LocationUuid,
					EnergySource: pb.EnergySource_ENERGY_SOURCE_SOLAR,
					Forecaster:   fc,
					TimeWindow: &pb.TimeWindow{
						StartTimestampUtc: timestamppb.New(t0),
						EndTimestampUtc:   timestamppb.New(t2),
					},
				},
			)
			require.NoError(t, err)
			require.Len(t, fcResp.Values, 2)

			for _, v := range fcResp.Values {
				require.Equal(t, uint64(400_000_000), v.EffectiveCapacityWatts)
				require.InDelta(t, 0.20, v.P50ValueFraction, 1e-4)
				require.Contains(t, v.OtherStatisticsFractions, "p10")
				require.InDelta(t, 0.1375, v.OtherStatisticsFractions["p10"], 1e-4)
				require.NotContains(t, v.OtherStatisticsFractions, "p90")
			}

			// Observations timeseries on vlocSum: only t0 and t1 (t2 omitted because locB lacks t2)
			obsTsResp, err := dc.GetObservationsAsTimeseries(
				t.Context(),
				&pb.GetObservationsAsTimeseriesRequest{
					LocationUuid: vlocSum.LocationUuid,
					EnergySource: pb.EnergySource_ENERGY_SOURCE_SOLAR,
					ObserverName: obsResp.ObserverName,
					TimeWindow: &pb.TimeWindow{
						StartTimestampUtc: timestamppb.New(t0),
						EndTimestampUtc:   timestamppb.New(t2),
					},
				},
			)
			require.NoError(t, err)
			require.Len(t, obsTsResp.Values, 2)

			for _, v := range obsTsResp.Values {
				require.Equal(t, uint64(400_000_000), v.EffectiveCapacityWatts)
				require.InDelta(t, 0.20, v.ValueFraction, 1e-4)
			}
		},
	)

	t.Run(
		"GetLatestObservations covers fast path, lagging tip slow path, and staggered gap slow path",
		func(t *testing.T) {
			t3 := baseTime.Add(90 * time.Minute)
			locC := createTestLocation(
				t,
				"test_cloc_math_c",
				"POINT(-1.4 51.4)",
				100_000_000,
				baseTime.Add(-24*time.Hour),
				&structpb.Struct{},
				"GB",
			)
			_, err := dc.CreateObservations(t.Context(), &pb.CreateObservationsRequest{
				LocationUuid:        locC.LocationUuid,
				EnergySource:        pb.EnergySource_ENERGY_SOURCE_SOLAR,
				ObserverName:        obsResp.ObserverName,
				CreatedTimestampUtc: timestamppb.New(t0),
				Values: []*pb.CreateObservationsRequest_Value{
					{TimestampUtc: timestamppb.New(t0), ValueWatts: 50_000_000},
					{TimestampUtc: timestamppb.New(t3), ValueWatts: 50_000_000},
				},
			})
			require.NoError(t, err)

			vlocStaggered := createTestLocation(
				t,
				"test_cloc_math_staggered",
				"POINT(-1.45 51.45)",
				1,
				baseTime.Add(-24*time.Hour),
				&structpb.Struct{},
				"GB",
			)
			_, err = dc.SetLocationComponents(t.Context(), &pb.SetLocationComponentsRequest{
				LocationUuid: vlocStaggered.LocationUuid,
				Components: []*pb.LocationComponent{
					{SourceLocationUuid: locB.LocationUuid, Weight: 1.0},
					{SourceLocationUuid: locC.LocationUuid, Weight: 1.0},
				},
			})
			require.NoError(t, err)

			latestResp, err := dc.GetLatestObservations(
				t.Context(),
				&pb.GetLatestObservationsRequest{
					LocationUuids: []string{
						locA.LocationUuid,
						vlocSplit.LocationUuid,
						vlocSum.LocationUuid,
						vlocStaggered.LocationUuid,
					},
					EnergySource: pb.EnergySource_ENERGY_SOURCE_SOLAR,
					ObserverName: obsResp.ObserverName,
				},
			)
			require.NoError(t, err)
			require.Len(t, latestResp.Observations, 4)

			byLoc := make(map[string]*pb.GetLatestObservationsResponse_Observation)
			for _, o := range latestResp.Observations {
				byLoc[o.LocationUuid] = o
			}

			// Physical locA (fast path): t2
			require.Equal(t, t2, byLoc[locA.LocationUuid].TimestampUtc.AsTime())
			require.InDelta(t, 0.50, byLoc[locA.LocationUuid].ValueFraction, 1e-4)

			// Composite vlocSplit (fast path, shares locA with locA and vlocSum): t2
			require.Equal(t, t2, byLoc[vlocSplit.LocationUuid].TimestampUtc.AsTime())
			require.Equal(
				t,
				uint64(27_000_000),
				byLoc[vlocSplit.LocationUuid].EffectiveCapacityWatts,
			)
			require.InDelta(t, 0.50, byLoc[vlocSplit.LocationUuid].ValueFraction, 1e-4)

			// Composite vlocSum (slow path - lagging tip): locA tip=t2, locB tip=t1 -> t1
			require.Equal(t, t1, byLoc[vlocSum.LocationUuid].TimestampUtc.AsTime())
			require.Equal(
				t,
				uint64(400_000_000),
				byLoc[vlocSum.LocationUuid].EffectiveCapacityWatts,
			)
			require.InDelta(t, 0.20, byLoc[vlocSum.LocationUuid].ValueFraction, 1e-4)

			// Composite vlocStaggered (slow path - staggered gap): locB tip=t1, locC tip=t3 (missing t1) -> t0
			require.Equal(t, t0, byLoc[vlocStaggered.LocationUuid].TimestampUtc.AsTime())
			require.Equal(
				t,
				uint64(400_000_000),
				byLoc[vlocStaggered.LocationUuid].EffectiveCapacityWatts,
			)
			require.InDelta(t, 0.20, byLoc[vlocStaggered.LocationUuid].ValueFraction, 1e-4)

			// Now test the fast path on multi-component vlocSum (min_tip_utc = max_tip_utc = t2)
			_, err = dc.CreateObservations(t.Context(), &pb.CreateObservationsRequest{
				LocationUuid:        locB.LocationUuid,
				EnergySource:        pb.EnergySource_ENERGY_SOURCE_SOLAR,
				ObserverName:        obsResp.ObserverName,
				CreatedTimestampUtc: timestamppb.New(t3),
				Values: []*pb.CreateObservationsRequest_Value{
					{TimestampUtc: timestamppb.New(t2), ValueWatts: 90_000_000}, // 30% of 300 MW
				},
			})
			require.NoError(t, err)

			fastPathResp, err := dc.GetLatestObservations(
				t.Context(),
				&pb.GetLatestObservationsRequest{
					LocationUuids:     []string{vlocSum.LocationUuid},
					EnergySource:      pb.EnergySource_ENERGY_SOURCE_SOLAR,
					ObserverName:      obsResp.ObserverName,
					PivotTimestampUtc: timestamppb.New(t3),
				},
			)
			require.NoError(t, err)
			require.Len(t, fastPathResp.Observations, 1)
			// At t2: locA = 50 MW, locB = 90 MW -> (50 + 90) / 400 = 140 / 400 = 0.35
			require.Equal(t, t2, fastPathResp.Observations[0].TimestampUtc.AsTime())
			require.Equal(
				t,
				uint64(400_000_000),
				fastPathResp.Observations[0].EffectiveCapacityWatts,
			)
			require.InDelta(t, 0.35, fastPathResp.Observations[0].ValueFraction, 1e-4)
		},
	)

	t.Run("Zero-capacity safety returns 0 without division by zero", func(t *testing.T) {
		fcResp, err := dc.GetForecastAsTimeseries(
			t.Context(),
			&pb.GetForecastAsTimeseriesRequest{
				LocationUuid: vlocZero.LocationUuid,
				EnergySource: pb.EnergySource_ENERGY_SOURCE_SOLAR,
				Forecaster:   fc,
				TimeWindow: &pb.TimeWindow{
					StartTimestampUtc: timestamppb.New(t0),
					EndTimestampUtc:   timestamppb.New(t1),
				},
			},
		)
		require.NoError(t, err)
		require.Len(t, fcResp.Values, 2)

		for _, v := range fcResp.Values {
			require.Equal(t, uint64(0), v.EffectiveCapacityWatts)
			require.Equal(t, float32(0), v.P50ValueFraction)
		}
	})

	t.Run(
		"Map snapshots GetForecastAtTimestamp and GetObservationsAtTimestamp mix physical and virtual",
		func(t *testing.T) {
			fcSnap, err := dc.GetForecastAtTimestamp(t.Context(), &pb.GetForecastAtTimestampRequest{
				LocationUuids: []string{locA.LocationUuid, vlocSum.LocationUuid},
				EnergySource:  pb.EnergySource_ENERGY_SOURCE_SOLAR,
				Forecaster:    fc,
				TimestampUtc:  timestamppb.New(t1),
			})
			require.NoError(t, err)
			require.Len(t, fcSnap.Values, 2)

			fcByLoc := make(map[string]*pb.GetForecastAtTimestampResponse_Value)
			for _, v := range fcSnap.Values {
				fcByLoc[v.LocationUuid] = v
			}

			require.InDelta(t, 0.50, fcByLoc[locA.LocationUuid].ValueFraction, 1e-4)
			require.InDelta(t, 0.20, fcByLoc[vlocSum.LocationUuid].ValueFraction, 1e-4)
			require.Equal(
				t,
				uint64(400_000_000),
				fcByLoc[vlocSum.LocationUuid].EffectiveCapacityWatts,
			)

			obsSnap, err := dc.GetObservationsAtTimestamp(
				t.Context(),
				&pb.GetObservationsAtTimestampRequest{
					LocationUuids: []string{locA.LocationUuid, vlocSum.LocationUuid},
					EnergySource:  pb.EnergySource_ENERGY_SOURCE_SOLAR,
					ObserverName:  obsResp.ObserverName,
					TimestampUtc:  timestamppb.New(t1),
				},
			)
			require.NoError(t, err)
			require.Len(t, obsSnap.Values, 2)

			obsByLoc := make(map[string]*pb.GetObservationsAtTimestampResponse_Value)
			for _, v := range obsSnap.Values {
				obsByLoc[v.LocationUuid] = v
			}

			require.InDelta(t, 0.50, obsByLoc[locA.LocationUuid].ValueFraction, 1e-4)
			require.InDelta(t, 0.20, obsByLoc[vlocSum.LocationUuid].ValueFraction, 1e-4)
			require.Equal(
				t,
				uint64(400_000_000),
				obsByLoc[vlocSum.LocationUuid].EffectiveCapacityWatts,
			)
		},
	)
}
