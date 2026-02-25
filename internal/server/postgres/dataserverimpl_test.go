package postgres

import (
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"
	timestamppb "google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/openclimatefix/data-platform/internal/gen/ocf/dp"
)

// --- HELPERS ------------------------------------------------------------------------------------.
func seed(tb testing.TB, pgConnString string, params seedDBParams) (output struct {
	NumPgvs       int
	LocationUuids []string
},
) {
	seedfiles, _ := filepath.Glob(filepath.Join(".", "testdata", "seed*.sql"))
	conn, err := pgx.Connect(tb.Context(), pgConnString)

	require.NoError(tb, err)

	defer func() {
		err = conn.Close(tb.Context())
		require.NoError(tb, err)
	}()

	for _, f := range seedfiles {
		sql, err := os.ReadFile(f)
		require.NoError(tb, err)

		_, err = conn.Exec(tb.Context(), string(sql))
		require.NoError(tb, err)

		var result struct {
			NumPgvs       int
			LocationUuids []pgtype.UUID
		}

		err = conn.QueryRow(
			tb.Context(),
			fmt.Sprintf(
				"SELECT seed_db("+
					"name_prefix=>'%s'::TEXT,"+
					"target_total_forecasts=>%d::INTEGER,"+
					"pivot_time=>'%s'::TIMESTAMP"+
					");",
				params.NamePrefix,
				params.TargetTotalForecasts,
				params.PivotTime.UTC().Format(time.RFC3339),
			),
		).Scan(&result)
		require.NoError(tb, err)
		tb.Logf(
			"Seeded %d predicted generation values for %d locations",
			output.NumPgvs,
			len(output.LocationUuids),
		)

		stringUuids := make([]string, len(result.LocationUuids))
		for i, u := range result.LocationUuids {
			stringUuids[i] = u.String()
		}

		output.NumPgvs = result.NumPgvs
		output.LocationUuids = stringUuids
	}

	return output
}

type seedDBParams struct {
	NamePrefix           string
	TargetTotalForecasts int
	PivotTime            time.Time
}

// --- Tests --------------------------------------------------------------------------------------

func TestCreateLocation(t *testing.T) {
	metadata, err := structpb.NewStruct(map[string]any{"source": "test"})
	require.NoError(t, err)

	testcases := []struct {
		name           string
		req            *pb.CreateLocationRequest
		expectedLatLng *pb.LatLng
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
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := dc.CreateLocation(t.Context(), tc.req)

			if strings.Split(tc.name, " ")[0] == "Shouldn't" {
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
	metadata, err := structpb.NewStruct(map[string]any{"source": "test"})
	require.NoError(t, err)

	pivotTime := time.Date(2019, 5, 6, 6, 0, 0, 0, time.UTC)

	createResp, err := dc.CreateLocation(t.Context(), &pb.CreateLocationRequest{
		LocationName:           "test_update_location_site",
		GeometryWkt:            "POINT(-0.1 51.5)",
		EffectiveCapacityWatts: 1234e6,
		Metadata:               metadata,
		EnergySource:           pb.EnergySource_ENERGY_SOURCE_SOLAR,
		LocationType:           pb.LocationType_LOCATION_TYPE_SITE,
		ValidFromUtc:           timestamppb.New(pivotTime.Add(-10 * time.Hour)),
	})

	require.NoError(t, err)

	newMetadata, err := structpb.NewStruct(map[string]any{"source": "test", "updated": true})
	require.NoError(t, err)

	testcases := []struct {
		name                  string
		req                   *pb.UpdateLocationRequest
		expectedName          string
		expectedCapacityWatts uint64
		expectedMetadata      map[string]any
	}{
		{
			name: "Should update capacity to higher value",
			req: &pb.UpdateLocationRequest{
				LocationUuid:              createResp.LocationUuid,
				EnergySource:              pb.EnergySource_ENERGY_SOURCE_SOLAR,
				NewEffectiveCapacityWatts: func() *uint64 { v := uint64(1235e6); return &v }(),
				ValidFromUtc:              timestamppb.New(pivotTime.Add(-5 * time.Hour)),
			},
			expectedName:          "test_update_location_site",
			expectedCapacityWatts: 1235e6,
			expectedMetadata:      map[string]any{"source": "test"},
		},
		{
			name: "Shouldn't update anything when nothing is set",
			req: &pb.UpdateLocationRequest{
				LocationUuid: "invalid-uuid",
				EnergySource: pb.EnergySource_ENERGY_SOURCE_SOLAR,
				ValidFromUtc: timestamppb.New(pivotTime.Add(-4 * time.Hour)),
			},
		},
		{
			name: "Should update name and metadata",
			req: &pb.UpdateLocationRequest{
				LocationUuid:    createResp.LocationUuid,
				EnergySource:    pb.EnergySource_ENERGY_SOURCE_SOLAR,
				NewLocationName: func() *string { s := "test_updated_location_site"; return &s }(),
				NewMetadata:     newMetadata,
				ValidFromUtc:    timestamppb.New(pivotTime.Add(-3 * time.Hour)),
			},
			expectedName:          "test_updated_location_site",
			expectedCapacityWatts: 1235e6,
			expectedMetadata:      map[string]any{"source": "test", "updated": true},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := dc.UpdateLocation(t.Context(), tc.req)

			if strings.Split(tc.name, " ")[0] == "Shouldn't" {
				require.Error(t, err)
			} else {
				require.NoError(t, err)

				newGetResp, err := dc.GetLocation(t.Context(), &pb.GetLocationRequest{
					LocationUuid:      resp.LocationUuid,
					EnergySource:      pb.EnergySource_ENERGY_SOURCE_SOLAR,
					IncludeGeometry:   false,
					PivotTimestampUtc: timestamppb.New(tc.req.ValidFromUtc.AsTime().Add(time.Minute)),
				})
				require.NoError(t, err)
				require.Equal(t, tc.expectedName, newGetResp.LocationName)
				require.Equal(t, int(tc.expectedCapacityWatts), int(newGetResp.EffectiveCapacityWatts))
				require.Equal(t, tc.expectedMetadata, newGetResp.Metadata.AsMap())
			}
		})
	}
}

func TestCreateUpdateForecaster(t *testing.T) {
	testcases := []struct {
		name      string
		createReq *pb.CreateForecasterRequest
		updateReq *pb.UpdateForecasterRequest
	}{
		{
			name: "Should create forecaster",
			createReq: &pb.CreateForecasterRequest{
				Name:    "test_forecaster_1",
				Version: "v1",
			},
		},
		{
			name: "Should update existing forecaster",
			updateReq: &pb.UpdateForecasterRequest{
				Name:       "test_forecaster_1",
				NewVersion: "v2",
			},
		},
		{
			name: "Shouldn't update with non-unique version",
			updateReq: &pb.UpdateForecasterRequest{
				Name:       "test_forecaster_1",
				NewVersion: "v2",
			},
		},
		{
			name: "Shouldn't update non-existent forecaster",
			updateReq: &pb.UpdateForecasterRequest{
				Name:       "non_existent_forecaster",
				NewVersion: "v1",
			},
		},
		{
			name: "Shouldn't create existing forecaster",
			createReq: &pb.CreateForecasterRequest{
				Name:    "test_forecaster_1",
				Version: "v2",
			},
		},
		{
			name: "Shouldn't create forecaster with invalid name",
			createReq: &pb.CreateForecasterRequest{
				Name:    "",
				Version: "v1",
			},
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

		if strings.Split(tc.name, " ")[0] == "Shouldn't" {
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
		_, err := dc.CreateForecaster(t.Context(), &pb.CreateForecasterRequest{
			Name:    name,
			Version: "v0",
		})
		require.NoError(t, err)

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
		},
		{
			name: "Should list only forecasters with filtered names",
			req: &pb.ListForecastersRequest{
				ForecasterNamesFilter: []string{"test_list_forecaster_1"},
			},
			expectedCount: 5,
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
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := dc.ListForecasters(t.Context(), tc.req)
			if strings.Contains(tc.name, "Shouldn't") {
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
	metadata, err := structpb.NewStruct(map[string]any{"source": "test"})
	require.NoError(t, err)
	siteResp, err := dc.CreateLocation(t.Context(), &pb.CreateLocationRequest{
		LocationName:           "test_get_forecast_at_timestamp_site",
		GeometryWkt:            "POINT(-0.6 51.8)",
		EffectiveCapacityWatts: 1000000,
		Metadata:               metadata,
		EnergySource:           pb.EnergySource_ENERGY_SOURCE_SOLAR,
		LocationType:           pb.LocationType_LOCATION_TYPE_SITE,
		ValidFromUtc:           timestamppb.New(pivotTime.Add(-time.Hour * 1)),
	})
	require.NoError(t, err)
	siteResp2, err := dc.CreateLocation(t.Context(), &pb.CreateLocationRequest{
		LocationName:           "test_get_forecast_at_timestamp_site_2",
		GeometryWkt:            "POINT(-0.5 58.6)",
		EffectiveCapacityWatts: 2000000,
		Metadata:               metadata,
		EnergySource:           pb.EnergySource_ENERGY_SOURCE_SOLAR,
		LocationType:           pb.LocationType_LOCATION_TYPE_SITE,
		ValidFromUtc:           timestamppb.New(pivotTime.Add(-time.Hour * 1)),
	})
	require.NoError(t, err)

	// Create a forecaster
	forecasterResp, err := dc.CreateForecaster(t.Context(), &pb.CreateForecasterRequest{
		Name:    "test_get_forecast_at_timestamp_forecaster",
		Version: "v1",
	})
	require.NoError(t, err)

	yields := make([]*pb.CreateForecastRequest_ForecastValue, 10)
	for i := range yields {
		yields[i] = &pb.CreateForecastRequest_ForecastValue{
			HorizonMins: uint32(i * 30),
			P50Fraction: float32(0.5 + float64(i)*0.05),
			OtherStatisticsFractions: map[string]float32{
				"p90": float32(0.6 + float32(i)*0.05),
				"p10": float32(0.4 + float32(i)*0.05),
			},
			Metadata: metadata,
		}
	}

	for _, locationUuid := range [2]string{siteResp.LocationUuid, siteResp2.LocationUuid} {
		req := &pb.CreateForecastRequest{
			LocationUuid: locationUuid,
			Forecaster:   forecasterResp.Forecaster,
			EnergySource: pb.EnergySource_ENERGY_SOURCE_SOLAR,
			InitTimeUtc:  timestamppb.New(pivotTime),
			Values:       yields,
		}
		_, err = dc.CreateForecast(t.Context(), req)
		require.NoError(t, err)
	}

	testcases := []struct {
		name         string
		timestamp    time.Time
		expectedp50s []float32
	}{
		{
			name:         "Should get forecast at init time",
			timestamp:    pivotTime,
			expectedp50s: []float32{0.5, 0.5},
		},
		{
			name:         "Should get forecast at first horizon",
			timestamp:    pivotTime.Add(30 * time.Minute),
			expectedp50s: []float32{0.55, 0.55},
		},
		{
			name:         "Should return no values where no predicted values exist",
			timestamp:    pivotTime.Add(45 * time.Minute),
			expectedp50s: []float32{},
		},
		{
			name:         "Should get forecast at last horizon",
			timestamp:    pivotTime.Add(270 * time.Minute),
			expectedp50s: []float32{0.95, 0.95},
		},
		{
			name:         "Should return no values before init time",
			timestamp:    pivotTime.Add(-15 * time.Minute),
			expectedp50s: []float32{},
		},
		{
			name:         "Should return no values after last horizon",
			timestamp:    pivotTime.Add(300 * time.Minute),
			expectedp50s: []float32{},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := dc.GetForecastAtTimestamp(t.Context(), &pb.GetForecastAtTimestampRequest{
				LocationUuids: []string{
					siteResp.LocationUuid,
					siteResp2.LocationUuid,
				},
				Forecaster:   forecasterResp.Forecaster,
				EnergySource: pb.EnergySource_ENERGY_SOURCE_SOLAR,
				TimestampUtc: timestamppb.New(tc.timestamp),
			})
			if strings.Contains(tc.name, "Shouldn't") {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.NotNil(t, resp)
				require.Len(t, resp.Values, len(tc.expectedp50s))

				for i, forecast := range resp.Values {
					require.Equal(t, tc.expectedp50s[i], forecast.ValueFraction)
				}
			}
		})
	}
}

func TestGetLocationsAsGeoJSON(t *testing.T) {
	// Create some locations
	siteUuids := make([]string, 3)
	for i := range siteUuids {
		resp, err := dc.CreateLocation(t.Context(), &pb.CreateLocationRequest{
			LocationName: fmt.Sprintf("testsite%02d", i),
			GeometryWkt: fmt.Sprintf(
				"POINT(%f %f)",
				-0.1+float32(i)*0.01,
				51.5+float32(i)*0.01,
			),
			EffectiveCapacityWatts: uint64(1000000 + i*100),
			EnergySource:           pb.EnergySource_ENERGY_SOURCE_SOLAR,
			Metadata:               &structpb.Struct{},
			LocationType:           pb.LocationType_LOCATION_TYPE_SITE,
		})
		require.NoError(t, err)

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
	metadata, err := structpb.NewStruct(map[string]any{"source": "test"})
	require.NoError(t, err)
	siteResp, err := dc.CreateLocation(t.Context(), &pb.CreateLocationRequest{
		LocationName:           "test_get_forecast_as_timeseries_site",
		GeometryWkt:            "POINT(-60.25 57.5)",
		EffectiveCapacityWatts: 1000000,
		Metadata:               metadata,
		EnergySource:           pb.EnergySource_ENERGY_SOURCE_SOLAR,
		LocationType:           pb.LocationType_LOCATION_TYPE_SITE,
		ValidFromUtc:           timestamppb.New(pivotTime.Add(-time.Hour * 49)),
	})
	require.NoError(t, err)
	// Update the capacity of the site to check it is reflected in the values
	_, err = dc.UpdateLocation(t.Context(), &pb.UpdateLocationRequest{
		LocationUuid:              siteResp.LocationUuid,
		EnergySource:              pb.EnergySource_ENERGY_SOURCE_SOLAR,
		NewEffectiveCapacityWatts: func() *uint64 { v := uint64(1500000); return &v }(),
		ValidFromUtc:              timestamppb.New(pivotTime.Add(-time.Hour * 1)),
	})
	require.NoError(t, err)

	// Create a forecaster to make the forecasts
	forecasterResp, err := dc.CreateForecaster(t.Context(), &pb.CreateForecasterRequest{
		Name:    "test_get_forecast_as_timeseries_forecaster",
		Version: "v1",
	})
	require.NoError(t, err)

	// Create 4, hour-long forecasts, each 30 minutes apart, with a resolution of 5 minutes.
	// The forecast values increase linearly from 0% to 100% of capacity over each forecast.
	// The last forecast begins at the pivot time.
	yields := make([]*pb.CreateForecastRequest_ForecastValue, 60/5)
	for i := range yields {
		yields[i] = &pb.CreateForecastRequest_ForecastValue{
			HorizonMins: uint32(i * 5),
			P50Fraction: float32(i) * float32(100/len(yields)) / 100.0,
			OtherStatisticsFractions: map[string]float32{
				"p10": float32(max(float32(i-1)*float32(100/len(yields))/100.0, 0)),
				"p90": float32(min(float32(i+1)*float32(100/len(yields))/100.0, 1.1)),
			},
			Metadata: metadata,
		}
	}

	for i := 3; i >= 0; i-- {
		req := &pb.CreateForecastRequest{
			LocationUuid: siteResp.LocationUuid,
			Forecaster:   forecasterResp.Forecaster,
			EnergySource: pb.EnergySource_ENERGY_SOURCE_SOLAR,
			InitTimeUtc:  timestamppb.New(pivotTime.Add(time.Duration(-i*30) * time.Minute)),
			Values:       yields,
		}
		_, err = dc.CreateForecast(t.Context(), req)
		require.NoError(t, err)
	}

	// For each horizon, get the predicted timeseries
	testcases := []struct {
		name           string
		horizonMins    int32
		pivotTime      time.Time
		expectedValues []float32
	}{
		{
			name:        "Should return expected values for horizon 0 mins",
			horizonMins: 0,
			// For horizon 0, we should get all the values from the latest forecast,
			// plus the values from the previous forecasts that have the lowest horizon
			// for each target time.
			// Since the predicted values are every 5 minutes, and the forecasts are every 30,
			// we should get 6 values from each forecast, until the latest where we get all 12.
			// The forecast values are seeded increasing from 0% to 100% in regular intervals,
			// and there are 12 values per forecast - 100 // 12 = 8, so
			// this means the values we are fetching should be
			// 0, 8, 16, 24, 32, 40 (horizons 0 to 25 minutes from forecast 3)
			// Then the same from forecast 2, as it's horizon is smaller - likewise then forecast 1
			// 0, 8, 16, 24, 32, 40, 48, 56, 64, 72, 80, 88 (horizons 0 to 55 minutes from forecast 0)
			expectedValues: []float32{
				0.00, 0.08, 0.16, 0.24, 0.32, 0.40,
				0.00, 0.08, 0.16, 0.24, 0.32, 0.40,
				0.00, 0.08, 0.16, 0.24, 0.32, 0.40,
				0.00, 0.08, 0.16, 0.24, 0.32, 0.40, 0.48, 0.56, 0.64, 0.72, 0.80, 0.88,
			},
		},
		{
			name:        "Should return expected values for horizon 14 mins",
			horizonMins: 14,
			// For horizon of 14 minutes, anything with a lesser horizon should not be included.
			// So the value for 0, 5, and 10 minutes should not be included.
			expectedValues: []float32{
				0.24, 0.32, 0.40, 0.48, 0.56, 0.64,
				0.24, 0.32, 0.40, 0.48, 0.56, 0.64,
				0.24, 0.32, 0.40, 0.48, 0.56, 0.64,
				0.24, 0.32, 0.40, 0.48, 0.56, 0.64, 0.72, 0.80, 0.88,
			},
		},
		{
			name:        "Should return expected values for horizon 30 mins",
			horizonMins: 30,
			expectedValues: []float32{
				0.48, 0.56, 0.64, 0.72, 0.80, 0.88,
				0.48, 0.56, 0.64, 0.72, 0.80, 0.88,
				0.48, 0.56, 0.64, 0.72, 0.80, 0.88,
				0.48, 0.56, 0.64, 0.72, 0.80, 0.88,
			},
		},
		{
			name:        "Shouldn't return successfully for horizon 60 mins",
			horizonMins: 60,
		},
		{
			name:        "Should return expected values for horizon 14 minutes with pivot time",
			horizonMins: 14,
			pivotTime:   pivotTime.Add(-15 * time.Minute),
			// For horizon of 14 minutes and a pivot time of 15 minutes before the latest,
			// we should expect the same as for the 14 minute horizon no pivot time case,
			// only this time the latest forecast should not be included at all.
			// Hence we only see data for three forecasts.
			expectedValues: []float32{
				0.24, 0.32, 0.40, 0.48, 0.56, 0.64,
				0.24, 0.32, 0.40, 0.48, 0.56, 0.64,
				0.24, 0.32, 0.40, 0.48, 0.56, 0.64, 0.72, 0.80, 0.88,
			},
		},
	}

	for _, tc := range testcases {
		t.Run(fmt.Sprintf("Horizon %d mins", tc.horizonMins), func(t *testing.T) {
			if tc.pivotTime.Equal((time.Time{})) {
				tc.pivotTime = pivotTime
			}

			resp, err := dc.GetForecastAsTimeseries(t.Context(), &pb.GetForecastAsTimeseriesRequest{
				LocationUuid:      siteResp.LocationUuid,
				HorizonMins:       uint32(tc.horizonMins),
				Forecaster:        forecasterResp.Forecaster,
				EnergySource:      pb.EnergySource_ENERGY_SOURCE_SOLAR,
				PivotTimestampUtc: timestamppb.New(tc.pivotTime),
				TimeWindow: &pb.TimeWindow{
					StartTimestampUtc: timestamppb.New(pivotTime.Add(-time.Hour * 48)),
					EndTimestampUtc:   timestamppb.New(pivotTime.Add(time.Hour * 36)),
				},
			})
			if strings.Contains(tc.name, "Shouldn't") {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.NotNil(t, resp)

				targetTimes := make([]int64, len(resp.Values))

				actualValues := make([]float32, len(resp.Values))
				for i, v := range resp.Values {
					targetTimes[i] = v.TargetTimestampUtc.AsTime().Unix()
					actualValues[i] = v.P50ValueFraction

					// Assert that the capacity change has been picked up
					if v.TargetTimestampUtc.AsTime().
						After(pivotTime.Add(-1 * time.Hour).Add(-1 * time.Second)) {
						require.Equal(t, 1500000, int(v.EffectiveCapacityWatts))
					} else {
						require.Equal(t, 1000000, int(v.EffectiveCapacityWatts))
					}
				}

				require.IsIncreasing(t, targetTimes)
				require.Equal(t, tc.expectedValues, actualValues)
			}
		})
	}
}

func TestListLocationsLocationFilters(t *testing.T) {
	pivotTime := time.Now().Truncate(time.Minute)
	metadata, err := structpb.NewStruct(map[string]any{"source": "test"})
	require.NoError(t, err)

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
				LocationTypeFilter:  func() *pb.LocationType { t := pb.LocationType_LOCATION_TYPE_DNO; return &t }(),
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
				EnclosingLocationUuidFilter: func() *string { s := uuid.New().String(); return &s }(),
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
	siteResp, err := dc.CreateLocation(t.Context(), &pb.CreateLocationRequest{
		LocationName:           "test_get_observations_as_timeseries_site",
		GeometryWkt:            "POINT(-20.25 57.5)",
		EffectiveCapacityWatts: 1000000,
		Metadata:               &structpb.Struct{},
		EnergySource:           pb.EnergySource_ENERGY_SOURCE_SOLAR,
		LocationType:           pb.LocationType_LOCATION_TYPE_SITE,
		ValidFromUtc:           timestamppb.New(pivotTime.Add(-time.Hour * 64)),
	})
	require.NoError(t, err)

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
	siteResp, err := dc.CreateLocation(t.Context(), &pb.CreateLocationRequest{
		LocationName:           "test_get_latest_observations_site_1",
		GeometryWkt:            "POINT(-20.25 57.5)",
		EffectiveCapacityWatts: 1000000,
		Metadata:               &structpb.Struct{},
		EnergySource:           pb.EnergySource_ENERGY_SOURCE_SOLAR,
		LocationType:           pb.LocationType_LOCATION_TYPE_SITE,
		ValidFromUtc:           timestamppb.New(pivotTime.Add(-time.Hour * 4)),
	})
	require.NoError(t, err)

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
	siteResp, err := dc.CreateLocation(t.Context(), &pb.CreateLocationRequest{
		LocationName:           "test_create_observations_site",
		GeometryWkt:            "POINT(-0.1 51.5)",
		EffectiveCapacityWatts: 1000000,
		Metadata:               &structpb.Struct{},
		EnergySource:           pb.EnergySource_ENERGY_SOURCE_SOLAR,
		LocationType:           pb.LocationType_LOCATION_TYPE_SITE,
		ValidFromUtc:           timestamppb.New(pivotTime.Add(-time.Hour * 4)),
	})
	require.NoError(t, err)

	// Update the capacity
	updateResp, err := dc.UpdateLocation(t.Context(), &pb.UpdateLocationRequest{
		LocationUuid:              siteResp.LocationUuid,
		EnergySource:              pb.EnergySource_ENERGY_SOURCE_SOLAR,
		NewEffectiveCapacityWatts: func() *uint64 { v := uint64(2000000); return &v }(),
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
	metadata, err := structpb.NewStruct(map[string]any{"source": "test"})
	require.NoError(t, err)

	// Create a site to attach the observations to
	siteResp, err := dc.CreateLocation(t.Context(), &pb.CreateLocationRequest{
		LocationName:           "test_get_week_average_deltas_site",
		GeometryWkt:            "POINT(-20.25 59.5)",
		EffectiveCapacityWatts: 1000000,
		Metadata:               &structpb.Struct{},
		EnergySource:           pb.EnergySource_ENERGY_SOURCE_SOLAR,
		LocationType:           pb.LocationType_LOCATION_TYPE_SITE,
		ValidFromUtc:           timestamppb.New(pivotTime.Add(-time.Hour * 12 * 24)),
	})
	require.NoError(t, err)

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
	forecasterResp, err := dc.CreateForecaster(t.Context(), &pb.CreateForecasterRequest{
		Name:    "test_get_week_average_deltas_forecaster",
		Version: "v1",
	})
	require.NoError(t, err)

	// Create 8, 8 hour-long forecasts, each one day apart, with a resolution of 30 minutes.
	// The forecast values increase linearly from 0% to 100% of capacity over each forecast.
	// The last forecast begins at the pivot time.
	yields := make([]*pb.CreateForecastRequest_ForecastValue, 8*60/30)
	for i := range yields {
		yields[i] = &pb.CreateForecastRequest_ForecastValue{
			HorizonMins: uint32(i * 5),
			P50Fraction: float32(i) * float32(100/len(yields)) / 100.0,
			OtherStatisticsFractions: map[string]float32{
				"p10": float32(max(float32(i-1)*float32(100/len(yields))/100.0, 0)),
				"p90": float32(min(float32(i+1)*float32(100/len(yields))/100.0, 1.1)),
			},
			Metadata: metadata,
		}
	}

	for i := 7; i >= 0; i-- {
		req := &pb.CreateForecastRequest{
			LocationUuid: siteResp.LocationUuid,
			Forecaster:   forecasterResp.Forecaster,
			EnergySource: pb.EnergySource_ENERGY_SOURCE_SOLAR,
			InitTimeUtc:  timestamppb.New(pivotTime.Add(time.Duration(-i*24) * time.Hour)),
			Values:       yields,
		}
		_, err = dc.CreateForecast(t.Context(), req)
		require.NoError(t, err)
	}

	deltaResp, err := dc.GetWeekAverageDeltas(t.Context(), &pb.GetWeekAverageDeltasRequest{
		LocationUuid:      siteResp.LocationUuid,
		EnergySource:      pb.EnergySource_ENERGY_SOURCE_SOLAR,
		Forecaster:        forecasterResp.Forecaster,
		ObserverName:      obsResp.ObserverName,
		PivotTimestampUtc: timestamppb.New(pivotTime),
	})
	require.NoError(t, err)
	require.NotNil(t, deltaResp)
	require.Len(t, deltaResp.Deltas, 8*60/30) // One per horizon
}

func TestCreateForecast(t *testing.T) {
	pivotTime := time.Date(2024, 5, 5, 0, 30, 0, 0, time.UTC)
	metadata, err := structpb.NewStruct(map[string]any{"source": "test"})
	require.NoError(t, err)

	// Create a site to attach the forecast to
	siteResp, err := dc.CreateLocation(t.Context(), &pb.CreateLocationRequest{
		LocationName:           "test_create_forecast_site",
		GeometryWkt:            "POINT(-0.1 51.5)",
		EffectiveCapacityWatts: 1000000,
		Metadata:               metadata,
		EnergySource:           pb.EnergySource_ENERGY_SOURCE_SOLAR,
		LocationType:           pb.LocationType_LOCATION_TYPE_SITE,
		ValidFromUtc:           timestamppb.New(pivotTime.Add(-time.Hour * 24)),
	})
	require.NoError(t, err)

	// Create a forecaster
	fcResp, err := dc.CreateForecaster(t.Context(), &pb.CreateForecasterRequest{
		Name:    "test_create_forecast_forecaster",
		Version: "v1",
	})
	require.NoError(t, err)

	yieldsPopulated := make([]*pb.CreateForecastRequest_ForecastValue, 10)
	for i := range yieldsPopulated {
		yieldsPopulated[i] = &pb.CreateForecastRequest_ForecastValue{
			HorizonMins: uint32(i * 30),
			P50Fraction: 0.5 + float32(i)*0.05,
			OtherStatisticsFractions: map[string]float32{
				"p10": 0.4 + float32(i)*0.05,
				"p90": 0.6 + float32(i)*0.05,
			},
			Metadata: metadata,
		}
	}

	yieldsZeros := make([]*pb.CreateForecastRequest_ForecastValue, 10)
	for i := range yieldsZeros {
		yieldsZeros[i] = &pb.CreateForecastRequest_ForecastValue{
			HorizonMins:              uint32(i * 30),
			P50Fraction:              0.0,
			OtherStatisticsFractions: map[string]float32{},
			Metadata:                 nil,
		}
	}

	yieldsInvalid := make([]*pb.CreateForecastRequest_ForecastValue, 10)
	for i := range yieldsInvalid {
		yieldsInvalid[i] = &pb.CreateForecastRequest_ForecastValue{
			HorizonMins: uint32(i * 30),
			P50Fraction: 1.2,
			OtherStatisticsFractions: map[string]float32{
				"p10": 1.3,
				"p90": -0.2,
			},
			Metadata: metadata,
		}
	}

	testcases := []struct {
		name string
		req  *pb.CreateForecastRequest
	}{
		{
			name: "Should create forecast with populated values",
			req: &pb.CreateForecastRequest{
				LocationUuid: siteResp.LocationUuid,
				Forecaster:   fcResp.Forecaster,
				EnergySource: pb.EnergySource_ENERGY_SOURCE_SOLAR,
				InitTimeUtc:  timestamppb.New(pivotTime),
				Values:       yieldsPopulated,
			},
		},
		{
			name: "Should create forecast with zeroed values",
			req: &pb.CreateForecastRequest{
				LocationUuid: siteResp.LocationUuid,
				Forecaster:   fcResp.Forecaster,
				EnergySource: pb.EnergySource_ENERGY_SOURCE_SOLAR,
				InitTimeUtc:  timestamppb.New(pivotTime),
				Values:       yieldsZeros,
			},
		},
		{
			name: "Shouldn't create forecast with no values",
			req: &pb.CreateForecastRequest{
				LocationUuid: siteResp.LocationUuid,
				Forecaster:   fcResp.Forecaster,
				EnergySource: pb.EnergySource_ENERGY_SOURCE_SOLAR,
				InitTimeUtc:  timestamppb.New(pivotTime),
				Values:       []*pb.CreateForecastRequest_ForecastValue{},
			},
		},
		{
			name: "Shouldn't create forecast for non-existent location source",
			req: &pb.CreateForecastRequest{
				LocationUuid: siteResp.LocationUuid,
				Forecaster:   fcResp.Forecaster,
				EnergySource: pb.EnergySource_ENERGY_SOURCE_WIND,
				InitTimeUtc:  timestamppb.New(pivotTime),
				Values:       yieldsPopulated,
			},
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
				Values:       yieldsPopulated,
			},
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
				Values:       yieldsPopulated,
			},
		},
		{
			name: "Shouldn't create forecast with invalid value fractions",
			req: &pb.CreateForecastRequest{
				LocationUuid: siteResp.LocationUuid,
				Forecaster:   fcResp.Forecaster,
				EnergySource: pb.EnergySource_ENERGY_SOURCE_SOLAR,
				InitTimeUtc:  timestamppb.New(pivotTime),
				Values:       yieldsInvalid,
			},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := dc.CreateForecast(t.Context(), tc.req)
			if strings.Contains(tc.name, "Shouldn't") {
				require.Error(t, err)
			} else {
				require.NoError(t, err)

				fResp, err := dc.GetForecastAsTimeseries(t.Context(), &pb.GetForecastAsTimeseriesRequest{
					LocationUuid: siteResp.LocationUuid,
					EnergySource: pb.EnergySource_ENERGY_SOURCE_SOLAR,
					HorizonMins:  0,
					TimeWindow: &pb.TimeWindow{
						StartTimestampUtc: tc.req.InitTimeUtc,
						EndTimestampUtc:   timestamppb.New(tc.req.InitTimeUtc.AsTime().Add(5 * time.Hour)),
					},
					Forecaster: fcResp.Forecaster,
				})
				require.NoError(t, err)
				require.Equal(t, len(tc.req.Values), len(fResp.Values))
				_, err = dc.DeleteForecast(t.Context(), &pb.DeleteForecastRequest{
					ForecastUuid: resp.ForecastUuid,
				})
				require.NoError(t, err)
			}
		})
	}
}

func TestGetLatestForecasts(t *testing.T) {
	pivotTime := time.Date(2022, 1, 5, 12, 0, 0, 0, time.UTC)
	metadata, err := structpb.NewStruct(map[string]any{"source": "test"})
	require.NoError(t, err)

	// Create a site to attach the forecasts to
	siteResp, err := dc.CreateLocation(t.Context(), &pb.CreateLocationRequest{
		LocationName:           "test_get_latest_forecasts_site",
		GeometryWkt:            "POINT(-0.6 51.8)",
		EffectiveCapacityWatts: 1000000,
		Metadata:               metadata,
		EnergySource:           pb.EnergySource_ENERGY_SOURCE_SOLAR,
		LocationType:           pb.LocationType_LOCATION_TYPE_SITE,
		ValidFromUtc:           timestamppb.New(pivotTime.Add(-time.Hour * 24)),
	})
	require.NoError(t, err)

	// Create forecaster
	forecasterResp, err := dc.CreateForecaster(t.Context(), &pb.CreateForecasterRequest{
		Name:    "test_get_latest_forecasts_forecaster",
		Version: "v1",
	})
	require.NoError(t, err)
	// Update the forecaster a couple of times
	for i := range 2 {
		_, err = dc.UpdateForecaster(t.Context(), &pb.UpdateForecasterRequest{
			Name:       forecasterResp.Forecaster.ForecasterName,
			NewVersion: fmt.Sprintf("v%d", i+2),
		})
		require.NoError(t, err)
	}

	yields := make([]*pb.CreateForecastRequest_ForecastValue, 10)
	for i := range yields {
		yields[i] = &pb.CreateForecastRequest_ForecastValue{
			HorizonMins: uint32(i * 30),
			P50Fraction: 0.5 + float32(i)*0.05,
			OtherStatisticsFractions: map[string]float32{
				"p10": 0.4 + float32(i)*0.05,
				"p90": 0.6 + float32(i)*0.05,
			},
			Metadata: metadata,
		}
	}

	// Make a forecast at differring times for each forecaster
	for i := range 3 {
		req := &pb.CreateForecastRequest{
			LocationUuid: siteResp.LocationUuid,
			Forecaster: &pb.Forecaster{
				ForecasterName:    forecasterResp.Forecaster.ForecasterName,
				ForecasterVersion: fmt.Sprintf("v%d", i+1),
			},
			EnergySource: pb.EnergySource_ENERGY_SOURCE_SOLAR,
			InitTimeUtc:  timestamppb.New(pivotTime.Add(time.Duration(-i) * time.Hour)),
			Values:       yields,
		}
		_, err = dc.CreateForecast(t.Context(), req)
		require.NoError(t, err)
	}

	testcases := []struct {
		name              string
		req               *pb.GetLatestForecastsRequest
		expectedInitTimes []time.Time
	}{
		{
			name: "Should return latest forecasts for each forecaster name",
			req: &pb.GetLatestForecastsRequest{
				LocationUuid:      siteResp.LocationUuid,
				EnergySource:      pb.EnergySource_ENERGY_SOURCE_SOLAR,
				PivotTimestampUtc: timestamppb.New(pivotTime),
			},
			expectedInitTimes: []time.Time{
				pivotTime,
			},
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
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := dc.GetLatestForecasts(t.Context(), tc.req)
			if strings.Contains(tc.name, "Shouldn't") {
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

// --- BENCHMARKS ---------------------------------------------------------------------------------

// BenchmarkPostgresClient runs benchmarks against the Postgres client.
// It does not test for the validity of the responses, as these are covered in the unit test cases.
// Instead it determines how long each RPC takes to complete against a database of a given size.
func BenchmarkPostgresClient(b *testing.B) {
	zerolog.SetGlobalLevel(zerolog.InfoLevel)

	pivotTime := time.Now().Truncate(24 * time.Hour).Add(-7 * 24 * time.Hour)
	metadata, err := structpb.NewStruct(map[string]any{"source": "test"})
	require.NoError(b, err)

	testcases := []seedDBParams{
		{
			TargetTotalForecasts: 100000,
			PivotTime:            pivotTime,
		},
	}
	for _, tc := range testcases {
		output := seed(b, pgConnString, tc)

		// Create some test yields
		yields := make([]*pb.CreateForecastRequest_ForecastValue, 48)
		for i := range yields {
			yields[i] = &pb.CreateForecastRequest_ForecastValue{
				HorizonMins: uint32(i * 30),
				P50Fraction: 0.5,
				OtherStatisticsFractions: map[string]float32{
					"p10": 0.5,
					"p90": 0.5,
				},
				Metadata: metadata,
			}
		}

		b.Run(fmt.Sprintf("%d/GetForecastAsTimeseries", output.NumPgvs), func(b *testing.B) {
			for b.Loop() {
				resp, err := dc.GetForecastAsTimeseries(
					b.Context(),
					&pb.GetForecastAsTimeseriesRequest{
						LocationUuid: output.LocationUuids[0],
						EnergySource: pb.EnergySource_ENERGY_SOURCE_SOLAR,
						Forecaster: &pb.Forecaster{
							ForecasterName:    tc.NamePrefix + "_forecaster_1",
							ForecasterVersion: "v1",
						},
						TimeWindow: &pb.TimeWindow{
							StartTimestampUtc: timestamppb.New(pivotTime.Add(-time.Hour * 48)),
							EndTimestampUtc:   timestamppb.New(pivotTime.Add(time.Hour * 36)),
						},
					},
				)
				require.NoError(b, err)
				// There is a forecast value every 30 minutes, and the window is 84 hours long
				// But the forecast made at the pivot time is the latest one, and that is only
				// 12 hours long
				require.GreaterOrEqual(
					b,
					len(resp.Values),
					(48+12)*60/30,
				)
			}
		})
		b.Run(fmt.Sprintf("%d/GetForecastAtTimestamp", output.NumPgvs), func(b *testing.B) {
			if len(output.LocationUuids) > 100 {
				output.LocationUuids = output.LocationUuids[0:100]
			}

			for b.Loop() {
				crossSectionResp, err := dc.GetForecastAtTimestamp(
					b.Context(),
					&pb.GetForecastAtTimestampRequest{
						EnergySource:  pb.EnergySource_ENERGY_SOURCE_SOLAR,
						LocationUuids: output.LocationUuids,
						Forecaster: &pb.Forecaster{
							ForecasterName:    tc.NamePrefix + "_forecaster_1",
							ForecasterVersion: "v1",
						},
						TimestampUtc: timestamppb.New(pivotTime),
					},
				)
				require.NoError(b, err)
				require.NotNil(b, crossSectionResp)
				require.Equal(b, len(output.LocationUuids), len(crossSectionResp.Values))
			}
		})
		b.Run(fmt.Sprintf("%d/GetObservationsAsTimeseries", output.NumPgvs), func(b *testing.B) {
			for b.Loop() {
				obsResp, err := dc.GetObservationsAsTimeseries(
					b.Context(),
					&pb.GetObservationsAsTimeseriesRequest{
						LocationUuid: output.LocationUuids[0],
						ObserverName: tc.NamePrefix + "_observer",
						EnergySource: pb.EnergySource_ENERGY_SOURCE_SOLAR,
						TimeWindow: &pb.TimeWindow{
							StartTimestampUtc: timestamppb.New(pivotTime.Add(-time.Hour * 36)),
							EndTimestampUtc:   timestamppb.New(pivotTime),
						},
					},
				)
				require.NoError(b, err)
				require.GreaterOrEqual(b, len(obsResp.Values), 36*60/30)
			}
		})
		b.Run(fmt.Sprintf("%d/CreateForecast", output.NumPgvs), func(b *testing.B) {
			for b.Loop() {
				resp, err := dc.CreateForecast(b.Context(), &pb.CreateForecastRequest{
					Forecaster: &pb.Forecaster{
						ForecasterName:    tc.NamePrefix + "_forecaster_1",
						ForecasterVersion: "v1",
					},
					LocationUuid: output.LocationUuids[0],
					EnergySource: pb.EnergySource_ENERGY_SOURCE_SOLAR,
					InitTimeUtc: timestamppb.New(
						pivotTime.Add(
							time.Duration(12+2) * time.Hour,
						),
					),
					Values: yields,
				})
				require.NoError(b, err)
				_, err = dc.DeleteForecast(b.Context(), &pb.DeleteForecastRequest{
					ForecastUuid: resp.ForecastUuid,
				})
				require.NoError(b, err)
			}
		})
	}
}
