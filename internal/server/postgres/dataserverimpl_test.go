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
					"num_locations=>%d,"+
					"gv_resolution_mins=>%d,"+
					"forecast_resolution_mins=>%d,"+
					"forecast_length_mins=>%d,"+
					"num_forecasts_per_location=>%d,"+
					"pivot_time=>'%s'::TIMESTAMP"+
					");",
				params.NamePrefix,
				params.NumLocations,
				params.PgvResolutionMins,
				params.ForecastResolutionMins,
				params.ForecastLengthHours*60,
				params.NumForecastsPerLocation,
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
	NamePrefix              string
	NumLocations            int
	NumForecasters          int
	NumForecastsPerLocation int
	PgvResolutionMins       int
	ForecastResolutionMins  int
	ForecastLengthHours     int
	PivotTime               time.Time
}

func (s *seedDBParams) NumPgvsPerForecast() int {
	return (s.ForecastLengthHours * 60) / s.PgvResolutionMins
}

func (s *seedDBParams) NumPgvRows() int {
	return s.NumLocations * s.NumForecastsPerLocation * s.NumPgvsPerForecast()
}

// --- Tests --------------------------------------------------------------------------------------

func TestCapacityToMultiplier(t *testing.T) {
	type TestCase struct {
		capacityWatts      uint64
		expectedValue      int16
		expectedMultiplier int16
		shouldError        bool
	}

	testcases := []TestCase{
		{0, 0, 0, false},
		{500000, 500, 3, false},
		{32767000, 32767, 3, false},
		{32768000, 33, 6, false}, // Needs rounding, should go to 33 MW
		{33000000, 33, 6, false},
		{1000000000000, 1000, 9, false}, // 1TW
		{12345678000, 12346, 6, false},  // 12 GW
	}

	for _, test := range testcases {
		t.Run(fmt.Sprintf("capacityWatts=%d", test.capacityWatts), func(t *testing.T) {
			capacity, prefix, err := capacityToValueMultiplier(test.capacityWatts)
			if test.shouldError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.expectedValue, capacity)
				require.Equal(t, test.expectedMultiplier, prefix)
			}
		})
	}
}

func TestCreateLocation(t *testing.T) {
	metadata, err := structpb.NewStruct(map[string]any{"source": "test"})
	require.NoError(t, err)

	testcases := []struct {
		name string
		req  *pb.CreateLocationRequest
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

func TestUpdateLocationCapacity(t *testing.T) {
	metadata, err := structpb.NewStruct(map[string]any{"source": "test"})
	require.NoError(t, err)

	pivotTime := time.Now().Truncate(time.Minute)

	createResp, err := dc.CreateLocation(t.Context(), &pb.CreateLocationRequest{
		LocationName:           "test_update_location_capacity_site",
		GeometryWkt:            "POINT(-0.1 51.5)",
		EffectiveCapacityWatts: 1000000,
		Metadata:               metadata,
		EnergySource:           pb.EnergySource_ENERGY_SOURCE_SOLAR,
		LocationType:           pb.LocationType_LOCATION_TYPE_SITE,
		ValidFromUtc:           timestamppb.New(pivotTime.Add(-1 * time.Hour)),
	})

	require.NoError(t, err)

	testcases := []struct {
		name string
		req  *pb.UpdateLocationCapacityRequest
	}{
		{
			name: "Should update capacity to higher value",
			req: &pb.UpdateLocationCapacityRequest{
				LocationUuid:              createResp.LocationUuid,
				EnergySource:              pb.EnergySource_ENERGY_SOURCE_SOLAR,
				NewEffectiveCapacityWatts: 2000000,
			},
		},
		{
			name: "Shouldn't update capacity with invalid location uuid",
			req: &pb.UpdateLocationCapacityRequest{
				LocationUuid:              "invalid-uuid",
				EnergySource:              pb.EnergySource_ENERGY_SOURCE_SOLAR,
				NewEffectiveCapacityWatts: 1500000,
				ValidFromUtc:              timestamppb.New(pivotTime.Add(3 * time.Hour)),
			},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := dc.UpdateLocationCapacity(t.Context(), tc.req)

			if strings.Split(tc.name, " ")[0] == "Shouldn't" {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				// TODO: Can't test this worked without putting a time into GetLocation request
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
			P10Fraction: float32(0.4 + float64(i)*0.05),
			P90Fraction: float32(0.6 + float64(i)*0.05),
			Metadata:    metadata,
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
	pivotTime := time.Date(2025, 1, 5, 12, 0, 0, 0, time.UTC)
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
	_, err = dc.UpdateLocationCapacity(t.Context(), &pb.UpdateLocationCapacityRequest{
		LocationUuid:              siteResp.LocationUuid,
		EnergySource:              pb.EnergySource_ENERGY_SOURCE_SOLAR,
		NewEffectiveCapacityWatts: 1500000,
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
			P10Fraction: max(float32(i-1)*float32(100/len(yields))/100.0, 0),
			P90Fraction: min(float32(i+1)*float32(100/len(yields))/100.0, 1.1),
			Metadata:    metadata,
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
		name 		   string
		horizonMins    int32
		expectedValues []float32
	}{
		{
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
			name: "Should return expected values for horizon 0 mins",
			horizonMins: 0,
			expectedValues: []float32{
				0.00, 0.08, 0.16, 0.24, 0.32, 0.40,
				0.00, 0.08, 0.16, 0.24, 0.32, 0.40,
				0.00, 0.08, 0.16, 0.24, 0.32, 0.40,
				0.00, 0.08, 0.16, 0.24, 0.32, 0.40, 0.48, 0.56, 0.64, 0.72, 0.80, 0.88,
			},
		},
		{
			// For horizon of 14 minutes, anything with a lesser horizon should not be included.
			// So the value for 0, 5, and 10 minutes should not be included.
			name: "Should return expected values for horizon 14 mins",
			horizonMins: 14,
			expectedValues: []float32{
				0.24, 0.32, 0.40, 0.48, 0.56, 0.64,
				0.24, 0.32, 0.40, 0.48, 0.56, 0.64,
				0.24, 0.32, 0.40, 0.48, 0.56, 0.64,
				0.24, 0.32, 0.40, 0.48, 0.56, 0.64, 0.72, 0.80, 0.88,
			},
		},
		{
			name: "Should return expected values for horizon 30 mins",
			horizonMins: 30,
			expectedValues: []float32{
				0.48, 0.56, 0.64, 0.72, 0.80, 0.88,
				0.48, 0.56, 0.64, 0.72, 0.80, 0.88,
				0.48, 0.56, 0.64, 0.72, 0.80, 0.88,
				0.48, 0.56, 0.64, 0.72, 0.80, 0.88,
			},
		},
		{
			name: "Shouldn't return successfully for horizon 60 mins",
			horizonMins:    60,
		},
	}

	for _, tc := range testcases {
		t.Run(fmt.Sprintf("Horizon %d mins", tc.horizonMins), func(t *testing.T) {
			resp, err := dc.GetForecastAsTimeseries(t.Context(), &pb.GetForecastAsTimeseriesRequest{
				LocationUuid: siteResp.LocationUuid,
				HorizonMins:  uint32(tc.horizonMins),
				Forecaster:   forecasterResp.Forecaster,
				EnergySource: pb.EnergySource_ENERGY_SOURCE_SOLAR,
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
			ValueFraction:          rand.Float32(),
			EffectiveCapacityWatts: siteResp.EffectiveCapacityWatts,
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

func TestGetLatestObservation(t *testing.T) {
	pivotTime := time.Now().Truncate(time.Minute)

	// Create a site to attach the observations to
	siteResp, err := dc.CreateLocation(t.Context(), &pb.CreateLocationRequest{
		LocationName:           "test_get_latest_observation_site",
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
		Name: "test_get_latest_observation_observer",
	})
	require.NoError(t, err)

	// Seed some observations for the site.
	values := []*pb.CreateObservationsRequest_Value{
		{
			TimestampUtc:           timestamppb.New(pivotTime.Add(-time.Hour * 2)),
			ValueFraction:          0.3,
			EffectiveCapacityWatts: siteResp.EffectiveCapacityWatts,
		},
		{
			TimestampUtc:           timestamppb.New(pivotTime.Add(-time.Hour * 1)),
			ValueFraction:          0.5,
			EffectiveCapacityWatts: siteResp.EffectiveCapacityWatts,
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
		name             string
		req              *pb.GetLatestObservationRequest
		expectedFraction float32
	}{
		{
			name: "Should get latest observation",
			req: &pb.GetLatestObservationRequest{
				LocationUuid: siteResp.LocationUuid,
				EnergySource: pb.EnergySource_ENERGY_SOURCE_SOLAR,
				ObserverName: obsResp.ObserverName,
			},
			expectedFraction: 0.5,
		},
		{
			name: "Should get earlier observation before cutoff",
			req: &pb.GetLatestObservationRequest{
				LocationUuid:      siteResp.LocationUuid,
				EnergySource:      pb.EnergySource_ENERGY_SOURCE_SOLAR,
				ObserverName:      obsResp.ObserverName,
				PivotTimestampUtc: timestamppb.New(pivotTime.Add(-time.Hour * 1).Add(-time.Second)),
			},
			expectedFraction: 0.3,
		},
		{
			name: "Shouldn't fetch for non-existent observer",
			req: &pb.GetLatestObservationRequest{
				LocationUuid: siteResp.LocationUuid,
				EnergySource: pb.EnergySource_ENERGY_SOURCE_SOLAR,
				ObserverName: "non_existent_observer",
			},
		},
		{
			name: "Shouldn't fetch for non-existent location",
			req: &pb.GetLatestObservationRequest{
				LocationUuid: "non_existent_location",
				EnergySource: pb.EnergySource_ENERGY_SOURCE_SOLAR,
				ObserverName: obsResp.ObserverName,
			},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := dc.GetLatestObservation(t.Context(), tc.req)
			if strings.Contains(tc.name, "Shouldn't") {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.NotNil(t, resp)
				require.Equal(t, tc.expectedFraction, resp.ValueFraction)
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
			ValueFraction:          0.5,
			EffectiveCapacityWatts: siteResp.EffectiveCapacityWatts,
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
			P10Fraction: max(float32(i-1)*float32(100/len(yields))/100.0, 0),
			P90Fraction: min(float32(i+1)*float32(100/len(yields))/100.0, 1.1),
			Metadata:    metadata,
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
		LocationUuid:   siteResp.LocationUuid,
		EnergySource:   pb.EnergySource_ENERGY_SOURCE_SOLAR,
		Forecaster:     forecasterResp.Forecaster,
		ObserverName:   obsResp.ObserverName,
		PivotTimestamp: timestamppb.New(pivotTime),
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
			P10Fraction: 0.4 + float32(i)*0.05,
			P90Fraction: 0.6 + float32(i)*0.05,
			Metadata:    metadata,
		}
	}

	yieldsZeros := make([]*pb.CreateForecastRequest_ForecastValue, 10)
	for i := range yieldsZeros {
		yieldsZeros[i] = &pb.CreateForecastRequest_ForecastValue{
			HorizonMins: uint32(i * 30),
			P50Fraction: 0.0,
			P10Fraction: 0.0,
			P90Fraction: 0.0,
			Metadata:    nil,
		}
	}

	yieldsInvalid := make([]*pb.CreateForecastRequest_ForecastValue, 10)
	for i := range yieldsInvalid {
		yieldsInvalid[i] = &pb.CreateForecastRequest_ForecastValue{
			HorizonMins: uint32(i * 30),
			P50Fraction: 1.2,
			P10Fraction: -0.1,
			P90Fraction: 0.5,
			Metadata:    metadata,
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
				InitTimeUtc:  timestamppb.New(pivotTime.Add(6 * time.Hour)),
				Values:       yieldsZeros,
			},
		},
		{
			name: "Shouldn't create forecast with no values",
			req: &pb.CreateForecastRequest{
				LocationUuid: siteResp.LocationUuid,
				Forecaster:   fcResp.Forecaster,
				EnergySource: pb.EnergySource_ENERGY_SOURCE_SOLAR,
				InitTimeUtc:  timestamppb.New(pivotTime.Add(12 * time.Hour)),
				Values:       []*pb.CreateForecastRequest_ForecastValue{},
			},
		},
		{
			name: "Shouldn't create forecast for non-existent location source",
			req: &pb.CreateForecastRequest{
				LocationUuid: siteResp.LocationUuid,
				Forecaster:   fcResp.Forecaster,
				EnergySource: pb.EnergySource_ENERGY_SOURCE_WIND,
				InitTimeUtc:  timestamppb.New(pivotTime.Add(18 * time.Hour)),
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
				InitTimeUtc:  timestamppb.New(pivotTime.Add(24 * time.Hour)),
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
				InitTimeUtc:  timestamppb.New(pivotTime.Add(30 * time.Hour)),
				Values:       yieldsPopulated,
			},
		},
		{
			name: "Shouldn't create forecast with invalid value fractions",
			req: &pb.CreateForecastRequest{
				LocationUuid: siteResp.LocationUuid,
				Forecaster:   fcResp.Forecaster,
				EnergySource: pb.EnergySource_ENERGY_SOURCE_SOLAR,
				InitTimeUtc:  timestamppb.New(pivotTime.Add(36 * time.Hour)),
				Values:       yieldsInvalid,
			},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := dc.CreateForecast(t.Context(), tc.req)
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
			P10Fraction: 0.4 + float32(i)*0.05,
			P90Fraction: 0.6 + float32(i)*0.05,
			Metadata:    metadata,
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
			name: "Should return latest forecasts for all forecasters",
			req: &pb.GetLatestForecastsRequest{
				LocationUuid:      siteResp.LocationUuid,
				EnergySource:      pb.EnergySource_ENERGY_SOURCE_SOLAR,
				PivotTimestampUtc: timestamppb.New(pivotTime),
			},
			expectedInitTimes: []time.Time{
				pivotTime,
				pivotTime.Add(-time.Hour * 1),
				pivotTime.Add(-time.Hour * 2),
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
				pivotTime.Add(-time.Hour * 2),
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

	pivotTime := time.Date(2010, 1, 1, 1, 0, 0, 0, time.UTC)
	metadata, err := structpb.NewStruct(map[string]any{"source": "test"})
	require.NoError(b, err)

	testcases := []seedDBParams{
		{
			NamePrefix:              "benchmark_6m",
			NumLocations:            500,
			PgvResolutionMins:       30,
			ForecastResolutionMins:  30,
			ForecastLengthHours:     24,
			NumForecastsPerLocation: 256,
			PivotTime:               pivotTime,
		},
	}
	for _, tc := range testcases {
		output := seed(b, pgConnString, tc)

		// Create some test yields
		yields := make([]*pb.CreateForecastRequest_ForecastValue, tc.NumPgvsPerForecast())
		for i := range yields {
			yields[i] = &pb.CreateForecastRequest_ForecastValue{
				HorizonMins: uint32(i * 30),
				P50Fraction: 0.5,
				P10Fraction: 0.5,
				P90Fraction: 0.5,
				Metadata:    metadata,
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
							ForecasterName:    tc.NamePrefix + "_forecaster",
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
				// tc.ForecastLengthHours long
				require.Equal(
					b,
					(48+tc.ForecastLengthHours)*60/tc.PgvResolutionMins,
					len(resp.Values),
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
							ForecasterName:    tc.NamePrefix + "_forecaster",
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
				require.GreaterOrEqual(b, len(obsResp.Values), 36*60/tc.PgvResolutionMins)
			}
		})
		b.Run(fmt.Sprintf("%d/CreateForecast", output.NumPgvs), func(b *testing.B) {
			for b.Loop() {
				_, err := dc.CreateForecast(b.Context(), &pb.CreateForecastRequest{
					Forecaster: &pb.Forecaster{
						ForecasterName:    tc.NamePrefix + "_forecaster",
						ForecasterVersion: "v1",
					},
					LocationUuid: output.LocationUuids[0],
					EnergySource: pb.EnergySource_ENERGY_SOURCE_SOLAR,
					InitTimeUtc: timestamppb.New(
						pivotTime.Add(time.Duration(tc.ForecastLengthHours + rand.IntN(10000)) * time.Hour),
					),
					Values: yields,
				})
				require.NoError(b, err)
			}
		})
	}
}
