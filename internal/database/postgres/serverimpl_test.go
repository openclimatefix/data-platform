package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"buf.build/go/protovalidate"
	pb "github.com/devsjc/fcfs/dp/internal/gen/ocf/dp"
	"github.com/google/uuid"
	middleware "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/protovalidate"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/rs/zerolog"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/structpb"
	timestamppb "google.golang.org/protobuf/types/known/timestamppb"

	"github.com/stretchr/testify/require"
)

// --- HELPERS ------------------------------------------------------------------------------------

type TBLogConsumer struct{ tb testing.TB }

func (lc *TBLogConsumer) Accept(l testcontainers.Log) {
	if strings.Contains(string(l.Content), "NOTICE:") {
		lc.tb.Logf("pgcontainer: %s", l.Content)
	}
}

func createPostgresContainer(tb testing.TB) string {
	tb.Helper()

	g := TBLogConsumer{tb: tb}

	req := testcontainers.ContainerRequest{
		FromDockerfile: testcontainers.FromDockerfile{
			Context:    filepath.Join(".", "infra"),
			Dockerfile: "Containerfile",
			KeepImage:  true,
		},
		Env: map[string]string{
			"POSTGRES_USER":     "postgres",
			"POSTGRES_PASSWORD": "postgres",
			"POSTGRES_DB":       "postgres",
		},
		Cmd:          []string{"postgres", "-c", "fsync=off"},
		ExposedPorts: []string{"5432/tcp"},
		WaitingFor: wait.ForAll(
			wait.ForLog(
				"database system is ready to accept connections",
			).WithOccurrence(2),
			wait.ForListeningPort("5432/tcp"),
		),
		LogConsumerCfg: &testcontainers.LogConsumerConfig{
			Opts:      []testcontainers.LogProductionOption{testcontainers.WithLogProductionTimeout(10 * time.Second)},
			Consumers: []testcontainers.LogConsumer{&g},
		},
	}
	pgC, err := testcontainers.GenericContainer(
		tb.Context(),
		testcontainers.GenericContainerRequest{
			ContainerRequest: req,
			Started:          true,
		},
	)
	require.NoError(tb, err)
	containerPort, err := pgC.MappedPort(tb.Context(), "5432/tcp")
	require.NoError(tb, err)
	host, err := pgC.Host(tb.Context())
	require.NoError(tb, err)

	pgConnString := fmt.Sprintf(
		"postgres://postgres:postgres@%s/postgres",
		net.JoinHostPort(host, containerPort.Port()),
	)

	tb.Cleanup(func() {
		testcontainers.CleanupContainer(tb, pgC)
	})

	tb.Logf("Postgres container started at %s", pgConnString)
	return pgConnString
}

func seed(tb testing.TB, pgConnString string, params seedDBParams) (output struct {
	NumPgvs       int
	LocationUuids []string
},
) {
	seedfiles, _ := filepath.Glob(filepath.Join(".", "testdata", "seed*.sql"))
	conn, err := pgx.Connect(tb.Context(), pgConnString)
	require.NoError(tb, err)
	defer conn.Close(tb.Context())

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
					"num_locations=>%d, gv_resolution_mins=>%d, forecast_resolution_mins=>%d,"+
					"forecast_length_mins=>%d, num_forecasts_per_location=>%d, pivot_time=>'%s'::timestamp"+
					");",
				params.NumLocations, params.PgvResolutionMins, params.ForecastResolutionMins,
				params.ForecastLengthHours*60, params.NumForecastsPerLocation,
				params.PivotTime.UTC().Format(time.RFC3339),
			),
		).Scan(&result)
		require.NoError(tb, err)
		tb.Logf("Seeded %d predicted generation values for %d locations", output.NumPgvs, len(output.LocationUuids))
		stringUuids := make([]string, len(result.LocationUuids))
		for i, u := range result.LocationUuids {
			stringUuids[i] = u.String()
		}
		output.NumPgvs = result.NumPgvs
		output.LocationUuids = stringUuids
	}
	return output
}

// Create a GRPC client for running tests with
func setupClient(tb testing.TB, pgConnString string) pb.DataPlatformServiceClient {
	tb.Helper()
	// Create server using in-memory listener

	validator, err := protovalidate.New()
	require.NoError(tb, err)
	s := grpc.NewServer(
		grpc.UnaryInterceptor(middleware.UnaryServerInterceptor(validator)),
	)
	lis := bufconn.Listen(1024 * 1024)
	pb.RegisterDataPlatformServiceServer(s, NewPostgresDataPlatformServerImpl(pgConnString))
	go func() {
		if err := s.Serve(lis); err != nil {
			tb.Fatalf("Server exited with error: %v", err)
		}
	}()
	// Create client using same in-memory listener

	bufDialer := func(context.Context, string) (net.Conn, error) {
		return lis.Dial()
	}
	cc, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(bufDialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(tb, err)
	c := pb.NewDataPlatformServiceClient(cc)
	tb.Logf("GRPC client created successfully")

	tb.Cleanup(func() {
		tb.Logf("Cleaning up server and client")
		cc.Close()
		s.GracefulStop()
		lis.Close()
	})

	return c
}

type seedDBParams struct {
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
	tests := []TestCase{
		{0, 0, 0, false},
		{500000, 500, 3, false},
		{32767000, 32767, 3, false},
		{32768000, 33, 6, false}, // Needs rounding, should go to 33 MW
		{33000000, 33, 6, false},
		{1000000000000, 1000, 9, false}, // 1TW
		{12345678000, 12346, 6, false},  // 12 GW
	}

	for _, test := range tests {
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
	c := setupClient(t, createPostgresContainer(t))
	metadata, err := structpb.NewStruct(map[string]any{"source": "test"})
	require.NoError(t, err)

	tests := []struct {
		name string
		req  *pb.CreateLocationRequest
	}{
		{
			name: "Should create solar location",
			req: &pb.CreateLocationRequest{
				LocationName:  "GREENWICH_OBSERVATORY",
				EnergySource:  pb.EnergySource_SOLAR,
				GeometryWkt:   "POINT(0.0 51.5)",
				CapacityWatts: 1230,
				LocationType:  pb.LocationType_SITE,
				Metadata:      metadata,
			},
		},
		{
			name: "Shouldn't create unknown energy source",
			req: &pb.CreateLocationRequest{
				LocationName:  "UNKNOWN_ENERGY_SOURCE",
				EnergySource:  pb.EnergySource_ENERGY_SOURCE_UNSPECIFIED,
				GeometryWkt:   "POINT(0.0 51.5)",
				CapacityWatts: 1230,
				LocationType:  pb.LocationType_SITE,
				Metadata:      metadata,
			},
		},
		{
			name: "Should create wind location",
			req: &pb.CreateLocationRequest{
				LocationName:  "LONDON_EYE",
				EnergySource:  pb.EnergySource_WIND,
				GeometryWkt:   "POINT(0.0 51.5)",
				CapacityWatts: 4560,
				LocationType:  pb.LocationType_SITE,
				Metadata:      metadata,
			},
		},
		{
			name: "Shouldn't create unknown location type",
			req: &pb.CreateLocationRequest{
				LocationName:  "UNKNOWN_LOCATION_TYPE",
				EnergySource:  pb.EnergySource_SOLAR,
				GeometryWkt:   "POINT(0.0 51.5)",
				CapacityWatts: 1230,
				LocationType:  pb.LocationType_LOCATION_TYPE_UNSPECIFIED,
				Metadata:      metadata,
			},
		},
		{
			name: "Shouldn't create location with empty name",
			req: &pb.CreateLocationRequest{
				LocationName:  "",
				EnergySource:  pb.EnergySource_SOLAR,
				GeometryWkt:   "POINT(0.0 51.5)",
				CapacityWatts: 1230,
				LocationType:  pb.LocationType_SITE,
				Metadata:      metadata,
			},
		},
		{
			name: "Should create location with large capacity",
			req: &pb.CreateLocationRequest{
				LocationName:  "OXFORDSHIRE",
				EnergySource:  pb.EnergySource_SOLAR,
				GeometryWkt:   "POLYGON((0.0 51.5, 1.0 51.5, 1.0 52.0, 0.0 52.0, 0.0 51.5))",
				CapacityWatts: 1000e9,
				LocationType:  pb.LocationType_GSP,
				Metadata:      metadata,
			},
		},
		{
			name: "Shouldn't create location with non-closed POLYGON geometry",
			req: &pb.CreateLocationRequest{
				LocationName:  "UNCLOSED_POLYGON",
				EnergySource:  pb.EnergySource_SOLAR,
				GeometryWkt:   "POLYGON((0.0 51.5, 1.0 51.5, 1.0 52.0, 0.0 52.0))",
				CapacityWatts: 14e6,
				LocationType:  pb.LocationType_DNO,
				Metadata:      metadata,
			},
		},
		{
			name: "Should create location with closed MULTIPOLYGON geometry",
			req: &pb.CreateLocationRequest{
				LocationName:  "CLOSED_MULTIPOLYGON",
				EnergySource:  pb.EnergySource_WIND,
				GeometryWkt:   "MULTIPOLYGON(((0.0 51.5, 1.0 51.5, 1.0 52.0, 0.0 52.0, 0.0 51.5)),((2.0 51.5, 3.0 51.5, 3.0 52.0, 2.0 52.0, 2.0 51.5)))",
				CapacityWatts: 1100e6,
				LocationType:  pb.LocationType_DNO,
				Metadata:      metadata,
			},
		},
		{
			name: "Shouldn't create location with non-closed MULTIPOLYGON geometry",
			req: &pb.CreateLocationRequest{
				LocationName:  "UNCLOSED_MULTIPOLYGON",
				EnergySource:  pb.EnergySource_WIND,
				GeometryWkt:   "MULTIPOLYGON(((0.0 51.5, 1.0 51.5, 1.0 52.0, 0.0 52.0)),((2.0 51.5, 3.0 51.5, 3.0 52.0, 2.0 52.0)))",
				CapacityWatts: 14e6,
				LocationType:  pb.LocationType_DNO,
				Metadata:      metadata,
			},
		},
		{
			name: "Shouldn't create location with non WSG84 geometry",
			req: &pb.CreateLocationRequest{
				LocationName:  "NON_WGS84",
				EnergySource:  pb.EnergySource_SOLAR,
				GeometryWkt:   "POINT(1000000 1000000)",
				CapacityWatts: 10289e3,
				LocationType:  pb.LocationType_SITE,
				Metadata:      metadata,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp, err := c.CreateLocation(t.Context(), tt.req)

			if strings.Split(tt.name, " ")[0] == "Shouldn't" {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				resp2, err := c.GetLocation(
					t.Context(),
					&pb.GetLocationRequest{
						LocationUuid:    resp.LocationUuid,
						EnergySource:    tt.req.EnergySource,
						IncludeGeometry: false,
					},
				)
				require.NoError(t, err)
				require.Equal(t, tt.req.LocationName, resp2.LocationName)
				// require.Equal(t, tt.req.GeometryWkt, string(resp2.GeometryWkb))
				require.Equal(t, tt.req.CapacityWatts, resp2.CapacityWatts)
				require.Equal(t, tt.req.Metadata.AsMap(), resp2.Metadata.AsMap())
			}
		})
	}
	t.Run("Shouldn't get non-existent location", func(t *testing.T) {
		_, err := c.GetLocation(
			t.Context(),
			&pb.GetLocationRequest{LocationUuid: uuid.New().String()},
		)
		require.Error(t, err)
	})
}

func TestCreateUpdateForecaster(t *testing.T) {
	c := setupClient(t, createPostgresContainer(t))

	tests := []struct {
		name      string
		createReq *pb.CreateForecasterRequest
		updateReq *pb.UpdateForecasterRequest
	}{
		{
			name: "Should create forecaster",
			createReq: &pb.CreateForecasterRequest{
				Name:    "test_model_1",
				Version: "v1",
			},
		},
		{
			name: "Should update existing forecaster",
			updateReq: &pb.UpdateForecasterRequest{
				Name:       "test_model_1",
				NewVersion: "v2",
			},
		},
		{
			name: "Shouldn't update with non-unique version",
			updateReq: &pb.UpdateForecasterRequest{
				Name:       "test_model_1",
				NewVersion: "v2",
			},
		},
		{
			name: "Shouldn't update non-existant forecaster",
			updateReq: &pb.UpdateForecasterRequest{
				Name:       "non_existent_model",
				NewVersion: "v1",
			},
		},
		{
			name: "Shouldn't create existing forecaster",
			createReq: &pb.CreateForecasterRequest{
				Name:    "test_model_1",
				Version: "v2",
			},
		},
		{
			name: "Shouldn't create forecaster with invalid name",
			createReq: &pb.CreateForecasterRequest{
				Name:    "DROP TABLE USERS;",
				Version: "v1",
			},
		},
	}

	for _, tt := range tests {
		var err error
		if tt.createReq != nil {
			_, err = c.CreateForecaster(t.Context(), tt.createReq)
		}
		if tt.updateReq != nil {
			_, err = c.UpdateForecaster(t.Context(), tt.updateReq)
		}
		if strings.Split(tt.name, " ")[0] == "Shouldn't" {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
		}
	}
}

func TestGetForecastAtTimestamp(t *testing.T) {
	pivotTime := time.Date(2025, 1, 5, 12, 0, 0, 0, time.UTC)
	pgConnString := createPostgresContainer(t)
	c := setupClient(t, pgConnString)
	// Create 100 locations with 4 forecasts each
	output := seed(t, pgConnString, seedDBParams{
		NumLocations:            100,
		NumForecastsPerLocation: 1,
		PgvResolutionMins:       30,
		ForecastResolutionMins:  30,
		ForecastLengthHours:     1,
		PivotTime:               pivotTime,
	})

	crossSectionResp, err := c.GetForecastAtTimestamp(t.Context(), &pb.GetForecastAtTimestampRequest{
		EnergySource:  pb.EnergySource_SOLAR,
		TimestampUtc:  timestamppb.New(pivotTime),
		LocationUuids: output.LocationUuids,
		Forecaster:    &pb.Forecaster{ForecasterName: "test_model_1", ForecasterVersion: "v1"},
	})
	require.NoError(t, err)
	require.NotNil(t, crossSectionResp)
	require.Len(t, crossSectionResp.Values, len(output.LocationUuids))
}

func TestGetLocationsAsGeoJSON(t *testing.T) {
	c := setupClient(t, createPostgresContainer(t))

	// Create some locations
	siteUuids := make([]string, 3)
	for i := range siteUuids {
		resp, err := c.CreateLocation(t.Context(), &pb.CreateLocationRequest{
			LocationName:  fmt.Sprintf("TESTSITE%02d", i),
			GeometryWkt:   fmt.Sprintf("POINT(%f %f)", -0.1+float32(i)*0.01, 51.5+float32(i)*0.01),
			CapacityWatts: uint64(1000000 + i*100),
			EnergySource:  pb.EnergySource_SOLAR,
			Metadata:      &structpb.Struct{},
			LocationType:  pb.LocationType_SITE,
		})
		require.NoError(t, err)
		siteUuids[i] = resp.LocationUuid
	}

	geojson, err := c.GetLocationsAsGeoJSON(t.Context(), &pb.GetLocationsAsGeoJSONRequest{
		LocationUuids: siteUuids,
	})
	require.NoError(t, err)
	var result map[string]any
	json.Unmarshal([]byte(geojson.Geojson), &result)
	features := result["features"].([]any)
	require.Equal(t, len(siteUuids), len(features))
}

func TestGetForecastAsTimeseries(t *testing.T) {
	pivotTime := time.Date(2025, 1, 5, 12, 0, 0, 0, time.UTC)
	pgConnString := createPostgresContainer(t)
	c := setupClient(t, pgConnString)

	// Create four forecasts, each half an hour apart, up to the latestForecastTime
	// Give each forecast one hour's worth of predicted generation values, occurring every 5 minutes
	output := seed(t, pgConnString, seedDBParams{
		NumLocations:            1,
		NumForecasters:          1,
		NumForecastsPerLocation: 4,
		PgvResolutionMins:       5,
		ForecastResolutionMins:  30,
		ForecastLengthHours:     1,
		PivotTime:               pivotTime,
	})

	// For each horizon, get the predicted timeseries
	tests := []struct {
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
			horizonMins: 0,
			expectedValues: []float32{
				0, 8, 16, 24, 32, 40,
				0, 8, 16, 24, 32, 40,
				0, 8, 16, 24, 32, 40,
				0, 8, 16, 24, 32, 40, 48, 56, 64, 72, 80, 88,
			},
		},
		{
			// For horizon of 14 minutes, anything with a lesser horizon should not be included.
			// So the value for 0, 5, and 10 minutes should not be included.
			horizonMins: 14,
			expectedValues: []float32{
				24, 32, 40, 48, 56, 64,
				24, 32, 40, 48, 56, 64,
				24, 32, 40, 48, 56, 64,
				24, 32, 40, 48, 56, 64, 72, 80, 88,
			},
		},
		{
			horizonMins: 30,
			expectedValues: []float32{
				48, 56, 64, 72, 80, 88,
				48, 56, 64, 72, 80, 88,
				48, 56, 64, 72, 80, 88,
				48, 56, 64, 72, 80, 88,
			},
		},
		{
			horizonMins:    60,
			expectedValues: []float32{},
		},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("Horizon %d mins", tt.horizonMins), func(t *testing.T) {
			resp, err := c.GetForecastAsTimeseries(t.Context(), &pb.GetForecastAsTimeseriesRequest{
				LocationUuid: output.LocationUuids[0],
				HorizonMins:  uint32(tt.horizonMins),
				Forecaster:   &pb.Forecaster{ForecasterName: "test_model_1", ForecasterVersion: "v1"},
				EnergySource: pb.EnergySource_SOLAR,
				TimeWindow: &pb.TimeWindow{
					StartTimestampUtc: timestamppb.New(pivotTime.Add(-time.Hour * 48)),
					EndTimestampUtc:   timestamppb.New(pivotTime.Add(time.Hour * 36)),
				},
			})
			require.NoError(t, err)
			require.NotNil(t, resp)

			targetTimes := make([]int64, len(resp.Values))
			actualValues := make([]float32, len(resp.Values))
			for i, v := range resp.Values {
				targetTimes[i] = v.TimestampUtc.AsTime().Unix()
				actualValues[i] = v.P50ValuePercent
			}
			require.IsIncreasing(t, targetTimes)
			require.Equal(t, tt.expectedValues, actualValues)
		})
	}
}

func TestGetObservationsAsTimeseries(t *testing.T) {
	pivotTime := time.Now().Truncate(time.Minute)
	pgConnString := createPostgresContainer(t)
	c := setupClient(t, pgConnString)
	output := seed(t, pgConnString, seedDBParams{
		NumLocations:            1,
		NumForecasters:          1,
		NumForecastsPerLocation: 48,
		PgvResolutionMins:       5,
		ForecastResolutionMins:  30,
		ForecastLengthHours:     8,
		PivotTime:               pivotTime,
	})

	tests := []struct {
		startTime    time.Time
		endTime      time.Time
		expectedSize int
	}{
		{
			startTime:    pivotTime.Add(-time.Hour * 24),
			endTime:      pivotTime,
			expectedSize: 24 * 60 / 5,
		},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("Size %d", tt.expectedSize), func(t *testing.T) {
			resp, err := c.GetObservationsAsTimeseries(t.Context(), &pb.GetObservationsAsTimeseriesRequest{
				LocationUuid: output.LocationUuids[0],
				EnergySource: pb.EnergySource_SOLAR,
				TimeWindow: &pb.TimeWindow{
					StartTimestampUtc: timestamppb.New(tt.startTime),
					EndTimestampUtc:   timestamppb.New(tt.endTime),
				},
				ObserverName: "test_observer",
			})
			require.NoError(t, err)
			require.Equal(t, tt.expectedSize, len(resp.Values))
		})
	}
}

func TestGetWeekAverageDeltas(t *testing.T) {
	pivotTime := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)
	pgConnString := createPostgresContainer(t)
	c := setupClient(t, pgConnString)
	output := seed(t, pgConnString, seedDBParams{
		NumLocations:            1,
		NumForecasters:          1,
		NumForecastsPerLocation: 500,
		PgvResolutionMins:       30,
		ForecastResolutionMins:  30,
		ForecastLengthHours:     8,
		PivotTime:               pivotTime,
	})

	deltaResp, err := c.GetWeekAverageDeltas(t.Context(), &pb.GetWeekAverageDeltasRequest{
		LocationUuid: output.LocationUuids[0],
		EnergySource: pb.EnergySource_SOLAR,
		Forecaster:   &pb.Forecaster{ForecasterName: "test_model_1", ForecasterVersion: "v1"},
		ObserverName: "test_observer",
		PivotTime:    timestamppb.New(pivotTime),
	})
	require.NoError(t, err)
	require.NotNil(t, deltaResp)
	require.Len(t, deltaResp.Deltas, 8*60/30) // One per horizon
}

func TestGetLocationsWithin(t *testing.T) {
	pgConnString := createPostgresContainer(t)
	c := setupClient(t, pgConnString)
	metadata, err := structpb.NewStruct(map[string]any{"source": "test"})
	require.NoError(t, err)

	// Create a GSP with a bounding box between 0 and 5
	resp, err := c.CreateLocation(t.Context(), &pb.CreateLocationRequest{
		LocationName:  "GSP_OUTER_BOX",
		EnergySource:  pb.EnergySource_SOLAR,
		GeometryWkt:   "POLYGON((0 0, 0 5, 5 5, 5 0, 0 0))",
		LocationType:  pb.LocationType_GSP,
		CapacityWatts: 1000,
		Metadata:      metadata,
	})
	require.NoError(t, err)

	// Create a site within the box
	lls := []struct {
		lat float32
		lon float32
	}{
		{0.1, 0.1},
		{4, 4},
		{1, 1},
		{1, 5},
		{0, 170},
	}
	inner_uuids := make([]string, len(lls))
	for i, ll := range lls {
		sResp, err := c.CreateLocation(t.Context(), &pb.CreateLocationRequest{
			LocationName:  fmt.Sprintf("SITE_%d", i),
			EnergySource:  pb.EnergySource_SOLAR,
			GeometryWkt:   fmt.Sprintf("POINT(%f %f)", ll.lon, ll.lat),
			LocationType:  pb.LocationType_SITE,
			CapacityWatts: 50,
			Metadata:      metadata,
		})
		require.NoError(t, err)
		inner_uuids[i] = sResp.LocationUuid
	}

	result, err := c.GetLocationsWithin(t.Context(), &pb.GetLocationsWithinRequest{
		LocationUuid: resp.LocationUuid,
	})
	require.NoError(t, err)
	expected := []*pb.GetLocationsWithinResponse_LocationData{
		{LocationUuid: resp.LocationUuid, LocationName: "GSP_OUTER_BOX"},
		{LocationUuid: inner_uuids[0], LocationName: "SITE_0"},
		{LocationUuid: inner_uuids[1], LocationName: "SITE_1"},
		{LocationUuid: inner_uuids[2], LocationName: "SITE_2"},
	}
	for _, g := range result.Locations {
		t.Log("Location:", g.LocationName, "ID:", g.LocationUuid)
	}
	require.Equal(t, expected, result.Locations)
}

func TestCreateForecast(t *testing.T) {
	pgConnString := createPostgresContainer(t)
	c := setupClient(t, pgConnString)
	metadata, err := structpb.NewStruct(map[string]any{"source": "test"})
	require.NoError(t, err)

	// Create a predictor
	_, err = c.CreateForecaster(t.Context(), &pb.CreateForecasterRequest{
		Name:    "test_model_1",
		Version: "v1",
	})
	require.NoError(t, err)

	// Create a site to attach the forecast to
	siteResp, err := c.CreateLocation(t.Context(), &pb.CreateLocationRequest{
		LocationName:  "TEST_SITE",
		GeometryWkt:   "POINT(-0.1 51.5)",
		CapacityWatts: 1000000,
		Metadata:      metadata,
		EnergySource:  pb.EnergySource_SOLAR,
		LocationType:  pb.LocationType_SITE,
	})
	require.NoError(t, err)

	yields := make([]*pb.CreateForecastRequest_ForecastValue, 10)
	for i := range yields {
		yields[i] = &pb.CreateForecastRequest_ForecastValue{
			HorizonMins: uint32(i * 30),
			P50Pct:      float32(50 + i*5),
			P10Pct:      float32(40 + i*5),
			P90Pct:      float32(60 + i*5),
			Metadata:    metadata,
		}
	}

	req := &pb.CreateForecastRequest{
		Forecast: &pb.CreateForecastRequest_Forecast{
			LocationUuid: siteResp.LocationUuid,
			Forecaster:   &pb.Forecaster{ForecasterName: "test_model_1", ForecasterVersion: "v1"},
			EnergySource: pb.EnergySource_SOLAR,
			InitTimeUtc:  timestamppb.New(time.Now().UTC()),
		},
		Values: yields,
	}
	resp, err := c.CreateForecast(t.Context(), req)
	require.NoError(t, err)
	require.NotNil(t, resp)
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

	tests := []seedDBParams{
		// {NumLocations: 373, PgvResolutionMins: 30, ForecastResolutionMins: 60, ForecastLengthHours: 8, NumForecastsPerLocation: 10, PivotTime: pivotTime},
		// {NumLocations: 373, PgvResolutionMins: 5, ForecastResolutionMins: 60, ForecastLengthHours: 16, NumForecastsPerLocation: 48, PivotTime: pivotTime},
		{NumLocations: 500, PgvResolutionMins: 30, ForecastResolutionMins: 30, ForecastLengthHours: 24, NumForecastsPerLocation: 256, PivotTime: pivotTime},
		// {NumLocations: 10000, PgvResolutionMins: 5, ForecastResolutionMins: 30, ForecastLengthHours: 8, NumForecastsPerLocation: 76, PivotTime: pivotTime},
		// {NumLocations: 10000, PgvResolutionMins: 5, ForecastResolutionMins: 30, ForecastLengthHours: 8, NumForecastsPerLocation: 256, PivotTime: pivotTime},
	}
	for _, tt := range tests {
		pgConnString := createPostgresContainer(b)
		c := setupClient(b, pgConnString)
		output := seed(b, pgConnString, tt)

		// Create some test yields
		yields := make([]*pb.CreateForecastRequest_ForecastValue, tt.NumPgvsPerForecast())
		for i := range yields {
			yields[i] = &pb.CreateForecastRequest_ForecastValue{
				HorizonMins: uint32(i * 30),
				P50Pct:      50,
				P10Pct:      50,
				P90Pct:      50,
				Metadata:    metadata,
			}
		}

		b.Run(fmt.Sprintf("%d/GetForecastAsTimeseries", output.NumPgvs), func(b *testing.B) {
			for b.Loop() {
				resp, err := c.GetForecastAsTimeseries(b.Context(), &pb.GetForecastAsTimeseriesRequest{
					LocationUuid: output.LocationUuids[0],
					EnergySource: pb.EnergySource_SOLAR,
					Forecaster:   &pb.Forecaster{ForecasterName: "test_model_1", ForecasterVersion: "v1"},
					TimeWindow: &pb.TimeWindow{
						StartTimestampUtc: timestamppb.New(pivotTime.Add(-time.Hour * 48)),
						EndTimestampUtc:   timestamppb.New(pivotTime.Add(time.Hour * 36)),
					},
				})
				require.NoError(b, err)
				require.GreaterOrEqual(b, len(resp.Values), 1)
			}
		})
		b.Run(fmt.Sprintf("%d/GetForecastAtTimestamp", output.NumPgvs), func(b *testing.B) {
			if len(output.LocationUuids) > 100 {
				output.LocationUuids = output.LocationUuids[0:100]
			}
			for b.Loop() {
				crossSectionResp, err := c.GetForecastAtTimestamp(b.Context(), &pb.GetForecastAtTimestampRequest{
					EnergySource:  pb.EnergySource_SOLAR,
					LocationUuids: output.LocationUuids,
					Forecaster:    &pb.Forecaster{ForecasterName: "test_model_1", ForecasterVersion: "v1"},
					TimestampUtc:  timestamppb.New(pivotTime),
				})
				require.NoError(b, err)
				require.NotNil(b, crossSectionResp)
				require.GreaterOrEqual(b, len(crossSectionResp.Values), 1)
			}
		})
		b.Run(fmt.Sprintf("%d/GetObservationsAsTimeseries", output.NumPgvs), func(b *testing.B) {
			for b.Loop() {
				obsResp, err := c.GetObservationsAsTimeseries(b.Context(), &pb.GetObservationsAsTimeseriesRequest{
					LocationUuid: output.LocationUuids[0],
					ObserverName: "test_observer",
					EnergySource: pb.EnergySource_SOLAR,
					TimeWindow: &pb.TimeWindow{
						StartTimestampUtc: timestamppb.New(pivotTime.Add(-time.Hour * 36)),
						EndTimestampUtc:   timestamppb.New(pivotTime),
					},
				})
				require.NoError(b, err)
				require.GreaterOrEqual(b, len(obsResp.Values), 36*60/tt.PgvResolutionMins)
			}
		})
		b.Run(fmt.Sprintf("%d/CreateForecast", output.NumPgvs), func(b *testing.B) {
			for b.Loop() {
				_, err := c.CreateForecast(b.Context(), &pb.CreateForecastRequest{
					Forecast: &pb.CreateForecastRequest_Forecast{
						Forecaster:   &pb.Forecaster{ForecasterName: "test_model_1", ForecasterVersion: "v1"},
						LocationUuid: output.LocationUuids[0],
						EnergySource: pb.EnergySource_SOLAR,
						InitTimeUtc: timestamppb.New(
							pivotTime.Add(time.Duration(rand.Int64N(2000000)) * time.Minute),
						),
					},
					Values: yields,
				})
				require.NoError(b, err)
			}
		})
	}
}
