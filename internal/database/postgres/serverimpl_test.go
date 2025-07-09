package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	pb "github.com/devsjc/fcfs/dp/internal/protogen/ocf/dp"
	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	timestamppb "google.golang.org/protobuf/types/known/timestamppb"
	"buf.build/go/protovalidate"
	middleware "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/protovalidate"

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

func seed(tb testing.TB, pgConnString string, params seedDBParams) (numPgvs int) {
	seedfiles, _ := filepath.Glob(filepath.Join(".", "*.sql"))
	conn, err := pgx.Connect(tb.Context(), pgConnString)
	require.NoError(tb, err)
	defer conn.Close(tb.Context())

	// Seed data if desired
	for _, f := range seedfiles {
		sql, err := os.ReadFile(f)
		require.NoError(tb, err)
		_, err = conn.Exec(tb.Context(), string(sql))
		require.NoError(tb, err)
		err = conn.QueryRow(
			tb.Context(),
			fmt.Sprintf(
				"SELECT seed_db(num_locations=>%d, gv_resolution_mins=>%d, forecast_resolution_mins=>%d, forecast_length_mins=>%d, num_forecasts_per_location=>%d)",
				params.NumLocations, params.PgvResolutionMins, params.ForecastResolutionMins, params.ForecastLengthHours*60, params.NumForecastsPerLocation,
			),
		).Scan(&numPgvs)
		require.NoError(tb, err)
		tb.Logf("Seeded %d predicted generation values", numPgvs)
	}
	return numPgvs
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
	NumModels               int
	NumForecastsPerLocation int
	PgvResolutionMins       int
	ForecastResolutionMins  int
	ForecastLengthHours     int
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

func TestCreateSolarSite(t *testing.T) {
	c := setupClient(t, createPostgresContainer(t))

	defaultReq := &pb.CreateSiteRequest{
		Name: "GREENWICH OBSERVATORY",
		Latlng: &pb.LatLng{
			Latitude:  51.4769,
			Longitude: -0.0005,
		},
		CapacityWatts: 1280000,
		Metadata:      `{"group": "test-sites"}`,
		EnergySource:  pb.EnergySource_SOLAR,
	}

	tests := []struct {
		name        string
		req        *pb.CreateSiteRequest
		shouldError bool
	}{
		{
			name:        "Should create default site",
			req:        defaultReq,
			shouldError: false,
		},
		{
			name: "Should create site with large capacity",
			req: &pb.CreateSiteRequest{
				Name:          "LARGE CAPACITY SITE",
				Latlng:        defaultReq.Latlng,
				CapacityWatts: 100000000000, // 100 GW
				Metadata:      defaultReq.Metadata,
				EnergySource:  defaultReq.EnergySource,
			},
			shouldError: false,
		},
		{
			name: "Shouldn't create site with invalid metadata",
			req: &pb.CreateSiteRequest{
				Name:          "INVALID METADATA SITE",
				Latlng:        defaultReq.Latlng,
				CapacityWatts: defaultReq.CapacityWatts,
				Metadata:      "{}", // Empty metadata
				EnergySource:  defaultReq.EnergySource,
			},
			shouldError: true,
		},
		{
			name: "Shouldn't create site with invalid name",
			req: &pb.CreateSiteRequest{
				Name:          "",
				Latlng:        defaultReq.Latlng,
				CapacityWatts: defaultReq.CapacityWatts,
				Metadata:      defaultReq.Metadata,
				EnergySource:  defaultReq.EnergySource,
			},
			shouldError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp, err := c.CreateSite(t.Context(), tt.req)
			if tt.shouldError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				// Try to read it back
				resp2, err := c.GetLocation(
					t.Context(),
					&pb.GetLocationRequest{
						LocationId: resp.LocationId,
						EnergySource: defaultReq.EnergySource,
					},
				)
				require.NoError(t, err)
				require.Equal(t, tt.req.Name, resp2.Name)
				require.Equal(t, tt.req.Latlng.Latitude, resp2.Latlng.Latitude)
				require.Equal(t, tt.req.Latlng.Longitude, resp2.Latlng.Longitude)
				require.Equal(t, tt.req.CapacityWatts, resp2.CapacityWatts)
				require.Equal(t, tt.req.Metadata, resp2.Metadata)
			}
		})
	}
	t.Run("Shouldn't get non-existent site", func(t *testing.T) {
		_, err := c.GetLocation(
			t.Context(),
			&pb.GetLocationRequest{LocationId: 999999},
		)
		require.Error(t, err)
	})
}

func TestCreateSolarGSP(t *testing.T) {
	c := setupClient(t, createPostgresContainer(t))

	defaultGsp := &pb.CreateGspRequest{
		Name:          "OXFORDSHIRE",
		Metadata:      `{"group": "test-gsps"}`,
		Geometry:      "POLYGON((0.0 51.5, 1.0 51.5, 1.0 52.0, 0.0 52.0, 0.0 51.5))",
		CapacityWatts: 2002000000, // 2002 MW
		EnergySource:  pb.EnergySource_SOLAR,
	}

	tests := []struct {
		name        string
		gsp         *pb.CreateGspRequest
		shouldError bool
	}{
		{
			name:        "Should create default GSP",
			gsp:         defaultGsp,
			shouldError: false,
		},
		{
			name: "Should create GSP with large capacity",
			gsp: &pb.CreateGspRequest{
				Name:          "LARGE CAPACITY GSP",
				Metadata:      defaultGsp.Metadata,
				Geometry:      defaultGsp.Geometry,
				CapacityWatts: 1000000000000, // 1000 GW
				EnergySource:  defaultGsp.EnergySource,
			},
			shouldError: false,
		},
		{
			name: "Shouldn't create GSP with invalid geometry 1 (non-WKT)",
			gsp: &pb.CreateGspRequest{
				Name:          "INVALID GEOMETRY GSP",
				Metadata:      defaultGsp.Metadata,
				Geometry:      "INVALID GEOMETRY",
				CapacityWatts: defaultGsp.CapacityWatts,
				EnergySource:  defaultGsp.EnergySource,
			},
			shouldError: true,
		},
		{
			name: "Shouldn't create a GSP with invalid geometry 2 (3D geometry)",
			gsp: &pb.CreateGspRequest{
				Name:          "3D GEOMETRY GSP",
				Metadata:      defaultGsp.Metadata,
				Geometry:      "POLYGON((0.0 51.5 0.0, 1.0 51.5 0.0, 1.0 52.0 0.0, 0.0 52.0 0.0, 0.0 51.5 0.0))",
				CapacityWatts: defaultGsp.CapacityWatts,
				EnergySource:  defaultGsp.EnergySource,
			},
			shouldError: true,
		},
		{
			name: "Shouldn't create GSP with empty geometry",
			gsp: &pb.CreateGspRequest{
				Name:          "EMPTY GEOMETRY GSP",
				Metadata:      defaultGsp.Metadata,
				Geometry:      "",
				CapacityWatts: defaultGsp.CapacityWatts,
				EnergySource:  defaultGsp.EnergySource,
			},
			shouldError: true,
		},
		{
			name: "Shouldn't create GSP with empty name",
			gsp: &pb.CreateGspRequest{
				Name:          "",
				Metadata:      defaultGsp.Metadata,
				Geometry:      defaultGsp.Geometry,
				CapacityWatts: defaultGsp.CapacityWatts,
				EnergySource:  defaultGsp.EnergySource,
			},
			shouldError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp, err := c.CreateGsp(t.Context(), tt.gsp)
			if tt.shouldError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				// Try to read it back
				resp2, err := c.GetLocation(
					t.Context(), &pb.GetLocationRequest{
						LocationId: resp.LocationId,
						EnergySource: tt.gsp.EnergySource,
					},
				)
				require.NoError(t, err)
				require.Equal(t, tt.gsp.Name, resp2.Name)
				require.Equal(t, tt.gsp.Metadata, resp2.Metadata)
				require.Equal(t, tt.gsp.CapacityWatts, resp2.CapacityWatts)
			}
		})
	}

	t.Run("Shouldn't get non-existent GSP", func(t *testing.T) {
		_, err := c.GetLocation(t.Context(), &pb.GetLocationRequest{LocationId: 999999})
		require.Error(t, err)
	})
}

func TestGetPredictedCrossSection(t *testing.T) {
	pivotTime := time.Now().Truncate(time.Minute)
	pgConnString := createPostgresContainer(t)
	c := setupClient(t, pgConnString)
	// Create 100 locations with 4 forecasts each
	_ = seed(t, pgConnString, seedDBParams{
		NumLocations:            100,
		NumForecastsPerLocation: 1,
		PgvResolutionMins:       30,
		ForecastResolutionMins:  30,
		ForecastLengthHours:     1,
	})
	locationIds := make([]int32, 100)
	for i := range locationIds {
		locationIds[i] = int32(i) + 1
	}

	crossSectionResp, err := c.GetPredictedCrossSection(t.Context(), &pb.GetPredictedCrossSectionRequest{
		EnergySource:  pb.EnergySource_SOLAR,
		TimestampUnix: timestamppb.New(pivotTime),
		LocationIds:   locationIds,
		Model: &pb.Model{ModelName: "test_model", ModelVersion: "v10",
	}})
	require.NoError(t, err)
	require.NotNil(t, crossSectionResp)
	require.Len(t, crossSectionResp.Yields, len(locationIds))
}

func TestGetLocationsAsGeoJSON(t *testing.T) {
	c := setupClient(t, createPostgresContainer(t))

	// Create some locations
	siteIds := make([]int32, 3)
	for i := range siteIds {
		resp, err := c.CreateSite(t.Context(), &pb.CreateSiteRequest{
			Name: fmt.Sprintf("TESTSITE%02d", i),
			Latlng: &pb.LatLng{
				Latitude:  51.5 + float32(i)*0.01,
				Longitude: -0.1 + float32(i)*0.01,
			},
			CapacityWatts: uint64(1000000 + i*100),
			Metadata:      "",
			EnergySource:  pb.EnergySource_SOLAR,
		})
		require.NoError(t, err)
		siteIds[i] = resp.LocationId
	}

	geojson, err := c.GetLocationsAsGeoJSON(t.Context(), &pb.GetLocationsAsGeoJSONRequest{
		LocationIds: siteIds,
	})
	require.NoError(t, err)
	var result map[string]any
	json.Unmarshal([]byte(geojson.Geojson), &result)
	features := result["features"].([]any)
	require.Equal(t, len(siteIds), len(features))
}

func TestGetPredictedTimeseries(t *testing.T) {
	pgConnString := createPostgresContainer(t)
	c := setupClient(t, pgConnString)

	// Create four forecasts, each half an hour apart, up to the latestForecastTime
	// Give each forecast one hour's worth of predicted generation values, occurring every 5 minutes
	_ = seed(t, pgConnString, seedDBParams{
		NumLocations:            1,
		NumModels:               1,
		NumForecastsPerLocation: 4,
		PgvResolutionMins:       5,
		ForecastResolutionMins:  30,
		ForecastLengthHours:     1,
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
			stream, err := c.GetPredictedTimeseries(t.Context(), &pb.GetPredictedTimeseriesRequest{
				LocationIds: []int32{0},
				HorizonMins: int32(tt.horizonMins),
				Model: &pb.Model{ModelName: "test_model", ModelVersion: "v10"},
			})
			require.NoError(t, err)

			for {
				resp, err := stream.Recv()
				if err != nil {
					break
				}
				require.NotNil(t, resp)

				targetTimes := make([]int64, len(resp.Yields))
				actualValues := make([]int64, len(resp.Yields))
				for i, v := range resp.Yields {
					targetTimes[i] = v.TimestampUnix.AsTime().Unix()
					actualValues[i] = int64(v.YieldPercent)
				}
				require.IsIncreasing(t, targetTimes)
				require.Equal(t, tt.expectedValues, actualValues)
			}
		})
	}
}

func TestGetObservedTimeseries(t *testing.T) {
	pivotTime := time.Now().Truncate(time.Minute)
	pgConnString := createPostgresContainer(t)
	c := setupClient(t, pgConnString)
	_ = seed(t, pgConnString, seedDBParams{
		NumLocations:            1,
		NumModels:               1,
		NumForecastsPerLocation: 48,
		PgvResolutionMins:       5,
		ForecastResolutionMins:  30,
		ForecastLengthHours:     8,
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
		t.Run(fmt.Sprintf("Start %s End %s", tt.startTime, tt.endTime), func(t *testing.T) {
			resp, err := c.GetObservedTimeseries(t.Context(), &pb.GetObservedTimeseriesRequest{
				LocationId: 1,
				EnergySource: pb.EnergySource_SOLAR,
				TimeWindow: &pb.TimeWindow{
					StartTimestampUnix: timestamppb.New(tt.startTime),
					EndTimestampUnix:   timestamppb.New(tt.endTime),
				},
				ObserverName: "test_observer",
			})
			require.NoError(t, err)
			require.Equal(t, len(resp.Yields), tt.expectedSize)
		})
	}
}

func TestGetPredictedTimeseriesDeltas(t *testing.T) {
	pgConnString := createPostgresContainer(t)
	c := setupClient(t, pgConnString)
	_ = seed(t, pgConnString, seedDBParams{
		NumLocations:            1,
		NumModels:               1,
		NumForecastsPerLocation: 4,
		PgvResolutionMins:       5,
		ForecastResolutionMins:  30,
		ForecastLengthHours:     1,
	})

	tests := []struct {
		horizonMins    int32
		expectedValues []float32
	}{
		{
			// Observed values are half of the capacity, so subtract 50 from the expected values from
			// the non-delta test. Observed values also aren't seeded into the future, so only the
			// zero-horizon value from the latest forecast is included at the end.
			horizonMins: 0,
			expectedValues: []float32{
				0 - 50, 8 - 50, 16 - 50, 24 - 50, 32 - 50, 40 - 50,
				0 - 50, 8 - 50, 16 - 50, 24 - 50, 32 - 50, 40 - 50,
				0 - 50, 8 - 50, 16 - 50, 24 - 50, 32 - 50, 40 - 50,
				0 - 50,
			},
		},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("Horizon %d mins (deltas)", tt.horizonMins), func(t *testing.T) {
			deltaResp, err := c.GetPredictedTimeseriesDeltas(t.Context(), &pb.GetPredictedTimeseriesDeltasRequest{
				LocationId:   1,
				HorizonMins:  int32(tt.horizonMins),
				EnergySource: pb.EnergySource_SOLAR,
				ObserverName: "test_observer",
				Model: &pb.Model{ModelName: "test_model", ModelVersion: "v10"},
			})
			require.NoError(t, err)

			targetTimes := make([]int64, len(deltaResp.Deltas))
			actualValues := make([]float32, len(deltaResp.Deltas))
			for i, v := range deltaResp.Deltas {
				targetTimes[i] = v.TimestampUnix.AsTime().Unix()
				actualValues[i] = v.DeltaPercent
			}
			require.IsIncreasing(t, targetTimes)
			require.Equal(t, tt.expectedValues, actualValues)
		})
	}
}

func TestGetWeekAverageDeltas(t *testing.T) {
	pgConnString := createPostgresContainer(t)
	c := setupClient(t, pgConnString)
	_ = seed(t, pgConnString, seedDBParams{
		NumLocations:            1,
		NumModels:               1,
		NumForecastsPerLocation: 500,
		PgvResolutionMins:       30,
		ForecastResolutionMins:  30,
		ForecastLengthHours:     8,
	})

	deltaResp, err := c.GetWeekAverageDeltas(t.Context(), &pb.GetWeekAverageDeltasRequest{
		LocationId:   1,
		EnergySource: pb.EnergySource_SOLAR,
		Model:        &pb.Model{ModelName: "test_model", ModelVersion: "v10"},
		ObserverName: "test_observer",
		PivotTime: timestamppb.New(time.Now().UTC().Truncate(time.Minute)),
	})
	require.NoError(t, err)
	require.NotNil(t, deltaResp)
	require.Len(t, deltaResp.Deltas, 8 * 60 / 30) // One per horizon
}

// --- BENCHMARKS ---------------------------------------------------------------------------------

func BenchmarkPostgresClient(b *testing.B) {
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	pivotTime := time.Now().UTC().Truncate(time.Minute)

	tests := []seedDBParams{
		{NumLocations: 373, PgvResolutionMins: 30, ForecastResolutionMins: 60, ForecastLengthHours: 8, NumForecastsPerLocation: 10},
		{NumLocations: 373, PgvResolutionMins: 5, ForecastResolutionMins: 60, ForecastLengthHours: 16, NumForecastsPerLocation: 48},
		{NumLocations: 1000, PgvResolutionMins: 5, ForecastResolutionMins: 30, ForecastLengthHours: 8, NumForecastsPerLocation: 256},
		{NumLocations: 10000, PgvResolutionMins: 5, ForecastResolutionMins: 30, ForecastLengthHours: 8, NumForecastsPerLocation: 76},
		// {NumLocations: 10000, PgvResolutionMins: 5, ForecastResolutionMins: 30, ForecastLengthHours: 8, NumForecastsPerLocation: 256},
	}
	for _, tt := range tests {
		pgConnString := createPostgresContainer(b)
		c := setupClient(b, pgConnString)
		numPgvs := seed(b, pgConnString, tt)

		b.Run(fmt.Sprintf("%d/GetPredictedTimeseries", numPgvs), func(b *testing.B) {
			for b.Loop() {
				stream, err := c.GetPredictedTimeseries(b.Context(), &pb.GetPredictedTimeseriesRequest{
					LocationIds:  []int32{1},
					EnergySource: pb.EnergySource_SOLAR,
					Model: &pb.Model{ModelName: "test_model", ModelVersion: "v10"},
				})
				require.NoError(b, err)
				for {
					resp, err := stream.Recv()
					if err != nil {
						break // End of stream
					}
					require.NotNil(b, resp)
					require.Equal(b, int32(1), resp.LocationId)
					require.GreaterOrEqual(b, len(resp.Yields), 1)
				}
			}
		})
		b.Run(fmt.Sprintf("%d/GetPredictedCrossSection", numPgvs), func(b *testing.B) {
			locationIds := make([]int32, 373)
			for i := range locationIds {
				locationIds[i] = int32(i) + 1
			}
			for b.Loop() {
				crossSectionResp, err := c.GetPredictedCrossSection(b.Context(), &pb.GetPredictedCrossSectionRequest{
					EnergySource:  pb.EnergySource_SOLAR,
					LocationIds:   locationIds,
					Model: &pb.Model{ModelName: "test_model", ModelVersion: "v10"},
					TimestampUnix: timestamppb.New(pivotTime),
				})
				require.NoError(b, err)
				require.NotNil(b, crossSectionResp)
				require.GreaterOrEqual(b, len(crossSectionResp.Yields), 1)
			}
		})
		b.Run(fmt.Sprintf("%d/GetObservedTimeseries", numPgvs), func(b *testing.B) {
			for b.Loop() {
				obsResp, err := c.GetObservedTimeseries(b.Context(), &pb.GetObservedTimeseriesRequest{
					LocationId:   1,
					ObserverName: "test_observer",
					EnergySource: pb.EnergySource_SOLAR,
					TimeWindow: &pb.TimeWindow{
						StartTimestampUnix: timestamppb.New(pivotTime.Add(-time.Hour * 36)),
						EndTimestampUnix:   timestamppb.New(pivotTime),
					},
				})
				require.NoError(b, err)
				require.GreaterOrEqual(b, len(obsResp.Yields), 36*60/tt.PgvResolutionMins)
			}
		})
		b.Run(fmt.Sprintf("%d/GetPredictedTimeseriesDeltas", numPgvs), func(b *testing.B) {
			for b.Loop() {
				deltasResp, err := c.GetPredictedTimeseriesDeltas(b.Context(), &pb.GetPredictedTimeseriesDeltasRequest{
					LocationId:   1,
					EnergySource: pb.EnergySource_SOLAR,
					ObserverName: "test_observer",
					Model: &pb.Model{ModelName: "test_model", ModelVersion: "v10"},
				})
				require.NoError(b, err)
				require.NotNil(b, deltasResp)
				require.Equal(b, int32(1), deltasResp.LocationId)
				require.GreaterOrEqual(b, len(deltasResp.Deltas), 1)
			}
		})
	}
}
