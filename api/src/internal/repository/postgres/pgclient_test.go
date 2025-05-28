package postgres

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/devsjc/fcfs/api/src/internal/models/fcfsapi"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/stretchr/testify/require"
)

func TestCapacityKWToMultiplier(t *testing.T) {
	// Test cases
	type TestCase struct{
		capacityKw int64
		expectedValue int16
		expectedMultiplier int16
		shouldError bool
	}
	tests := []TestCase{
		{-1, 0, 0, true},
		{0, 0, 0, false},
		{500, 500, 3, false},
		{32767, 32767, 3, false},
		{32768, 33, 6, false}, // Needs rounding, should go to 33 MW
		{33000, 33, 6, false},
		{1000000000, 1000, 9, false}, // 1TW
		{12345678, 12346, 6, false}, // 12 GW
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("capacityKw=%d", test.capacityKw), func(t *testing.T) {
			capacity, prefix, err := capacityKwToValueMultiplier(test.capacityKw)
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

// Create a GRPC client for running tests with
// * talks via an in-memory buffer to a Quartz Postgres Server instance
// * this instance is backed with a Postgres container with the relevant extensions 
func setupSuite(tb testing.TB, ctx context.Context) (fcfsapi.QuartzAPIClient, func(testing.TB)) {
	req := testcontainers.ContainerRequest{
		FromDockerfile: testcontainers.FromDockerfile{
			Context:    filepath.Join(".", "infra"),
			Dockerfile: "Containerfile",
			KeepImage: true,
		},
		Env: map[string]string{
			"POSTGRES_USER":     "postgres",
			"POSTGRES_PASSWORD": "postgres",
			"POSTGRES_DB":       "postgres",
		},
		Cmd:          []string{"postgres", "-c", "fsync=off"},
		ExposedPorts: []string{"5432/tcp"},
		WaitingFor:   wait.ForAll(
			wait.ForLog(
				"database system is ready to accept connections",
			).WithOccurrence(2),
			wait.ForListeningPort("5432/tcp"),
		),
	}
	pgC, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
        ContainerRequest: req,
        Started:          true,
    })
	require.NoError(tb, err)
	containerPort, err := pgC.MappedPort(ctx, "5432/tcp")
	require.NoError(tb, err)
	host, err := pgC.Host(ctx)
	require.NoError(tb, err)

	lis := bufconn.Listen(1024 * 1024)

	// Create server using in-memory listener
	s := grpc.NewServer()
	pgConnString := fmt.Sprintf(
		"postgres://postgres:postgres@%s/postgres",
		net.JoinHostPort(host, containerPort.Port()),
	)
	fcfsapi.RegisterQuartzAPIServer(s, NewQuartzAPIPostgresServer(pgConnString))
	go func() {
        if err := s.Serve(lis); err != nil {
            tb.Fatalf("Server exited with error: %v", err)
        }
    }()
	tb.Logf("Created server backed by fully migrated postgres container at %s", pgConnString)

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
	c := fcfsapi.NewQuartzAPIClient(cc)
	tb.Logf("Created client for server")

	return c, func(tb testing.TB) {
		tb.Logf("Cleaning up postgres container")
		cc.Close()
		s.GracefulStop()
		testcontainers.CleanupContainer(tb, pgC)
	}
}

func TestMigrate(t *testing.T) {
	ctx := context.Background()
	_, cleanup := setupSuite(t, ctx)
	defer cleanup(t)
}

func TestCreateSolarSite(t *testing.T) {
	ctx := context.Background()
	s, cleanup := setupSuite(t, ctx)
	defer cleanup(t)

	resp, err := s.CreateSolarSite(ctx, &fcfsapi.CreateSiteRequest{
		Name:       "GREENWICH OBSERVATORY",
		Latitude:   51.4769,
		Longitude:  -0.0005,
		CapacityKw: 1280,
		Metadata:   `{"group": "test-sites"}`,
	})

	require.NoError(t, err)
	// Expected to be insterted after the UK GSPs
	require.Equal(t, int64(344), resp.LocationId)
}

func TestCreateForecast(t *testing.T) {
	ctx := context.Background()
	s, cleanup := setupSuite(t, ctx)
	defer cleanup(t)

	// Create a model
	modelResp, err := s.CreateModel(ctx, &fcfsapi.CreateModelRequest{
		Name: "testmodel01",
		Version: "0.1.0",
	})
	require.NoError(t, err)

	init_time := time.Now().Truncate(24 * time.Hour)
	predictedGenerationValues := []*fcfsapi.PredictedGenerationValue{
		{
			HorizonMins:       0,
			P50:               85,
			P10:               81,
			P90:               88,
			Metadata:          `{"group": "test"}`,
		},
	}

	req := &fcfsapi.CreateForecastRequest{
		Forecast: &fcfsapi.Forecast{
			ModelId: modelResp.ModelId,
			InitTimeUtc: timestamppb.New(init_time),
			LocationId: 0,
		},
		PredictedGenerationValues: predictedGenerationValues,
	}
	resp, err := s.CreateSolarForecast(ctx, req)
	require.NoError(t, err)
	require.NotNil(t, resp)
}

func TestGetPredictedTimeseries(t *testing.T) {
	ctx := context.Background()
	s, cleanup := setupSuite(t, ctx)
	defer cleanup(t)

	// Create a model
	modelResp, err := s.CreateModel(ctx, &fcfsapi.CreateModelRequest{
		Name: "testmodel01",
		Version: "0.1.0",
	})
	require.NoError(t, err)


	// Make 10 forecasts created an hour apart from each other
	init_time := time.Now().Truncate(24 * time.Hour)
	for i := 9; i >= 0; i-- {

		init_time_i := init_time.Add(-1 * time.Duration(i) * time.Hour)
		predictedGenerationValues := make([]*fcfsapi.PredictedGenerationValue, 0, 12 * 48) // 12 predictions per hour, 48 hours
		for j, _ := range predictedGenerationValues {
			// Each forecast predicts the value of its time offset as the generation value
			predictedGenerationValues[i] = &fcfsapi.PredictedGenerationValue{
					HorizonMins:       int64(j * 5),
					P50:               int32(i),
					P10:               int32(i),
					P90:               int32(i),
					Metadata:          `{"group": "test"}`,
			}
		}

		req := &fcfsapi.CreateForecastRequest{
			Forecast: &fcfsapi.Forecast{
				ModelId: modelResp.ModelId,
				InitTimeUtc: timestamppb.New(init_time_i),
				LocationId: 0,
			},
			PredictedGenerationValues: predictedGenerationValues,
		}
		resp, err := s.CreateSolarForecast(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, resp)
	}

	// Now get the predicted timeseries for the last 10 hours
	stream, err := s.GetPredictedTimeseries(ctx, &fcfsapi.GetPredictedTimeseriesRequest{
		LocationIds: []int32{0},
	})
	
	require.NoError(t, err)
	var count int
	for {
		resp, err := stream.Recv()
		if err != nil {
			break // End of stream
		}
		require.NotNil(t, resp)
		require.Equal(t, int64(0), resp.LocationId)

		for _, v := range resp.Yields {
			count++
			require.Equal(t, int32(10), v.YieldKw) // All forecasts should have P50 = 10
		}
	}

}



func BenchmarkCreateForecast(b *testing.B) {
	ctx := context.Background()
	s, cleanup := setupSuite(b, ctx)
	defer cleanup(b)

	num_sites := 500
	num_predictions := 12 * 48 // 12 predictions per hour, 48 hours

	// Create Sites
	siteIds := make([]int64, num_sites)
	for i := range siteIds {
		createSiteResponse, err := s.CreateSolarSite(ctx, &fcfsapi.CreateSiteRequest{
			Name:       fmt.Sprintf("TESTSITE%03d", i),
			Latitude:   55.5,
			Longitude:  float32(0.05 * float64(i)),
			CapacityKw: int32(i),
			Metadata:   `{"group": "test"}`,
		})
		require.NoError(b, err)
		siteIds[i] = createSiteResponse.LocationId
	}

	// Create a model
	modelResp, err := s.CreateModel(ctx, &fcfsapi.CreateModelRequest{
		Name: "testmodel01",
		Version: "0.1.0",
	})
	require.NoError(b, err)

	// Create a forecast and set of predicted values for each site
	init_time := time.Now().Truncate(24 * time.Hour)
	predictedGenerationValues := make([]*fcfsapi.PredictedGenerationValue, num_predictions)
	for i := range predictedGenerationValues {
		predictedGenerationValues[i] = &fcfsapi.PredictedGenerationValue{
			HorizonMins:       int64(i * 5),
			P50:               85,
			P10:               81,
			P90:               88,
			Metadata:          `{"group": "test"}`,
		}
	}

	// Benchmark inserting forecasts for each site
	for b.Loop() {
		for _, v := range siteIds {
			req := &fcfsapi.CreateForecastRequest{
				Forecast: &fcfsapi.Forecast{
					ModelId: modelResp.ModelId,
					LocationId: v,
					InitTimeUtc: timestamppb.New(init_time),
				},
				PredictedGenerationValues: predictedGenerationValues,
			}
			_, err := s.CreateSolarForecast(ctx, req)
			require.NoError(b, err)
		}
	}

	b.Logf("Inserted %d forecasts with %d predictions each (%d rows)", num_sites, num_predictions, num_sites*num_predictions)

}

