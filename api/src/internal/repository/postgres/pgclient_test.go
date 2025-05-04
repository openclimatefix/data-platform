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

// Build a Postgres container with the relevant extensions and some test data
func setupSuite(tb testing.TB, ctx context.Context) (fcfsapi.QuartzAPIServer, func(testing.TB)) {
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

	connString := fmt.Sprintf(
		"postgres://postgres:postgres@%s/postgres",
		net.JoinHostPort(host, containerPort.Port()),
	)

	s := NewQuartzAPIPostgresServer(connString)
	tb.Logf("Connected to fully migrated postgres container at %s", connString)

	return s, func(tb testing.TB) {
		tb.Logf("Cleaning up postgres container")
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

	resp, err := s.CreateSolarSite (ctx, &fcfsapi.CreateSiteRequest{
		Name:       "testsite01",
		Latitude:   55.5,
		Longitude:  0.05,
		CapacityKw: 1280,
		Metadata:   `{"group": "test"}`,
	})

	require.NoError(t, err)
	require.Equal(t, int64(1), resp.LocationId)
}

func TestCreateForecast(t *testing.T) {
	ctx := context.Background()
	s, cleanup := setupSuite(t, ctx)
	defer cleanup(t)

	// Create a site
	createSiteResponse, err := s.CreateSolarSite(ctx, &fcfsapi.CreateSiteRequest{
		Name:       "testsite01",
		Latitude:   55.5,
		Longitude:  0.05,
		CapacityKw: 1280,
		Metadata:   `{"group": "test"}`,
	})
	require.NoError(t, err)

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
			LocationId: createSiteResponse.LocationId,
		},
		PredictedGenerationValues: predictedGenerationValues,
	}
	resp, err := s.CreateSolarForecast(ctx, req)
	require.NoError(t, err)
	require.NotNil(t, resp)
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
			Name:       fmt.Sprintf("testsite%03d", i),
			Latitude:   55.5,
			Longitude:  0.05,
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

