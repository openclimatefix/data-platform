package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/devsjc/fcfs/api/src/internal/models/fcfsapi"
	"github.com/jackc/pgx/v5"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/stretchr/testify/require"
)

const (
	// Buffer size for the in-memory GRPC server
	bufSize = 1024 * 1024 // 1MB
	// Time resolution for forecast's predicted generation values in minutes
	pgvResolutionMins = 5
	// Time between successive forecasts in minutes
	forecastResolutionMins = 30
	// Forecast length in hours
	forecastLengthHours = 48
	// Number of predicted generation values in an individual forecast
	numPgvsPerForecast = (forecastLengthHours * 60) / pgvResolutionMins
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

	lis := bufconn.Listen(bufSize)

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

	// Write seed data to the Postgres container
	seedfiles, _ := filepath.Glob(filepath.Join(".", "sql", "seeding", "*.sql"))
	conn, err := pgx.Connect(ctx, pgConnString)
	require.NoError(tb, err)
	defer conn.Close(ctx)
	// Run the seeding SQL files
	for _, f := range seedfiles {
		tb.Logf("Running seed file: %s", f)
		sql, err := os.ReadFile(f)
		require.NoError(tb, err)
		// Execute the SQL file
		_, err = conn.Exec(ctx, string(sql))
		require.NoError(tb, err)
		tb.Logf("Seed file %s executed successfully", f)
	}

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

	defaultSite := &fcfsapi.CreateSiteRequest{
		Name: "GREENWICH OBSERVATORY",
		Latitude:   51.4769,
		Longitude:  -0.0005,
		CapacityKw: 1280,
		Metadata:   `{"group": "test-sites"}`,
	}

	tests := []struct {
		name string
		site *fcfsapi.CreateSiteRequest
		shouldError bool
	}{
		{
			name: "Should create default site",
			site: defaultSite,
			shouldError: false,
		},
		{
			name: "Should create site with large capacity",
			site: &fcfsapi.CreateSiteRequest{
				Name: "LARGE CAPACITY SITE",
				Latitude: defaultSite.Latitude,
				Longitude: defaultSite.Longitude,
				CapacityKw: 100000000, // 100 GW
				Metadata: defaultSite.Metadata,
			},
			shouldError: false,
		},
		{
			name: "Shouldn't create site with negative capacity",
			site: &fcfsapi.CreateSiteRequest{
				Name: "NEGATIVE CAPACITY SITE",
				Latitude: defaultSite.Latitude,
				Longitude: defaultSite.Longitude,
				CapacityKw: -1000, // Invalid capacity
				Metadata: defaultSite.Metadata,
			},
			shouldError: true,
		},
		{
			name: "Shoudln't create site with invalid metadata",
			site: &fcfsapi.CreateSiteRequest{
				Name: "INVALID METADATA SITE",
				Latitude: defaultSite.Latitude,
				Longitude: defaultSite.Longitude,
				CapacityKw: defaultSite.CapacityKw,
				Metadata: "{}", // Empty metadata
			},
			shouldError: true,
		},
		{
			name: "Shouldn't create site with invalid name",
			site: &fcfsapi.CreateSiteRequest{
				Name: "",
				Latitude: defaultSite.Latitude,
				Longitude: defaultSite.Longitude,
				CapacityKw: defaultSite.CapacityKw,
				Metadata: defaultSite.Metadata,
			},
			shouldError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp, err := s.CreateSolarSite(ctx, tt.site)
			if tt.shouldError {
				t.Logf("Expected error: %v", err)
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				// Try to read it back
				resp2, err := s.GetSolarLocation(ctx, &fcfsapi.GetLocationRequest{LocationId: resp.LocationId})
				require.NoError(t, err)
				require.Equal(t, tt.site.Name, resp2.Name)
				require.Equal(t, tt.site.Latitude, resp2.Latitude)
				require.Equal(t, tt.site.Longitude, resp2.Longitude)
				require.Equal(t, tt.site.CapacityKw, resp2.CapacityKw)
				require.Equal(t, tt.site.Metadata, resp2.Metadata)
			}
		})  
	}
	t.Run("Shouldn't get non-existent site", func(t *testing.T) {
		_, err := s.GetSolarLocation(ctx, &fcfsapi.GetLocationRequest{LocationId: 999999})
		t.Log("Expected error for non-existent location: ", err)
		require.Error(t, err)
	})
}

func TestCreateSolarGSP(t *testing.T) {
	ctx := context.Background()
	s, cleanup := setupSuite(t, ctx)
	defer cleanup(t)

	defaultGsp := &fcfsapi.CreateGspRequest{
		Name:       "OXFORDSHIRE",
		Metadata:   `{"group": "test-gsps"}`,
		Geometry:   "POLYGON((0.0 51.5, 1.0 51.5, 1.0 52.0, 0.0 52.0, 0.0 51.5))",
		CapacityMw: 2002,
	}

	tests := []struct {
		name string
		gsp  *fcfsapi.CreateGspRequest
		shouldError bool
	}{
		{
			name: "Should create default GSP",
			gsp:  defaultGsp,
			shouldError: false,
		},
		{
			name: "Should create GSP with large capacity",
			gsp: &fcfsapi.CreateGspRequest{
				Name:       "LARGE CAPACITY GSP",
				Metadata:   defaultGsp.Metadata,
				Geometry:   defaultGsp.Geometry,
				CapacityMw: 1000000, // 1000 GW
			},
			shouldError: false,
		},
		{
			name: "Shouldn't create GSP with negative capacity",
			gsp: &fcfsapi.CreateGspRequest{
				Name:       "NEGATIVE CAPACITY GSP",
				Metadata:   defaultGsp.Metadata,
				Geometry:   defaultGsp.Geometry,
				CapacityMw: -1000, // Invalid capacity
			},
			shouldError: true,
		},
		{
			name: "Shouldn't create GSP with invalid geometry 1 (non-WKT)",
			gsp: &fcfsapi.CreateGspRequest{
				Name:       "INVALID GEOMETRY GSP",
				Metadata:   defaultGsp.Metadata,
				Geometry:   "INVALID GEOMETRY",
				CapacityMw: defaultGsp.CapacityMw,
			},
			shouldError: true,
		},
		{
			name: "Shouldn't create a GSP with invalid geometry 2 (3D geometry)",
			gsp: &fcfsapi.CreateGspRequest{
				Name:       "3D GEOMETRY GSP",
				Metadata:   defaultGsp.Metadata,
				Geometry:   "POLYGON((0.0 51.5 0.0, 1.0 51.5 0.0, 1.0 52.0 0.0, 0.0 52.0 0.0, 0.0 51.5 0.0))",
				CapacityMw: defaultGsp.CapacityMw,
			},
			shouldError: true,
		},
		{
			name: "Shouldn't create GSP with empty geometry",
			gsp: &fcfsapi.CreateGspRequest{
				Name:       "EMPTY GEOMETRY GSP",
				Metadata:   defaultGsp.Metadata,
				Geometry:   "",
				CapacityMw: defaultGsp.CapacityMw,
			},
			shouldError: true,
		},
		{
			name: "Shouldn't create GSP with empty name",
			gsp: &fcfsapi.CreateGspRequest{
				Name:       "",
				Metadata:   defaultGsp.Metadata,
				Geometry:   defaultGsp.Geometry,
				CapacityMw: defaultGsp.CapacityMw,
			},
			shouldError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp, err := s.CreateSolarGsp(ctx, tt.gsp)
			if tt.shouldError {
				t.Logf("Expected error: %v", err)
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				// Try to read it back
				resp2, err := s.GetSolarLocation(ctx, &fcfsapi.GetLocationRequest{LocationId: resp.LocationId})
				require.NoError(t, err)
				require.Equal(t, tt.gsp.Name, resp2.Name)
				require.Equal(t, tt.gsp.Metadata, resp2.Metadata)
				require.Equal(t, tt.gsp.CapacityMw * 1000, resp2.CapacityKw)
			}
		})
	}
	
	t.Run("Shouldn't get non-existent GSP", func(t *testing.T) {
		_, err := s.GetSolarLocation(ctx, &fcfsapi.GetLocationRequest{LocationId: 999999})
		t.Log("Expected error for non-existent GSP: ", err)
		require.Error(t, err)
	})
}

func TestGetLocationsAsGeoJSON(t *testing.T) {
	ctx := context.Background()
	s, cleanup := setupSuite(t, ctx)
	defer cleanup(t)

	// Create some locations
	siteIds := make([]int32, 3)
	for i, _ := range siteIds {
		resp, err := s.CreateSolarSite(ctx, &fcfsapi.CreateSiteRequest{
			Name:       fmt.Sprintf("TESTSITE%02d", i),
			Latitude:   51.5 + float32(i)*0.01,
			Longitude:  -0.1 + float32(i)*0.01,
			CapacityKw: int64(1000 + i*100),
			Metadata:   "",
		})
		require.NoError(t, err)
		siteIds[i] = resp.LocationId
	}	

	geojson, err := s.GetLocationsAsGeoJSON(ctx, &fcfsapi.GetLocationsAsGeoJSONRequest{
		LocationIds: siteIds,
	})
	require.NoError(t, err)
	var result map[string]any
	json.Unmarshal([]byte(geojson.Geojson), &result)
	features := result["features"].([]any)
	require.Equal(t, len(siteIds), len(features))
}

func TestGetPredictedTimeseries(t *testing.T) {
	ctx := context.Background()
	s, cleanup := setupSuite(t, ctx)
	defer cleanup(t)

	numForecasts := 10

	latest_forecast_time := time.Now().Truncate(time.Minute)

	for i := numForecasts; i >= 0; i-- {
		init_time := latest_forecast_time.Add(-1 * time.Duration(i) * forecastResolutionMins * time.Minute)
		predictedGenerationValues := make([]*fcfsapi.PredictedGenerationValue, numPgvsPerForecast)
	
		for j := 0; j < numPgvsPerForecast; j++ {
			predictedGenerationValues[j] = &fcfsapi.PredictedGenerationValue{
				HorizonMins: int32(j * pgvResolutionMins),
				// Each forecast's P50 will be equal to their difference from the 
				// latest forecast time, plus the horizon
				P50:         int32(i * forecastResolutionMins) + int32(j * pgvResolutionMins),
				P10:         int32(i),
				P90:         int32(i),
				Metadata:    "",
			}
		}

		req := &fcfsapi.CreateForecastRequest{
			Forecast: &fcfsapi.Forecast{
				ModelId: 10, // See the seeding SQL for this model ID
				InitTimeUtc: timestamppb.New(init_time),
				LocationId: 0,
			},
			PredictedGenerationValues: predictedGenerationValues,
		}
		resp, err := s.CreateSolarForecast(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, resp)
	}

	// For each horizon, get the predicted timeseries
	for f := range numForecasts {
		t.Run(fmt.Sprintf("Should get timeseries for horizon %d", f * forecastResolutionMins), func(t *testing.T) {
			stream, err := s.GetPredictedTimeseries(ctx, &fcfsapi.GetPredictedTimeseriesRequest{
				LocationIds: []int32{0},
				HorizonMins: int32(f * forecastResolutionMins),
			})
	
			require.NoError(t, err)
			for {
				resp, err := stream.Recv()
				if err != nil {
					break
				}
				require.NotNil(t, resp)
				require.Equal(t, int32(0), resp.LocationId)
				require.Greater(t, len(resp.Yields), numPgvsPerForecast)
				t.Logf("Received %d predicted values for horizon %d mins", len(resp.Yields), f * forecastResolutionMins)

				targetTimes := make([]int64, len(resp.Yields))
				for i, v := range resp.Yields {
					targetTimes[i] = v.TimestampUnix
				}
				require.IsIncreasing(t, targetTimes)
			}
		})
	}
}

func BenchmarkCreateForecast(b *testing.B) {
	ctx := context.Background()
	s, cleanup := setupSuite(b, ctx)
	defer cleanup(b)

	numLocations := 500

	// Create Sites
	siteIds := make([]int32, numLocations)
	for i := range siteIds {
		createSiteResponse, err := s.CreateSolarSite(ctx, &fcfsapi.CreateSiteRequest{
			Name:       fmt.Sprintf("TESTSITE%03d", i),
			Latitude:   55.5,
			Longitude:  float32(0.05 * float64(i)),
			CapacityKw: int64(i),
			Metadata:   `{"group": "test"}`,
		})
		require.NoError(b, err)
		siteIds[i] = createSiteResponse.LocationId
	}

	// Create a forecast and set of predicted values for each site
	init_time := time.Now().Truncate(24 * time.Hour)
	predictedGenerationValues := make([]*fcfsapi.PredictedGenerationValue, numPgvsPerForecast)
	for i := range predictedGenerationValues {
		predictedGenerationValues[i] = &fcfsapi.PredictedGenerationValue{
			HorizonMins:       int32(i * pgvResolutionMins),
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
					ModelId: 10, // See the seeding SQL for this model ID
					LocationId: v,
					InitTimeUtc: timestamppb.New(init_time),
				},
				PredictedGenerationValues: predictedGenerationValues,
			}
			_, err := s.CreateSolarForecast(ctx, req)
			require.NoError(b, err)
		}
	}

	b.Logf("Inserted %d location forecasts with %d predictions each (%d rows)", numLocations, numPgvsPerForecast, numLocations*numPgvsPerForecast)
}

func BenchmarkGetPredictedTimeseries(b *testing.B) {
	ctx := context.Background()
	s, cleanup := setupSuite(b, ctx)
	defer cleanup(b)

	numLocations := 500
	numForecastsPerLocation := int((24 * 60) / forecastResolutionMins) // One day of forecasts

	// Create locations and forecasts
	siteIds := make([]int32, numLocations)
	for i := range siteIds {
		createSiteResponse, err := s.CreateSolarSite(ctx, &fcfsapi.CreateSiteRequest{
			Name:       fmt.Sprintf("TESTSITE%03d", i),
			Latitude:   55.5,
			Longitude:  float32(0.05 * float64(i)),
			CapacityKw: int64(i),
			Metadata:   "",
		})
		require.NoError(b, err)
		siteIds[i] = createSiteResponse.LocationId

		for j := range numForecastsPerLocation {
			init_time := time.Now().Truncate(24 * time.Hour).Add(-time.Duration(j) * 24 * time.Hour)
			predictedGenerationValues := make([]*fcfsapi.PredictedGenerationValue, numPgvsPerForecast)
			for k := range predictedGenerationValues {
				predictedGenerationValues[k] = &fcfsapi.PredictedGenerationValue{
					HorizonMins: int32(k * 5),
					P50:         85,
					P10:         81,
					P90:         88,
					Metadata:    "",
				}
			}

			req := &fcfsapi.CreateForecastRequest{
				Forecast: &fcfsapi.Forecast{
					ModelId: 10, // See the seeding SQL for this model ID
					LocationId: siteIds[i],
					InitTimeUtc: timestamppb.New(init_time),
				},
				PredictedGenerationValues: predictedGenerationValues,
			}
			_, err := s.CreateSolarForecast(ctx, req)
			require.NoError(b, err)
		}
	}

	for b.Loop() {
		stream, err := s.GetPredictedTimeseries(ctx, &fcfsapi.GetPredictedTimeseriesRequest{
			LocationIds: siteIds[0:1],
		})
		require.NoError(b, err)
		for {
			resp, err := stream.Recv()
			if err != nil {
				break // End of stream
			}
			require.NotNil(b, resp)
			require.Equal(b, siteIds[0], resp.LocationId)
			require.Equal(b, numPgvsPerForecast, len(resp.Yields))
		}
	}

	b.Logf(
		"Retrieved forecast values from table of size %d rows",
		numLocations*numPgvsPerForecast*numForecastsPerLocation,
	)
}

