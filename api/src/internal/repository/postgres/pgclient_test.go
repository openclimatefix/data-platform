package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/devsjc/fcfs/api/src/internal/models/fcfsapi"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/stretchr/testify/require"
)

// --- HELPERS ------------------------------------------------------------------------------------

func createPostgresContainer(tb testing.TB) string {
	tb.Helper()
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

	return pgConnString
}

// Create a GRPC client for running tests with
func setupClient(tb testing.TB, pgConnString string) fcfsapi.QuartzAPIClient {
	tb.Helper()
	// Create server using in-memory listener
	s := grpc.NewServer()
	lis := bufconn.Listen(1024 * 1024)
	fcfsapi.RegisterQuartzAPIServer(s, NewQuartzAPIPostgresServer(pgConnString))
	go func() {
        if err := s.Serve(lis); err != nil {
            tb.Fatalf("Server exited with error: %v", err)
        }
    }()

	/*
	// Write seed data to the Postgres container
	seedfiles, _ := filepath.Glob(filepath.Join(".", "sql", "seeding", "*.sql"))
	conn, err := pgx.Connect(tb.Context(), pgConnString)
	require.NoError(tb, err)
	defer conn.Close(tb.Context())

	// Run the seeding SQL files
	for _, f := range seedfiles {
		tb.Logf("Running seed file: %s", f)
		sql, err := os.ReadFile(f)
		require.NoError(tb, err)
		// Execute the SQL file
		_, err = conn.Exec(tb.Context(), string(sql))
		require.NoError(tb, err)
		tb.Logf("Seed file %s executed successfully", f)
	}
	*/

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
	NumLocations int
	NumModels int
	NumForecastsPerLocation int
	PgvResolutionMins int
	ForecastResolutionMins int
	ForecastLengthHours int
	SeedObservedValues bool
}

func (s *seedDBParams) NumPgvsPerForecast() int {
	return (s.ForecastLengthHours * 60) / s.PgvResolutionMins
}

func (s *seedDBParams) NumPgvRows() int {
	return s.NumLocations * s.NumForecastsPerLocation * s.NumPgvsPerForecast()
}

func (s *seedDBParams) SeededDataPeriod() (time.Time, time.Time) {
	// Calculate the start and end times for the seeded data
	endTime := time.Now().Truncate(time.Minute).Add(time.Duration(s.ForecastLengthHours) * time.Hour)
	startTime := time.Now().Truncate(time.Minute).Add(-time.Duration(s.ForecastResolutionMins * (s.NumForecastsPerLocation - 1)) * time.Minute)
	return startTime, endTime
}

func (s *seedDBParams) ObservationTimes() (times []time.Time) {
	if !s.SeedObservedValues {
		return times
	}
	start, end := s.SeededDataPeriod()
	numObs := int(end.Sub(start).Minutes() / float64(s.PgvResolutionMins))
	for i := range numObs {
		t := start.Add(time.Duration(i) * time.Duration(s.PgvResolutionMins) * time.Minute)
		times = append(times, t)
	}
	return times
}

// seedDB is a helper function to create a populated database.
// The default behaviour is to write deterministic values according to the following testable rules:
//
// - Each site has a capacity of 1000kW
// - Generation values move linearly from 0-100% of capacity over the forecast length
// - All observed values are half of the capacity of the location 
func seedDB(
	ctx context.Context,
	c fcfsapi.QuartzAPIClient,
	rSeed int64,
	ps *seedDBParams,
) (defaultModelId int32, locationIds[] int32, err error) {
	r := rand.New(rand.NewSource(rSeed))

	latestInitTime := time.Now().Truncate(time.Minute)

	// Seed models
	for i := range ps.NumModels {
		modelResp, err := c.CreateModel(ctx, &fcfsapi.CreateModelRequest{
			Name:        "testmodel",
			Version:     uuid.New().String(),
			MakeDefault: i == ps.NumModels - 1,
		})
		if err != nil {
			return defaultModelId, locationIds, fmt.Errorf("failed to create model: %w", err)
		}
		if i == ps.NumModels - 1 {
			defaultModelId = modelResp.ModelId
		}
	}

	for i := range ps.NumLocations {
		// Seed the locations
		locationResp, err := c.CreateSite(ctx, &fcfsapi.CreateSiteRequest{
			Name:       fmt.Sprintf("TESTSITE%03d", i),
			Latitude:   float32(r.Intn(180) - 90),
			Longitude:  float32(r.Intn(360) - 180),
			EnergySource: fcfsapi.EnergySource_ENERGY_SOURCE_SOLAR,
			CapacityKw: int64(1000),
			Metadata:   "",
		})
		if err != nil {
			return defaultModelId, locationIds, fmt.Errorf("failed to create site: %w", err)
		}
		locationIds = append(locationIds, locationResp.LocationId)

		// Seed location source forecasts and predicted generation values
		for j := range ps.NumForecastsPerLocation {
			initTime := latestInitTime.Add(
				-time.Duration(j) * time.Duration(ps.ForecastResolutionMins) * time.Minute,
			)
			predictedGenerationValues := make(
				[]*fcfsapi.CreateForecastRequest_PredictedGenerationValue, ps.NumPgvsPerForecast(),
			)

			for k := range predictedGenerationValues {
				p50pct := float32((100 / ps.NumPgvsPerForecast()) * k) 
				predictedGenerationValues[k] = &fcfsapi.CreateForecastRequest_PredictedGenerationValue{
					HorizonMins: int32(k * ps.PgvResolutionMins),
					P50Pct: float32(p50pct),
					P10Pct:         max(p50pct - r.Float32(), 0),
					P90Pct:         min(p50pct + r.Float32(), 109),
					Metadata:    "{\"source\": \"test\"}",
				}
			}

			_, err := c.CreateForecast(ctx, &fcfsapi.CreateForecastRequest{
				Forecast:                  &fcfsapi.Forecast{
					ModelId:     int32(defaultModelId),
					EnergySource: fcfsapi.EnergySource_ENERGY_SOURCE_SOLAR,
					LocationId:  locationResp.LocationId,
					InitTimeUtc: timestamppb.New(initTime),
				},
				PredictedGenerationValues: predictedGenerationValues,
			})
			if err != nil {
				return defaultModelId, locationIds, fmt.Errorf("failed to create solar forecast: %w", err)
			}
		}
	}

	if ps.SeedObservedValues {
		_, err := c.CreateObserver(ctx, &fcfsapi.CreateObserverRequest{Name: "test-observer"})
		if err != nil {
			return defaultModelId, locationIds, fmt.Errorf("failed to create observer: %w", err)
		}
		for i, locationId := range locationIds {
			observations := make([]*fcfsapi.Yield, len(ps.ObservationTimes()))
			for i, t := range ps.ObservationTimes() {
				observations[i] = &fcfsapi.Yield{
					YieldKw:       int64(500), // Always half of capacity
					TimestampUnix: t.UTC().Unix(),
				}
			}
			_, err := c.CreateObservations(ctx, &fcfsapi.CreateObservationsRequest{
				LocationId:   locationId,
				EnergySource: fcfsapi.EnergySource_ENERGY_SOURCE_SOLAR,
				ObserverName: "test-observer",
				Yields:       observations,
			})
			if err != nil {
				return defaultModelId, locationIds, fmt.Errorf("failed to create observations for location %d: %w", i, err)
			}
		}
	}


	return defaultModelId, locationIds, nil
}


// --- Tests --------------------------------------------------------------------------------------

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

func TestCreateSolarSite(t *testing.T) {
	c := setupClient(t, createPostgresContainer(t))

	defaultSite := &fcfsapi.CreateSiteRequest{
		Name: "GREENWICH OBSERVATORY",
		Latitude:   51.4769,
		Longitude:  -0.0005,
		CapacityKw: 1280,
		Metadata:   `{"group": "test-sites"}`,
		EnergySource: fcfsapi.EnergySource_ENERGY_SOURCE_SOLAR,
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
				EnergySource: defaultSite.EnergySource,
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
				EnergySource: defaultSite.EnergySource,
			},
			shouldError: true,
		},
		{
			name: "Shouldn't create site with invalid metadata",
			site: &fcfsapi.CreateSiteRequest{
				Name: "INVALID METADATA SITE",
				Latitude: defaultSite.Latitude,
				Longitude: defaultSite.Longitude,
				CapacityKw: defaultSite.CapacityKw,
				Metadata: "{}", // Empty metadata
				EnergySource: defaultSite.EnergySource,
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
				EnergySource: defaultSite.EnergySource,
			},
			shouldError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp, err := c.CreateSite(t.Context(), tt.site)
			if tt.shouldError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				// Try to read it back
				resp2, err := c.GetLocation(
					t.Context(),
					&fcfsapi.GetLocationRequest{LocationId: resp.LocationId},
				)
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
		_, err := c.GetLocation(
			t.Context(),
			&fcfsapi.GetLocationRequest{LocationId: 999999},
		)
		require.Error(t, err)
	})
}

func TestCreateSolarGSP(t *testing.T) {
	c := setupClient(t, createPostgresContainer(t))

	defaultGsp := &fcfsapi.CreateGspRequest{
		Name:       "OXFORDSHIRE",
		Metadata:   `{"group": "test-gsps"}`,
		Geometry:   "POLYGON((0.0 51.5, 1.0 51.5, 1.0 52.0, 0.0 52.0, 0.0 51.5))",
		CapacityMw: 2002,
		EnergySource: fcfsapi.EnergySource_ENERGY_SOURCE_SOLAR,
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
				EnergySource: defaultGsp.EnergySource,
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
				EnergySource: defaultGsp.EnergySource,
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
				EnergySource: defaultGsp.EnergySource,
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
				EnergySource: defaultGsp.EnergySource,
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
				EnergySource: defaultGsp.EnergySource,
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
				EnergySource: defaultGsp.EnergySource,
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
					t.Context(), &fcfsapi.GetLocationRequest{LocationId: resp.LocationId},
				)
				require.NoError(t, err)
				require.Equal(t, tt.gsp.Name, resp2.Name)
				require.Equal(t, tt.gsp.Metadata, resp2.Metadata)
				require.Equal(t, tt.gsp.CapacityMw * 1000, resp2.CapacityKw)
			}
		})
	}
	
	t.Run("Shouldn't get non-existent GSP", func(t *testing.T) {
		_, err := c.GetLocation(t.Context(), &fcfsapi.GetLocationRequest{LocationId: 999999})
		require.Error(t, err)
	})
}

func TestGetLocationsAsGeoJSON(t *testing.T) {
	c := setupClient(t, createPostgresContainer(t))

	// Create some locations
	siteIds := make([]int32, 3)
	for i := range siteIds {
		resp, err := c.CreateSite(t.Context(), &fcfsapi.CreateSiteRequest{
			Name:       fmt.Sprintf("TESTSITE%02d", i),
			Latitude:   51.5 + float32(i)*0.01,
			Longitude:  -0.1 + float32(i)*0.01,
			CapacityKw: int64(1000 + i*100),
			Metadata:   "",
			EnergySource: fcfsapi.EnergySource_ENERGY_SOURCE_SOLAR,
		})
		require.NoError(t, err)
		siteIds[i] = resp.LocationId
	}	

	geojson, err := c.GetLocationsAsGeoJSON(t.Context(), &fcfsapi.GetLocationsAsGeoJSONRequest{
		LocationIds: siteIds,
	})
	require.NoError(t, err)
	var result map[string]any
	json.Unmarshal([]byte(geojson.Geojson), &result)
	features := result["features"].([]any)
	require.Equal(t, len(siteIds), len(features))
}

func TestGetPredictedTimeseries(t *testing.T) {
	c := setupClient(t, createPostgresContainer(t))

	// Create four forecasts, each half an hour apart, up to the latestForecastTime
	// Give each forecast one hour's worth of predicted generation values, occurring every 5 minutes
	_, locationIDs, err := seedDB(t.Context(), c, 1, &seedDBParams{
		NumLocations:            1,
		NumModels:               1,
		NumForecastsPerLocation: 4,
		PgvResolutionMins:       5,
		ForecastResolutionMins:  30,
		ForecastLengthHours:     1,
		SeedObservedValues:      false,
	})
	require.NoError(t, err)

	// For each horizon, get the predicted timeseries
	tests := []struct{
		horizonMins int32
		expectedValues []int64
	}{
		{
			// For horizon 0, we should get all the values from the latest forecast,
			// plus the values from the previous forecasts that have the lowest horizon
			// for each target time.
			// Since the predicted values are every 5 minutes, and the forecasts are every 30,
			// we should get 6 values from each forecast, until the latest where we get all 12.
			// 100 // 12 = 8, so
			// This means the values we are fetching should be
			// 0, 8, 16, 24, 32, 40 (horizons 0 to 25 minutes from forecast 3)
			// Then the same from forecast 2, as it's horizon is smaller - likewise then forecast 1
			// 0, 8, 16, 24, 32, 40, 48, 56, 64, 72, 80, 88 (horizons 0 to 55 minutes from forecast 0)
			// These values are all percentages of the capacity, which is 1000kW, so they should be
			// all multiplied by 10 to get the actual values in kW.
			horizonMins: 0,
			expectedValues: []int64{
				0, 80, 160, 240, 320, 400,
				0, 80, 160, 240, 320, 400,
				0, 80, 160, 240, 320, 400,
				0, 80, 160, 240, 320, 400, 480, 560, 640, 720, 800, 880,
			},
		},
		{
			// For horizon of 14 minutes, anything with a lesser horizon should not be included.
			// So the value for 0, 5, and 10 minutes should not be included.
			horizonMins: 14,
			expectedValues: []int64{
				240, 320, 400, 480, 560, 640,
				240, 320, 400, 480, 560, 640,
				240, 320, 400, 480, 560, 640,
				240, 320, 400, 480, 560, 640, 720, 800, 880,
			},
		},
		{
			horizonMins: 30,
			expectedValues: []int64{
				480, 560, 640, 720, 800, 880,
				480, 560, 640, 720, 800, 880,
				480, 560, 640, 720, 800, 880,
				480, 560, 640, 720, 800, 880,
			},
		},
		{
			horizonMins: 60,
			expectedValues: []int64{},
		},
	}

	for _, tt := range tests {

		t.Run(fmt.Sprintf("Horizon %d mins", tt.horizonMins), func(t *testing.T) {
			stream, err := c.GetPredictedTimeseries(t.Context(), &fcfsapi.GetPredictedTimeseriesRequest{
				LocationIds: locationIDs,
				HorizonMins: int32(tt.horizonMins),
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
					targetTimes[i] = v.TimestampUnix
					actualValues[i] = v.YieldKw
				}
				require.IsIncreasing(t, targetTimes)
				require.Equal(t, tt.expectedValues, actualValues)
			}
		})

	}
}

func TestGetPredictedTimeseriesDeltas(t *testing.T) {
	c := setupClient(t, createPostgresContainer(t))

	_, locationIds, err := seedDB(t.Context(), c, 0, &seedDBParams{
		NumLocations:            1,
		NumModels:               1,
		NumForecastsPerLocation: 4,
		PgvResolutionMins:       5,
		ForecastResolutionMins:  30,
		ForecastLengthHours:     1,
		SeedObservedValues:      true,
	})
	require.NoError(t, err)

	tests := []struct {
		horizonMins int32
		expectedValues []int64
	}{
		{
			// Observed values are half of the capacity, so subtract 500 from the expected values from
			// the non-delta test.
			horizonMins: 0,
			expectedValues: []int64{
				0-500, 80-500, 160-500, 240-500, 320-500, 400-500,
				0-500, 80-500, 160-500, 240-500, 320-500, 400-500,
				0-500, 80-500, 160-500, 240-500, 320-500, 400-500,
				0-500, 80-500, 160-500, 240-500, 320-500, 400-500,
				480-500, 560-500, 640-500, 720-500, 800-500, 880-500,
			},
		},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("Horizon %d mins (deltas)", tt.horizonMins), func(t *testing.T) {
			deltaResp, err := c.GetPredictedTimeseriesDeltas(t.Context(), &fcfsapi.GetPredictedTimeseriesDeltasRequest{
				LocationId:   locationIds[0],
				HorizonMins:  int32(tt.horizonMins),
				EnergySource: fcfsapi.EnergySource_ENERGY_SOURCE_SOLAR,
				ObserverName: "test-observer",
			})
			require.NoError(t, err)

			targetTimes := make([]int64, len(deltaResp.Deltas))
			actualValues := make([]int64, len(deltaResp.Deltas))
			for i, v := range deltaResp.Deltas {
				targetTimes[i] = v.TimestampUnix
				actualValues[i] = v.DeltaKw
			}
			require.IsIncreasing(t, targetTimes)
			require.Equal(t, tt.expectedValues, actualValues)
		})
	}

}

// --- BENCHMARKS ---------------------------------------------------------------------------------

func BenchmarkGetPredictedTimeseries(b *testing.B) {
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	c := setupClient(b, createPostgresContainer(b))

	tests := []seedDBParams{
		{
			NumLocations:                  1,
			NumModels:                	   10,
			NumForecastsPerLocation:       24,
			PgvResolutionMins:             30,
			ForecastResolutionMins:        60,
			ForecastLengthHours:           8,
		},
		{
			NumLocations:                  100,
			NumModels:                     10,
			NumForecastsPerLocation:       48,
			PgvResolutionMins:             30,
			ForecastResolutionMins:        30,
			ForecastLengthHours:           8,
		},
		{
			NumLocations:                  100,
			NumModels:                     10,
			NumForecastsPerLocation:       7 * 48,
			PgvResolutionMins:             5,
			ForecastResolutionMins:        30,
			ForecastLengthHours:           24,
		},
	}

	for i, tt := range tests {
		b.Run(fmt.Sprintf("NumRows=%d", tt.NumPgvRows()), func(b *testing.B) {
			_, locationIds, err := seedDB(b.Context(), c, int64(i), &tt)
			require.NoError(b, err)

			for b.Loop() {
				stream, err := c.GetPredictedTimeseries(b.Context(), &fcfsapi.GetPredictedTimeseriesRequest{
					LocationIds: locationIds[0:1],
				})
				require.NoError(b, err)
				for {
					resp, err := stream.Recv()
					if err != nil {
						break // End of stream
					}
					require.NotNil(b, resp)
					require.Equal(b, int32(locationIds[0]), resp.LocationId)
					require.GreaterOrEqual(b, len(resp.Yields), tt.NumPgvsPerForecast())
				}
			}
		})
	}
}

