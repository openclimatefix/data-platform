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
	NumDaysOfForecastsPerLocation int
	PgvResolutionMins int
	ForecastResolutionMins int
	ForecastLengthHours int
}

func (s *seedDBParams) NumPgvsPerForecast() int {
	return (s.ForecastLengthHours * 60) / s.PgvResolutionMins
}

func (s *seedDBParams) NumForecastsPerLocation() int {
	return (s.NumDaysOfForecastsPerLocation * 24 * 60) / s.ForecastResolutionMins
}

func (s *seedDBParams) NumPgvRows() int {
	return s.NumLocations * s.NumForecastsPerLocation() * s.NumPgvsPerForecast()
}

// seedDB is a helper function to create a populated database
// with a configurable number of entries.
func seedDB(
	c fcfsapi.QuartzAPIClient,
	ps *seedDBParams,
	ctx context.Context,
	rSeed int64,
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
		locationResp, err := c.CreateSolarSite(ctx, &fcfsapi.CreateSiteRequest{
			Name:       fmt.Sprintf("TESTSITE%03d", i),
			Latitude:   float32(r.Intn(180) - 90),
			Longitude:  float32(r.Intn(360) - 180),
			CapacityKw: int64(r.Intn(100000)), // 100GW
			Metadata:   "",
		})
		if err != nil {
			return defaultModelId, locationIds, fmt.Errorf("failed to create solar site: %w", err)
		}
		locationIds = append(locationIds, locationResp.LocationId)

		// Seed location source forecasts and predicted generation values
		for j := range ps.NumForecastsPerLocation() {
			initTime := latestInitTime.Add(-time.Duration(j) * time.Duration(ps.ForecastResolutionMins) * time.Minute)
			predictedGenerationValues := make([]*fcfsapi.PredictedGenerationValue, ps.NumPgvsPerForecast())

			for k := range predictedGenerationValues {
				p50 := int32(r.Intn(29000)) + 101
				predictedGenerationValues[k] = &fcfsapi.PredictedGenerationValue{
					HorizonMins: int32(k * ps.PgvResolutionMins),
					P50:         p50,
					P10:         p50 - int32(r.Intn(100)),
					P90:         p50 + int32(r.Intn(100)),
					Metadata:    "{\"source\": \"test\"}",
				}
			}

			_, err := c.CreateSolarForecast(ctx, &fcfsapi.CreateForecastRequest{
				Forecast:                  &fcfsapi.Forecast{
					ModelId:     int32(defaultModelId),
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
			resp, err := c.CreateSolarSite(t.Context(), tt.site)
			if tt.shouldError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				// Try to read it back
				resp2, err := c.GetSolarLocation(
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
		_, err := c.GetSolarLocation(
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
			resp, err := c.CreateSolarGsp(t.Context(), tt.gsp)
			if tt.shouldError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				// Try to read it back
				resp2, err := c.GetSolarLocation(t.Context(), &fcfsapi.GetLocationRequest{LocationId: resp.LocationId})
				require.NoError(t, err)
				require.Equal(t, tt.gsp.Name, resp2.Name)
				require.Equal(t, tt.gsp.Metadata, resp2.Metadata)
				require.Equal(t, tt.gsp.CapacityMw * 1000, resp2.CapacityKw)
			}
		})
	}
	
	t.Run("Shouldn't get non-existent GSP", func(t *testing.T) {
		_, err := c.GetSolarLocation(t.Context(), &fcfsapi.GetLocationRequest{LocationId: 999999})
		require.Error(t, err)
	})
}

func TestGetLocationsAsGeoJSON(t *testing.T) {
	c := setupClient(t, createPostgresContainer(t))

	// Create some locations
	siteIds := make([]int32, 3)
	for i := range siteIds {
		resp, err := c.CreateSolarSite(t.Context(), &fcfsapi.CreateSiteRequest{
			Name:       fmt.Sprintf("TESTSITE%02d", i),
			Latitude:   51.5 + float32(i)*0.01,
			Longitude:  -0.1 + float32(i)*0.01,
			CapacityKw: int64(1000 + i*100),
			Metadata:   "",
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

	latestForecastTime := time.Now().Truncate(time.Minute)

	capacityKw := int64(1200)

	// Create a location, source, and model to use for the forecasts
	locResp, err := c.CreateSolarSite(t.Context(), &fcfsapi.CreateSiteRequest{
		Name:       "TEST LOCATION",
		Latitude:   51.5,
		Longitude:  -0.1,
		CapacityKw: capacityKw,
		Metadata:   "",
	})
	require.NoError(t, err)
	modelResp, err := c.CreateModel(t.Context(), &fcfsapi.CreateModelRequest{
		Name:        "testmodel",
		Version:     "1.0.0",
		MakeDefault: true,
	})
	require.NoError(t, err)

	for i := 3; i >= 0; i-- {
		// Create four forecasts, each half an hour apart, up to the latestForecastTime
		init_time := latestForecastTime.Add(-1 * time.Duration(i) * 30 * time.Minute)
		// Give each forecast one hour's worth of predicted generation values, occurring every 10 minutes
		predictedGenerationValues := make([]*fcfsapi.PredictedGenerationValue, 60 / 5)

		for j := range predictedGenerationValues {
			predictedGenerationValues[j] = &fcfsapi.PredictedGenerationValue{
				HorizonMins: int32(j * 5),
				P50:         (int32(i * 100) + int32(j * 5)) * 10,
				P10:         int32(i),
				P90:         int32(i),
				Metadata:    "",
			}
		}

		req := &fcfsapi.CreateForecastRequest{
			Forecast: &fcfsapi.Forecast{
				ModelId: modelResp.ModelId,
				InitTimeUtc: timestamppb.New(init_time),
				LocationId: locResp.LocationId,
			},
			PredictedGenerationValues: predictedGenerationValues,
		}
		resp, err := c.CreateSolarForecast(t.Context(), req)
		require.NoError(t, err)
		require.NotNil(t, resp)
	}

	// We now have four forecasts.
	// The values of the first forecast are 3000, 3050, 3100, 3150, 3200... 3550
	// The second, 2000, 2050, 2100, 2150, 2200... 2550 and so on

	// For each horizon, get the predicted timeseries
	tests := []struct{
		horizonMins int32
		expectedValues []int32
	}{
		{
			// For horizon 0, we should get all the values from the latest forecast,
			// plus the values from the previous forecasts that have the lowest horizon
			// for each target time.
			// Since the predicted values are every 5 minutes, and the forecasts are every 30,
			// we should get 6 values from each forecast, until the latest where we get all 12.
			// This means the values we are fetching should be
			// 3000, 3050, 3100, 3150, 3200, 3250 (horizons 0 to 25 minutes from forecast 3)
			// 2000, 2050, 2100, 2150, 2200, 2250 (horizons 0 to 25 minutes from forecast 2)
			// (forecast 3's values for the same target time here have a greater horizon so are not wanted)
			// 1000, 1050, 1100, 1150, 1200, 1250 (horizons 0 to 25 minutes from forecast 1)
			// 0, 50, 100, 150, 200, 250, 300, 350, 400, 450, 500, 550 (horizons 0 to 55 minutes from forecast 0)
			// For simplicity, the values are written in the tests as the P50 values,
			// but remember they will be in kilowatts according to capacity in the response.
			horizonMins: 0,
			expectedValues: []int32{
				300, 305, 310, 315, 320, 325,
				200, 205, 210, 215, 220, 225,
				100, 105, 110, 115, 120, 125,
				0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55,
			},
		},
		{
			horizonMins: 14,
			expectedValues: []int32{
				315, 320, 325, 330, 335, 340,
				215, 220, 225, 230, 235, 240,
				115, 120, 125, 130, 135, 140,
				15, 20, 25, 30, 35, 40, 45, 50, 55,
			},
		},
		{
			horizonMins: 30,
			expectedValues: []int32{
				330, 335, 340, 345, 350, 355,
				230, 235, 240, 245, 250, 255,
				130, 135, 140, 145, 150, 155,
				30, 35, 40, 45, 50, 55,
			},
		},
		{
			horizonMins: 60,
			expectedValues: []int32{},
		},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("Horizon %d mins", tt.horizonMins), func(t *testing.T) {
			stream, err := c.GetPredictedTimeseries(t.Context(), &fcfsapi.GetPredictedTimeseriesRequest{
				LocationIds: []int32{locResp.LocationId},
				HorizonMins: int32(tt.horizonMins),
			})
			require.NoError(t, err)

			for {
				resp, err := stream.Recv()
				if err != nil {
					break
				}
				require.NotNil(t, resp)
				require.Equal(t, locResp.LocationId, resp.LocationId)

				expectedValues := make([]int32, len(tt.expectedValues))
				for i, v := range tt.expectedValues {
					expectedValues[i] = v * 10 * int32(capacityKw) / 30000
				}

				targetTimes := make([]int64, len(resp.Yields))
				actualValues := make([]int32, len(resp.Yields))
				for i, v := range resp.Yields {
					targetTimes[i] = v.TimestampUnix
					// Don't forget the values were multiplied by 10 to be significant,
					// and we need to get them as a function of the capacity in Kw
					actualValues[i] = int32(v.YieldKw)
				}
				require.IsIncreasing(t, targetTimes)
				require.Equal(t, expectedValues, actualValues)
			}
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
			NumDaysOfForecastsPerLocation: 1,
			PgvResolutionMins:             30,
			ForecastResolutionMins:        60,
			ForecastLengthHours:           8,
		},
		{
			NumLocations:                  100,
			NumModels:                     10,
			NumDaysOfForecastsPerLocation: 1,
			PgvResolutionMins:             30,
			ForecastResolutionMins:        30,
			ForecastLengthHours:           8,
		},
		{
			NumLocations:                  100,
			NumModels:                     10,
			NumDaysOfForecastsPerLocation: 7,
			PgvResolutionMins:             30,
			ForecastResolutionMins:        5,
			ForecastLengthHours:           24,
		},
	}

	for i, tt := range tests {
		b.Run(fmt.Sprintf("NumRows=%d", tt.NumPgvRows()), func(b *testing.B) {
			_, locationIds, err := seedDB(c, &tt, b.Context(), int64(i))
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

