// Package postgres defines a client for a PostgreSQL database that conforms to the
// QuartzAPIServer interface generated py protoc. It uses the sqlc package to generate
// type-safe Go code from pure SQL queries.
package postgres

import (
	"context"
	"embed"
	"fmt"
	"math"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/codes"

	"github.com/devsjc/fcfs/api/src/internal/models/fcfsapi"
	db "github.com/devsjc/fcfs/api/src/internal/repository/postgres/gen"

	"github.com/rs/zerolog/log"
)

//go:generate sqlc generate
//go:embed sql/migrations/*.sql
var embedMigrations embed.FS

const migrationsDir = "sql/migrations"

// capacityKwToValueMultiplier return a number, plus the index to raise 10 to the power to
// to get the resultant number of Watts, to the closest power of 3.
// This is an important function which tries to preserve accuracy whilst also enabling a
// large range of values to be represented by two 16 bit integers.
func capacityKwToValueMultiplier(capacityKw int64) (int16, int16, error) {
	if capacityKw < 0 {
		return 0, 0, fmt.Errorf("input capacity %d cannot be negative", capacityKw)
	}
	if capacityKw == 0 {
		return 0, 0, nil
	}

	currentValue := capacityKw * 1000 // Convert to Watts
	exponent := int16(0)
	const maxExponent = 18 // Limit to ExaWatts - current generation is ~20PW for the whole world!

	// Keep scaling up as long as the value exceeds the int16 limit
	for currentValue > math.MaxInt16 {
		if exponent >= maxExponent {
			return 0, exponent, fmt.Errorf(
				"input represents a value greater than %d ExaWatts, which is not supported",
				math.MaxInt16,
			)
		}

		// Divide by 1000 to get to the next SI unit prefix
		// * add on 500 to round up numbers that are over halfway to the next 10^3
		nextValue := (currentValue + 500) / 1000

		// If rounding resulted in 0 for a value that was previously > 0.
		// This is very unlikely with rounding unless the number is enormous and precision is lost,
		// but good to keep a check.
		if nextValue == 0 && currentValue > 0 {
			return 0, exponent + 3, fmt.Errorf(
				"scaled value rounded to zero from large input %d at potential exponent %d",
				capacityKw, exponent+3)
		}

		currentValue = nextValue // Update currentValue with the rounded scaled value
		exponent += 3
	}

	// This is safe as currentValue is now less than or equal to int16 max
	// but I've put a check to really be as safe as possible
	if currentValue > math.MaxInt16 {
		return 0, exponent, fmt.Errorf(
			"scaled value %d exceeds int16 max %d at exponent %d",
			currentValue, math.MaxInt16, exponent,
		)
	}
	resultValue := int16(currentValue)
	return resultValue, exponent, nil
}

type QuartzAPIPostgresServer struct {
	pool *pgxpool.Pool
}

func (q *QuartzAPIPostgresServer) GetSolarLocation(ctx context.Context, req *fcfsapi.GetLocationRequest) (*fcfsapi.GetLocationResponse, error) {
	l := log.With().Str("method", "GetLocation").Logger()
	l.Info().Str("params", fmt.Sprintf("%+v", req)).Msg("recieved method call")

	// Establish a transaction with the database
	tx, err := q.pool.Begin(ctx)
	if err != nil {
		l.Err(err).Msg("failed to begin transaction")
		return nil, status.Error(codes.Internal,"Encountered database connection error")
	}
	defer tx.Rollback(ctx)
	querier := db.New(tx)

	// Get the location by ID
	dbLocationData, err := querier.GetLocationById(ctx, int32(req.LocationId))
	if err != nil {
		l.Err(err).Msgf("failed to get location with id %d", req.LocationId)
		return nil, status.Errorf(codes.NotFound, "No location with id %d", req.LocationId)
	}
	l.Debug().Msgf("Retrieved location with id %d", dbLocationData.LocationID)

	// Get the solar sources associated with the location
	dbSourceData, err := querier.GetLocationSourceByType(ctx, db.GetLocationSourceByTypeParams{
		LocationID:     int32(req.LocationId),
		SourceTypeName: "solar",
	})
	if err != nil {
		l.Err(err).Msg("failed to get solar source for location")
		return nil, status.Errorf( codes.NotFound, "No solar source associated with location with id %d", req.LocationId)
	}
	l.Debug().Msgf("Retrieved solar source for location %d", req.LocationId)

	return &fcfsapi.GetLocationResponse{
		LocationId: int32(req.LocationId),
		Name:       dbLocationData.LocationName,
		Latitude:   dbLocationData.Latitude,
		Longitude:  dbLocationData.Longitude,
		CapacityKw: int64(float64(dbSourceData.Capacity) * math.Pow10(int(dbSourceData.CapacityUnitPrefixFactor)) / 1000.0),
		Metadata:   string(dbSourceData.Metadata),
	}, tx.Commit(ctx)
}

// CreateSolarForecast implements fcfsapi.QuartzAPIServer.
func (q *QuartzAPIPostgresServer) CreateSolarForecast(ctx context.Context, req *fcfsapi.CreateForecastRequest) (*fcfsapi.CreateForecastResponse, error) {
	l := log.With().Str("method", "CreateSolarForecast").Logger()
	l.Info().Str("params", fmt.Sprintf("%+v", req.Forecast)).Msg("recieved method call")

	if len(req.PredictedGenerationValues) == 0 {
		return nil, fmt.Errorf("no predicted generation values provided")
	}

	// Establish a transaction with the database
	tx, err := q.pool.Begin(ctx)
	if err != nil {
		l.Err(err).Msg("failed to begin transaction")
		return nil, status.Error(codes.Internal, "Encountered database connection error")
	}
	defer tx.Rollback(ctx)
	querier := db.New(tx)

	// Create a new forecast
	createForecastParams := db.CreateForecastParams{
		LocationID:     int32(req.Forecast.LocationId),
		SourceTypeName: "solar",
		ModelID:        int32(req.Forecast.ModelId),
		InitTimeUtc: pgtype.Timestamp{
			Time:  req.Forecast.InitTimeUtc.AsTime(),
			Valid: true,
		},
	}
	forecastID, err := querier.CreateForecast(ctx, createForecastParams)
	if err != nil {
		l.Err(err).Msg("failed to create forecast")
		return nil, status.Error(codes.InvalidArgument, "Invalid forecast")
	}
	l.Debug().Msgf("Created forecast with ID %d", forecastID)

	// Create the forecast data
	predictedGenerationValues := make([]db.CreatePredictedGenerationValuesParams, len(req.PredictedGenerationValues))
	for i, value := range req.PredictedGenerationValues {
		p10 := int16(value.P10)
		p90 := int16(value.P90)
		metadata := []byte(value.Metadata)
		if value.Metadata == "" {
			metadata = nil
		}

		predictedGenerationValues[i] = db.CreatePredictedGenerationValuesParams{
			HorizonMins: int16(value.HorizonMins),
			P50:         int16(value.P50),
			ForecastID:  forecastID,
			TargetTimeUtc: pgtype.Timestamp{
				Time: req.Forecast.InitTimeUtc.AsTime().Add(
					time.Duration(value.HorizonMins) * time.Minute,
				),
				Valid: true,
			},
			Metadata: metadata,
			P10:      &p10,
			P90:      &p90,
		}
	}

	_, err = querier.CreatePredictedGenerationValues(ctx, predictedGenerationValues)
	if err != nil {
		l.Err(err).Msg("failed to create predicted generation values")
		return nil, status.Errorf(codes.InvalidArgument, "Invalid predicted generation values")
	}
	l.Debug().Msgf("Inserted %d predicted generation values for forecast %d", len(predictedGenerationValues), forecastID)

	return &fcfsapi.CreateForecastResponse{}, tx.Commit(ctx)
}

// CreateModel implements fcfsapi.QuartzAPIServer.
func (q *QuartzAPIPostgresServer) CreateModel(ctx context.Context, req *fcfsapi.CreateModelRequest) (*fcfsapi.CreateModelResponse, error) {
	log.Info().Msg("CreateModel called")
	// Establish a transaction with the database
	tx, err := q.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %v", err)
	}
	defer tx.Rollback(ctx)
	querier := db.New(tx)

	// Create a new model
	params := db.CreateModelParams{
		ModelName:    req.Name,
		ModelVersion: req.Version,
	}
	modelID, err := querier.CreateModel(ctx, params)
	if req.MakeDefault == true {
		err = querier.SetDefaultModel(ctx, modelID)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to create model: %v", err)
	}

	return &fcfsapi.CreateModelResponse{ModelId: int64(modelID)}, tx.Commit(ctx)
}

func (q *QuartzAPIPostgresServer) CreateSolarSite(ctx context.Context, req *fcfsapi.CreateSiteRequest) (*fcfsapi.CreateLocationResponse, error) {
	l := log.With().Str("method", "CreateSolarSite").Logger()
	l.Info().Str("params", fmt.Sprintf("%+v", req)).Msg("recieved method call")

	// Establish a transaction with the database
	tx, err := q.pool.Begin(ctx)
	if err != nil {
		l.Err(err).Msg("failed to begin transaction")
		return nil, status.Error(codes.Internal, "Encountered database connection error")
	}
	defer tx.Rollback(ctx)
	querier := db.New(tx)

	// Create a new location as a Site
	params := db.CreateLocationParams{
		LocationTypeName: "site",
		LocationName:     req.Name,
		Geom:             fmt.Sprintf("POINT(%.8f %.8f)", req.Longitude, req.Latitude),
	}
	locationID, err := querier.CreateLocation(ctx, params)
	if err != nil {
		l.Err(err).Msg("failed to create site location")
		return nil, status.Errorf(codes.InvalidArgument, "Invalid Site. Ensure name is not empty and uppercase, and that coordinates are valid WGS84.")
	}
	l.Debug().Msgf("Created location of type 'site' with ID %d", locationID)

	// Create a Solar energy source associated with the location
	capacity, prefix, err := capacityKwToValueMultiplier(int64(req.CapacityKw))
	if err != nil {
		l.Err(err).Msg("failed to convert capacity")
		return nil, status.Errorf(codes.InvalidArgument, "Invalid capacity value: %d", req.CapacityKw)
	}
	metadata := []byte(req.Metadata)
	if req.Metadata == "" {
		metadata = nil
	}
	sourceParams := db.CreateLocationSourceParams{
		LocationID:               locationID,
		SourceTypeName:           "solar",
		Capacity:                 capacity,
		CapacityUnitPrefixFactor: prefix,
		Metadata:                 metadata,
	}
	_, err = querier.CreateLocationSource(ctx, sourceParams)
	if err != nil {
		l.Err(err).Msgf("failed to create solar source for location %d", locationID)
		return nil, status.Error(codes.InvalidArgument, "Invalid site. Ensure metadata is NULL or a non-empty JSON object.")
	}
	l.Debug().Msgf("Created source of type 'solar' for location %d with capacity %dx10^%dW", locationID, capacity, prefix)
	return &fcfsapi.CreateLocationResponse{LocationId: int64(locationID)}, tx.Commit(ctx)
}

// CreateWindGsp implements proto.QuartzAPIServer.
func (q *QuartzAPIPostgresServer) CreateSolarGsp(ctx context.Context, req *fcfsapi.CreateGspRequest) (*fcfsapi.CreateLocationResponse, error) {
	l := log.With().Str("method", "CreateSolarGsp").Logger()
	l.Info().Str("params", fmt.Sprintf("%+v", req)).Msg("recieved method call")

	// Establish a transaction with the database
	tx, err := q.pool.Begin(ctx)
	if err != nil {
		l.Err(err).Msg("failed to begin transaction")
		return nil, status.Error(codes.Internal, "Encountered database connection error")
	}
	defer tx.Rollback(ctx)
	querier := db.New(tx)

	// Create a new location as a GSP
	params := db.CreateLocationParams{
		LocationTypeName: "gsp",
		LocationName:     req.Name,
		Geom:             req.Geometry,
	}
	locationID, err := querier.CreateLocation(ctx, params)
	if err != nil {
		l.Err(err).Msg("failed to create GSP")
		return nil, status.Errorf(codes.InvalidArgument, "Invalid GSP. Ensure name is not empty and uppercase, and that geometry is valid WGS84.")
	}
	// Create a Solar source associated with the location
	capacity, prefix, err := capacityKwToValueMultiplier(int64(req.CapacityMw * 1000))
	if err != nil {
		l.Err(err).Msg("failed to convert capacity")
		return nil, status.Errorf(codes.InvalidArgument, "Invalid capacity value: %d", req.CapacityMw)
	}
	sourceParams := db.CreateLocationSourceParams{
		LocationID:               locationID,
		SourceTypeName:           "solar",
		Capacity:                 capacity,
		CapacityUnitPrefixFactor: prefix,
		Metadata:                 []byte(req.Metadata),
	}
	_, err = querier.CreateLocationSource(ctx, sourceParams)
	if err != nil {
		l.Err(err).Msgf("failed to create solar source for location with id %d", locationID)
		return nil, status.Error(codes.InvalidArgument, "Invalid GSP. Ensure metadata is NULL or a non-empty JSON object.")
	}

	return &fcfsapi.CreateLocationResponse{LocationId: int64(locationID)}, tx.Commit(ctx)
}

// GetActualCrossSection implements proto.QuartzAPIServer.
func (q *QuartzAPIPostgresServer) GetActualCrossSection(context.Context, *fcfsapi.GetActualCrossSectionRequest) (*fcfsapi.GetActualCrossSectionResponse, error) {
	panic("unimplemented")
}

// GetActualTimeseries implements proto.QuartzAPIServer.
func (q *QuartzAPIPostgresServer) GetActualTimeseries(*fcfsapi.GetActualTimeseriesRequest, grpc.ServerStreamingServer[fcfsapi.GetActualTimeseriesResponse]) error {
	panic("unimplemented")
}

// GetPredictedCrossSection implements proto.QuartzAPIServer.
func (q *QuartzAPIPostgresServer) GetPredictedCrossSection(context.Context, *fcfsapi.GetPredictedCrossSectionRequest) (*fcfsapi.GetPredictedCrossSectionResponse, error) {
	panic("unimplemented")
}

// GetPredictedTimeseries implements proto.QuartzAPIServer.
func (q *QuartzAPIPostgresServer) GetPredictedTimeseries(req *fcfsapi.GetPredictedTimeseriesRequest, stream grpc.ServerStreamingServer[fcfsapi.GetPredictedTimeseriesResponse]) error {
	l := log.With().Str("method", "GetPredictedTimeseries").Logger()
	l.Info().Str("params", fmt.Sprintf("%+v", req)).Msg("recieved method call")

	// Establish a transaction with the database
	tx, err := q.pool.Begin(stream.Context())
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %v", err)
	}
	defer tx.Rollback(stream.Context())
	querier := db.New(tx)

	for _, locationId := range req.LocationIds {

		// Get the latest forecast for the location
		modelResp, err := querier.GetDefaultModel(stream.Context())
		if err != nil {
			return fmt.Errorf("failed to get default model: %v", err)
		}
		l.Debug().Msgf("Using default model with ID %d", modelResp.ModelID)
		forecastResp, err := querier.GetLatestForecastForLocationAtHorizon(stream.Context(), db.GetLatestForecastForLocationAtHorizonParams{
			LocationID:     locationId,
			SourceTypeName: "solar",
			ModelID:        modelResp.ModelID,
			HorizonMins:    0,
		})
		if err != nil {
			return fmt.Errorf("failed to get latest forecast for location %d: %v", locationId, err)
		}
		l.Debug().Msgf("Found latest forecast with ID %d for location %d", forecastResp.ForecastID, locationId)

		dbYields, err := querier.GetPredictedGenerationValuesForForecast(stream.Context(), forecastResp.ForecastID)
		if err != nil {
			return fmt.Errorf("failed to get predicted generation values: %v", err)
		}
		l.Debug().Msgf("Found %d predicted generation values for forecast %d", len(dbYields), forecastResp.ForecastID)

		yields := make([]*fcfsapi.PredictedYield, len(dbYields))
		for i, yield := range dbYields {
			yields[i] = &fcfsapi.PredictedYield{
				YieldKw:       int32(yield.P50),
				TimestampUnix: yield.TargetTimeUtc.Time.Unix(),
				Uncertainty:   &fcfsapi.PredictedYieldUncertainty{},
			}
		}

		err = stream.Send(&fcfsapi.GetPredictedTimeseriesResponse{
			LocationId: locationId,
			Yields:     yields,
		})
		if err != nil {
			return fmt.Errorf("failed to send predicted timeseries response: %v", err)
		}
	}

	return nil
}

// NewPostgresClient creates a new PostgresClient instance and connects to the database
func NewQuartzAPIPostgresServer(connString string) *QuartzAPIPostgresServer {
	pool, err := pgxpool.New(
		context.Background(), connString,
	)
	if err != nil {
		log.Fatal().Msg("Unable to connect to database. Ensure DATABASE_URL is set correctly")
	}

	log.Debug().Msg("Running migrations")
	goose.SetBaseFS(embedMigrations)
	_ = goose.SetDialect("postgres")
	db := stdlib.OpenDBFromPool(pool)
	err = goose.Up(db, migrationsDir)
	if err != nil {
		log.Fatal().Msgf("Unable to apply migrations: %v", err)
	}
	err = db.Close()
	if err != nil {
		log.Fatal().Msgf("Unable to close database connection: %v", err)
	}

	return &QuartzAPIPostgresServer{pool: pool}
}

var _ fcfsapi.QuartzAPIServer = (*QuartzAPIPostgresServer)(nil)
