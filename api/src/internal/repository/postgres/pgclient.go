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

// CreateSolarForecast implements fcfsapi.QuartzAPIServer.
func (q *QuartzAPIPostgresServer) CreateSolarForecast(ctx context.Context, req *fcfsapi.CreateForecastRequest) (*fcfsapi.CreateForecastResponse, error) {
	log.Info().Msg("CreateSolarForecast called")
	// Establish a transaction with the database
	tx, err := q.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %v", err)
	}
	defer tx.Rollback(ctx)
	querier := db.New(tx)

	// Get the location source metadata

	// Create a new forecast
	createForecastParams := db.CreateForecastParams{
		LocationID:     int32(req.Forecast.LocationId),
		SourceTypeName: "solar",
		ModelID:        int32(req.Forecast.ModelId),
		InitTimeUtc:    pgtype.Timestamp{
			Time:             req.Forecast.InitTimeUtc.AsTime(),
			Valid: true,
		},
	}
	forecastID, err := querier.CreateForecast(ctx, createForecastParams)
	if err != nil {
		return nil, fmt.Errorf("failed to create forecast: %v", err)
	}
	// Create the forecast data
	predictedGenerationValues := make([]db.CreatePredictedGenerationValuesParams, len(req.PredictedGenerationValues))
	for i, value := range req.PredictedGenerationValues {
		p10 := int16(value.P10)
		p90 := int16(value.P90)
		predictedGenerationValues[i] = db.CreatePredictedGenerationValuesParams{
			HorizonMins: int16(value.HorizonMins),
			P50:         int16(value.P50),
			ForecastID:  forecastID,
			LocationID:  int32(req.Forecast.LocationId),
			TargetTimeUtc: pgtype.Timestamp{
				Time: req.Forecast.InitTimeUtc.AsTime().Add(
					time.Duration(value.HorizonMins) * time.Minute,
				),
				Valid: true,
			},
			Metadata: []byte(value.Metadata),
			P10:      &p10,
			P90:      &p90,
		}
	}

	_, err = querier.CreatePredictedGenerationValues(ctx, predictedGenerationValues)
	if err != nil {
		return nil, fmt.Errorf("failed to create predicted generation values: %v", err)
	}

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
		Name:    req.Name,
		Version: req.Version,
	}
	modelID, err := querier.CreateModel(ctx, params)
	if err != nil {
		return nil, fmt.Errorf("failed to create model: %v", err)
	}

	return &fcfsapi.CreateModelResponse{ModelId: int64(modelID)}, tx.Commit(ctx)
}

// GetSolarGsp implements fcfsapi.QuartzAPIServer.
func (q *QuartzAPIPostgresServer) GetSolarGsp(context.Context, *fcfsapi.GetLocationRequest) (*fcfsapi.GetLocationResponse, error) {
	panic("unimplemented")
}

// GetSolarSite implements fcfsapi.QuartzAPIServer.
func (q *QuartzAPIPostgresServer) GetSolarSite(context.Context, *fcfsapi.GetLocationRequest) (*fcfsapi.GetLocationResponse, error) {
	panic("unimplemented")
}

// CreateSolarGsp implements proto.QuartzAPIServer.
func (q *QuartzAPIPostgresServer) CreateSolarGsp(ctx context.Context, req *fcfsapi.CreateGspRequest) (*fcfsapi.CreateLocationResponse, error) {
	log.Info().Msg("CreateSolarGsp called")
	// Establish a transaction with the database
	tx, err := q.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %v", err)
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
		return nil, fmt.Errorf("failed to create GSP: %v", err)
	}
	// Create a Solar source associated with the location
	capacity, prefix, err := capacityKwToValueMultiplier(int64(req.CapacityMw * 1000))
	if err != nil {
		return nil, fmt.Errorf("failed to convert capacity: %v", err)
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
		return nil, fmt.Errorf("failed to create source: %v", err)
	}
	return &fcfsapi.CreateLocationResponse{LocationId: int64(locationID)}, tx.Commit(ctx)
}

// CreateSolarSite implements proto.QuartzAPIServer.
func (q *QuartzAPIPostgresServer) CreateSolarSite(ctx context.Context, req *fcfsapi.CreateSiteRequest) (*fcfsapi.CreateLocationResponse, error) {
	log.Info().Msg("CreateSolarSite called")
	// Establish a transaction with the database
	tx, err := q.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %v", err)
	}
	defer tx.Rollback(ctx)
	querier := db.New(tx)

	// Create a new location as a GSP
	params := db.CreateLocationParams{
		LocationTypeName: "site",
		LocationName:     req.Name,
		Geom:             fmt.Sprintf("POINT(%.8f %.8f)", req.Latitude, req.Longitude),
	}
	log.Debug().Msgf("CreateSolarSite params: %v", params)
	locationID, err := querier.CreateLocation(ctx, params)
	if err != nil {
		return nil, fmt.Errorf("failed to create Site: %v", err)
	}
	// Create a Solar source associated with the location
	capacity, prefix, err := capacityKwToValueMultiplier(int64(req.CapacityKw))
	if err != nil {
		return nil, fmt.Errorf("failed to convert capacity: %v", err)
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
		return nil, fmt.Errorf("failed to create source: %v", err)
	}
	return &fcfsapi.CreateLocationResponse{LocationId: int64(locationID)}, tx.Commit(ctx)
}

// CreateWindGsp implements proto.QuartzAPIServer.
func (q *QuartzAPIPostgresServer) CreateWindGsp(ctx context.Context, req *fcfsapi.CreateGspRequest) (*fcfsapi.CreateLocationResponse, error) {
	log.Info().Msg("CreateSolarGsp called")
	// Establish a transaction with the database
	tx, err := q.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %v", err)
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
		return nil, fmt.Errorf("failed to create GSP: %v", err)
	}
	// Create a Solar source associated with the location
	capacity, prefix, err := capacityKwToValueMultiplier(int64(req.CapacityMw * 1000))
	if err != nil {
		return nil, fmt.Errorf("failed to convert capacity: %v", err)
	}
	sourceParams := db.CreateLocationSourceParams{
		LocationID:               locationID,
		SourceTypeName:           "wind",
		Capacity:                 capacity,
		CapacityUnitPrefixFactor: prefix,
		Metadata:                 []byte(req.Metadata),
	}
	_, err = querier.CreateLocationSource(ctx, sourceParams)
	if err != nil {
		return nil, fmt.Errorf("failed to create source: %v", err)
	}
	err = tx.Commit(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %v", err)
	}
	return &fcfsapi.CreateLocationResponse{LocationId: int64(locationID)}, tx.Commit(ctx)
}

// CreateWindSite implements proto.QuartzAPIServer.
func (q *QuartzAPIPostgresServer) CreateWindSite(ctx context.Context, req *fcfsapi.CreateSiteRequest) (*fcfsapi.CreateLocationResponse, error) {
	log.Info().Msg("CreateWindSite called")
	// Establish a transaction with the database
	tx, err := q.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %v", err)
	}
	defer tx.Rollback(ctx)
	querier := db.New(tx)

	// Create a new location as a GSP
	params := db.CreateLocationParams{
		LocationTypeName: "site",
		LocationName:     req.Name,
		Geom:             fmt.Sprintf("POINT(%f %f)", req.Latitude, req.Longitude),
	}
	locationID, err := querier.CreateLocation(ctx, params)
	if err != nil {
		return nil, fmt.Errorf("failed to create Site: %v", err)
	}
	// Create a Solar source associated with the location
	capacity, prefix, err := capacityKwToValueMultiplier(int64(req.CapacityKw))
	if err != nil {
		return nil, fmt.Errorf("failed to convert capacity: %v", err)
	}
	sourceParams := db.CreateLocationSourceParams{
		LocationID:               locationID,
		SourceTypeName:           "wind",
		Capacity:                 capacity,
		CapacityUnitPrefixFactor: prefix,
		Metadata:                 []byte(req.Metadata),
	}
	_, err = querier.CreateLocationSource(ctx, sourceParams)
	if err != nil {
		return nil, fmt.Errorf("failed to create source: %v", err)
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
func (q *QuartzAPIPostgresServer) GetPredictedTimeseries(*fcfsapi.GetPredictedTimeseriesRequest, grpc.ServerStreamingServer[fcfsapi.GetPredictedTimeseriesResponse]) error {
	panic("unimplemented")
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
