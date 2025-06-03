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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/devsjc/fcfs/api/src/internal/models/fcfsapi"
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

// GetObservedTimeseries implements pb.QuartzAPIServer.
func (q *QuartzAPIPostgresServer) GetObservedTimeseries(*pb.GetObservedTimeseriesRequest, grpc.ServerStreamingServer[pb.GetObservedTimeseriesResponse]) error {
	panic("unimplemented")
}

// GetPredictedCrossSection implements pb.QuartzAPIServer.
func (q *QuartzAPIPostgresServer) GetPredictedCrossSection(context.Context, *pb.GetPredictedCrossSectionRequest) (*pb.GetPredictedCrossSectionResponse, error) {
	panic("unimplemented")
}

// GetPredictedTimeseriesDeltas implements pb.QuartzAPIServer.
func (q *QuartzAPIPostgresServer) GetPredictedTimeseriesDeltas(*pb.GetPredictedTimeseriesRequest, grpc.ServerStreamingServer[pb.GetPredictedTimeseriesDeltasResponse]) error {
	panic("unimplemented")
}

func (q *QuartzAPIPostgresServer) GetLatestForecast(ctx context.Context, req *pb.GetLatestForecastRequest) (*pb.GetLatestForecastResponse, error) {
	l := log.With().Str("method", "GetLatestForecast").Logger()
	l.Debug().Str("params", fmt.Sprintf("%+v", "GetLatestForecastRequest")).Msg("recieved method call")

	// Establish a transaction with the database
	tx, err := q.pool.Begin(ctx)
	if err != nil {
		l.Err(err).Msg("failed to begin transaction")
		return nil, status.Error(codes.Internal, "Encountered database connection error")
	}
	defer tx.Rollback(ctx)
	querier := db.New(tx)

	dbModel, err := querier.GetDefaultModel(ctx)
	if err != nil {
		l.Err(err).Msg("querier.GetDefaultModel()")
		return nil, status.Error(codes.Internal, "Failed to get default model. Ensure a default model is set.")
	}

	dbLocation, err := querier.GetLocationSource(ctx, db.GetLocationSourceParams{
		LocationID:     int32(req.LocationId),
		SourceTypeName: "solar",
	})
	if err != nil {
		l.Err(err).Msgf("querier.GetLocationSource({locationID: %d, sourceTypeName: 'solar'})", req.LocationId)
		return nil, status.Errorf(codes.NotFound, "No solar source found for location %d", req.LocationId)
	}

	dbForecast, err := querier.GetLatestForecastForLocationAtHorizon(
		ctx,
		db.GetLatestForecastForLocationAtHorizonParams{
			LocationID:     req.LocationId,
			ModelID:        dbModel.ModelID,
			HorizonMins:    0,
		},
	)
	if err != nil {
		l.Err(err).Msgf("querier.GetLatestForecastForLocationAtHorizon({locationID: %d, sourceTypeName: 'solar', modelID: %d, horizonMins: 0})", req.LocationId, dbModel.ModelID)
		return nil, status.Errorf(codes.NotFound, "No forecast found for location %d", req.LocationId)
	}

	l.Debug().Msgf("Found forecast with ID %d for location %d", dbForecast.ForecastID, req.LocationId)

	dbValues, err := querier.GetPredictedGenerationValuesForForecast(ctx, dbForecast.ForecastID)
	if err != nil {
		l.Err(err).Msgf("querier.GetPredictedGenerationValuesForForecast({forecastID: %d})", dbForecast.ForecastID)
		return nil, status.Errorf(codes.NotFound, "No predicted generation values found for forecast %d", dbForecast.ForecastID)
	}
	l.Debug().Msgf("Found %d predicted generation values for forecast %d", len(dbValues), dbForecast.ForecastID)
	predictedYields := make([]*pb.YieldPrediction, len(dbValues))
	for i, value := range dbValues {
		predictedYields[i] = &pb.YieldPrediction{
			YieldKw:       int64(float64((value.P50 / 30000) * dbLocation.Capacity) * math.Pow10(int(dbLocation.CapacityUnitPrefixFactor)) / 1000),
			TimestampUnix: value.TargetTimeUtc.Time.Unix(),
			Uncertainty:   &pb.YieldPrediction_Uncertainty{},
		}
	}

	return &pb.GetLatestForecastResponse{
		LocationId: int32(req.LocationId),
		Yields:     predictedYields,
	}, tx.Commit(ctx)
}

func (q *QuartzAPIPostgresServer) GetSolarLocation(ctx context.Context, req *pb.GetLocationRequest) (*pb.GetLocationResponse, error) {
	l := log.With().Str("method", "GetLocation").Logger()
	l.Debug().Str("params", fmt.Sprintf("%+v", req)).Msg("recieved method call")

	// Establish a transaction with the database
	tx, err := q.pool.Begin(ctx)
	if err != nil {
		l.Err(err).Msg("failed to begin transaction")
		return nil, status.Error(codes.Internal, "Encountered database connection error")
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
	dbSourceData, err := querier.GetLocationSource(ctx, db.GetLocationSourceParams{
		LocationID:     int32(req.LocationId),
		SourceTypeName: "solar",
	})
	if err != nil {
		l.Err(err).Msg("failed to get solar source for location")
		return nil, status.Errorf(codes.NotFound, "No solar source associated with location with id %d", req.LocationId)
	}
	l.Debug().Msgf("Retrieved solar source for location %d", req.LocationId)

	return &pb.GetLocationResponse{
		LocationId: int32(req.LocationId),
		Name:       dbLocationData.LocationName,
		Latitude:   dbLocationData.Latitude,
		Longitude:  dbLocationData.Longitude,
		CapacityKw: int64(float64(dbSourceData.Capacity) * math.Pow10(int(dbSourceData.CapacityUnitPrefixFactor)) / 1000.0),
		Metadata:   string(dbSourceData.Metadata),
	}, tx.Commit(ctx)
}

func (q *QuartzAPIPostgresServer) CreateSolarForecast(ctx context.Context, req *pb.CreateForecastRequest) (*pb.CreateForecastResponse, error) {
	l := log.With().Str("method", "CreateSolarForecast").Logger()
	l.Debug().Str("params", fmt.Sprintf("%+v", req.Forecast)).Msg("recieved method call")

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

	return &pb.CreateForecastResponse{}, tx.Commit(ctx)
}

func (q *QuartzAPIPostgresServer) CreateModel(ctx context.Context, req *pb.CreateModelRequest) (*pb.CreateModelResponse, error) {
	l := log.With().Str("method", "CreateModel").Logger()
	l.Debug().Str("params", fmt.Sprintf("%+v", req)).Msg("recieved method call")

	// Establish a transaction with the database
	tx, err := q.pool.Begin(ctx)
	if err != nil {
		l.Err(err).Msg("q.pool.Begin()")
		return nil, status.Error(codes.Internal, "Encountered database connection error")
	}
	defer tx.Rollback(ctx)
	querier := db.New(tx)

	// Create a new model
	params := db.CreateModelParams{
		ModelName:    req.Name,
		ModelVersion: req.Version,
	}
	modelID, err := querier.CreateModel(ctx, params)
	if err != nil {
		l.Err(err).Msgf("querier.CreateModel({ModelName: %s, ModelVersion: %s})", req.Name, req.Version)
		return nil, status.Errorf(codes.InvalidArgument, "Invalid model. Ensure name and version are not empty and are lowercase")
	}
	if req.MakeDefault {
		err = querier.SetDefaultModel(ctx, modelID)
		if err != nil {
			l.Err(err).Msgf("querier.SetDefaultModel({modelID: %d})", modelID)
			return nil, status.Errorf(codes.NotFound, "Model with ID %d not found to set as default", modelID)
		}
	}

	return &pb.CreateModelResponse{ModelId: modelID}, tx.Commit(ctx)
}

func (q *QuartzAPIPostgresServer) CreateSolarSite(ctx context.Context, req *pb.CreateSiteRequest) (*pb.CreateLocationResponse, error) {
	l := log.With().Str("method", "CreateSolarSite").Logger()
	l.Debug().Str("params", fmt.Sprintf("%+v", req)).Msg("recieved method call")

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
	return &pb.CreateLocationResponse{LocationId: locationID}, tx.Commit(ctx)
}

func (q *QuartzAPIPostgresServer) CreateSolarGsp(ctx context.Context, req *pb.CreateGspRequest) (*pb.CreateLocationResponse, error) {
	l := log.With().Str("method", "CreateSolarGsp").Logger()
	l.Debug().Str("params", fmt.Sprintf("%+v", req)).Msg("recieved method call")

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

	return &pb.CreateLocationResponse{LocationId: locationID}, tx.Commit(ctx)
}

func (q *QuartzAPIPostgresServer) GetLocationsAsGeoJSON(ctx context.Context, req *pb.GetLocationsAsGeoJSONRequest) (*pb.GetLocationsAsGeoJSONResponse, error) {
	l := log.With().Str("method", "GetLocationsAsGeoJSON").Logger()
	l.Debug().Str("params", fmt.Sprintf("%+v", req)).Msg("recieved method call")

	// Establish a transaction with the database
	tx, err := q.pool.Begin(ctx)
	if err != nil {
		l.Err(err).Msg("failed to begin transaction")
		return nil, status.Error(codes.Internal, "Encountered database connection error")
	}
	defer tx.Rollback(ctx)
	querier := db.New(tx)

	// Get the locations as GeoJSON
	var simplificationLevel float32
	if req.Unsimplified {
		simplificationLevel = 0
	} else {
		simplificationLevel = 0.5
	}
	geojson, err := querier.GetLocationGeoJSONByIds(ctx, db.GetLocationGeoJSONByIdsParams{
		SimplificationLevel: simplificationLevel,
		LocationIds:         req.LocationIds,
	})
	if err != nil {
		l.Err(err).Msg("failed to get locations as GeoJSON")
		return nil, status.Error(codes.InvalidArgument, "No locations found for input IDs")
	}

	return &pb.GetLocationsAsGeoJSONResponse{Geojson: string(geojson)}, tx.Commit(ctx)
}

// GetPredictedTimeseries implements proto.QuartzAPIServer.
func (q *QuartzAPIPostgresServer) GetPredictedTimeseries(req *pb.GetPredictedTimeseriesRequest, stream grpc.ServerStreamingServer[pb.GetPredictedTimeseriesResponse]) error {
	l := log.With().Str("method", "GetPredictedTimeseries").Logger()
	l.Debug().Str("params", fmt.Sprintf("%+v", req)).Msg("recieved method call")

	// Establish a transaction with the database
	tx, err := q.pool.Begin(stream.Context())
	if err != nil {
		l.Err(err).Msg("q.pool.Begin()")
		return status.Errorf(codes.Internal, "Encountered database connection error")
	}
	defer tx.Rollback(stream.Context())
	querier := db.New(tx)

	for _, locationId := range req.LocationIds {

		// Get the location source data
		dbSource, err := querier.GetLocationSource(stream.Context(), db.GetLocationSourceParams{
			LocationID:     locationId,
			SourceTypeName: "solar",
		})
		if err != nil {
			l.Err(err).Msgf("querier.GetLocationSource({locationID: %d, sourceTypeName: 'solar'})", locationId)
			return status.Errorf(codes.NotFound, "No solar source found for location %d", locationId)
		}

		// Get the latest forecast for the location
		dbModel, err := querier.GetDefaultModel(stream.Context())
		if err != nil {
			l.Err(err).Msg("querier.GetDefaultModel()")
			return status.Errorf(codes.Internal, "Couldn't get default model. Ensure a default model is set.")
		}

		dbValues, err := querier.GetWindowedPredictedGenerationValuesAtHorizon(
			stream.Context(), db.GetWindowedPredictedGenerationValuesAtHorizonParams{
				LocationID:     locationId,
				SourceTypeName: "solar",
				ModelID:        dbModel.ModelID,
				HorizonMins:    req.HorizonMins,
			},
		)
		if err != nil {
			l.Err(err).Msgf(
				"querier.GetWindowedPredictedGenerationValuesAtHorizon("+
					"{locationID: %d, sourceTypeName: 'solar', modelID: %d, horizonMins: %d}"+
					")",
				locationId, dbModel.ModelID, req.HorizonMins,
			)
			return status.Errorf(
				codes.NotFound,
				"No values found for location %d with horizon %d minutes",
				locationId, req.HorizonMins,
			)
		}
		l.Debug().Msgf(
			"Found %d values for location %d with horizon %d minutes",
			len(dbValues), locationId, req.HorizonMins,
		)

		yields := make([]*pb.YieldPrediction, len(dbValues))
		for i, yield := range dbValues {

			yieldKw := int64((float64(yield.P50) * float64(dbSource.Capacity) * math.Pow10(int(dbSource.CapacityUnitPrefixFactor)) / 1000) / 30000)

			yields[i] = &pb.YieldPrediction{
				YieldKw: yieldKw,
				TimestampUnix: yield.TargetTimeUtc.Time.Unix(),
				Uncertainty:   &pb.YieldPrediction_Uncertainty{},
			}
		}

		err = stream.Send(&pb.GetPredictedTimeseriesResponse{
			LocationId: locationId,
			Yields:     yields,
		})
		if err != nil {
			l.Err(err).Msgf(
				"stream.Send(GetPredictedTimeseriesResponse({locationID: %d, yields: (arr, len %d)}))",
				locationId, len(yields),
			)
			return status.Errorf(codes.Internal, "Failed to send predicted timeseries response for location %d", locationId)
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

var _ pb.QuartzAPIServer = (*QuartzAPIPostgresServer)(nil)
