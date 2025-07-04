// Package postgres defines a server implementation for the DataPlatformServiceServer.
// This implementation is backed by a PostgreSQL database.
//
// Functions and structs for connecting to the database are generated from SQL using
// the sqlc library, whilst the Server interface that is being implemented comes from
// the top-level proto definitions.
package postgres

import (
	"context"
	"embed"
	"fmt"
	"math"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/devsjc/fcfs/dp/internal/protogen/ocf/dp"
	db "github.com/devsjc/fcfs/dp/internal/database/postgres/gen"

	"github.com/rs/zerolog/log"
)

//go:generate sqlc generate --file .sqlc.yaml

var (
	//go:embed sql/migrations/*.sql
	embedMigrations embed.FS

	energySourceMap = map[pb.EnergySource]string{
		pb.EnergySource_ENERGY_SOURCE_UNSPECIFIED: "solar",
		pb.EnergySource_ENERGY_SOURCE_SOLAR:       "solar",
		pb.EnergySource_ENERGY_SOURCE_WIND:        "wind",
	}
)

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

		// Check we haven't accidentally rounded to 0
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

type PostgresDataPlatformServerImpl struct {
	pool *pgxpool.Pool
}

// GetObservedTimeseries implements fcfsapi.QuartzAPIServer.
func (q *PostgresDataPlatformServerImpl) GetObservedTimeseries(ctx context.Context, req *pb.GetObservedTimeseriesRequest) (*pb.GetObservedTimeseriesResponse, error) {
	l := log.With().Str("method", "GetObservedTimeseries").Logger()
	l.Debug().Msg("recieved method call")

	// Establish a transaction with the database
	tx, err := q.pool.Begin(ctx)
	if err != nil {
		l.Err(err).Msg("q.pool.Begin()")
		return nil, status.Errorf(codes.Internal, "Encountered database connection error")
	}
	defer tx.Rollback(ctx)
	querier := db.New(tx)

	// Get capacity of location source
	params := db.GetLocationSourceParams{
		LocationID:     req.LocationId,
		SourceTypeName: "solar", // TODO: make this dynamic
	}
	dbSource, err := querier.GetLocationSource(ctx, params)
	if err != nil {
		l.Err(err).Msgf("querier.GetLocationSource(%+v)", params)
		return nil, status.Errorf(
			codes.NotFound,
			"Cannot get observations for location %d as it does not have any recorded operational source of type 'solar'.",
			req.LocationId,
		)
	}

	// Get the observations
	params2 := db.GetObservationsAsInt16BetweenParams{
		LocationID:     req.LocationId,
		SourceTypeName: "solar", // TODO
		ObserverName:   req.ObserverName,
		StartTimeUtc:   pgtype.Timestamp{Time: req.StartTime.AsTime(), Valid: true},
		EndTimeUtc:     pgtype.Timestamp{Time: req.EndTime.AsTime(), Valid: true},
	}
	dbObs, err := querier.GetObservationsAsInt16Between(ctx, params2)
	if err != nil {
		l.Err(err).Msgf("querier.GetObservationsAsInt16Between(%+v)", params2)
		return nil, status.Errorf(codes.NotFound, "No observations found for location %d", req.LocationId)
	}

	yields := make([]*pb.Yield, len(dbObs))
	for i, obs := range dbObs {
		yields[i] = &pb.Yield{
			YieldKw:       int64((float64(obs.Value) / 30000.0) * float64(dbSource.CapacityKw)),
			TimestampUnix: obs.ObservationTimeUtc.Time.Unix(),
		}
	}
	return &pb.GetObservedTimeseriesResponse{
		LocationId: req.LocationId,
		Yields:     yields,
	}, nil
}

func (q *PostgresDataPlatformServerImpl) CreateObservations(ctx context.Context, req *pb.CreateObservationsRequest) (*pb.CreateObservationsResponse, error) {
	l := log.With().Str("method", "CreateObservations").Logger()
	l.Debug().Msg("recieved method call")

	// Establish a transaction with the database
	tx, err := q.pool.Begin(ctx)
	if err != nil {
		l.Err(err).Msg("q.pool.Begin()")
		return nil, status.Errorf(codes.Internal, "Encountered database connection error")
	}
	defer tx.Rollback(ctx)
	querier := db.New(tx)

	// Check the location has a relevant associated source
	params := db.GetLocationSourceParams{
		LocationID:     req.LocationId,
		SourceTypeName: energySourceMap[req.EnergySource],
	}
	dbSource, err := querier.GetLocationSource(ctx, params)
	if err != nil {
		l.Err(err).Msgf("querier.GetLocationSource(%+v)", params)
		return nil, status.Errorf(
			codes.NotFound,
			"Cannot create %s observations for location %d"+
				"as it does not have any recorded operational source of type %s.",
			energySourceMap[req.EnergySource], req.LocationId, energySourceMap[req.EnergySource],
		)
	}

	// Get the observer ID
	dbObserver, err := querier.GetObserverByName(ctx, strings.ToLower(req.ObserverName))
	if err != nil {
		l.Err(err).Msgf("querier.GetObserverByName({name: '%s'})", strings.ToLower(req.ObserverName))
		return nil, status.Errorf(
			codes.NotFound,
			"No observer of name '%s', found. Choose an existing observer or create a new one",
			req.ObserverName,
		)
	}

	params2 := make([]db.CreateObservationsAsPercentUsingBatchParams, len(req.Yields))
	for i, obs := range req.Yields {

		// IMPORTANT - the convertion to floats here has to happen in this way to avoid
		// integer division truncation errors.
		yield_pct := (float64(obs.YieldKw) / float64(dbSource.CapacityKw)) * 100
		params2[i] = db.CreateObservationsAsPercentUsingBatchParams{
			LocationID: req.LocationId,
			ObserverID: dbObserver.ObserverID,
			ObservationTimeUtc: pgtype.Timestamp{
				Time:  time.Unix(obs.TimestampUnix, 0).UTC(),
				Valid: true,
			},
			SourceTypeName: energySourceMap[req.EnergySource],
			YieldPct:       float32(yield_pct),
		}
	}

	batchResults := querier.CreateObservationsAsPercentUsingBatch(ctx, params2)
	count := 0
	batchResults.Exec(func(i int, err error) {
		if err != nil {
			l.Err(err).Msgf("querier.BatchCreateObservations(%+v)", params2[i])
		} else {
			count++
		}
	})

	if count < len(req.Yields) {
		return nil, status.Error(
			codes.InvalidArgument,
			"Invalid observation values. "+
				"Ensure the values are greater than zero and less than 110%.",
		)
	}

	log.Debug().Msgf(
		"Created %d observations from %s to %s for location %d and observer '%s'",
		count, params2[0].ObservationTimeUtc.Time, params2[len(params2)-1].ObservationTimeUtc.Time,
		req.LocationId, req.ObserverName,
	)

	return &pb.CreateObservationsResponse{}, tx.Commit(ctx)
}

func (q *PostgresDataPlatformServerImpl) CreateObserver(ctx context.Context, req *pb.CreateObserverRequest) (*pb.CreateObserverResponse, error) {
	l := log.With().Str("method", "CreateObserver").Logger()
	// Establish a transaction with the database
	tx, err := q.pool.Begin(ctx)
	if err != nil {
		l.Err(err).Msg("q.pool.Begin()")
		return nil, status.Errorf(codes.Internal, "Encountered database connection error")
	}
	defer tx.Rollback(ctx)

	querier := db.New(tx)

	dbObserverId, err := querier.CreateObserver(ctx, req.Name)
	if err != nil {
		l.Err(err).Msgf("querier.CreateObserver({name: %s})", req.Name)
		return nil, status.Error(codes.InvalidArgument, "Invalid observer name. Ensure it is not empty and is lowercase")
	}

	return &pb.CreateObserverResponse{ObserverId: dbObserverId}, tx.Commit(ctx)
}

func (q *PostgresDataPlatformServerImpl) GetPredictedCrossSection(ctx context.Context, req *pb.GetPredictedCrossSectionRequest) (*pb.GetPredictedCrossSectionResponse, error) {
	l := log.With().Str("method", "GetPredictedCrossSection").Logger()
	l.Debug().Msg("recieved method call")

	// Establish a transaction with the database
	tx, err := q.pool.Begin(ctx)
	if err != nil {
		l.Err(err).Msg("q.pool.Begin()")
		return nil, status.Errorf(codes.Internal, "Encountered database connection error")
	}
	defer tx.Rollback(ctx)
	querier := db.New(tx)

	// Get the default model
	dbModel, err := querier.GetDefaultModel(context.Background())
	if err != nil {
		l.Err(err).Msg("querier.GetDefaultModel()")
		return nil, status.Errorf(
			codes.Internal,
			"Couldn't get default model. Ensure a default model is set.",
		)
	}

	// Get the capacities of the locations
	params := db.GetLocationSourcesParams{
		SourceTypeName: energySourceMap[req.EnergySource],
		LocationIds:    req.LocationIds,
	}
	dbSources, err := querier.GetLocationSources(ctx, params)
	if err != nil {
		l.Err(err).Msgf("querier.GetLocationSources(%+v)", params)
		return nil, status.Errorf(
			codes.NotFound,
			"No '%s' sources found for the specified locations",
			energySourceMap[req.EnergySource],
		)
	}
	if len(dbSources) != len(req.LocationIds) {
		l.Warn().Msgf(
			"Expected %d location sources, but found %d. Some locations may not have sources.",
			len(req.LocationIds), len(dbSources),
		)
	}

	params2 := db.GetPredictionsAsPercentAtTimeAndHorizonForLocationsParams{
		SourceTypeName: energySourceMap[req.EnergySource],
		ModelID:        dbModel.ModelID,
		Time:           pgtype.Timestamp{Time: time.Unix(req.TimestampUnix, 0), Valid: true},
		HorizonMins:    0,
		LocationIds:    req.LocationIds,
	}
	dbCrossSection, err := querier.GetPredictionsAsPercentAtTimeAndHorizonForLocations(ctx, params2)
	if err != nil {
		l.Err(err).Msgf("querier.GetPredictionsAsPercentAtTimeAndHorizonForLocations(%+v)", params2)
		return nil, status.Errorf(
			codes.NotFound, "No predicted values found for the specified locations at the given time",
		)
	}

	yields := []*pb.GetPredictedCrossSectionResponse_YieldPredictionAtLocation{}
	// Only loop over the locations that have energy sources associated
	for _, value := range dbSources {
		// Find the cross section corresponding to the location with a source
		idx := slices.IndexFunc(dbCrossSection, func(row db.GetPredictionsAsPercentAtTimeAndHorizonForLocationsRow) bool {
			return row.LocationID == value.LocationID
		})
		if idx > -1 {
			yields = append(yields, &pb.GetPredictedCrossSectionResponse_YieldPredictionAtLocation{
				YieldKw:    int64(dbCrossSection[idx].P50Pct * float32(value.CapacityKw) / 100.0),
				LocationId: value.LocationID,
			})
		} else {
			log.Warn().Msgf("No predicted value found for location %d with source type '%s' at time %s",
				value.LocationID, energySourceMap[req.EnergySource], time.Unix(req.TimestampUnix, 0))
		}
	}

	return &pb.GetPredictedCrossSectionResponse{
		TimestampUnix: req.TimestampUnix,
		Yields:        yields,
	}, nil
}

func (q *PostgresDataPlatformServerImpl) GetPredictedTimeseriesDeltas(ctx context.Context, req *pb.GetPredictedTimeseriesDeltasRequest) (*pb.GetPredictedTimeseriesDeltasResponse, error) {
	currentTime := time.Now().UTC().Truncate(time.Minute)
	l := log.With().
		Str("method", "GetPredictedTimeseriesDeltas").
		Int32("locationID", req.LocationId).
		Str("energySource", energySourceMap[req.EnergySource]).
		Logger()
	l.Debug().Msg("recieved method call")

	// Establish a transaction with the database
	tx, err := q.pool.Begin(ctx)
	if err != nil {
		l.Err(err).Msg("q.pool.Begin()")
		return nil, status.Errorf(codes.Internal, "Encountered database connection error")
	}
	defer tx.Rollback(ctx)
	querier := db.New(tx)

	params := db.GetLocationSourceParams{
		LocationID:     req.LocationId,
		SourceTypeName: energySourceMap[req.EnergySource],
	}
	dbSource, err := querier.GetLocationSource(ctx, params)
	if err != nil {
		l.Err(err).Msgf("querier.GetLocationSource(%+v)", params)
		return nil, status.Errorf(codes.NotFound, "No '%s' source found for location %d", energySourceMap[req.EnergySource], req.LocationId)
	}

	var modelID int32
	if req.ModelName == "" {
		dbModel, err := querier.GetDefaultModel(ctx)
		if err != nil {
			l.Err(err).Msg("querier.GetDefaultModel()")
			return nil, status.Errorf(
				codes.Internal,
				"Couldn't get default model. Ensure a default model is set.",
			)
		}
		modelID = dbModel.ModelID
	} else {
		dbModel, err := querier.GetLatestModelByName(ctx, req.ModelName)
		if err != nil {
			l.Err(err).Msgf("querier.GetLatestModelByName({modelName: %s})", req.ModelName)
			return nil, status.Errorf(codes.NotFound, "No model found with name %s", req.ModelName)
		}
		modelID = dbModel.ModelID
	}

	params2 := db.GetPredictionsTimeseriesAsPercentAtHorizonParams{
		LocationID:     req.LocationId,
		SourceTypeName: energySourceMap[req.EnergySource],
		ModelID:        modelID,
		HorizonMins:    req.HorizonMins,
		PivotTimestamp: pgtype.Timestamp{Time: currentTime, Valid: true},
	}
	dbPredictions, err := querier.GetPredictionsTimeseriesAsPercentAtHorizon(ctx, params2)
	if err != nil {
		l.Err(err).Msgf("querier.GetWindowedPredictedGenerationValuesAtHorizon(%+v)", params2)
		return nil, status.Errorf(
			codes.NotFound,
			"No values found for location %d with horizon %d minutes",
			req.LocationId, req.HorizonMins,
		)
	}

	params3 := db.GetObservationsAsPercentBetweenParams{
		LocationID:     req.LocationId,
		SourceTypeName: energySourceMap[req.EnergySource],
		ObserverName:   req.ObserverName,
		StartTimeUtc:   dbPredictions[0].TargetTimeUtc,
		EndTimeUtc:     dbPredictions[len(dbPredictions)-1].TargetTimeUtc,
	}
	dbObservations, err := querier.GetObservationsAsPercentBetween(ctx, params3)
	if err != nil {
		l.Err(err).Msgf("querier.GetObservationsAsPercentBetween(%+v)", params3)
		return nil, status.Errorf(
			codes.NotFound,
			"No observations found for location %d with source type '%s' and observer '%s' in the specified time range",
			req.LocationId, energySourceMap[req.EnergySource], req.ObserverName,
		)
	}

	deltas := []*pb.YieldDelta{}
	for _, yield := range dbPredictions {

		// Find the corresponding observation value. Returns -1 if not found.
		obsIdx := slices.IndexFunc(dbObservations, func(obs db.GetObservationsAsPercentBetweenRow) bool {
			return obs.ObservationTimeUtc.Time.Equal(yield.TargetTimeUtc.Time)
		})
		if obsIdx > -1 {
			deltas = append(deltas, &pb.YieldDelta{
				DeltaKw:       int64(yield.P50Pct-dbObservations[obsIdx].YieldPct) * dbSource.CapacityKw / 100,
				TimestampUnix: yield.TargetTimeUtc.Time.Unix(),
			})
		}
	}
	if len(deltas) == 0 {
		l.Err(fmt.Errorf("no observations correspond to the predicted value timestamps for location %d and source type '%s'",
			req.LocationId, energySourceMap[req.EnergySource],
		)).Msg("No deltas found")
		return nil, status.Errorf(
			codes.NotFound,
			"No observations correspond to the predicted value timestamps for location %d and source type '%s'",
			req.LocationId, energySourceMap[req.EnergySource],
		)
	}

	return &pb.GetPredictedTimeseriesDeltasResponse{
		LocationId: req.LocationId,
		Deltas:     deltas,
	}, tx.Commit(ctx)
}

func (q *PostgresDataPlatformServerImpl) GetLatestForecast(ctx context.Context, req *pb.GetLatestForecastRequest) (*pb.GetLatestForecastResponse, error) {
	l := log.With().Str("method", "GetLatestForecast").Logger()
	l.Debug().Msg("recieved method call")

	currentTime := time.Now().UTC().Truncate(time.Minute)

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

	params := db.GetLocationSourceParams{
		LocationID:     int32(req.LocationId),
		SourceTypeName: energySourceMap[req.EnergySource],
	}
	dbLocation, err := querier.GetLocationSource(ctx, params)
	if err != nil {
		l.Err(err).Msgf("querier.GetLocationById(%+v)", params)
		return nil, status.Errorf(
			codes.NotFound,
			"No '%s' source found for location %d",
			energySourceMap[req.EnergySource], req.LocationId,
		)
	}

	params2 := db.GetLatestForecastAtHorizonParams{
		LocationID:     req.LocationId,
		ModelID:        dbModel.ModelID,
		SourceTypeName: energySourceMap[req.EnergySource],
		HorizonMins:    0,
		PivotTimestamp: pgtype.Timestamp{Time: currentTime, Valid: true},
	}
	dbForecast, err := querier.GetLatestForecastAtHorizon(ctx, params2)
	if err != nil {
		l.Err(err).Msgf("querier.GetLatestForecastAtHorizon(%+v)", params2)
		return nil, status.Errorf(codes.NotFound, "No forecast found for location %d", req.LocationId)
	}

	l.Debug().Msgf("Found forecast with ID %d for location %d", dbForecast.ForecastID, req.LocationId)

	dbValues, err := querier.GetPredictionsAsPercentByForecastID(ctx, dbForecast.ForecastID)
	if err != nil {
		l.Err(err).Msgf(
			"querier.GetPredictedGenerationValuesForForecast({forecastID: %d})",
			dbForecast.ForecastID,
		)
		return nil, status.Errorf(
			codes.NotFound,
			"No predicted generation values found for forecast %d",
			dbForecast.ForecastID,
		)
	}
	l.Debug().Msgf("Found %d predicted generation values for forecast %d", len(dbValues), dbForecast.ForecastID)

	predictedYields := make([]*pb.YieldPrediction, len(dbValues))
	for i, value := range dbValues {
		predictedYields[i] = &pb.YieldPrediction{
			YieldKw:       int64(value.P50Pct) * dbLocation.CapacityKw / 100,
			TimestampUnix: value.TargetTimeUtc.Time.Unix(),
			Uncertainty: &pb.YieldPrediction_Uncertainty{
				UpperKw: int64(value.P90Pct) * dbLocation.CapacityKw / 100,
				LowerKw: int64(value.P10Pct) * dbLocation.CapacityKw / 100,
			},
		}
	}

	return &pb.GetLatestForecastResponse{
		LocationId: int32(req.LocationId),
		Yields:     predictedYields,
	}, tx.Commit(ctx)
}

func (q *PostgresDataPlatformServerImpl) GetLocation(ctx context.Context, req *pb.GetLocationRequest) (*pb.GetLocationResponse, error) {
	l := log.With().Str("method", "GetLocation").Logger()
	l.Debug().Msg("recieved method call")

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

	// Get the sources associated with the location
	params := db.GetLocationSourceParams{
		LocationID:     int32(req.LocationId),
		SourceTypeName: energySourceMap[req.EnergySource],
	}
	dbSourceData, err := querier.GetLocationSource(ctx, params)
	if err != nil {
		l.Err(err).Msgf("querier.GetLocationSource(%+v)", params)
		return nil, status.Errorf(
			codes.NotFound,
			"No %s source associated with location with id %d",
			energySourceMap[req.EnergySource], req.LocationId,
		)
	}
	l.Debug().Msgf("Retrieved source for location %d", req.LocationId)

	return &pb.GetLocationResponse{
		LocationId: int32(req.LocationId),
		Name:       dbLocationData.LocationName,
		Latitude:   dbLocationData.Latitude,
		Longitude:  dbLocationData.Longitude,
		CapacityKw: dbSourceData.CapacityKw,
		Metadata:   string(dbSourceData.Metadata),
	}, tx.Commit(ctx)
}

func (q *PostgresDataPlatformServerImpl) CreateForecast(ctx context.Context, req *pb.CreateForecastRequest) (*pb.CreateForecastResponse, error) {
	l := log.With().Str("method", "CreateForecast").Logger()
	l.Debug().Msg("recieved method call")

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

	// Check the location has a relevant associated source
	params := db.GetLocationSourceParams{
		LocationID:     int32(req.Forecast.LocationId),
		SourceTypeName: energySourceMap[req.Forecast.EnergySource],
	}
	_, err = querier.GetLocationSource(ctx, params)
	if err != nil {
		l.Err(err).Msgf("querier.GetLocationSource(%+v)", params)
		return nil, status.Errorf(
			codes.NotFound,
			"Cannot make forecast for location %d "+
				"as it does not have any recorded operational source of type '%s'",
			req.Forecast.LocationId, energySourceMap[req.Forecast.EnergySource],
		)
	}

	// Create a new forecast
	params2 := db.CreateForecastParams{
		LocationID:     int32(req.Forecast.LocationId),
		SourceTypeName: energySourceMap[req.Forecast.EnergySource],
		ModelID:        int32(req.Forecast.ModelId),
		InitTimeUtc: pgtype.Timestamp{
			Time:  req.Forecast.InitTimeUtc.AsTime(),
			Valid: true,
		},
	}
	dbForecast, err := querier.CreateForecast(ctx, params2)
	if err != nil {
		l.Err(err).Msgf("querier.CreateForecast(%+v)", params2)
		return nil, status.Error(codes.InvalidArgument, "Invalid forecast")
	}
	l.Debug().Msgf("Created forecast with ID %d and init time %s", dbForecast.ForecastID, dbForecast.InitTimeUtc.Time)

	// Create the forecast data
	predictedGenerationValues := make([]db.CreatePredictionsAsPercentUsingBatchParams, len(req.PredictedGenerationValues))
	for i, value := range req.PredictedGenerationValues {
		metadata := []byte(value.Metadata)
		if value.Metadata == "" {
			metadata = nil
		}

		predictedGenerationValues[i] = db.CreatePredictionsAsPercentUsingBatchParams{
			HorizonMins: value.HorizonMins,
			P50Pct:      value.P50Pct,
			ForecastID:  dbForecast.ForecastID,
			TargetTimeUtc: pgtype.Timestamp{
				Time: req.Forecast.InitTimeUtc.AsTime().Add(
					time.Duration(value.HorizonMins) * time.Minute,
				),
				Valid: true,
			},
			P10Pct:   &value.P10Pct,
			P90Pct:   &value.P90Pct,
			Metadata: metadata,
		}
	}

	batchResults := querier.CreatePredictionsAsPercentUsingBatch(ctx, predictedGenerationValues)
	count := 0
	batchResults.Exec(func(i int, err error) {
		if err != nil {
			l.Err(err).Msgf("querier.BatchCreatePredictedGenerationValues(%+v)", predictedGenerationValues[i])
		} else {
			count++
		}
	})

	if count < len(predictedGenerationValues) {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid predicted generation values")
	}

	return &pb.CreateForecastResponse{}, tx.Commit(ctx)
}

func (q *PostgresDataPlatformServerImpl) CreateModel(ctx context.Context, req *pb.CreateModelRequest) (*pb.CreateModelResponse, error) {
	l := log.With().Str("method", "CreateModel").Logger()
	l.Debug().Msg("recieved method call")

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
		l.Err(err).Msgf("querier.CreateModel(%+v)", params)
		return nil, status.Errorf(
			codes.InvalidArgument,
			"Invalid model. Ensure name and version are not empty and are lowercase",
		)
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

func (q *PostgresDataPlatformServerImpl) CreateSite(ctx context.Context, req *pb.CreateSiteRequest) (*pb.CreateSiteResponse, error) {
	l := log.With().Str("method", "CreateSite").Logger()
	l.Debug().Msg("recieved method call")

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
	dbLocation, err := querier.CreateLocation(ctx, params)
	if err != nil {
		l.Err(err).Msgf("querier.CreateLocation(%+v)", params)
		return nil, status.Error(
			codes.InvalidArgument,
			"Invalid Site. Ensure name is not empty and uppercase, and that coordinates are valid WGS84.",
		)
	}
	l.Debug().Msgf("Created location '%s' of type 'site' with ID %d", dbLocation.LocationName, dbLocation.LocationID)

	metadata := []byte(req.Metadata)
	if req.Metadata == "" {
		metadata = nil
	}
	params2 := db.CreateLocationSourceParams{
		LocationID:     dbLocation.LocationID,
		SourceTypeName: energySourceMap[req.EnergySource],
		CapacityKw:     req.CapacityKw,
		Metadata:       metadata,
	}
	dbSource, err := querier.CreateLocationSource(ctx, params2)
	if err != nil {
		l.Err(err).Msgf("querier.CreateLocationSource(%+v)", params2)
		return nil, status.Error(
			codes.InvalidArgument,
			"Invalid site. Ensure metadata is NULL or a non-empty JSON object, and capacity is non-negative.",
		)
	}
	l.Debug().Msgf(
		"Created source of type '%s' for location %d with capacity %dx10^%d W",
		energySourceMap[req.EnergySource], dbLocation.LocationID, dbSource.Capacity, dbSource.CapacityUnitPrefixFactor,
	)
	return &pb.CreateSiteResponse{LocationId: dbLocation.LocationID}, tx.Commit(ctx)
}

func (q *PostgresDataPlatformServerImpl) CreateGsp(ctx context.Context, req *pb.CreateGspRequest) (*pb.CreateGspResponse, error) {
	l := log.With().Str("method", "CreateGsp").Logger()
	l.Debug().Msg("recieved method call")

	// Establish a transaction with the database
	tx, err := q.pool.Begin(ctx)
	if err != nil {
		l.Err(err).Msg("q.pool.Begin()")
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
	dbLocation, err := querier.CreateLocation(ctx, params)
	if err != nil {
		l.Err(err).Msgf("querier.CreateLocation(%+v)", params)
		return nil, status.Error(
			codes.InvalidArgument,
			"Invalid GSP. Ensure name is not empty and uppercase, and that geometry is valid WGS84.",
		)
	}

	metadata := []byte(req.Metadata)
	if req.Metadata == "" {
		metadata = nil
	}
	params2 := db.CreateLocationSourceParams{
		LocationID:     dbLocation.LocationID,
		SourceTypeName: energySourceMap[req.EnergySource],
		CapacityKw:     req.CapacityMw * 1000,
		Metadata:       metadata,
	}
	dbSource, err := querier.CreateLocationSource(ctx, params2)
	if err != nil {
		l.Err(err).Msgf("querier.CreateLocationSource(%+v)", params)
		return nil, status.Error(
			codes.InvalidArgument, "Invalid GSP. Ensure metadata is NULL or a non-empty JSON object.",
		)
	}

	l.Debug().Msgf(
		"Created source of type '%s' for location %d with capacity %dx10^%d W",
		energySourceMap[req.EnergySource], dbLocation.LocationID, dbSource.Capacity, dbSource.CapacityUnitPrefixFactor,
	)

	return &pb.CreateGspResponse{LocationId: dbLocation.LocationID}, tx.Commit(ctx)
}

func (q *PostgresDataPlatformServerImpl) GetLocationsAsGeoJSON(ctx context.Context, req *pb.GetLocationsAsGeoJSONRequest) (*pb.GetLocationsAsGeoJSONResponse, error) {
	l := log.With().Str("method", "GetLocationsAsGeoJSON").Logger()
	l.Debug().Msg("recieved method call")

	// Establish a transaction with the database
	tx, err := q.pool.Begin(ctx)
	if err != nil {
		l.Err(err).Msg("q.pool.Begin()")
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
	params := db.GetLocationGeoJSONByIdsParams{
		SimplificationLevel: simplificationLevel,
		LocationIds:         req.LocationIds,
	}
	geojson, err := querier.GetLocationGeoJSONByIds(ctx, params)
	if err != nil {
		l.Err(err).Msgf("querier.GetLocationGeoJSONByIds(%+v)", params)
		return nil, status.Error(codes.InvalidArgument, "No locations found for input IDs")
	}

	return &pb.GetLocationsAsGeoJSONResponse{Geojson: string(geojson)}, tx.Commit(ctx)
}

// GetPredictedTimeseries implements proto.QuartzAPIServer.
func (q *PostgresDataPlatformServerImpl) GetPredictedTimeseries(req *pb.GetPredictedTimeseriesRequest, stream grpc.ServerStreamingServer[pb.GetPredictedTimeseriesResponse]) error {
	l := log.With().Str("method", "GetPredictedTimeseries").Logger()
	l.Debug().Msg("recieved method call")

	currentTime := time.Now().UTC().Truncate(time.Minute)

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
		params := db.GetLocationSourceParams{
			LocationID:     locationId,
			SourceTypeName: energySourceMap[req.EnergySource],
		}
		dbSource, err := querier.GetLocationSource(stream.Context(), params)
		if err != nil {
			l.Err(err).Msgf("querier.GetLocationSource(%+v)", params)
			return status.Errorf(
				codes.NotFound,
				"No %s source found for location %d",
				energySourceMap[req.EnergySource], locationId,
			)
		}

		// Get the latest forecast for the location
		dbModel, err := querier.GetDefaultModel(stream.Context())
		if err != nil {
			l.Err(err).Msg("querier.GetDefaultModel()")
			return status.Errorf(codes.Internal, "Couldn't get default model. Ensure a default model is set.")
		}

		params2 := db.GetPredictionsTimeseriesAsPercentAtHorizonParams{
			LocationID:     locationId,
			ModelID:        dbModel.ModelID,
			SourceTypeName: energySourceMap[req.EnergySource],
			HorizonMins:    req.HorizonMins,
			PivotTimestamp: pgtype.Timestamp{Time: currentTime, Valid: true},
		}
		dbValues, err := querier.GetPredictionsTimeseriesAsPercentAtHorizon(stream.Context(), params2)
		if err != nil {
			l.Err(err).Msgf("querier.GetWindowedPredictedGenerationValuesAtHorizon(%+v)", params)
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
			yields[i] = &pb.YieldPrediction{
				YieldKw:       int64(float64(yield.P50Pct) * float64(dbSource.CapacityKw) / 100),
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
			return status.Errorf(
				codes.Internal,
				"Failed to send predicted timeseries response for location %d",
				locationId,
			)
		}
	}

	return nil
}

// NewPostgresDataPlatformServerImpl creates a new instance of the PostgresDataPlatformServer
// connecting to - and migrating - the postgres database at the provided connection URL. 
func NewPostgresDataPlatformServerImpl(connString string) *PostgresDataPlatformServerImpl {
	pool, err := pgxpool.New(
		context.Background(), connString,
	)
	if err != nil {
		log.Fatal().Msg("Unable to connect to database. Ensure DATABASE_URL is set correctly")
	}

	log.Debug().Msg("Running migrations")
	goose.SetBaseFS(embedMigrations)
	goose.SetLogger(goose.NopLogger())
	_ = goose.SetDialect("postgres")
	db := stdlib.OpenDBFromPool(pool)
	err = goose.Up(db, "sql/migrations")
	if err != nil {
		log.Fatal().Msgf("Unable to apply migrations: %v", err)
	}
	err = db.Close()
	if err != nil {
		log.Fatal().Msgf("Unable to close database connection: %v", err)
	}

	return &PostgresDataPlatformServerImpl{pool: pool}
}

var _ pb.DataPlatformServiceServer = (*PostgresDataPlatformServerImpl)(nil)
