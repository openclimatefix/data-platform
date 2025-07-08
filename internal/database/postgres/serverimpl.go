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
	"google.golang.org/protobuf/types/known/timestamppb"

	db "github.com/devsjc/fcfs/dp/internal/database/postgres/gen"
	pb "github.com/devsjc/fcfs/dp/internal/protogen/ocf/dp"

	"github.com/rs/zerolog/log"
)

//go:generate sqlc generate --file .sqlc.yaml

//go:embed sql/migrations/*.sql
var embedMigrations embed.FS

// --- Reuseable Functions for Route Logic -------------------------------------------------------

// capacityToValueMultiplier return a number, plus the index to raise 10 to the power to
// to get the resultant number of Watts, to the closest power of 3.
// This is an important function which tries to preserve accuracy whilst also enabling a
// large range of values to be represented by two 16 bit integers.
func capacityToValueMultiplier(capacityWatts uint64) (int16, int16, error) {
	if capacityWatts < 0 {
		return 0, 0, fmt.Errorf("input capacity %d cannot be negative", capacityWatts)
	}
	if capacityWatts == 0 {
		return 0, 0, nil
	}

	currentValue := capacityWatts
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
				capacityWatts, exponent+3)
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

// timeWindowToPgWindow converts a TimeWindow protobuf message to a pair of pgtype.Timestamp values.
func timeWindowToPgWindow(window *pb.TimeWindow) (start pgtype.Timestamp, end pgtype.Timestamp, err error) {
	currentTime := time.Now().UTC()
	if window == nil || (window.StartTimestampUnix == nil && window.EndTimestampUnix == nil) {
		start = pgtype.Timestamp{Time: currentTime.Add(-48 * time.Hour), Valid: true}
		end = pgtype.Timestamp{Time: currentTime.Add(36 * time.Hour), Valid: true}
	} else if window.StartTimestampUnix != nil && window.EndTimestampUnix != nil {
		start = pgtype.Timestamp{Time: window.StartTimestampUnix.AsTime(), Valid: true}
		end = pgtype.Timestamp{Time: window.EndTimestampUnix.AsTime(), Valid: true}
	} else {
		err = fmt.Errorf("Invalid time window: both start and end timestamps must be provided or neither")
	}
	return start, end, err
}

// --- Server Implementation ---------------------------------------------------------------------

type PostgresDataPlatformServerImpl struct {
	pool *pgxpool.Pool
}

func (s *PostgresDataPlatformServerImpl) GetWeekAverageDeltas(ctx context.Context, req *pb.GetWeekAverageDeltasRequest) (*pb.GetWeekAverageDeltasResponse, error) {
	l := log.With().Str("method", "GetWeekAverageDeltas").Logger()
	l.Debug().Msg("recieved method call")

	// Establish a transaction with the database
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		l.Err(err).Msg("q.pool.Begin()")
		return nil, status.Errorf(codes.Internal, "Encountered database connection error")
	}
	defer tx.Rollback(ctx)
	querier := db.New(tx)

	// Get the location and source
	params := db.GetLocationSourceParams{LocationID: req.LocationId, SourceTypeName: req.EnergySource.String()}
	dbLocationSource, err := querier.GetLocationSource(ctx, params)
	if err != nil {
		l.Err(err).Msgf("querier.GetLocationSource(%+v)", params)
		return nil, status.Errorf(
			codes.NotFound, "No location found for ID %d with source type '%s'.",
			req.LocationId, req.EnergySource,
		)
	}

	// Get the relevant predictor
	pctParams := db.GetPredictorElseLatestParams{
		PredictorName:    req.Model.ModelName,
		PredictorVersion: req.Model.ModelVersion,
	}
	dbPredictor, err := querier.GetPredictorElseLatest(ctx, pctParams)
	if err != nil {
		l.Err(err).Msgf("querier.GetPredictorElseLatest(%+v)", pctParams)
		return nil, status.Errorf(
			codes.NotFound, "No model found for name '%s' and version '%s'.",
			req.Model.ModelName, req.Model.ModelVersion,
		)
	}

	// Get the observer
	l.Info().Msgf("Queried observer '%s'", req.ObserverName)
	dbObserver, err := querier.GetObserverByName(ctx, req.ObserverName)
	if err != nil {
		l.Err(err).Msgf("querier.GetObserverByName({name: '%s'})", req.ObserverName)
		return nil, status.Errorf(
			codes.NotFound,
			"No observer of name '%s' found. Choose an existing observer or create a new one.",
			req.ObserverName,
		)
	}

	// Get the deltas
	avgParams := db.GetWeekAverageDeltasForLocationsParams{
		SourceTypeID:   dbLocationSource.SourceTypeID,
		PredictorID:    dbPredictor.PredictorID,
		ObserverID:   	dbObserver.ObserverID,
		PivotTimestamp: pgtype.Timestamp{Time: req.PivotTime.AsTime(), Valid: true},
		LocationIds:    []int32{req.LocationId},
	}
	dbDeltas, err := querier.GetWeekAverageDeltasForLocations(ctx, avgParams)
	if err != nil {
		l.Err(err).Msgf("querier.GetWeekAverageDeltasForLocations(%+v)", avgParams)
		return nil, status.Errorf(
			codes.NotFound, "No deltas found for location %d with source type '%s' and observer ID %d",
			req.LocationId, req.EnergySource, dbObserver.ObserverID,
		)
	}

	// Convert the deltas to the response format
	deltas := make([]*pb.GetWeekAverageDeltasResponse_AverageDelta, len(dbDeltas))
	for i, delta := range dbDeltas {
		deltas[i] = &pb.GetWeekAverageDeltasResponse_AverageDelta{
			DeltaPercent:  (float32(delta.AvgDeltaSip)/ 30000.0) * 100.0,
			HorizonMins: int32(delta.HorizonMins),
		}
	}
	return &pb.GetWeekAverageDeltasResponse{
		Deltas:        deltas,
		CapacityWatts: uint64(dbLocationSource.Capacity) * uint64(math.Pow10(int(dbLocationSource.CapacityUnitPrefixFactor))),
		InitTime: req.PivotTime.AsTime().Format("03:04"),
	}, nil
}

func (s *PostgresDataPlatformServerImpl) GetObservedTimeseries(ctx context.Context, req *pb.GetObservedTimeseriesRequest) (*pb.GetObservedTimeseriesResponse, error) {
	l := log.With().Str("method", "GetObservedTimeseries").Logger()
	l.Debug().Msg("recieved method call")

	// Establish a transaction with the database
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		l.Err(err).Msg("q.pool.Begin()")
		return nil, status.Errorf(codes.Internal, "Encountered database connection error")
	}
	defer tx.Rollback(ctx)
	querier := db.New(tx)

	// Get the location and source
	params := db.GetLocationSourceParams{LocationID: req.LocationId, SourceTypeName: req.EnergySource.String()}
	dbLocationSource, err := querier.GetLocationSource(ctx, params)
	if err != nil {
		l.Err(err).Msgf("querier.GetLocationSource(%+v)", params)
		return nil, status.Errorf(
			codes.NotFound, "No location found for ID %d with source type '%s'.",
			req.LocationId, req.EnergySource,
		)
	}

	// Get the observer
	dbObserver, err := querier.GetObserverByName(ctx, req.ObserverName)
	if err != nil {
		l.Err(err).Msgf("querier.GetObserverByName({name: '%s'})", req.ObserverName)
		return nil, status.Errorf(
			codes.NotFound,
			"No observer of name '%s' found. Choose an existing observer or create a new one.",
			req.ObserverName,
		)
	}

	// Get the observations
	start, end, err := timeWindowToPgWindow(req.TimeWindow)
	if err != nil {
		l.Err(err).Msgf("timeWindowToPgWindow(%+v)", req.TimeWindow)
		return nil, status.Errorf(codes.InvalidArgument, "Invalid time window: %v", err)
	}
	params2 := db.GetObservationsAsInt16BetweenParams{
		LocationID:   req.LocationId,
		SourceTypeID: dbLocationSource.SourceTypeID,
		ObserverID:   dbObserver.ObserverID,
		StartTimeUtc: start,
		EndTimeUtc:   end,
	}
	dbObs, err := querier.GetObservationsAsInt16Between(ctx, params2)
	if err != nil {
		l.Err(err).Msgf("querier.GetObservationsAsInt16Between(%+v)", params2)
		return nil, status.Errorf(codes.NotFound, "No observations found for location %d", req.LocationId)
	}

	yields := make([]*pb.Yield, len(dbObs))
	for i, obs := range dbObs {
		yields[i] = &pb.Yield{
			YieldPercent:  (float32(obs.ValueSip) / 30000.0) * 100.0,
			TimestampUnix: &timestamppb.Timestamp{Seconds: obs.ObservationTimeUtc.Time.Unix()},
		}
	}
	return &pb.GetObservedTimeseriesResponse{
		LocationId:    req.LocationId,
		Yields:        yields,
		CapacityWatts: uint64(dbLocationSource.Capacity) * uint64(math.Pow10(int(dbLocationSource.CapacityUnitPrefixFactor))),
	}, nil
}

func (s *PostgresDataPlatformServerImpl) CreateObservations(ctx context.Context, req *pb.CreateObservationsRequest) (*pb.CreateObservationsResponse, error) {
	l := log.With().Str("method", "CreateObservations").Logger()
	l.Debug().Msg("recieved method call")

	// Establish a transaction with the database
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		l.Err(err).Msg("q.pool.Begin()")
		return nil, status.Errorf(codes.Internal, "Encountered database connection error")
	}
	defer tx.Rollback(ctx)
	querier := db.New(tx)

	// Get the location and source
	params := db.GetLocationSourceParams{LocationID: req.LocationId, SourceTypeName: req.EnergySource.String()}
	dbLocationSource, err := querier.GetLocationSource(ctx, params)
	if err != nil {
		l.Err(err).Msgf("querier.GetLocationSource(%+v)", params)
		return nil, status.Errorf(
			codes.NotFound, "No location found for ID %d with source type '%s'.",
			req.LocationId, req.EnergySource,
		)
	}

	// Get the observer ID
	dbObserver, err := querier.GetObserverByName(ctx, strings.ToLower(req.ObserverName))
	if err != nil {
		l.Err(err).Msgf("querier.GetObserverByName({name: '%s'})", strings.ToLower(req.ObserverName))
		return nil, status.Errorf(
			codes.NotFound,
			"No observer of name '%s', found. Choose an existing observer or create a new one.",
			req.ObserverName,
		)
	}

	// Insert the observations
	params2 := make([]db.CreateObservationsAsInt16UsingCopyParams, len(req.Yields))
	for i, obs := range req.Yields {
		params2[i] = db.CreateObservationsAsInt16UsingCopyParams{
			LocationID: req.LocationId,
			ObserverID: dbObserver.ObserverID,
			ObservationTimeUtc: pgtype.Timestamp{
				Time:  obs.TimestampUnix.AsTime(),
				Valid: true,
			},
			SourceTypeID: dbLocationSource.SourceTypeID,
			ValueSip:     int16((obs.YieldPercent / 100.0) * 30000.0),
		}
	}
	count, err := querier.CreateObservationsAsInt16UsingCopy(ctx, params2)
	if err != nil {
		return nil, status.Error(
			codes.InvalidArgument,
			"Invalid observation values. Ensure the values are greater than zero and less than 110%.",
		)
	}

	log.Debug().Msgf(
		"Created %d observations from %s to %s for location %d and observer '%s'",
		count, params2[0].ObservationTimeUtc.Time, params2[len(params2)-1].ObservationTimeUtc.Time,
		req.LocationId, req.ObserverName,
	)

	return &pb.CreateObservationsResponse{}, tx.Commit(ctx)
}

func (s *PostgresDataPlatformServerImpl) CreateObserver(ctx context.Context, req *pb.CreateObserverRequest) (*pb.CreateObserverResponse, error) {
	l := log.With().Str("method", "CreateObserver").Logger()
	// Establish a transaction with the database
	tx, err := s.pool.Begin(ctx)
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

func (s *PostgresDataPlatformServerImpl) GetPredictedCrossSection(ctx context.Context, req *pb.GetPredictedCrossSectionRequest) (*pb.GetPredictedCrossSectionResponse, error) {
	l := log.With().Str("method", "GetPredictedCrossSection").Logger()
	l.Debug().Msg("recieved method call")

	// Establish a transaction with the database
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		l.Err(err).Msg("q.pool.Begin()")
		return nil, status.Errorf(codes.Internal, "Encountered database connection error")
	}
	defer tx.Rollback(ctx)
	querier := db.New(tx)

	// Get the energy source type
	dbSourceType, err := querier.GetSourceTypeByName(ctx, req.EnergySource.String())
	if err != nil {
		l.Err(err).Msgf("querier.GetSourceTypeByName({name: '%s'})", req.EnergySource)
		return nil, status.Errorf(codes.NotFound, "Unknown source type '%s'.", req.EnergySource)
	}

	// Get the relevant predictor
	params := db.GetPredictorElseLatestParams{
		PredictorName:    req.Model.ModelName,
		PredictorVersion: req.Model.ModelVersion,
	}
	dbPredictor, err := querier.GetPredictorElseLatest(ctx, params)
	if err != nil {
		l.Err(err).Msgf("querier.GetPredictorElseLatest(%+v)", params)
		return nil, status.Errorf(
			codes.NotFound, "No model found for name '%s' and version '%s'.",
			req.Model.ModelName, req.Model.ModelVersion,
		)
	}
	l.Debug().Msgf(
		"Using predictor '%s:%s' with ID %d",
		dbPredictor.PredictorName, dbPredictor.PredictorVersion, dbPredictor.PredictorID,
	)

	// Get the capacities of the locations
	params2 := db.ListLocationsSourcesParams{
		SourceTypeID: dbSourceType.SourceTypeID,
		LocationIds:  req.LocationIds,
	}
	dbSources, err := querier.ListLocationsSources(ctx, params2)
	if err != nil {
		l.Err(err).Msgf("querier.ListLocationsSources(%+v)", params2)
		return nil, status.Errorf(
			codes.NotFound,
			"No '%s' sources found for the specified locations", dbSourceType.SourceTypeName,
		)
	}
	if len(dbSources) != len(req.LocationIds) {
		l.Warn().Msgf(
			"Expected %d location sources, but found %d. Some locations may not have associated sources.",
			len(req.LocationIds), len(dbSources),
		)
	}

	params3 := db.ListPredictionsAtTimeForLocationsParams{
		LocationIds:  req.LocationIds,
		SourceTypeID: dbSourceType.SourceTypeID,
		PredictorID:  dbPredictor.PredictorID,
		Time:         pgtype.Timestamp{Time: req.TimestampUnix.AsTime(), Valid: true},
		HorizonMins:  0,
	}
	dbCrossSection, err := querier.ListPredictionsAtTimeForLocations(ctx, params3)
	if err != nil {
		l.Err(err).Msgf("querier.GetPredictionsAsPercentAtTimeAndHorizonForLocations(%+v)", params3)
		return nil, status.Errorf(
			codes.NotFound, "No predicted values found for the specified locations at the given time",
		)
	}

	yields := []*pb.GetPredictedCrossSectionResponse_YieldPredictionAtLocation{}
	// Only loop over the locations that have energy sources associated
	for _, value := range dbSources {
		// Find the cross section corresponding to the location with a source
		idx := slices.IndexFunc(dbCrossSection, func(row db.ListPredictionsAtTimeForLocationsRow) bool {
			return row.LocationID == value.LocationID
		})
		if idx > -1 {
			yields = append(yields, &pb.GetPredictedCrossSectionResponse_YieldPredictionAtLocation{
				YieldPercent:  (float32(dbCrossSection[idx].P50Sip) / 30000.0) * 100.0,
				CapacityWatts: uint64(value.Capacity) * uint64(math.Pow10(int(value.CapacityUnitPrefixFactor))),
				LocationId:    value.LocationID,
			})
		}
	}

	return &pb.GetPredictedCrossSectionResponse{
		TimestampUnix: req.TimestampUnix,
		Yields:        yields,
	}, nil
}

func (s *PostgresDataPlatformServerImpl) GetPredictedTimeseriesDeltas(ctx context.Context, req *pb.GetPredictedTimeseriesDeltasRequest) (*pb.GetPredictedTimeseriesDeltasResponse, error) {
	l := log.With().
		Str("method", "GetPredictedTimeseriesDeltas").
		Int32("locationID", req.LocationId).
		Str("energySource", req.EnergySource.String()).
		Logger()
	l.Debug().Msg("recieved method call")

	// Establish a transaction with the database
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		l.Err(err).Msg("q.pool.Begin()")
		return nil, status.Errorf(codes.Internal, "Encountered database connection error")
	}
	defer tx.Rollback(ctx)
	querier := db.New(tx)

	// Get the location and source
	params := db.GetLocationSourceParams{LocationID: req.LocationId, SourceTypeName: req.EnergySource.String()}
	dbLocationSource, err := querier.GetLocationSource(ctx, params)
	if err != nil {
		l.Err(err).Msgf("querier.GetLocationSource(%+v)", params)
		return nil, status.Errorf(
			codes.NotFound, "No location found for ID %d with source type '%s'.",
			req.LocationId, req.EnergySource,
		)
	}

	// Get the relevant predictor
	params2 := db.GetPredictorElseLatestParams{
		PredictorName:    req.Model.ModelName,
		PredictorVersion: req.Model.ModelVersion,
	}
	dbPredictor, err := querier.GetPredictorElseLatest(ctx, params2)
	if err != nil {
		l.Err(err).Msgf("querier.GetPredictorElseLatest(%+v)", params2)
		return nil, status.Errorf(
			codes.NotFound, "No model found for name '%s' and version '%s'.",
			req.Model.ModelName, req.Model.ModelVersion,
		)
	}

	// Get the predictions
	start, end, err := timeWindowToPgWindow(req.TimeWindow)
	if err != nil {
		l.Err(err).Msgf("timeWindowToPgWindow(%+v)", req.TimeWindow)
		return nil, status.Errorf(codes.InvalidArgument, "Invalid time window: %v", err)
	}
	params3 := db.ListPredictionsForLocationParams{
		LocationID:     req.LocationId,
		SourceTypeID:   dbLocationSource.SourceTypeID,
		PredictorID:    dbPredictor.PredictorID,
		HorizonMins:    req.HorizonMins,
		StartTimestamp: start,
		EndTimestamp:   end,
	}
	dbPredictions, err := querier.ListPredictionsForLocation(ctx, params3)
	if err != nil {
		l.Err(err).Msgf("querier.GetWindowedPredictedGenerationValuesAtHorizon(%+v)", params3)
		return nil, status.Errorf(
			codes.NotFound,
			"No values found for location %d with horizon %d minutes",
			req.LocationId, req.HorizonMins,
		)
	}

	// Get the observer ID
	dbObserver, err := querier.GetObserverByName(ctx, req.ObserverName)
	if err != nil {
		l.Err(err).Msgf("querier.GetObserverByName({name: '%s'})", req.ObserverName)
		return nil, status.Errorf(
			codes.NotFound,
			"No observer of name '%s' found. Choose an existing observer or create a new one.",
			req.ObserverName,
		)
	}

	params4 := db.GetObservationsAsInt16BetweenParams{
		LocationID:   req.LocationId,
		SourceTypeID: dbLocationSource.SourceTypeID,
		ObserverID:   dbObserver.ObserverID,
		StartTimeUtc: dbPredictions[0].TargetTimeUtc,
		EndTimeUtc:   dbPredictions[len(dbPredictions)-1].TargetTimeUtc,
	}
	dbObservations, err := querier.GetObservationsAsInt16Between(ctx, params4)
	if err != nil {
		l.Err(err).Msgf("querier.GetObservationsAsPercentBetween(%+v)", params4)
		return nil, status.Errorf(
			codes.NotFound,
			"No observations found for location %d with source type '%s' and observer '%s' in the specified time range",
			req.LocationId, req.EnergySource, req.ObserverName,
		)
	}

	deltas := []*pb.GetPredictedTimeseriesDeltasResponse_YieldDelta{}
	for _, yield := range dbPredictions {

		// Find the corresponding observation value. Returns -1 if not found.
		obsIdx := slices.IndexFunc(dbObservations, func(obs db.GetObservationsAsInt16BetweenRow) bool {
			return obs.ObservationTimeUtc.Time.Equal(yield.TargetTimeUtc.Time)
		})
		if obsIdx > -1 {
			deltas = append(deltas, &pb.GetPredictedTimeseriesDeltasResponse_YieldDelta{
				DeltaPercent:  (float32(yield.P50Sip-dbObservations[obsIdx].ValueSip) / 30000.0) * 100,
				TimestampUnix: timestamppb.New(yield.TargetTimeUtc.Time),
			})
		}
	}
	if len(deltas) == 0 {
		l.Err(fmt.Errorf("no observations correspond to the predicted value timestamps for location %d and source type '%s'",
			req.LocationId, req.EnergySource,
		)).Msg("No deltas found")
		return nil, status.Errorf(
			codes.NotFound,
			"No observations correspond to the predicted value timestamps for location %d and source type '%s'",
			req.LocationId, req.EnergySource,
		)
	}

	return &pb.GetPredictedTimeseriesDeltasResponse{
		LocationId:    req.LocationId,
		CapacityWatts: uint64(dbLocationSource.Capacity) * uint64(math.Pow10(int(dbLocationSource.CapacityUnitPrefixFactor))),
		Deltas:        deltas,
	}, tx.Commit(ctx)
}

func (s *PostgresDataPlatformServerImpl) GetLatestPredictions(ctx context.Context, req *pb.GetLatestPredictionsRequest) (*pb.GetLatestPredictionsResponse, error) {
	l := log.With().Str("method", "GetLatestForecast").Logger()
	l.Debug().Msg("recieved method call")

	currentTime := time.Now().UTC().Truncate(time.Minute)

	// Establish a transaction with the database
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		l.Err(err).Msg("failed to begin transaction")
		return nil, status.Error(codes.Internal, "Encountered database connection error")
	}
	defer tx.Rollback(ctx)
	querier := db.New(tx)

	// Get the location and source
	params := db.GetLocationSourceParams{LocationID: req.LocationId, SourceTypeName: req.EnergySource.String()}
	dbLocationSource, err := querier.GetLocationSource(ctx, params)
	if err != nil {
		l.Err(err).Msgf("querier.GetLocationSource(%+v)", params)
		return nil, status.Errorf(
			codes.NotFound, "No location found for ID %d with source type '%s'.",
			req.LocationId, req.EnergySource,
		)
	}

	// Get the relevant predictor
	params2 := db.GetPredictorElseLatestParams{
		PredictorName:    req.Model.ModelName,
		PredictorVersion: req.Model.ModelVersion,
	}
	dbPredictor, err := querier.GetPredictorElseLatest(ctx, params2)
	if err != nil {
		l.Err(err).Msgf("querier.GetPredictorElseLatest(%+v)", params2)
		return nil, status.Errorf(
			codes.NotFound, "No model found for name '%s' and version '%s'.",
			req.Model.ModelName, req.Model.ModelVersion,
		)
	}

	params3 := db.GetLatestForecastAtHorizonSincePivotParams{
		LocationID:     req.LocationId,
		PredictorID:    dbPredictor.PredictorID,
		SourceTypeID:   dbLocationSource.SourceTypeID,
		HorizonMins:    0,
		PivotTimestamp: pgtype.Timestamp{Time: currentTime, Valid: true},
	}
	dbForecast, err := querier.GetLatestForecastAtHorizonSincePivot(ctx, params3)
	if err != nil {
		l.Err(err).Msgf("querier.GetLatestForecastAtHorizon(%+v)", params3)
		return nil, status.Errorf(codes.NotFound, "No forecast found for location %d", req.LocationId)
	}

	l.Debug().Msgf("Found forecast with ID %d for location %d", dbForecast.ForecastID, req.LocationId)

	dbValues, err := querier.ListPredictionsForForecast(ctx, dbForecast.ForecastID)
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
			YieldPercent:  (float32(value.P50Sip) / 30000.0) * 100.0,
			TimestampUnix: timestamppb.New(value.TargetTimeUtc.Time),
			Uncertainty: &pb.YieldPrediction_Uncertainty{
				// TODO: Check this is okay to do?
				UpperPercent: (float32(*value.P90Sip) / 30000.0) * 100.0,
				LowerPercent: (float32(*value.P10Sip) / 30000.0) * 100.0,
			},
		}
	}

	return &pb.GetLatestPredictionsResponse{
		LocationId:    int32(req.LocationId),
		CapacityWatts: uint64(dbLocationSource.Capacity) * uint64(math.Pow10(int(dbLocationSource.CapacityUnitPrefixFactor))),
		Yields:        predictedYields,
	}, tx.Commit(ctx)
}

func (s *PostgresDataPlatformServerImpl) GetLocation(ctx context.Context, req *pb.GetLocationRequest) (*pb.GetLocationResponse, error) {
	l := log.With().Str("method", "GetLocation").Logger()
	l.Debug().Msg("recieved method call")

	// Establish a transaction with the database
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		l.Err(err).Msg("failed to begin transaction")
		return nil, status.Error(codes.Internal, "Encountered database connection error")
	}
	defer tx.Rollback(ctx)
	querier := db.New(tx)

	// Get the location and source
	params := db.GetLocationSourceParams{LocationID: req.LocationId, SourceTypeName: req.EnergySource.String()}
	dbLocationSource, err := querier.GetLocationSource(ctx, params)
	if err != nil {
		l.Err(err).Msgf("querier.GetLocationSource(%+v)", params)
		return nil, status.Errorf(
			codes.NotFound, "No location found for ID %d with source type '%s'.",
			req.LocationId, req.EnergySource,
		)
	}

	return &pb.GetLocationResponse{
		LocationId: int32(req.LocationId),
		Name:       dbLocationSource.LocationName,
		Latlng: &pb.LatLng{
			Latitude:  dbLocationSource.Latitude,
			Longitude: dbLocationSource.Longitude,
		},
		CapacityWatts: uint64(dbLocationSource.Capacity) * uint64(math.Pow10(int(dbLocationSource.CapacityUnitPrefixFactor))),
		Metadata:      string(dbLocationSource.Metadata),
	}, tx.Commit(ctx)
}

func (s *PostgresDataPlatformServerImpl) CreateForecast(ctx context.Context, req *pb.CreateForecastRequest) (*pb.CreateForecastResponse, error) {
	l := log.With().Str("method", "CreateForecast").Logger()
	l.Debug().Msg("recieved method call")

	if len(req.PredictedGenerationValues) == 0 {
		return nil, fmt.Errorf("no predicted generation values provided")
	}

	// Establish a transaction with the database
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		l.Err(err).Msg("failed to begin transaction")
		return nil, status.Error(codes.Internal, "Encountered database connection error")
	}
	defer tx.Rollback(ctx)
	querier := db.New(tx)

	// Get the location and source
	params := db.GetLocationSourceParams{LocationID: req.Forecast.LocationId, SourceTypeName: req.Forecast.EnergySource.String()}
	dbLocationSource, err := querier.GetLocationSource(ctx, params)
	if err != nil {
		l.Err(err).Msgf("querier.GetLocationSource(%+v)", params)
		return nil, status.Errorf(
			codes.NotFound, "No location found for ID %d with source type '%s'.",
			req.Forecast.LocationId, req.Forecast.EnergySource,
		)
	}

	// Create a new forecast
	params2 := db.CreateForecastParams{
		LocationID:   int32(req.Forecast.LocationId),
		SourceTypeID: dbLocationSource.SourceTypeID,
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
	paramsList := make([]db.CreatePredictionsAsInt16UsingCopyParams, len(req.PredictedGenerationValues))
	for i, value := range req.PredictedGenerationValues {
		metadata := []byte(value.Metadata)
		if value.Metadata == "" {
			metadata = nil
		}
		var p10sip int16
		p10sip = int16((value.P10Pct / 100.0) * 30000.0)
		var p90sip int16
		p90sip = int16((value.P90Pct / 100.0) * 30000.0)

		paramsList[i] = db.CreatePredictionsAsInt16UsingCopyParams{
			HorizonMins: int16(value.HorizonMins),
			P50Sip:      int16((value.P50Pct / 100.0) * 30000.0),
			ForecastID:  dbForecast.ForecastID,
			TargetTimeUtc: pgtype.Timestamp{
				Time: req.Forecast.InitTimeUtc.AsTime().Add(
					time.Duration(value.HorizonMins) * time.Minute,
				),
				Valid: true,
			},
			//
			P10Sip:   &p10sip,
			P90Sip:   &p90sip,
			Metadata: metadata,
		}
	}

	count, err := querier.CreatePredictionsAsInt16UsingCopy(ctx, paramsList)
	if err != nil || count < int64(len(req.PredictedGenerationValues)) {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid predicted generation values")
	}

	return &pb.CreateForecastResponse{}, tx.Commit(ctx)
}

func (s *PostgresDataPlatformServerImpl) CreateModel(ctx context.Context, req *pb.CreateModelRequest) (*pb.CreateModelResponse, error) {
	l := log.With().Str("method", "CreateModel").Logger()
	l.Debug().Msg("recieved method call")

	// Establish a transaction with the database
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		l.Err(err).Msg("q.pool.Begin()")
		return nil, status.Error(codes.Internal, "Encountered database connection error")
	}
	defer tx.Rollback(ctx)
	querier := db.New(tx)

	// Create a new predictor
	params := db.CreatePredictorParams{PredictorName: req.Name, PredictorVersion: req.Version}
	modelID, err := querier.CreatePredictor(ctx, params)
	if err != nil {
		l.Err(err).Msgf("querier.CreatePredictor(%+v)", params)
		return nil, status.Errorf(
			codes.InvalidArgument,
			"Invalid model. Ensure name and version are not empty and are lowercase",
		)
	}

	return &pb.CreateModelResponse{ModelId: modelID}, tx.Commit(ctx)
}

func (s *PostgresDataPlatformServerImpl) CreateSite(ctx context.Context, req *pb.CreateSiteRequest) (*pb.CreateSiteResponse, error) {
	l := log.With().Str("method", "CreateSite").Logger()
	l.Debug().Msg("recieved method call")

	// Establish a transaction with the database
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		l.Err(err).Msg("failed to begin transaction")
		return nil, status.Error(codes.Internal, "Encountered database connection error")
	}
	defer tx.Rollback(ctx)
	querier := db.New(tx)

	// Get the energy source type
	dbEnergySource, err := querier.GetSourceTypeByName(ctx, req.EnergySource.String())
	if err != nil {
		l.Err(err).Msgf("querier.GetSourceTypeByName({name: '%s'})", req.EnergySource)
		return nil, status.Errorf(codes.NotFound, "Unknown energy source '%s'.", req.EnergySource)
	}

	// Create a new location as a Site
	params := db.CreateLocationParams{
		LocationTypeName: "site",
		LocationName:     req.Name,
		Geom:             fmt.Sprintf("POINT(%.8f %.8f)", req.Latlng.Longitude, req.Latlng.Latitude),
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

	// Create a source associated with the location
	cp, ex, err := capacityToValueMultiplier(uint64(req.CapacityWatts))
	if err != nil {
		l.Err(err).Msgf("capacityKwToValueMultiplier(%d)", req.CapacityWatts)
		return nil, status.Error(codes.InvalidArgument, "Invalid capacity. Ensure capacity is non-negative.")
	}
	metadata := []byte(req.Metadata)
	if req.Metadata == "" {
		metadata = nil
	}
	params2 := db.CreateLocationSourceParams{
		LocationID:               dbLocation.LocationID,
		SourceTypeID:             dbEnergySource.SourceTypeID,
		Capacity:                 cp,
		CapacityUnitPrefixFactor: ex,
		CapacityLimitPercent:     nil, // TODO: Put this on the request object
		Metadata:                 metadata,
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
		dbEnergySource.SourceTypeName, dbLocation.LocationID, dbSource.Capacity, dbSource.CapacityUnitPrefixFactor,
	)
	return &pb.CreateSiteResponse{LocationId: dbLocation.LocationID}, tx.Commit(ctx)
}

func (s *PostgresDataPlatformServerImpl) CreateGsp(ctx context.Context, req *pb.CreateGspRequest) (*pb.CreateGspResponse, error) {
	l := log.With().Str("method", "CreateGsp").Logger()
	l.Debug().Msg("recieved method call")

	// Establish a transaction with the database
	tx, err := s.pool.Begin(ctx)
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

	// Get the energy source type
	dbEnergySource, err := querier.GetSourceTypeByName(ctx, req.EnergySource.String())
	if err != nil {
		l.Err(err).Msgf("querier.GetSourceTypeByName({name: '%s'})", req.EnergySource)
		return nil, status.Errorf(codes.NotFound, "Unknown energy source '%s'.", req.EnergySource)
	}

	// Create a source associated with the location
	metadata := []byte(req.Metadata)
	if req.Metadata == "" {
		metadata = nil
	}
	cp, ex, err := capacityToValueMultiplier(req.CapacityWatts)
	if err != nil {
		l.Err(err).Msgf("capacityMwToValueMultiplier(%d)", req.CapacityWatts)
		return nil, status.Error(codes.InvalidArgument, "Invalid capacity. Ensure capacity is non-negative.")
	}
	params2 := db.CreateLocationSourceParams{
		LocationID:               dbLocation.LocationID,
		SourceTypeID:             dbEnergySource.SourceTypeID,
		Capacity:                 cp,
		CapacityUnitPrefixFactor: ex,
		Metadata:                 metadata,
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
		req.EnergySource, dbLocation.LocationID, dbSource.Capacity, dbSource.CapacityUnitPrefixFactor,
	)

	return &pb.CreateGspResponse{LocationId: dbLocation.LocationID}, tx.Commit(ctx)
}

func (s *PostgresDataPlatformServerImpl) GetLocationsAsGeoJSON(ctx context.Context, req *pb.GetLocationsAsGeoJSONRequest) (*pb.GetLocationsAsGeoJSONResponse, error) {
	l := log.With().Str("method", "GetLocationsAsGeoJSON").Logger()
	l.Debug().Msg("recieved method call")

	// Establish a transaction with the database
	tx, err := s.pool.Begin(ctx)
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
func (s *PostgresDataPlatformServerImpl) GetPredictedTimeseries(req *pb.GetPredictedTimeseriesRequest, stream grpc.ServerStreamingServer[pb.GetPredictedTimeseriesResponse]) error {
	l := log.With().Str("method", "GetPredictedTimeseries").Logger()
	l.Debug().Msg("recieved method call")

	// Establish a transaction with the database
	tx, err := s.pool.Begin(stream.Context())
	if err != nil {
		l.Err(err).Msg("q.pool.Begin()")
		return status.Errorf(codes.Internal, "Encountered database connection error")
	}
	defer tx.Rollback(stream.Context())
	querier := db.New(tx)

	for _, locationId := range req.LocationIds {

		// Get the location and source
		params := db.GetLocationSourceParams{LocationID: locationId, SourceTypeName: req.EnergySource.String()}
		dbLocationSource, err := querier.GetLocationSource(stream.Context(), params)
		if err != nil {
			l.Err(err).Msgf("querier.GetLocationSource(%+v)", params)
			return status.Errorf(
				codes.NotFound, "No location found for ID %d with source type '%s'.",
				locationId, req.EnergySource,
			)
		}

		// Get the relevant predictor
		params2 := db.GetPredictorElseLatestParams{
			PredictorName:    req.Model.ModelName,
			PredictorVersion: req.Model.ModelVersion,
		}
		dbPredictor, err := querier.GetPredictorElseLatest(stream.Context(), params2)
		if err != nil {
			l.Err(err).Msgf("querier.GetPredictorElseLatest(%+v)", params2)
			return status.Errorf(
				codes.NotFound, "No model found for name '%s' and version '%s'.",
				req.Model.ModelName, req.Model.ModelVersion,
			)
		}

		// Get the predictions for the given location source
		start, end, err := timeWindowToPgWindow(req.TimeWindow)
		params3 := db.ListPredictionsForLocationParams{
			LocationID:     locationId,
			PredictorID:    dbPredictor.PredictorID,
			SourceTypeID:   dbLocationSource.SourceTypeID,
			HorizonMins:    req.HorizonMins,
			StartTimestamp: start,
			EndTimestamp:   end,
		}
		dbValues, err := querier.ListPredictionsForLocation(stream.Context(), params3)
		if err != nil {
			l.Err(err).Msgf("querier.GetWindowedPredictedGenerationValuesAtHorizon(%+v)", params3)
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
				YieldPercent:  (float32(yield.P50Sip) / 30000.0) * 100.0,
				TimestampUnix: timestamppb.New(yield.TargetTimeUtc.Time),
				Uncertainty: &pb.YieldPrediction_Uncertainty{
					UpperPercent: (float32(*yield.P90Sip) / 30000.0) * 100.0,
					LowerPercent: (float32(*yield.P10Sip) / 30000.0) * 100.0,
				},
			}
		}

		err = stream.Send(&pb.GetPredictedTimeseriesResponse{
			LocationId:    locationId,
			CapacityWatts: uint64(dbLocationSource.Capacity) * uint64(math.Pow10(int(dbLocationSource.CapacityUnitPrefixFactor))),
			Yields:        yields,
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
