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
	"encoding/json"
	"fmt"
	"math"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	db "github.com/devsjc/fcfs/dp/internal/database/postgres/gen"
	pb "github.com/devsjc/fcfs/dp/internal/gen/ocf/dp"

	"github.com/rs/zerolog/log"
)

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

type DataPlatformServerImpl struct {
	pool *pgxpool.Pool
}

func (s *DataPlatformServerImpl) StreamForecastData(req *pb.StreamForecastDataRequest, stream grpc.ServerStreamingServer[pb.StreamForecastDataResponse]) error {
	l := log.With().Str("method", "StreamForecastData").Logger()

	// Establish a transaction with the database
	tx, err := s.pool.Begin(stream.Context())
	if err != nil {
		l.Err(err).Msg("q.pool.Begin()")
		return status.Errorf(codes.Internal, "Encountered database connection error")
	}
	defer tx.Rollback(stream.Context())
	querier := db.New(tx)

	locationUuid, err := uuid.Parse(req.LocationUuid)
	if err != nil {
		l.Err(err).Msgf("uuid.Parse(%s)", req.LocationUuid)
		return status.Errorf(codes.InvalidArgument, "Invalid location UUID: %v", err)
	}
	srcParams := db.GetSourceAtTimestampParams{
		LocationUuid:   locationUuid,
		SourceTypeName: req.EnergySource.String(),
		AtTimestampUtc: pgtype.Timestamp{Time: req.TimeWindow.StartTimestampUnix.AsTime(), Valid: true},
	}
	dbSource, err := querier.GetSourceAtTimestamp(stream.Context(), srcParams)
	if err != nil {
		l.Err(err).Msgf("querier.GetSourceAtTimestamp(%+v)", srcParams)
		return status.Errorf(
			codes.NotFound, "No location found for uuid %s with source type '%s'.",
			req.LocationUuid, req.EnergySource,
		)
	}

	forecasts := make([]db.ListForecastsRow, 0)
	for _, model := range req.Models {
		fcParams := db.ListForecastsParams{
			LocationUuid:     dbSource.LocationUuid,
			SourceTypeID:     dbSource.SourceTypeID,
			PredictorName:    model.ModelName,
			PredictorVersion: model.ModelVersion,
			StartTimestamp:   pgtype.Timestamp{Time: req.TimeWindow.StartTimestampUnix.AsTime(), Valid: true},
			EndTimestamp:     pgtype.Timestamp{Time: req.TimeWindow.EndTimestampUnix.AsTime(), Valid: true},
		}
		dbForecasts, err := querier.ListForecasts(stream.Context(), fcParams)
		if err != nil {
			l.Err(err).Msgf("querier.ListForecasts(%+v)", fcParams)
			return status.Errorf(
				codes.NotFound, "No forecasts found for location '%s' and model %s:%s between %s and %s.",
				req.LocationUuid, model.ModelName, model.ModelVersion,
				req.TimeWindow.StartTimestampUnix.AsTime(), req.TimeWindow.EndTimestampUnix.AsTime(),
			)
		}
		forecasts = append(forecasts, dbForecasts...)
	}

	for _, forecast := range forecasts {
		psParams := db.ListPredictionsForForecastParams{ForecastUuid: forecast.ForecastUuid}
		dbPreds, err := querier.ListPredictionsForForecast(stream.Context(), psParams)
		if err != nil {
			l.Err(err).Msgf("querier.ListPredictionsForForecast(%+v)", psParams)
			return status.Errorf(
				codes.NotFound, "No predicted generation values found for forecast with init time %s",
				forecast.InitTimeUtc.Time,
			)
		}

		for i := range dbPreds {
			var p90 *float32
			if dbPreds[i].P90Sip == nil {
				p90 = nil
			} else {
				*p90 = (float32(*dbPreds[i].P90Sip) / 30000.0) * 100.0
			}

			var p10 *float32
			if dbPreds[i].P10Sip == nil {
				p10 = nil
			} else {
				*p10 = (float32(*dbPreds[i].P10Sip) / 30000.0) * 100.0
			}

			err = stream.Send(&pb.StreamForecastDataResponse{
				InitTimestamp: timestamppb.New(forecast.InitTimeUtc.Time),
				LocationUuid:  forecast.LocationUuid.String(),
				ModelFullname: fmt.Sprintf("%s:%s", forecast.PredictorName, forecast.PredictorVersion),
				HorizonMins:   uint32(dbPreds[i].HorizonMins),
				P50Percent:    (float32(dbPreds[i].P50Sip) / 30000.0) * 100.0,
				P10Percent:    p10,
				P90Percent:    p90,
			})
			if err != nil {
				return err
			}

		}

	}

	return nil
}

func (s *DataPlatformServerImpl) GetLocationsWithin(ctx context.Context, req *pb.GetLocationsWithinRequest) (*pb.GetLocationsWithinResponse, error) {
	l := log.With().Str("method", "GetLocationsWithin").Logger()

	// Establish a transaction with the database
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		l.Err(err).Msg("q.pool.Begin()")
		return nil, status.Errorf(codes.Internal, "Encountered database connection error")
	}
	defer tx.Rollback(ctx)
	querier := db.New(tx)

	locationUuid, err := uuid.Parse(req.LocationUuid)
	if err != nil {
		l.Err(err).Msgf("uuid.Parse(%s)", req.LocationUuid)
		return nil, status.Errorf(codes.InvalidArgument, "Invalid location UUID: %v", err)
	}
	lwParams := db.GetLocationsWithinParams{LocationUuid: locationUuid}
	dbLocations, err := querier.GetLocationsWithin(ctx, lwParams)
	if err != nil {
		l.Err(err).Msgf("querier.GetLocationIdsWithin(%+v)", lwParams)
		return nil, status.Errorf(
			codes.NotFound,
			"No locations found within the specified location '%s'", req.LocationUuid,
		)
	}

	locations := make([]*pb.GetLocationsWithinResponse_LocationData, len(dbLocations))
	for i := range dbLocations {
		locations[i] = &pb.GetLocationsWithinResponse_LocationData{
			LocationUuid: dbLocations[i].LocationUuid.String(),
			LocationName: strings.ToUpper(dbLocations[i].LocationName),
		}
	}

	return &pb.GetLocationsWithinResponse{
		Locations: locations,
	}, tx.Commit(ctx)
}

func (s *DataPlatformServerImpl) GetWeekAverageDeltas(ctx context.Context, req *pb.GetWeekAverageDeltasRequest) (*pb.GetWeekAverageDeltasResponse, error) {
	l := log.With().Str("method", "GetWeekAverageDeltas").Logger()

	// Establish a transaction with the database
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		l.Err(err).Msg("q.pool.Begin()")
		return nil, status.Errorf(codes.Internal, "Encountered database connection error")
	}
	defer tx.Rollback(ctx)
	querier := db.New(tx)

	// Get the location and source
	locationUuid, err := uuid.Parse(req.LocationUuid)
	if err != nil {
		l.Err(err).Msgf("uuid.Parse(%s)", req.LocationUuid)
		return nil, status.Errorf(codes.InvalidArgument, "Invalid location UUID: %v", err)
	}
	params := db.GetSourceAtTimestampParams{
		LocationUuid:   locationUuid,
		SourceTypeName: req.EnergySource.String(),
		AtTimestampUtc: pgtype.Timestamp{Time: req.PivotTime.AsTime(), Valid: true},
	}
	dbSource, err := querier.GetSourceAtTimestamp(ctx, params)
	if err != nil {
		l.Err(err).Msgf("querier.GetSourceAtTimestamp(%+v)", params)
		return nil, status.Errorf(
			codes.NotFound, "No location source found for name '%s' with source type '%s'.",
			req.LocationUuid, req.EnergySource,
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
	obParams := db.GetObserverByNameParams{ObserverName: req.ObserverName}
	dbObserver, err := querier.GetObserverByName(ctx, obParams)
	if err != nil {
		l.Err(err).Msgf("querier.GetObserverByName(%+v)", obParams)
		return nil, status.Errorf(
			codes.NotFound,
			"No observer of name '%s' found. Choose an existing observer or create a new one.",
			req.ObserverName,
		)
	}

	// Get the deltas
	avgParams := db.GetWeekAverageDeltasForLocationsParams{
		SourceTypeID:   dbSource.SourceTypeID,
		PredictorID:    dbPredictor.PredictorID,
		ObserverID:     dbObserver.ObserverID,
		PivotTimestamp: pgtype.Timestamp{Time: req.PivotTime.AsTime(), Valid: true},
		LocationUuids:  []uuid.UUID{locationUuid},
	}
	dbDeltas, err := querier.GetWeekAverageDeltasForLocations(ctx, avgParams)
	if err != nil {
		l.Err(err).Msgf("querier.GetWeekAverageDeltasForLocations(%+v)", avgParams)
		return nil, status.Errorf(
			codes.NotFound, "No deltas found for location '%s' with source type '%s' and observer ID %d",
			req.LocationUuid, req.EnergySource, dbObserver.ObserverID,
		)
	}

	// Convert the deltas to the response format
	deltas := make([]*pb.GetWeekAverageDeltasResponse_AverageDelta, len(dbDeltas))
	for i, delta := range dbDeltas {
		deltas[i] = &pb.GetWeekAverageDeltasResponse_AverageDelta{
			DeltaPercent:           (float32(delta.AvgDeltaSip) / 30000.0) * 100.0,
			HorizonMins:            uint32(delta.HorizonMins),
			EffectiveCapacityWatts: 3, // TODO
		}
	}
	return &pb.GetWeekAverageDeltasResponse{
		Deltas:        deltas,
		InitTimeOfDay: req.PivotTime.AsTime().Format("03:04"),
	}, nil
}

func (s *DataPlatformServerImpl) GetObservedTimeseries(ctx context.Context, req *pb.GetObservedTimeseriesRequest) (*pb.GetObservedTimeseriesResponse, error) {
	l := log.With().Str("method", "GetObservedTimeseries").Logger()

	// Establish a transaction with the database
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		l.Err(err).Msg("q.pool.Begin()")
		return nil, status.Errorf(codes.Internal, "Encountered database connection error")
	}
	defer tx.Rollback(ctx)
	querier := db.New(tx)

	// Get the location and source
	locationUuid, err := uuid.Parse(req.LocationUuid)
	if err != nil {
		l.Err(err).Msgf("uuid.Parse(%s)", req.LocationUuid)
		return nil, status.Errorf(codes.InvalidArgument, "Invalid location UUID: %v", err)
	}
	gsParams := db.GetSourceAtTimestampParams{
		LocationUuid:   locationUuid,
		SourceTypeName: req.EnergySource.String(),
		AtTimestampUtc: pgtype.Timestamp{Time: req.TimeWindow.StartTimestampUnix.AsTime(), Valid: true},
	}
	dbSource, err := querier.GetSourceAtTimestamp(ctx, gsParams)
	if err != nil {
		l.Err(err).Msgf("querier.GetSourceAtTimestamp(%+v)", gsParams)
		return nil, status.Errorf(
			codes.NotFound, "No location found for ID '%s' with source type '%s'.",
			req.LocationUuid, req.EnergySource,
		)
	}

	// Get the observer
	obParams := db.GetObserverByNameParams{ObserverName: req.ObserverName}
	dbObserver, err := querier.GetObserverByName(ctx, obParams)
	if err != nil {
		l.Err(err).Msgf("querier.GetObserverByName(%+v)", obParams)
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
	goParams := db.GetObservationsBetweenParams{
		LocationUuid: locationUuid,
		SourceTypeID: dbSource.SourceTypeID,
		ObserverID:   dbObserver.ObserverID,
		StartTimeUtc: start,
		EndTimeUtc:   end,
	}
	dbObs, err := querier.GetObservationsBetween(ctx, goParams)
	if err != nil {
		l.Err(err).Msgf("querier.GetObservationsAsInt16Between(%+v)", goParams)
		return nil, status.Errorf(codes.NotFound, "No observations found for location '%s'", req.LocationUuid)
	}

	values := make([]*pb.GetObservedTimeseriesResponse_Value, len(dbObs))
	for i, obs := range dbObs {
		values[i] = &pb.GetObservedTimeseriesResponse_Value{
			ValuePercent:           (float32(obs.ValueSip) / 30000.0) * 100.0,
			TimestampUnix:          &timestamppb.Timestamp{Seconds: obs.ObservationTimestampUtc.Time.Unix()},
			EffectiveCapacityWatts: uint64(float64(obs.EffectiveCapacity) * math.Pow10(int(obs.CapacityUnitPrefixFactor))),
		}
	}
	return &pb.GetObservedTimeseriesResponse{
		LocationUuid: dbSource.LocationUuid.String(),
		LocationName: strings.ToUpper(dbSource.LocationName),
		Values:       values,
	}, nil
}

func (s *DataPlatformServerImpl) CreateObservations(ctx context.Context, req *pb.CreateObservationsRequest) (*pb.CreateObservationsResponse, error) {
	l := log.With().Str("method", "CreateObservations").Logger()

	// Establish a transaction with the database
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		l.Err(err).Msg("q.pool.Begin()")
		return nil, status.Errorf(codes.Internal, "Encountered database connection error")
	}
	defer tx.Rollback(ctx)
	querier := db.New(tx)

	// Get the location and source
	locationUuid, err := uuid.Parse(req.LocationUuid)
	if err != nil {
		l.Err(err).Msgf("uuid.Parse(%s)", req.LocationUuid)
		return nil, status.Errorf(codes.InvalidArgument, "Invalid location UUID: %v", err)
	}
	params := db.GetSourceAtTimestampParams{
		LocationUuid:   locationUuid,
		SourceTypeName: req.EnergySource.String(),
		AtTimestampUtc: pgtype.Timestamp{Time: req.Values[0].TimestampUnix.AsTime(), Valid: true},
	}
	dbSource, err := querier.GetSourceAtTimestamp(ctx, params)
	if err != nil {
		l.Err(err).Msgf("querier.GetSourceAtTimestamp(%+v)", params)
		return nil, status.Errorf(
			codes.NotFound, "No location found for name '%s' with source type '%s'.",
			req.LocationUuid, req.EnergySource,
		)
	}

	// Get the observer ID
	obParams := db.GetObserverByNameParams{ObserverName: req.ObserverName}
	dbObserver, err := querier.GetObserverByName(ctx, obParams)
	if err != nil {
		l.Err(err).Msgf("querier.GetObserverByName(%+v)", obParams)
		return nil, status.Errorf(
			codes.NotFound,
			"No observer of name '%s', found. Choose an existing observer or create a new one.",
			req.ObserverName,
		)
	}

	// Insert the observations
	coParams := make([]db.CreateObservationsParams, len(req.Values))
	for i, v := range req.Values {
		coParams[i] = db.CreateObservationsParams{
			LocationUuid: locationUuid,
			ObserverID:   dbObserver.ObserverID,
			ObservationTimestampUtc: pgtype.Timestamp{
				Time:  v.TimestampUnix.AsTime(),
				Valid: true,
			},
			SourceTypeID: dbSource.SourceTypeID,
			ValueSip:     int16((v.ValuePercent / 100.0) * 30000.0),
		}
	}
	count, err := querier.CreateObservations(ctx, coParams)
	if err != nil {
		return nil, status.Error(
			codes.InvalidArgument,
			"Invalid observation values. Ensure the values are greater than zero and less than 110%.",
		)
	}

	log.Debug().Msgf(
		"Created %d observations from %s to %s for location '%s' and observer '%s'",
		count, coParams[0].ObservationTimestampUtc.Time, coParams[len(coParams)-1].ObservationTimestampUtc.Time,
		dbSource.LocationUuid, req.ObserverName,
	)

	return &pb.CreateObservationsResponse{}, tx.Commit(ctx)
}

func (s *DataPlatformServerImpl) CreateObserver(ctx context.Context, req *pb.CreateObserverRequest) (*pb.CreateObserverResponse, error) {
	l := log.With().Str("method", "CreateObserver").Logger()
	// Establish a transaction with the database
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		l.Err(err).Msg("q.pool.Begin()")
		return nil, status.Errorf(codes.Internal, "Encountered database connection error")
	}
	defer tx.Rollback(ctx)

	querier := db.New(tx)

	obParams := db.CreateObserverParams{ObserverName: req.Name}
	dbObserverId, err := querier.CreateObserver(ctx, obParams)
	if err != nil {
		l.Err(err).Msgf("querier.CreateObserver(%+v)", obParams)
		return nil, status.Error(codes.InvalidArgument, "Invalid observer name. Ensure it is not empty and is lowercase")
	}

	return &pb.CreateObserverResponse{ObserverId: dbObserverId}, tx.Commit(ctx)
}

func (s *DataPlatformServerImpl) GetPredictedCrossSection(ctx context.Context, req *pb.GetPredictedCrossSectionRequest) (*pb.GetPredictedCrossSectionResponse, error) {
	l := log.With().Str("method", "GetPredictedCrossSection").Logger()

	// Establish a transaction with the database
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		l.Err(err).Msg("q.pool.Begin()")
		return nil, status.Errorf(codes.Internal, "Encountered database connection error")
	}
	defer tx.Rollback(ctx)
	querier := db.New(tx)

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
	locationUuids := make([]uuid.UUID, len(req.LocationUuids))
	for i, loc := range req.LocationUuids {
		locationUuids[i], err = uuid.Parse(loc)
		if err != nil {
			l.Err(err).Msgf("uuid.Parse(%s)", loc)
			return nil, status.Errorf(codes.InvalidArgument, "Invalid location UUID: %v", err)
		}
	}
	lsParams := db.ListSourcesAtTimestampParams{
		SourceTypeName: req.EnergySource.String(),
		LocationUuids:  locationUuids,
		AtTimestampUtc: pgtype.Timestamp{Time: req.TimestampUnix.AsTime(), Valid: true},
	}
	dbSources, err := querier.ListSourcesAtTimestamp(ctx, lsParams)
	if err != nil || len(dbSources) == 0 {
		l.Err(err).Msgf("querier.ListLocationsSources(%+v)", lsParams)
		return nil, status.Errorf(
			codes.NotFound,
			"No '%s' sources found for the specified locations", req.EnergySource.String(),
		)
	}
	if len(dbSources) != len(req.LocationUuids) {
		l.Warn().Msgf(
			"Expected %d location sources, but found %d. Some locations may not have associated sources.",
			len(req.LocationUuids), len(dbSources),
		)
	}
	ids := make([]uuid.UUID, len(dbSources))
	for i := range dbSources {
		ids[i] = dbSources[i].LocationUuid
	}

	params3 := db.ListPredictionsAtTimeForLocationsParams{
		LocationUuids: ids,
		SourceTypeID:  dbSources[0].SourceTypeID,
		PredictorID:   dbPredictor.PredictorID,
		Time:          pgtype.Timestamp{Time: req.TimestampUnix.AsTime(), Valid: true},
		HorizonMins:   0,
	}
	dbCrossSection, err := querier.ListPredictionsAtTimeForLocations(ctx, params3)
	if err != nil {
		l.Err(err).Msgf("querier.GetPredictionsAsPercentAtTimeAndHorizonForLocations(%+v)", params3)
		return nil, status.Errorf(
			codes.NotFound, "No predicted values found for the specified locations at the given time",
		)
	}

	values := []*pb.GetPredictedCrossSectionResponse_Value{}
	// Only loop over the locations that have energy sources associated
	for _, value := range dbSources {
		// Find the cross section corresponding to the location with a source
		idx := slices.IndexFunc(dbCrossSection, func(row db.ListPredictionsAtTimeForLocationsRow) bool {
			return row.LocationUuid == value.LocationUuid
		})
		if idx > -1 {
			values = append(values, &pb.GetPredictedCrossSectionResponse_Value{
				ValuePercent:           (float32(dbCrossSection[idx].P50Sip) / 30000.0) * 100.0,
				EffectiveCapacityWatts: uint64(value.Capacity) * uint64(math.Pow10(int(value.CapacityUnitPrefixFactor))),
				LocationUuid:           value.LocationUuid.String(),
				LocationName:           strings.ToUpper(value.LocationName),
				Latlng: &pb.LatLng{
					Latitude:  value.Latitude,
					Longitude: value.Longitude,
				},
			})
		}
	}

	return &pb.GetPredictedCrossSectionResponse{
		TimestampUnix: req.TimestampUnix,
		Values:        values,
	}, nil
}

func (s *DataPlatformServerImpl) GetPredictedTimeseriesDeltas(ctx context.Context, req *pb.GetPredictedTimeseriesDeltasRequest) (*pb.GetPredictedTimeseriesDeltasResponse, error) {
	l := log.With().Str("method", "GetPredictedTimeseriesDeltas").Logger()

	// Establish a transaction with the database
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		l.Err(err).Msg("q.pool.Begin()")
		return nil, status.Errorf(codes.Internal, "Encountered database connection error")
	}
	defer tx.Rollback(ctx)
	querier := db.New(tx)

	// Get the location and source
	locationUuid, err := uuid.Parse(req.LocationUuid)
	if err != nil {
		l.Err(err).Msgf("uuid.Parse(%s)", req.LocationUuid)
		return nil, status.Errorf(codes.InvalidArgument, "Invalid location UUID: %v", err)
	}
	params := db.GetSourceAtTimestampParams{
		LocationUuid:   locationUuid,
		SourceTypeName: req.EnergySource.String(),
		AtTimestampUtc: pgtype.Timestamp{Time: req.TimeWindow.StartTimestampUnix.AsTime(), Valid: true},
	}
	dbSource, err := querier.GetSourceAtTimestamp(ctx, params)
	if err != nil {
		l.Err(err).Msgf("querier.GetSourceAtTimestamp(%+v)", params)
		return nil, status.Errorf(
			codes.NotFound, "No location found for ID '%s' with source type '%s'.",
			req.LocationUuid, req.EnergySource,
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
	lpParams := db.ListPredictionsForLocationParams{
		LocationUuid:   dbSource.LocationUuid,
		SourceTypeID:   dbSource.SourceTypeID,
		PredictorID:    dbPredictor.PredictorID,
		HorizonMins:    int32(req.HorizonMins),
		StartTimestamp: start,
		EndTimestamp:   end,
	}
	dbPredictions, err := querier.ListPredictionsForLocation(ctx, lpParams)
	if err != nil {
		l.Err(err).Msgf("querier.GetWindowedPredictedGenerationValuesAtHorizon(%+v)", lpParams)
		return nil, status.Errorf(
			codes.NotFound,
			"No values found for location '%s' with horizon %d minutes",
			req.LocationUuid, req.HorizonMins,
		)
	}

	// Get the observer ID
	obParams := db.GetObserverByNameParams{ObserverName: req.ObserverName}
	dbObserver, err := querier.GetObserverByName(ctx, obParams)
	if err != nil {
		l.Err(err).Msgf("querier.GetObserverByName(%+v)", obParams)
		return nil, status.Errorf(
			codes.NotFound,
			"No observer of name '%s' found. Choose an existing observer or create a new one.",
			req.ObserverName,
		)
	}

	goParams := db.GetObservationsBetweenParams{
		LocationUuid: dbSource.LocationUuid,
		SourceTypeID: dbSource.SourceTypeID,
		ObserverID:   dbObserver.ObserverID,
		StartTimeUtc: dbPredictions[0].TargetTimeUtc,
		EndTimeUtc:   dbPredictions[len(dbPredictions)-1].TargetTimeUtc,
	}
	dbObservations, err := querier.GetObservationsBetween(ctx, goParams)
	if err != nil {
		l.Err(err).Msgf("querier.GetObservationsAsPercentBetween(%+v)", goParams)
		return nil, status.Errorf(
			codes.NotFound,
			"No observations found for location '%s' with source type '%s' and observer '%s' in the specified time range",
			req.LocationUuid, req.EnergySource, req.ObserverName,
		)
	}

	values := []*pb.GetPredictedTimeseriesDeltasResponse_Value{}
	for _, yield := range dbPredictions {

		// Find the corresponding observation value. Returns -1 if not found.
		obsIdx := slices.IndexFunc(dbObservations, func(obs db.GetObservationsBetweenRow) bool {
			return obs.ObservationTimestampUtc.Time.Equal(yield.TargetTimeUtc.Time)
		})
		if obsIdx > -1 {
			values = append(values, &pb.GetPredictedTimeseriesDeltasResponse_Value{
				DeltaPercent:           (float32(yield.P50Sip-dbObservations[obsIdx].ValueSip) / 30000.0) * 100,
				TimestampUnix:          timestamppb.New(yield.TargetTimeUtc.Time),
				EffectiveCapacityWatts: uint64(dbSource.Capacity) * uint64(math.Pow10(int(dbSource.CapacityUnitPrefixFactor))), // TODO: Capacity
			})
		}
	}
	if len(values) == 0 {
		l.Err(fmt.Errorf("no observations correspond to the predicted value timestamps for location '%s' and source type '%s'",
			req.LocationUuid, req.EnergySource,
		)).Msg("No deltas found")
		return nil, status.Errorf(
			codes.NotFound,
			"No observations correspond to the predicted value timestamps for location '%s' and source type '%s'",
			req.LocationUuid, req.EnergySource,
		)
	}

	return &pb.GetPredictedTimeseriesDeltasResponse{
		LocationUuid: dbSource.LocationUuid.String(),
		LocationName: strings.ToUpper(dbSource.LocationName),
		Values:       values,
	}, tx.Commit(ctx)
}

func (s *DataPlatformServerImpl) GetLatestPredictions(ctx context.Context, req *pb.GetLatestPredictionsRequest) (*pb.GetLatestPredictionsResponse, error) {
	l := log.With().Str("method", "GetLatestPredictions").Logger()

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
	locationUuid, err := uuid.Parse(req.LocationUuid)
	if err != nil {
		l.Err(err).Msgf("uuid.Parse(%s)", req.LocationUuid)
		return nil, status.Errorf(codes.InvalidArgument, "Invalid location UUID: %v", err)
	}
	gsParams := db.GetSourceAtTimestampParams{
		LocationUuid:   locationUuid,
		SourceTypeName: req.EnergySource.String(),
		AtTimestampUtc: pgtype.Timestamp{Time: currentTime, Valid: true},
	}
	dbSource, err := querier.GetSourceAtTimestamp(ctx, gsParams)
	if err != nil {
		l.Err(err).Msgf("querier.GetSourceAtTimestamp(%+v)", gsParams)
		return nil, status.Errorf(
			codes.NotFound, "No location found for ID '%s' with source type '%s'.",
			req.LocationUuid, req.EnergySource,
		)
	}

	// Get the relevant predictor
	gpParams := db.GetPredictorElseLatestParams{
		PredictorName:    req.Model.ModelName,
		PredictorVersion: req.Model.ModelVersion,
	}
	dbPredictor, err := querier.GetPredictorElseLatest(ctx, gpParams)
	if err != nil {
		l.Err(err).Msgf("querier.GetPredictorElseLatest(%+v)", gpParams)
		return nil, status.Errorf(
			codes.NotFound, "No model found for name '%s' and version '%s'.",
			req.Model.ModelName, req.Model.ModelVersion,
		)
	}

	params3 := db.GetLatestForecastAtHorizonSincePivotParams{
		LocationUuid:   locationUuid,
		PredictorID:    dbPredictor.PredictorID,
		SourceTypeID:   dbSource.SourceTypeID,
		HorizonMins:    0,
		PivotTimestamp: pgtype.Timestamp{Time: currentTime, Valid: true},
	}
	dbForecast, err := querier.GetLatestForecastAtHorizonSincePivot(ctx, params3)
	if err != nil {
		l.Err(err).Msgf("querier.GetLatestForecastAtHorizon(%+v)", params3)
		return nil, status.Errorf(codes.NotFound, "No forecast found for location '%s'", req.LocationUuid)
	}

	l.Debug().Msgf("Found forecast with ID '%s' for location '%s'", dbForecast.ForecastUuid, req.LocationUuid)

	psParams := db.ListPredictionsForForecastParams{ForecastUuid: dbForecast.ForecastUuid}
	dbValues, err := querier.ListPredictionsForForecast(ctx, psParams)
	if err != nil {
		l.Err(err).Msgf("querier.GetPredictedGenerationValuesForForecast(%+v)", psParams)
		return nil, status.Errorf(
			codes.NotFound,
			"No predicted generation values found for forecast %s",
			dbForecast.ForecastUuid,
		)
	}
	l.Debug().Msgf("Found %d predicted generation values for forecast %d", len(dbValues), dbForecast.ForecastUuid)

	values := make([]*pb.GetLatestPredictionsResponse_Value, len(dbValues))
	for i, value := range dbValues {

		var p10 float32
		if value.P10Sip == nil {
			p10 = float32(math.NaN())
		} else {
			p10 = (float32(*value.P10Sip) / 30000.0) * 100.0
		}

		var p90 float32
		if value.P90Sip == nil {
			p90 = float32(math.NaN())
		} else {
			p90 = (float32(*value.P90Sip) / 30000.0) * 100.0
		}

		values[i] = &pb.GetLatestPredictionsResponse_Value{
			TimestampUnix:          timestamppb.New(value.TargetTimeUtc.Time),
			P50Percent:             (float32(value.P50Sip) / 30000.0) * 100.0,
			P10Percent:             p10,
			P90Percent:             p90,
			EffectiveCapacityWatts: uint64(dbSource.Capacity) * uint64(math.Pow10(int(dbSource.CapacityUnitPrefixFactor))), // TODO: Capacity
		}
	}

	return &pb.GetLatestPredictionsResponse{
		LocationUuid: dbSource.LocationUuid.String(),
		LocationName: strings.ToUpper(dbSource.LocationName),
		Values:       values,
	}, tx.Commit(ctx)
}

func (s *DataPlatformServerImpl) GetLocation(ctx context.Context, req *pb.GetLocationRequest) (*pb.GetLocationResponse, error) {
	l := log.With().Str("method", "GetLocation").Logger()

	// Establish a transaction with the database
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		l.Err(err).Msg("failed to begin transaction")
		return nil, status.Error(codes.Internal, "Encountered database connection error")
	}
	defer tx.Rollback(ctx)
	querier := db.New(tx)

	// Get the location and source
	locationUuid, err := uuid.Parse(req.LocationUuid)
	if err != nil {
		l.Err(err).Msgf("uuid.Parse(%s)", req.LocationUuid)
		return nil, status.Errorf(codes.InvalidArgument, "Invalid location UUID: %v", err)
	}
	params := db.GetSourceAtTimestampParams{
		LocationUuid:   locationUuid,
		SourceTypeName: req.EnergySource.String(),
		AtTimestampUtc: pgtype.Timestamp{Time: time.Now().UTC(), Valid: true},
	}
	dbSource, err := querier.GetSourceAtTimestamp(ctx, params)
	if err != nil {
		l.Err(err).Msgf("querier.GetSourceAtTimestamp(%+v)", params)
		return nil, status.Errorf(
			codes.NotFound, "No location source found for name '%s' with source type '%s'. Ensure the location has an associated source and it is not decomissioned.",
			req.LocationUuid, req.EnergySource,
		)
	}

	var metadataMap map[string]any
	if dbSource.MetadataJsonb == nil {
		metadataMap = map[string]any{}
	} else {
		err = json.Unmarshal(dbSource.MetadataJsonb, &metadataMap)
		if err != nil {
			l.Err(err).Msgf("json.Unmarshal(%s)", dbSource.MetadataJsonb)
			return nil, status.Errorf(codes.Internal, "Failed to parse metadata for location '%s'", req.LocationUuid)
		}
	}

	metadata, err := structpb.NewStruct(metadataMap)
	if err != nil {
		l.Err(err).Msgf("structpb.NewStruct(%+v)", metadataMap)
		return nil, status.Errorf(codes.Internal, "Failed to convert metadata for location '%s'", req.LocationUuid)
	}

	return &pb.GetLocationResponse{
		LocationUuid: dbSource.LocationUuid.String(),
		LocationName: strings.ToUpper(dbSource.LocationName),
		Latlng: &pb.LatLng{
			Latitude:  dbSource.Latitude,
			Longitude: dbSource.Longitude,
		},
		CapacityWatts: uint64(dbSource.Capacity) * uint64(math.Pow10(int(dbSource.CapacityUnitPrefixFactor))),
		Metadata:      metadata,
	}, tx.Commit(ctx)
}

func (s *DataPlatformServerImpl) CreateForecast(ctx context.Context, req *pb.CreateForecastRequest) (*pb.CreateForecastResponse, error) {
	l := log.With().Str("method", "CreateForecast").Logger()

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
	locationUuid, err := uuid.Parse(req.Forecast.LocationUuid)
	if err != nil {
		l.Err(err).Msgf("uuid.Parse(%s)", req.Forecast.LocationUuid)
		return nil, status.Errorf(codes.InvalidArgument, "Invalid location UUID: %v", err)
	}
	gsParams := db.GetSourceAtTimestampParams{
		LocationUuid:   locationUuid,
		SourceTypeName: req.Forecast.EnergySource.String(),
		AtTimestampUtc: pgtype.Timestamp{Time: req.Forecast.InitTimeUtc.AsTime(), Valid: true},
	}
	dbSource, err := querier.GetSourceAtTimestamp(ctx, gsParams)
	if err != nil {
		l.Err(err).Msgf("querier.GetSourceAtTimestamp(%+v)", gsParams)
		return nil, status.Errorf(
			codes.NotFound, "No location found for id '%s' with source type '%s'.",
			req.Forecast.LocationUuid, req.Forecast.EnergySource,
		)
	}
	resolution_mins := req.PredictedGenerationValues[1].HorizonMins - req.PredictedGenerationValues[0].HorizonMins // TODO: Check they are all the same

	// Create a new forecast
	params2 := db.CreateForecastParams{
		LocationUuid:        locationUuid,
		SourceTypeID:        dbSource.SourceTypeID,
		PredictorName:       req.Forecast.Model.ModelName,
		PredictorVersion:    req.Forecast.Model.ModelVersion,
		ValueResolutionMins: int16(resolution_mins),
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
	l.Debug().Msgf("Created forecast with ID '%s' and init time %s", dbForecast.ForecastUuid, dbForecast.InitTimeUtc.Time)

	// Create the forecast data
	paramsList := make([]db.CreatePredictedValuesParams, len(req.PredictedGenerationValues))
	for i, value := range req.PredictedGenerationValues {
		p10sip := int16((value.P10Pct / 100.0) * 30000.0)
		p90sip := int16((value.P90Pct / 100.0) * 30000.0)
		metadata, err := value.Metadata.MarshalJSON()
		if err != nil {
			l.Err(err).Msgf("value.Metadata.MarshalJSON()")
			return nil, status.Errorf(codes.InvalidArgument, "Invalid metadata for predicted generation value at horizon %d mins", value.HorizonMins)
		}

		paramsList[i] = db.CreatePredictedValuesParams{
			HorizonMins:  int16(value.HorizonMins),
			P50Sip:       int16((value.P50Pct / 100.0) * 30000.0),
			ForecastUuid: dbForecast.ForecastUuid,
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

	count, err := querier.CreatePredictedValues(ctx, paramsList)
	if err != nil || count < int64(len(req.PredictedGenerationValues)) {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid predicted generation values")
	}

	return &pb.CreateForecastResponse{}, tx.Commit(ctx)
}

func (s *DataPlatformServerImpl) CreateModel(ctx context.Context, req *pb.CreateModelRequest) (*pb.CreateModelResponse, error) {
	l := log.With().Str("method", "CreateModel").Logger()

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
	l.Debug().Msgf("Created model with ID %d", modelID)

	return &pb.CreateModelResponse{ModelId: modelID}, tx.Commit(ctx)
}

func (s *DataPlatformServerImpl) CreateSite(ctx context.Context, req *pb.CreateSiteRequest) (*pb.CreateSiteResponse, error) {
	l := log.With().Str("method", "CreateSite").Logger()

	// Establish a transaction with the database
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		l.Err(err).Msg("failed to begin transaction")
		return nil, status.Error(codes.Internal, "Encountered database connection error")
	}
	defer tx.Rollback(ctx)
	querier := db.New(tx)

	// Get the energy source type
	sParams := db.GetSourceTypeByNameParams{SourceTypeName: req.EnergySource.String()}
	dbSourceType, err := querier.GetSourceTypeByName(ctx, sParams)
	if err != nil {
		l.Err(err).Msgf("querier.GetSourceTypeByName(%+v)", sParams)
		return nil, status.Errorf(codes.NotFound, "Unknown source type '%s'.", req.EnergySource)
	}

	// Create a new location as a Site
	params := db.CreateLocationParams{
		LocationTypeName: "site",
		LocationName:     strings.ToUpper(req.Name),
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
	l.Debug().Msgf("Created location '%s' of type 'site' with ID %s", dbLocation.LocationName, dbLocation.LocationUuid)

	// Create a source associated with the location
	cp, ex, err := capacityToValueMultiplier(uint64(req.CapacityWatts))
	if err != nil {
		l.Err(err).Msgf("capacityKwToValueMultiplier(%d)", req.CapacityWatts)
		return nil, status.Error(codes.InvalidArgument, "Invalid capacity. Ensure capacity is non-negative.")
	}
	metadata, err := req.Metadata.MarshalJSON()
	if err != nil {
		l.Err(err).Msgf("req.Metadata.MarshalJSON()")
		return nil, status.Error(codes.InvalidArgument, "Invalid metadata. Ensure metadata is a valid JSON object.")
	}

	csParams := db.CreateSourceEntryParams{
		LocationUuid:             dbLocation.LocationUuid,
		SourceTypeID:             dbSourceType.SourceTypeID,
		Capacity:                 cp,
		CapacityUnitPrefixFactor: ex,
		CapacityLimitSip:         nil, // TODO: Put on request
		Metadata:                 metadata,
		ValidFromUtc:             pgtype.Timestamp{Time: time.Now().UTC(), Valid: true},
	}
	dbSource, err := querier.CreateSourceEntry(ctx, csParams)
	if err != nil {
		l.Err(err).Msgf("querier.CreateSource(%+v)", csParams)
		return nil, status.Error(
			codes.InvalidArgument,
			"Invalid site. Ensure metadata is NULL or a non-empty JSON object, and capacity is non-negative.",
		)
	}
	l.Debug().Msgf(
		"Created source of type '%s' for location '%s' with capacity %dx10^%d W",
		dbSourceType.SourceTypeName, dbLocation.LocationUuid, dbSource.Capacity, dbSource.CapacityUnitPrefixFactor,
	)
	err = querier.UpdateSourcesMaterializedView(ctx)
	if err != nil {
		l.Err(err).Msg("querier.UpdateSourcesMaterializedView()")
		return nil, status.Error(codes.Internal, "Failed to update sources materialized view")
	}

	return &pb.CreateSiteResponse{
		LocationUuid:  dbLocation.LocationUuid.String(),
		LocationName:  strings.ToUpper(dbLocation.LocationName),
		CapacityWatts: uint64(dbSource.Capacity) * uint64(math.Pow10(int(dbSource.CapacityUnitPrefixFactor))),
	}, tx.Commit(ctx)
}

func (s *DataPlatformServerImpl) CreateGsp(ctx context.Context, req *pb.CreateGspRequest) (*pb.CreateGspResponse, error) {
	l := log.With().Str("method", "CreateGsp").Logger()

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
		LocationName:     strings.ToUpper(req.Name),
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
	sParams := db.GetSourceTypeByNameParams{SourceTypeName: req.EnergySource.String()}
	dbSourceType, err := querier.GetSourceTypeByName(ctx, sParams)
	if err != nil {
		l.Err(err).Msgf("querier.GetSourceTypeByName(%+v)", sParams)
		return nil, status.Errorf(codes.NotFound, "Unknown source type '%s'.", req.EnergySource)
	}

	// Create a source associated with the location
	metadata, err := req.Metadata.MarshalJSON()
	if err != nil {
		l.Err(err).Msgf("req.Metadata.MarshalJSON()")
		return nil, status.Error(codes.InvalidArgument, "Invalid metadata. Ensure metadata is a valid JSON object.")
	}
	cp, ex, err := capacityToValueMultiplier(req.CapacityWatts)
	if err != nil {
		l.Err(err).Msgf("capacityMwToValueMultiplier(%d)", req.CapacityWatts)
		return nil, status.Error(codes.InvalidArgument, "Invalid capacity. Ensure capacity is non-negative.")
	}
	csParams := db.CreateSourceEntryParams{
		LocationUuid:             dbLocation.LocationUuid,
		SourceTypeID:             dbSourceType.SourceTypeID,
		Capacity:                 cp,
		CapacityUnitPrefixFactor: ex,
		Metadata:                 metadata,
		ValidFromUtc:             pgtype.Timestamp{Time: time.Now().UTC(), Valid: true},
	}
	dbSource, err := querier.CreateSourceEntry(ctx, csParams)
	if err != nil {
		l.Err(err).Msgf("querier.CreateSource(%+v)", params)
		return nil, status.Error(
			codes.InvalidArgument, "Invalid GSP. Ensure metadata is NULL or a non-empty JSON object.",
		)
	}

	l.Debug().Msgf(
		"Created source of type '%s' for location '%s' with capacity %dx10^%d W",
		req.EnergySource, dbLocation.LocationUuid, dbSource.Capacity, dbSource.CapacityUnitPrefixFactor,
	)
	err = querier.UpdateSourcesMaterializedView(ctx)
	if err != nil {
		l.Err(err).Msg("querier.UpdateSourcesMaterializedView()")
		return nil, status.Error(codes.Internal, "Failed to update sources materialized view")
	}

	return &pb.CreateGspResponse{
		LocationUuid:  dbLocation.LocationUuid.String(),
		LocationName:  strings.ToUpper(dbLocation.LocationName),
		CapacityWatts: uint64(dbSource.Capacity) * uint64(math.Pow10(int(dbSource.CapacityUnitPrefixFactor))),
	}, tx.Commit(ctx)
}

func (s *DataPlatformServerImpl) GetLocationsAsGeoJSON(ctx context.Context, req *pb.GetLocationsAsGeoJSONRequest) (*pb.GetLocationsAsGeoJSONResponse, error) {
	l := log.With().Str("method", "GetLocationsAsGeoJSON").Logger()

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
	locationUuids := make([]uuid.UUID, len(req.LocationUuids))
	for i, id := range req.LocationUuids {
		locationUuids[i], err = uuid.Parse(id)
		if err != nil {
			l.Err(err).Msgf("uuid.Parse(%s)", id)
			return nil, status.Errorf(codes.InvalidArgument, "Invalid location UUID: %v", err)
		}
	}
	params := db.GetLocationGeoJSONParams{
		SimplificationLevel: simplificationLevel,
		LocationUuids:       locationUuids,
	}
	geojson, err := querier.GetLocationGeoJSON(ctx, params)
	if err != nil {
		l.Err(err).Msgf("querier.GetLocationGeoJSONByIds(%+v)", params)
		return nil, status.Error(codes.InvalidArgument, "No locations found for input IDs")
	}

	return &pb.GetLocationsAsGeoJSONResponse{Geojson: string(geojson)}, tx.Commit(ctx)
}

// GetPredictedTimeseries implements proto.QuartzAPIServer.
func (s *DataPlatformServerImpl) GetPredictedTimeseries(ctx context.Context, req *pb.GetPredictedTimeseriesRequest) (*pb.GetPredictedTimeseriesResponse, error) {
	l := log.With().Str("method", "GetPredictedTimeseries").Logger()

	// Establish a transaction with the database
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		l.Err(err).Msg("q.pool.Begin()")
		return nil, status.Errorf(codes.Internal, "Encountered database connection error")
	}
	defer tx.Rollback(ctx)
	querier := db.New(tx)

	// Get the location and source
	locationUuid, err := uuid.Parse(req.LocationUuid)
	if err != nil {
		l.Err(err).Msgf("uuid.Parse(%s)", req.LocationUuid)
		return nil, status.Errorf(codes.InvalidArgument, "Invalid location UUID: %v", err)
	}
	gsParams := db.GetSourceAtTimestampParams{
		LocationUuid:   locationUuid,
		SourceTypeName: req.EnergySource.String(),
		AtTimestampUtc: pgtype.Timestamp{Time: req.TimeWindow.StartTimestampUnix.AsTime(), Valid: true},
	}
	dbSource, err := querier.GetSourceAtTimestamp(ctx, gsParams)
	if err != nil {
		l.Err(err).Msgf("querier.GetSourceAtTimestamp(%+v)", gsParams)
		return nil, status.Errorf(
			codes.NotFound, "No location found for name '%s' with source type '%s'.",
			req.LocationUuid, req.EnergySource,
		)
	}

	// Get the relevant predictor
	gpParams := db.GetPredictorElseLatestParams{
		PredictorName:    req.Model.ModelName,
		PredictorVersion: req.Model.ModelVersion,
	}
	dbPredictor, err := querier.GetPredictorElseLatest(ctx, gpParams)
	if err != nil {
		l.Err(err).Msgf("querier.GetPredictorElseLatest(%+v)", gpParams)
		return nil, status.Errorf(
			codes.NotFound, "No model found for name '%s' and version '%s'.",
			req.Model.ModelName, req.Model.ModelVersion,
		)
	}

	// Get the predictions for the given location source
	start, end, err := timeWindowToPgWindow(req.TimeWindow)
	lpParams := db.ListPredictionsForLocationParams{
		LocationUuid:   dbSource.LocationUuid,
		PredictorID:    dbPredictor.PredictorID,
		SourceTypeID:   dbSource.SourceTypeID,
		HorizonMins:    int32(req.HorizonMins),
		StartTimestamp: start,
		EndTimestamp:   end,
	}
	dbValues, err := querier.ListPredictionsForLocation(ctx, lpParams)
	if err != nil {
		l.Err(err).Msgf("querier.GetWindowedPredictedGenerationValuesAtHorizon(%+v)", lpParams)
		return nil, status.Errorf(
			codes.NotFound,
			"No values found for location '%s' with horizon %d minutes",
			req.LocationUuid, req.HorizonMins,
		)
	}
	l.Debug().Msgf(
		"Found %d values for location '%s' with horizon %d minutes",
		len(dbValues), req.LocationUuid, req.HorizonMins,
	)

	values := make([]*pb.GetPredictedTimeseriesResponse_Value, len(dbValues))
	for i, value := range dbValues {

		var p10 float32
		if value.P10Sip == nil {
			p10 = float32(math.NaN())
		} else {
			p10 = (float32(*value.P10Sip) / 30000.0) * 100.0
		}

		var p90 float32
		if value.P90Sip == nil {
			p90 = float32(math.NaN())
		} else {
			p90 = (float32(*value.P90Sip) / 30000.0) * 100.0
		}

		values[i] = &pb.GetPredictedTimeseriesResponse_Value{
			TimestampUnix:          timestamppb.New(value.TargetTimeUtc.Time),
			P50ValuePercent:        (float32(value.P50Sip) / 30000.0) * 100.0,
			P10ValuePercent:        p10,
			P90ValuePercent:        p90,
			EffectiveCapacityWatts: uint64(dbSource.Capacity) * uint64(math.Pow10(int(dbSource.CapacityUnitPrefixFactor))), // TODO: Capacity
		}
	}

	return &pb.GetPredictedTimeseriesResponse{
		LocationUuid: dbSource.LocationUuid.String(),
		LocationName: strings.ToUpper(dbSource.LocationName),
		Values:       values,
	}, tx.Commit(ctx)
}

// NewPostgresDataPlatformServerImpl creates a new instance of the PostgresDataPlatformServer
// connecting to - and migrating - the postgres database at the provided connection URL.
func NewPostgresDataPlatformServerImpl(connString string) *DataPlatformServerImpl {
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

	return &DataPlatformServerImpl{pool: pool}
}

var _ pb.DataPlatformServiceServer = (*DataPlatformServerImpl)(nil)
