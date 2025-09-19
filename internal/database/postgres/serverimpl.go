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
	"errors"
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
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	db "github.com/devsjc/fcfs/internal/database/postgres/gen"
	pb "github.com/devsjc/fcfs/internal/gen/ocf/dp"
)

//go:embed sql/migrations/*.sql
var embedMigrations embed.FS

// --- Reuseable Functions for Route Logic -------------------------------------------------------

// capacityToValueMultiplier return a number, plus the index to raise 10 to the power to
// to get the resultant number of Watts, to the closest power of 3.
// This is an important function which tries to preserve accuracy whilst also enabling a
// large range of values to be represented by two 16 bit integers.
func capacityToValueMultiplier(capacityWatts uint64) (int16, int16, error) {
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

		currentValue = nextValue

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
func timeWindowToPgWindow(
	window *pb.TimeWindow,
) (start pgtype.Timestamp, end pgtype.Timestamp, err error) {
	currentTime := time.Now().UTC()
	if window == nil || (window.StartTimestampUtc == nil && window.EndTimestampUtc == nil) {
		start = pgtype.Timestamp{Time: currentTime.Add(-48 * time.Hour), Valid: true}
		end = pgtype.Timestamp{Time: currentTime.Add(36 * time.Hour), Valid: true}
	} else if window.StartTimestampUtc != nil && window.EndTimestampUtc != nil {
		start = pgtype.Timestamp{Time: window.StartTimestampUtc.AsTime(), Valid: true}
		end = pgtype.Timestamp{Time: window.EndTimestampUtc.AsTime(), Valid: true}
	} else {
		err = errors.New("invalid time window: both start and end timestamps must be provided or neither")
	}

	return start, end, err
}

// --- Server Implementation ----------------------------------------------------------------------

type DataPlatformServerImpl struct {
	pool *pgxpool.Pool
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

// --- Server Method Implementations --------------------------------------------------------------
// GetLatestForecasts implements dp.DataPlatformServiceServer.
func (s *DataPlatformServerImpl) GetLatestForecasts(
	context.Context,
	*pb.GetLatestForecastsRequest,
) (*pb.GetLatestForecastsResponse, error) {
	_ = log.With().Str("method", "GetLatestForecasts").Logger()

	panic("unimplemented")
}

// CreateForecaster implements dp.DataPlatformServiceServer.
func (s *DataPlatformServerImpl) CreateForecaster(
	ctx context.Context,
	req *pb.CreateForecasterRequest,
) (*pb.CreateForecasterResponse, error) {
	l := log.With().Str("method", "CreateForecaster").Logger()

	// Establish a transaction with the database
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		l.Err(err).Msg("q.pool.Begin()")
		return nil, status.Error(codes.Internal, "Error communicating with backend")
	}

	querier := db.New(tx)
	defer tx.Rollback(ctx)

	// Check if the forecaster already exists and error out if so
	gpParams := db.GetForecasterElseLatestParams{
		ForecasterName:    req.Name,
		ForecasterVersion: req.Version,
	}

	dbForecaster, err := querier.GetForecasterElseLatest(ctx, gpParams)
	if err == nil {
		return nil, status.Errorf(
			codes.InvalidArgument,
			"Forecaster with name '%s' already exists (at version '%s'). Use the update method to add a new version, or create a non-existing forecaster.",
			dbForecaster.ForecasterName,
			dbForecaster.ForecasterVersion,
		)
	}

	// Create a new forecaster
	params := db.CreateForecasterParams{ForecasterName: req.Name, ForecasterVersion: req.Version}

	forecasterID, err := querier.CreateForecaster(ctx, params)
	if err != nil {
		l.Err(err).Msgf("querier.CreateForecaster(%+v)", params)

		return nil, status.Errorf(
			codes.InvalidArgument,
			"Invalid forecaster. Ensure name and version are not empty and are lowercase",
		)
	}

	l.Debug().Msgf("Created forecaster with ID %d", forecasterID)

	return &pb.CreateForecasterResponse{
		Forecaster: &pb.Forecaster{ForecasterName: req.Name, ForecasterVersion: req.Version},
	}, tx.Commit(ctx)
}

// UpdateForecaster implements dp.DataPlatformServiceServer.
func (s *DataPlatformServerImpl) UpdateForecaster(
	ctx context.Context,
	req *pb.UpdateForecasterRequest,
) (*pb.UpdateForecasterResponse, error) {
	l := log.With().Str("method", "UpdateForecaster").Logger()

	// Establish a transaction with the database
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		l.Err(err).Msg("q.pool.Begin()")
		return nil, status.Error(codes.Internal, "Error communicating with backend")
	}

	querier := db.New(tx)
	defer tx.Rollback(ctx)

	// Check if the forecaster already exists and error out if not
	gpParams := db.GetForecasterElseLatestParams{
		ForecasterName: req.Name,
	}

	dbForecaster, err := querier.GetForecasterElseLatest(ctx, gpParams)
	if err != nil {
		return nil, status.Errorf(
			codes.InvalidArgument,
			"No forecaster with name '%s' found. Use the create method to add it.",
			req.Name,
		)
	}

	// Update the forecaster
	params := db.CreateForecasterParams{
		ForecasterName:    dbForecaster.ForecasterName,
		ForecasterVersion: req.NewVersion,
	}

	forecasterID, err := querier.CreateForecaster(ctx, params)
	if err != nil {
		l.Err(err).Msgf("querier.CreateForecaster(%+v)", params)

		return nil, status.Errorf(
			codes.InvalidArgument,
			"Invalid forecaster. Ensure name and version are not empty and are lowercase",
		)
	}

	l.Debug().Msgf("Created forecaster with ID %d", forecasterID)

	return &pb.UpdateForecasterResponse{
		Forecaster: &pb.Forecaster{ForecasterName: req.Name, ForecasterVersion: req.NewVersion},
	}, tx.Commit(ctx)
}

// StreamForecastData implements dp.DataPlatformServiceServer.
func (s *DataPlatformServerImpl) StreamForecastData(
	req *pb.StreamForecastDataRequest,
	stream grpc.ServerStreamingServer[pb.StreamForecastDataResponse],
) error {
	l := log.With().Str("method", "StreamForecastData").Logger()

	// Establish a transaction with the database
	tx, err := s.pool.Begin(stream.Context())
	if err != nil {
		l.Err(err).Msg("q.pool.Begin()")
		return status.Error(codes.Internal, "Error communicating with backend")
	}

	querier := db.New(tx)
	defer tx.Rollback(stream.Context())

	locationUuid, err := uuid.Parse(req.LocationUuid)
	if err != nil {
		l.Err(err).Msgf("uuid.Parse(%s)", req.LocationUuid)
		return status.Errorf(codes.InvalidArgument, "Invalid location UUID: %v", err)
	}
	// Get the source as it was at the initial time of the time window
	srcParams := db.GetLocationSourceAtTimestampParams{
		LocationUuid:   locationUuid,
		SourceTypeName: req.EnergySource.String(),
		AtTimestampUtc: pgtype.Timestamp{
			Time:  req.TimeWindow.StartTimestampUtc.AsTime(),
			Valid: true,
		},
	}

	dbSource, err := querier.GetLocationSourceAtTimestamp(stream.Context(), srcParams)
	if err != nil {
		l.Err(err).Msgf("querier.GetLocationSourceAtTimestamp(%+v)", srcParams)

		return status.Errorf(
			codes.NotFound, "No location found for uuid %s with source type '%s'.",
			req.LocationUuid, req.EnergySource,
		)
	}

	forecasts := make([]db.ListForecastsRow, 0)
	for _, forecaster := range req.Forecasters {
		fcParams := db.ListForecastsParams{
			LocationUuid:      dbSource.LocationUuid,
			SourceTypeID:      dbSource.SourceTypeID,
			ForecasterName:    forecaster.ForecasterName,
			ForecasterVersion: forecaster.ForecasterVersion,
			StartTimestamp: pgtype.Timestamp{
				Time:  req.TimeWindow.StartTimestampUtc.AsTime(),
				Valid: true,
			},
			EndTimestamp: pgtype.Timestamp{
				Time:  req.TimeWindow.EndTimestampUtc.AsTime(),
				Valid: true,
			},
		}

		dbForecasts, err := querier.ListForecasts(stream.Context(), fcParams)
		if err != nil {
			l.Err(err).Msgf("querier.ListForecasts(%+v)", fcParams)

			return status.Errorf(
				codes.NotFound,
				"No forecasts found for location '%s' and forecaster %s:%s between %s and %s.",
				req.LocationUuid,
				forecaster.ForecasterName,
				forecaster.ForecasterVersion,
				req.TimeWindow.StartTimestampUtc.AsTime(),
				req.TimeWindow.EndTimestampUtc.AsTime(),
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
				codes.NotFound,
				"No predicted generation values found for forecast with init time %s",
				forecast.InitTimeUtc.Time,
			)
		}

		for i := range dbPreds {
			var p90 *float32
			if dbPreds[i].P90Sip != nil {
				p90val := (float32(*dbPreds[i].P90Sip) / 30000.0) * 100.0
				p90 = &p90val
			}

			var p10 *float32
			if dbPreds[i].P10Sip != nil {
				p10val := (float32(*dbPreds[i].P10Sip) / 30000.0) * 100.0

				p10 = &p10val
			}

			err = stream.Send(&pb.StreamForecastDataResponse{
				InitTimestamp: timestamppb.New(forecast.InitTimeUtc.Time),
				LocationUuid:  forecast.LocationUuid.String(),
				ForecasterFullname: fmt.Sprintf(
					"%s:%s",
					forecast.ForecasterName,
					forecast.ForecasterVersion,
				),
				HorizonMins: uint32(dbPreds[i].HorizonMins),
				P50Percent:  (float32(dbPreds[i].P50Sip) / 30000.0) * 100.0,
				P10Percent:  p10,
				P90Percent:  p90,
			})
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// GetLocationsWithin implements dp.DataPlatformServiceServer.
func (s *DataPlatformServerImpl) ListLocations(
	ctx context.Context,
	req *pb.ListLocationsRequest,
) (*pb.ListLocationsResponse, error) {
	l := log.With().Str("method", "ListLocations").Logger()

	// Establish a transaction with the database
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		l.Err(err).Msg("q.pool.Begin()")
		return nil, status.Error(codes.Internal, "Error communicating with backend")
	}

	querier := db.New(tx)
	defer tx.Rollback(ctx)

	var locations []*pb.ListLocationsResponse_LocationData

	if req.EnclosingLocationUuid != nil {
		// Only get the locations that are contained within a specific outer location
		locationUuid, err := uuid.Parse(*req.EnclosingLocationUuid)
		if err != nil {
			l.Err(err).Msgf("uuid.Parse(%s)", *req.EnclosingLocationUuid)
			return nil, status.Errorf(codes.InvalidArgument, "Invalid location UUID: %v", err)
		}

		lwParams := db.GetUserLocationsWithinParams{
			LocationUuid:   locationUuid,
			ServiceAccount: strings.ToUpper(req.UserRole),
		}

		dbLocations, err := querier.GetUserLocationsWithin(ctx, lwParams)
		if err != nil {
			l.Err(err).Msgf("querier.GetUserLocationsWithin(%+v)", lwParams)

			return nil, status.Errorf(
				codes.NotFound,
				"No locations found within the specified location '%s'", *req.EnclosingLocationUuid,
			)
		}

		for i := range dbLocations {
			locations = append(locations, &pb.ListLocationsResponse_LocationData{
				LocationUuid: dbLocations[i].LocationUuid.String(),
				LocationName: strings.ToUpper(dbLocations[i].LocationName),
			})
		}
	} else {
		// List all the locations
		glParams := db.GetUserLocationsParams{
			ServiceAccount: strings.ToUpper(req.UserRole),
		}

		dbLocations, err := querier.GetUserLocations(ctx, glParams)
		if err != nil {
			l.Err(err).Msgf("querier.GetUserLocations(%+v)", glParams)
			return nil, status.Errorf(codes.NotFound, "No locations found")
		}

		for i := range dbLocations {
			locations = append(locations, &pb.ListLocationsResponse_LocationData{
				LocationUuid: dbLocations[i].LocationUuid.String(),
				LocationName: strings.ToUpper(dbLocations[i].LocationName),
			})
		}
	}

	return &pb.ListLocationsResponse{
		Locations: locations,
	}, tx.Commit(ctx)
}

// GetWeekAverageDeltas implements dp.DataPlatformServiceServer.
func (s *DataPlatformServerImpl) GetWeekAverageDeltas(
	ctx context.Context,
	req *pb.GetWeekAverageDeltasRequest,
) (*pb.GetWeekAverageDeltasResponse, error) {
	l := log.With().Str("method", "GetWeekAverageDeltas").Logger()

	// Establish a transaction with the database
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		l.Err(err).Msg("q.pool.Begin()")
		return nil, status.Error(codes.Internal, "Error communicating with backend")
	}

	querier := db.New(tx)
	defer tx.Rollback(ctx)

	// Get the location and source
	locationUuid, err := uuid.Parse(req.LocationUuid)
	if err != nil {
		l.Err(err).Msgf("uuid.Parse(%s)", req.LocationUuid)
		return nil, status.Errorf(codes.InvalidArgument, "Invalid location UUID: %v", err)
	}

	gstParams := db.GetLocationSourceAtTimestampParams{
		LocationUuid:   locationUuid,
		SourceTypeName: req.EnergySource.String(),
		AtTimestampUtc: pgtype.Timestamp{Time: req.PivotTime.AsTime(), Valid: true},
	}

	dbSource, err := querier.GetLocationSourceAtTimestamp(ctx, gstParams)
	if err != nil {
		l.Err(err).Msgf("querier.GetLocationSourceAtTimestamp(%+v)", gstParams)

		return nil, status.Errorf(
			codes.NotFound, "No location source found for name '%s' with source type '%s'.",
			req.LocationUuid, req.EnergySource,
		)
	}

	// Get the relevant forecaster
	pctParams := db.GetForecasterElseLatestParams{
		ForecasterName:    req.Forecaster.ForecasterName,
		ForecasterVersion: req.Forecaster.ForecasterVersion,
	}

	dbForecaster, err := querier.GetForecasterElseLatest(ctx, pctParams)
	if err != nil {
		l.Err(err).Msgf("querier.GetForecasterElseLatest(%+v)", pctParams)

		return nil, status.Errorf(
			codes.NotFound, "No forecaster found for name '%s' and version '%s'.",
			req.Forecaster.ForecasterName, req.Forecaster.ForecasterVersion,
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
		ForecasterID:   dbForecaster.ForecasterID,
		ObserverID:     dbObserver.ObserverID,
		PivotTimestamp: pgtype.Timestamp{Time: req.PivotTime.AsTime(), Valid: true},
		LocationUuids:  []uuid.UUID{locationUuid},
	}

	dbDeltas, err := querier.GetWeekAverageDeltasForLocations(ctx, avgParams)
	if err != nil {
		l.Err(err).Msgf("querier.GetWeekAverageDeltasForLocations(%+v)", avgParams)

		return nil, status.Errorf(
			codes.NotFound,
			"No deltas found for location '%s' with source type '%s' and observer ID %d",
			req.LocationUuid,
			req.EnergySource,
			dbObserver.ObserverID,
		)
	}

	// Convert the deltas to the response format
	deltas := make([]*pb.GetWeekAverageDeltasResponse_AverageDelta, len(dbDeltas))
	for i, delta := range dbDeltas {
		deltas[i] = &pb.GetWeekAverageDeltasResponse_AverageDelta{
			DeltaPercent: (float32(delta.AvgDeltaSip) / 30000.0) * 100.0,
			HorizonMins:  uint32(delta.HorizonMins),
			EffectiveCapacityWatts: uint64(
				dbSource.Capacity,
			) * uint64(
				math.Pow10(int(dbSource.CapacityUnitPrefixFactor)),
			), // TODO: Do this over time
		}
	}

	return &pb.GetWeekAverageDeltasResponse{
		Deltas:        deltas,
		InitTimeOfDay: req.PivotTime.AsTime().Format("03:04"),
	}, nil
}

// GetObservationsAsTimeseries implements dp.DataPlatformServiceServer.
func (s *DataPlatformServerImpl) GetObservationsAsTimeseries(
	ctx context.Context,
	req *pb.GetObservationsAsTimeseriesRequest,
) (*pb.GetObservationsAsTimeseriesResponse, error) {
	l := log.With().Str("method", "GetObservationsAsTimeseries").Logger()

	// Establish a transaction with the database
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		l.Err(err).Msg("q.pool.Begin()")
		return nil, status.Error(codes.Internal, "Error communicating with backend")
	}

	querier := db.New(tx)
	defer tx.Rollback(ctx)

	// Get the location and source
	locationUuid, err := uuid.Parse(req.LocationUuid)
	if err != nil {
		l.Err(err).Msgf("uuid.Parse(%s)", req.LocationUuid)
		return nil, status.Errorf(codes.InvalidArgument, "Invalid location UUID: %v", err)
	}

	gsParams := db.GetUserLocationSourceAtTimestampParams{
		LocationUuid:   locationUuid,
		SourceTypeName: req.EnergySource.String(),
		AtTimestampUtc: pgtype.Timestamp{
			Time:  req.TimeWindow.StartTimestampUtc.AsTime(),
			Valid: true,
		},
		ServiceAccount: strings.ToUpper(req.UserRole),
	}

	dbSource, err := querier.GetUserLocationSourceAtTimestamp(ctx, gsParams)
	if err != nil {
		l.Err(err).Msgf("querier.GetUserLocationSourceAtTimestamp(%+v)", gsParams)

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
		l.Err(err).Msgf("querier.GetObservationsBetween(%+v)", goParams)

		return nil, status.Errorf(
			codes.NotFound,
			"No observations found for location '%s'",
			req.LocationUuid,
		)
	}

	values := make([]*pb.GetObservationsAsTimeseriesResponse_Value, len(dbObs))
	for i, obs := range dbObs {
		values[i] = &pb.GetObservationsAsTimeseriesResponse_Value{
			ValuePercent: (float32(obs.ValueSip) / 30000.0) * 100.0,
			TimestampUtc: timestamppb.New(obs.ObservationTimestampUtc.Time),
			EffectiveCapacityWatts: uint64(
				float64(obs.EffectiveCapacity) * math.Pow10(int(obs.CapacityUnitPrefixFactor)),
			),
		}
	}

	return &pb.GetObservationsAsTimeseriesResponse{
		LocationUuid: dbSource.LocationUuid.String(),
		LocationName: strings.ToUpper(dbSource.LocationName),
		Values:       values,
	}, nil
}

// CreateObservations implements dp.DataPlatformServiceServer.
func (s *DataPlatformServerImpl) CreateObservations(
	ctx context.Context,
	req *pb.CreateObservationsRequest,
) (*pb.CreateObservationsResponse, error) {
	l := log.With().Str("method", "CreateObservations").Logger()

	// Establish a transaction with the database
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		l.Err(err).Msg("q.pool.Begin()")
		return nil, status.Error(codes.Internal, "Error communicating with backend")
	}

	querier := db.New(tx)
	defer tx.Rollback(ctx)

	// Get the location and source
	locationUuid, err := uuid.Parse(req.LocationUuid)
	if err != nil {
		l.Err(err).Msgf("uuid.Parse(%s)", req.LocationUuid)
		return nil, status.Errorf(codes.InvalidArgument, "Invalid location UUID: %v", err)
	}

	params := db.GetUserLocationSourceAtTimestampParams{
		LocationUuid:   locationUuid,
		SourceTypeName: req.EnergySource.String(),
		AtTimestampUtc: pgtype.Timestamp{Time: req.Values[0].TimestampUtc.AsTime(), Valid: true},
		ServiceAccount: strings.ToUpper(req.UserRole),
	}

	dbSource, err := querier.GetUserLocationSourceAtTimestamp(ctx, params)
	if err != nil {
		l.Err(err).Msgf("querier.GetUserLocationSourceAtTimestamp(%+v)", params)

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
				Time:  v.TimestampUtc.AsTime(),
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

// CreateObserver implements dp.DataPlatformServiceServer.
func (s *DataPlatformServerImpl) CreateObserver(
	ctx context.Context,
	req *pb.CreateObserverRequest,
) (*pb.CreateObserverResponse, error) {
	l := log.With().Str("method", "CreateObserver").Logger()

	// Establish a transaction with the database
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		l.Err(err).Msg("q.pool.Begin()")
		return nil, status.Error(codes.Internal, "Error communicating with backend")
	}

	querier := db.New(tx)
	defer tx.Rollback(ctx)

	obParams := db.CreateObserverParams{ObserverName: req.Name}

	dbObserverId, err := querier.CreateObserver(ctx, obParams)
	if err != nil {
		l.Err(err).Msgf("querier.CreateObserver(%+v)", obParams)

		return nil, status.Error(
			codes.InvalidArgument,
			"Invalid observer name. Ensure it is not empty and is lowercase",
		)
	}

	return &pb.CreateObserverResponse{ObserverId: dbObserverId}, tx.Commit(ctx)
}

func (s *DataPlatformServerImpl) GetForecastAtTimestamp(
	ctx context.Context,
	req *pb.GetForecastAtTimestampRequest,
) (*pb.GetForecastAtTimestampResponse, error) {
	l := log.With().Str("method", "GetForecastAtTimestamp").Logger()

	// Establish a transaction with the database
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		l.Err(err).Msg("q.pool.Begin()")
		return nil, status.Error(codes.Internal, "Error communicating with backend")
	}

	querier := db.New(tx)
	defer tx.Rollback(ctx)

	// Get the relevant forecaster
	params := db.GetForecasterElseLatestParams{
		ForecasterName:    req.Forecaster.ForecasterName,
		ForecasterVersion: req.Forecaster.ForecasterVersion,
	}

	dbForecaster, err := querier.GetForecasterElseLatest(ctx, params)
	if err != nil {
		l.Err(err).Msgf("querier.GetForecasterElseLatest(%+v)", params)

		return nil, status.Errorf(
			codes.NotFound, "No forecaster found for name '%s' and version '%s'.",
			req.Forecaster.ForecasterName, req.Forecaster.ForecasterVersion,
		)
	}

	l.Debug().Msgf(
		"Using forecaster '%s:%s' with ID %d",
		dbForecaster.ForecasterName, dbForecaster.ForecasterVersion, dbForecaster.ForecasterID,
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

	lsParams := db.ListUserLocationSourcesAtTimestampParams{
		SourceTypeName: req.EnergySource.String(),
		LocationUuids:  locationUuids,
		AtTimestampUtc: pgtype.Timestamp{Time: req.TimestampUtc.AsTime(), Valid: true},
		ServiceAccount: strings.ToUpper(req.UserRole),
	}

	dbSources, err := querier.ListUserLocationSourcesAtTimestamp(ctx, lsParams)
	if err != nil || len(dbSources) == 0 {
		l.Err(err).Msgf("querier.ListUserLocationSourcesAtTimestamp(%+v)", lsParams)

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
		ForecasterID:  dbForecaster.ForecasterID,
		Time:          pgtype.Timestamp{Time: req.TimestampUtc.AsTime(), Valid: true},
		HorizonMins:   0,
	}

	dbCrossSection, err := querier.ListPredictionsAtTimeForLocations(ctx, params3)
	if err != nil {
		l.Err(err).Msgf("querier.ListPredictionsAtTimeForLocations(%+v)", params3)

		return nil, status.Errorf(
			codes.NotFound,
			"No predicted values found for the specified locations at the given time",
		)
	}

	values := []*pb.GetForecastAtTimestampResponse_Value{}
	// Only loop over the locations that have energy sources associated
	for _, value := range dbSources {
		// Find the cross section corresponding to the location with a source
		idx := slices.IndexFunc(
			dbCrossSection,
			func(row db.ListPredictionsAtTimeForLocationsRow) bool {
				return row.LocationUuid == value.LocationUuid
			},
		)
		if idx > -1 {
			values = append(values, &pb.GetForecastAtTimestampResponse_Value{
				ValuePercent: (float32(dbCrossSection[idx].P50Sip) / 30000.0) * 100.0,
				EffectiveCapacityWatts: uint64(
					value.Capacity,
				) * uint64(
					math.Pow10(int(value.CapacityUnitPrefixFactor)),
				),
				LocationUuid: value.LocationUuid.String(),
				LocationName: strings.ToUpper(value.LocationName),
				Latlng: &pb.LatLng{
					Latitude:  value.Latitude,
					Longitude: value.Longitude,
				},
			})
		}
	}

	return &pb.GetForecastAtTimestampResponse{
		TimestampUtc: req.TimestampUtc,
		Values:       values,
	}, nil
}

// GetLocation implements dp.DataPlatformServiceServer.
func (s *DataPlatformServerImpl) GetLocation(
	ctx context.Context,
	req *pb.GetLocationRequest,
) (*pb.GetLocationResponse, error) {
	l := log.With().Str("method", "GetLocation").Logger()

	// Establish a transaction with the database
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		l.Err(err).Msg("q.pool.Begin()")
		return nil, status.Error(codes.Internal, "Error communicating with backend")
	}

	querier := db.New(tx)
	defer tx.Rollback(ctx)

	// Get the location and source
	locationUuid, err := uuid.Parse(req.LocationUuid)
	if err != nil {
		l.Err(err).Msgf("uuid.Parse(%s)", req.LocationUuid)
		return nil, status.Errorf(codes.InvalidArgument, "Invalid location UUID: %v", err)
	}

	params := db.GetUserLocationSourceAtTimestampParams{
		LocationUuid:   locationUuid,
		SourceTypeName: req.EnergySource.String(),
		AtTimestampUtc: pgtype.Timestamp{Time: time.Now().UTC(), Valid: true},
		ServiceAccount: strings.ToUpper(req.UserRole),
	}

	dbSource, err := querier.GetUserLocationSourceAtTimestamp(ctx, params)
	if err != nil {
		l.Err(err).Msgf("querier.GetUserLocationSourceAtTimestamp(%+v)", params)

		return nil, status.Errorf(
			codes.NotFound,
			"No location source found for name '%s' with source type '%s'. Ensure the location has an associated source and it is not decommissioned.",
			req.LocationUuid,
			req.EnergySource,
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

		return nil, status.Errorf(
			codes.Internal,
			"Failed to convert metadata for location '%s'",
			req.LocationUuid,
		)
	}

	return &pb.GetLocationResponse{
		LocationUuid: dbSource.LocationUuid.String(),
		LocationName: strings.ToUpper(dbSource.LocationName),
		Latlng: &pb.LatLng{
			Latitude:  dbSource.Latitude,
			Longitude: dbSource.Longitude,
		},
		CapacityWatts: uint64(
			dbSource.Capacity,
		) * uint64(
			math.Pow10(int(dbSource.CapacityUnitPrefixFactor)),
		),
		Metadata: metadata,
	}, tx.Commit(ctx)
}

// CreateForecast implements dp.DataPlatformServiceServer.
func (s *DataPlatformServerImpl) CreateForecast(
	ctx context.Context,
	req *pb.CreateForecastRequest,
) (*pb.CreateForecastResponse, error) {
	l := log.With().Str("method", "CreateForecast").Logger()

	if len(req.Values) == 0 {
		return nil, status.Error(codes.InvalidArgument, "No forecast values provided")
	}

	// Establish a transaction with the database
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		l.Err(err).Msg("q.pool.Begin()")
		return nil, status.Error(codes.Internal, "Error communicating with backend")
	}

	querier := db.New(tx)
	defer tx.Rollback(ctx)

	// Get the location and source
	locationUuid, err := uuid.Parse(req.Forecast.LocationUuid)
	if err != nil {
		l.Err(err).Msgf("uuid.Parse(%s)", req.Forecast.LocationUuid)
		return nil, status.Errorf(codes.InvalidArgument, "Invalid location UUID: %v", err)
	}

	gsParams := db.GetLocationSourceAtTimestampParams{
		LocationUuid:   locationUuid,
		SourceTypeName: req.Forecast.EnergySource.String(),
		AtTimestampUtc: pgtype.Timestamp{Time: req.Forecast.InitTimeUtc.AsTime(), Valid: true},
	}

	dbSource, err := querier.GetLocationSourceAtTimestamp(ctx, gsParams)
	if err != nil {
		l.Err(err).Msgf("querier.GetLocationSourceAtTimestamp(%+v)", gsParams)

		return nil, status.Errorf(
			codes.NotFound, "No location found for id '%s' with source type '%s'.",
			req.Forecast.LocationUuid, req.Forecast.EnergySource,
		)
	}

	resolution_mins := req.Values[1].HorizonMins - req.Values[0].HorizonMins // TODO: Check they are all the same

	// Check the forecaster exists
	pctParams := db.GetForecasterElseLatestParams{
		ForecasterName:    req.Forecast.Forecaster.ForecasterName,
		ForecasterVersion: req.Forecast.Forecaster.ForecasterVersion,
	}

	_, err = querier.GetForecasterElseLatest(ctx, pctParams)
	if err != nil {
		l.Err(err).Msgf("querier.GetForecasterElseLatest(%+v)", pctParams)

		return nil, status.Errorf(
			codes.NotFound, "No forecaster found for name '%s' and version '%s'."+
				"Create the forecaster before submitting a forecast.",
			req.Forecast.Forecaster.ForecasterName, req.Forecast.Forecaster.ForecasterVersion,
		)
	}

	// Create a new forecast
	params2 := db.CreateForecastParams{
		LocationUuid:        locationUuid,
		SourceTypeID:        dbSource.SourceTypeID,
		ForecasterName:      req.Forecast.Forecaster.ForecasterName,
		ForecasterVersion:   req.Forecast.Forecaster.ForecasterVersion,
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

	l.Debug().
		Msgf("Created forecast with ID '%s' and init time %s", dbForecast.ForecastUuid, dbForecast.InitTimeUtc.Time)

	// Create the forecast data
	paramsList := make([]db.CreatePredictedValuesParams, len(req.Values))
	for i, value := range req.Values {
		p10sip := int16((value.P10Pct / 100.0) * 30000.0)
		p90sip := int16((value.P90Pct / 100.0) * 30000.0)

		metadata, err := value.Metadata.MarshalJSON()
		if err != nil {
			l.Err(err).Msgf("value.Metadata.MarshalJSON()")

			return nil, status.Errorf(
				codes.InvalidArgument,
				"Invalid metadata for predicted generation value at horizon %d mins",
				value.HorizonMins,
			)
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
	if err != nil || count < int64(len(req.Values)) {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid predicted generation values")
	}

	return &pb.CreateForecastResponse{}, tx.Commit(ctx)
}

func (s *DataPlatformServerImpl) CreateLocation(
	ctx context.Context,
	req *pb.CreateLocationRequest,
) (*pb.CreateLocationResponse, error) {
	l := log.With().Str("method", "CreateLocation").Logger()

	// Establish a transaction with the database
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		l.Err(err).Msg("q.pool.Begin()")
		return nil, status.Error(codes.Internal, "Error communicating with backend")
	}

	querier := db.New(tx)
	defer tx.Rollback(ctx)

	// Create a new location
	params := db.CreateLocationParams{
		LocationTypeName: strings.ToLower(req.LocationType.String()),
		LocationName:     strings.ToUpper(req.LocationName),
		Geom:             req.GeometryWkt,
	}

	dbLocation, err := querier.CreateLocation(ctx, params)
	if err != nil {
		l.Err(err).Msgf("querier.CreateLocation(%+v)", params)

		return nil, status.Error(
			codes.InvalidArgument,
			"Invalid location. Ensure name is not empty and uppercase, and that geometry is valid, closed,  WGS84.",
		)
	}

	// Make the user the owner of the location
	clp := db.CreateLocationPolicesParams{
		ServiceAccount: strings.ToUpper(req.UserRole),
		RoleName:       "OWNER",
		LocationUuids:  []uuid.UUID{dbLocation.LocationUuid},
	}

	err = querier.CreateLocationPolices(ctx, clp)
	if err != nil {
		l.Err(err).Msgf("querier.CreateLocationPolices(%+v)", clp)
		return nil, status.Error(codes.Internal, "Error assigning location ownership")
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

		return nil, status.Error(
			codes.InvalidArgument,
			"Invalid metadata. Ensure metadata is a valid JSON object.",
		)
	}

	cp, ex, err := capacityToValueMultiplier(req.CapacityWatts)
	if err != nil {
		l.Err(err).Msgf("capacityMwToValueMultiplier(%d)", req.CapacityWatts)

		return nil, status.Error(
			codes.InvalidArgument,
			"Invalid capacity. Ensure capacity is non-negative.",
		)
	}

	csParams := db.CreateUserLocationSourceEntryParams{
		LocationUuid:             dbLocation.LocationUuid,
		SourceTypeID:             dbSourceType.SourceTypeID,
		Capacity:                 cp,
		CapacityUnitPrefixFactor: ex,
		Metadata:                 metadata,
		ValidFromUtc:             pgtype.Timestamp{Time: time.Now().UTC(), Valid: true},
		ServiceAccount:           strings.ToUpper(req.UserRole),
	}

	dbSource, err := querier.CreateUserLocationSourceEntry(ctx, csParams)
	if err != nil {
		l.Err(err).Msgf("querier.CreateUserLocationSourceEntry(%+v)", params)

		return nil, status.Error(
			codes.InvalidArgument,
			"Invalid location. Ensure metadata is NULL or a non-empty JSON object.",
		)
	}

	err = querier.UpdateSourcesMaterializedView(ctx)
	if err != nil {
		l.Err(err).Msg("querier.UpdateSourcesMaterializedView()")
		return nil, status.Error(codes.Internal, "Failed to update sources materialised view")
	}

	return &pb.CreateLocationResponse{
		LocationUuid: dbLocation.LocationUuid.String(),
		LocationName: strings.ToUpper(dbLocation.LocationName),
		CapacityWatts: uint64(
			dbSource.Capacity,
		) * uint64(
			math.Pow10(int(dbSource.CapacityUnitPrefixFactor)),
		),
	}, tx.Commit(ctx)
}

func (s *DataPlatformServerImpl) GetLocationsAsGeoJSON(
	ctx context.Context,
	req *pb.GetLocationsAsGeoJSONRequest,
) (*pb.GetLocationsAsGeoJSONResponse, error) {
	l := log.With().Str("method", "GetLocationsAsGeoJSON").Logger()

	// Establish a transaction with the database
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		log.Err(err).Msg("q.pool.Begin()")
		return nil, status.Error(codes.Internal, "Error communicating with backend")
	}

	querier := db.New(tx)
	defer tx.Rollback(ctx)

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

// GetForecastAsTimeseries implements proto.QuartzAPIServer.
func (s *DataPlatformServerImpl) GetForecastAsTimeseries(
	ctx context.Context,
	req *pb.GetForecastAsTimeseriesRequest,
) (*pb.GetForecastAsTimeseriesResponse, error) {
	l := log.With().Str("method", "GetForecastAsTimeseries").Logger()

	// Establish a transaction with the database
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		l.Err(err).Msg("q.pool.Begin()")
		return nil, status.Error(codes.Internal, "Error communicating with backend")
	}

	querier := db.New(tx)
	defer tx.Rollback(ctx)

	// Get the location and source
	locationUuid, err := uuid.Parse(req.LocationUuid)
	if err != nil {
		l.Err(err).Msgf("uuid.Parse(%s)", req.LocationUuid)
		return nil, status.Errorf(codes.InvalidArgument, "Invalid location UUID: %v", err)
	}

	gsParams := db.GetUserLocationSourceAtTimestampParams{
		LocationUuid:   locationUuid,
		SourceTypeName: req.EnergySource.String(),
		AtTimestampUtc: pgtype.Timestamp{
			Time:  req.TimeWindow.StartTimestampUtc.AsTime(),
			Valid: true,
		},
		ServiceAccount: strings.ToUpper(req.UserRole),
	}

	dbSource, err := querier.GetUserLocationSourceAtTimestamp(ctx, gsParams)
	if err != nil {
		l.Err(err).Msgf("querier.GetUserLocationSourceAtTimestamp(%+v)", gsParams)

		return nil, status.Errorf(
			codes.NotFound, "No location found for name '%s' with source type '%s'.",
			req.LocationUuid, req.EnergySource,
		)
	}

	// Get the relevant forecaster
	gpParams := db.GetForecasterElseLatestParams{
		ForecasterName:    req.Forecaster.ForecasterName,
		ForecasterVersion: req.Forecaster.ForecasterVersion,
	}

	dbForecaster, err := querier.GetForecasterElseLatest(ctx, gpParams)
	if err != nil {
		l.Err(err).Msgf("querier.GetForecasterElseLatest(%+v)", gpParams)

		return nil, status.Errorf(
			codes.NotFound, "No forecaster found for name '%s' and version '%s'.",
			req.Forecaster.ForecasterName, req.Forecaster.ForecasterVersion,
		)
	}

	// Get the predictions for the given location source
	start, end, err := timeWindowToPgWindow(req.TimeWindow)
	if err != nil {
		l.Err(err).Msgf("timeWindowToPgWindow(%+v)", req.TimeWindow)
		return nil, status.Errorf(codes.InvalidArgument, "Invalid time window: %v", err)
	}

	lpParams := db.ListPredictionsForLocationParams{
		LocationUuid:   dbSource.LocationUuid,
		ForecasterID:   dbForecaster.ForecasterID,
		SourceTypeID:   dbSource.SourceTypeID,
		HorizonMins:    int32(req.HorizonMins),
		StartTimestamp: start,
		EndTimestamp:   end,
	}

	dbValues, err := querier.ListPredictionsForLocation(ctx, lpParams)
	if err != nil {
		l.Err(err).Msgf("querier.ListPredictionsForLocation(%+v)", lpParams)

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

	values := make([]*pb.GetForecastAsTimeseriesResponse_Value, len(dbValues))
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

		values[i] = &pb.GetForecastAsTimeseriesResponse_Value{
			TimestampUtc:    timestamppb.New(value.TargetTimeUtc.Time),
			P50ValuePercent: (float32(value.P50Sip) / 30000.0) * 100.0,
			P10ValuePercent: p10,
			P90ValuePercent: p90,
			EffectiveCapacityWatts: uint64(
				dbSource.Capacity,
			) * uint64(
				math.Pow10(int(dbSource.CapacityUnitPrefixFactor)),
			), // TODO: Capacity
		}
	}

	return &pb.GetForecastAsTimeseriesResponse{
		LocationUuid: dbSource.LocationUuid.String(),
		LocationName: strings.ToUpper(dbSource.LocationName),
		Values:       values,
	}, tx.Commit(ctx)
}

// AddLocationPolicy implements dp.DataPlatformServiceServer.
func (s *DataPlatformServerImpl) AddLocationPolicy(
	ctx context.Context,
	req *pb.AddLocationPolicyRequest,
) (*pb.AddLocationPolicyResponse, error) {
	l := log.With().Str("method", "AddLocationPolicy").Logger()

	// Establish a transaction with the database
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		l.Err(err).Msg("q.pool.Begin()")
		return nil, status.Error(codes.Internal, "Error communicating with backend")
	}

	querier := db.New(tx)
	defer tx.Rollback(ctx)

	// Parse the location UUID
	locationUuids := make([]uuid.UUID, len(req.LocationUuids))
	for i, u := range req.LocationUuids {
		locationUuid, err := uuid.Parse(u)
		if err != nil {
			l.Err(err).Msgf("uuid.Parse(%s)", u)
			return nil, status.Errorf(codes.InvalidArgument, "Invalid location UUID: %v", err)
		}

		locationUuids[i] = locationUuid
	}

	pParams := db.CreateLocationPolicesParams{
		RoleName:       req.Scope,
		ServiceAccount: strings.ToUpper(req.UserRole),
		LocationUuids:  locationUuids,
	}

	err = querier.CreateLocationPolices(ctx, pParams)
	if err != nil {
		l.Err(err).Msgf("querier.CreateLocationPolices(%+v)", pParams)
		return nil, status.Error(codes.Internal, "Failed to create location policy")
	}

	return &pb.AddLocationPolicyResponse{}, tx.Commit(ctx)
}

// Compile-time check to ensure the interface is implemented fully.
var _ pb.DataPlatformServiceServer = (*DataPlatformServerImpl)(nil)
