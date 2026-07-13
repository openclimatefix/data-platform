// Package postgres defines server implementations for the DataPlatform Services that
// are backed by a PostgreSQL database.
//
// Functions and structs for connecting to the database are generated from SQL using
// the sqlc library, whilst the Server interface that is being implemented comes from
// the top-level proto definitions.
package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/rs/zerolog"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/openclimatefix/data-platform/internal/gen/ocf/dp"
	ix "github.com/openclimatefix/data-platform/internal/interceptors"
	db "github.com/openclimatefix/data-platform/internal/server/postgres/gen"
)

// --- Reuseable Functions for Route Logic -------------------------------------------------------

// handleDBError maps database errors to appropriate gRPC status codes.
func handleDBError(err error, msg string) error {
	if errors.Is(err, pgx.ErrNoRows) {
		return status.Errorf(codes.NotFound, "%s: %v", msg, err)
	}

	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "23505": // unique_violation
			return status.Errorf(codes.AlreadyExists, "%s: %v", msg, err)
		case "23503": // foreign_key_violation
			return status.Errorf(codes.FailedPrecondition, "%s: %v", msg, err)
		case "23514", "23502", "22P02": // check_violation, not_null_violation, invalid_text_representation
			return status.Errorf(codes.InvalidArgument, "%s: %v", msg, err)
		}
	}

	return status.Errorf(codes.Internal, "%s: %v", msg, err)
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
		err = errors.New(
			"invalid time window: both start and end timestamps must be provided or neither",
		)
	}

	return start, end, err
}

// timeptrToPgTimestamp converts a protobuf Timestamp pointer to a pgtype.Timestamp.
// If the pointer is nil, it returns the current time truncated to the nearest minute.
func timeptrToPgTimestamp(t *timestamppb.Timestamp) pgtype.Timestamp {
	if t == nil {
		return pgtype.Timestamp{
			Time:  time.Now().UTC().Truncate(time.Minute),
			Valid: true,
		}
	}

	return pgtype.Timestamp{Time: t.AsTime().UTC(), Valid: true}
}

// extractSIPStatPtrFromMap gets a key's value from a map as a pointer, and converts it
// to a smallint percentage. If it doesn't exist, it returns nil.
func extractSIPStatPtrFromMap(m map[string]float32, key string) *int16 {
	val, exists := m[key]
	if !exists {
		return nil
	}

	sip_val := int16(val * 30000.0)

	return &sip_val
}

// --- Server Implementation ----------------------------------------------------------------------

func NewDataPlatformDataServiceServerImpl() *DataPlatformDataServiceServerImpl {
	return &DataPlatformDataServiceServerImpl{}
}

// DataPlatformDataServiceServerImpl implements the pb.DataPlatformDataServiceServer interface.
// It requires the database transaction for the request to be set in the context.
type DataPlatformDataServiceServerImpl struct{}

// --- Server Method Implementations --------------------------------------------------------------

// CreateForecast implements dp.DataPlatformDataServiceServer.
func (s *DataPlatformDataServiceServerImpl) CreateForecast(
	ctx context.Context,
	req *pb.CreateForecastRequest,
) (*pb.CreateForecastResponse, error) {
	l := zerolog.Ctx(ctx)

	querier := db.New(ix.GetTxFromContext(ctx))

	// Truncate the init time to the nearest minute
	req.InitTimeUtc = timestamppb.New(req.InitTimeUtc.AsTime().Truncate(time.Minute))

	gsprms := db.GetSourceAtTimestampParams{
		GeometryUuid:   uuid.MustParse(req.LocationUuid),
		SourceTypeID:   int16(req.EnergySource.Number()),
		AtTimestampUtc: timeptrToPgTimestamp(req.InitTimeUtc),
	}

	dbSource, err := querier.GetSourceAtTimestamp(ctx, gsprms)
	if err != nil {
		return nil, handleDBError(err, "No location found. Ensure the location exists, and has a registered capacity value for the given energy type valid for before the forecast init time.")
	}

	l.Debug().Str("dp.geometry.uuid", dbSource.GeometryUuid.String()).
		Int16("dp.source.type_id", dbSource.SourceTypeID).
		Msg("found source")

	// Check the forecast values have monotonically increasing horizons
	resolution_mins := req.Values[1].HorizonMins - req.Values[0].HorizonMins
	for i, value := range req.Values {
		if i > 0 {
			if resolution_mins != value.HorizonMins-req.Values[i-1].HorizonMins ||
				value.HorizonMins <= req.Values[i-1].HorizonMins {
				return nil, status.Error(
					codes.InvalidArgument,
					"Forecast horizon values must be monotonically spaced in time.",
				)
			}
		}
	}

	// Check the forecaster exists
	pctprms := db.GetForecasterElseLatestParams{
		ForecasterName:    req.Forecaster.ForecasterName,
		ForecasterVersion: req.Forecaster.ForecasterVersion,
	}

	dbForecaster, err := querier.GetForecasterElseLatest(ctx, pctprms)
	if err != nil {
		return nil, handleDBError(err, "No such forecaster. Create the forecaster before submitting a forecast.")
	}

	l.Debug().
		Int32("dp.forecaster.id", dbForecaster.ForecasterID).
		Str("dp.forecaster.name", dbForecaster.ForecasterName).
		Str("dp.forecaster.version", dbForecaster.ForecasterVersion).
		Msg("found forecaster")

	// Create a new forecast
	cfprms := db.CreateForecastParams{
		GeometryUuid:        uuid.MustParse(req.LocationUuid),
		SourceTypeID:        dbSource.SourceTypeID,
		ForecasterID:        dbForecaster.ForecasterID,
		ValueResolutionMins: int16(resolution_mins),
		InitTimeUtc:         timeptrToPgTimestamp(req.InitTimeUtc),
		FirstHorizonMins:    int32(req.Values[0].HorizonMins),
		// Okay to take the last value as we checked it was monotonically increasing above
		LastHorizonMins: int32(req.Values[len(req.Values)-1].HorizonMins),
		Metadata:        req.Metadata,
		CreatedAtUtc:    timeptrToPgTimestamp(req.CreatedTimestampUtc),
	}

	dbForecast, err := querier.CreateForecast(ctx, cfprms)
	if err != nil {
		return nil, handleDBError(err, "Invalid forecast. Ensure the forecast has a valid init_time and horizon values.")
	}

	// Create the forecast data
	paramsList := make([]db.CreatePredictedValuesParams, len(req.Values))
	for i, value := range req.Values {
		paramsList[i] = db.CreatePredictedValuesParams{
			HorizonMins:  int16(value.HorizonMins),
			P02Sip:       extractSIPStatPtrFromMap(value.OtherStatisticsFractions, "p02"),
			P10Sip:       extractSIPStatPtrFromMap(value.OtherStatisticsFractions, "p10"),
			P25Sip:       extractSIPStatPtrFromMap(value.OtherStatisticsFractions, "p25"),
			P50Sip:       int16(value.P50Fraction * 30000.0),
			P75Sip:       extractSIPStatPtrFromMap(value.OtherStatisticsFractions, "p75"),
			P90Sip:       extractSIPStatPtrFromMap(value.OtherStatisticsFractions, "p90"),
			P98Sip:       extractSIPStatPtrFromMap(value.OtherStatisticsFractions, "p98"),
			ForecastUuid: dbForecast.ForecastUuid,
		}
	}

	count, err := querier.CreatePredictedValues(ctx, paramsList)
	if err != nil || count < int64(len(req.Values)) {
		if err == nil {
			err = errors.New("inserted count less than requested")
		}
		return nil, handleDBError(err, "Invalid predicted generation values. Ensure the values are positive and correspond to less than 110% of capacity.")
	}

	l.Debug().
		Str("dp.forecast.uuid", dbForecast.ForecastUuid.String()).
		Str("dp.geometry.uuid", dbForecast.GeometryUuid.String()).
		Str("dp.forecast.init_time", dbForecast.InitTimeUtc.Time.String()).
		Str("dp.forecast.target_period", fmt.Sprintf(
			"%s - %s",
			dbForecast.TargetPeriod.Lower.Time.String(),
			dbForecast.TargetPeriod.Upper.Time.String(),
		)).Msgf("created forecast")

	return &pb.CreateForecastResponse{
		ForecastUuid: dbForecast.ForecastUuid.String(),
	}, nil
}

// DeleteForecast implements dp.DataPlatformDataServiceServer.
func (s *DataPlatformDataServiceServerImpl) DeleteForecast(
	ctx context.Context,
	req *pb.DeleteForecastRequest,
) (*pb.DeleteForecastResponse, error) {
	querier := db.New(ix.GetTxFromContext(ctx))

	// Check the forecaster exists
	pctprms := db.GetForecasterElseLatestParams{
		ForecasterName:    req.Forecaster.ForecasterName,
		ForecasterVersion: req.Forecaster.ForecasterVersion,
	}

	dbForecaster, err := querier.GetForecasterElseLatest(ctx, pctprms)
	if err != nil {
		return nil, handleDBError(err, "No such forecaster. Create the forecaster before submitting a forecast.")
	}

	// Delete the forecast
	dfcprms := db.DeleteForecastParams{
		ForecasterID:  dbForecaster.ForecasterID,
		GeometryUuid:  uuid.MustParse(req.LocationUuid),
		SourceTypeID:  int16(req.EnergySource.Number()),
		InitTimestamp: timeptrToPgTimestamp(req.InitTimeUtc),
	}

	err = querier.DeleteForecast(ctx, dfcprms)
	if err != nil {
		return nil, handleDBError(err, "Could not delete forecast. Ensure the forecast exists.")
	}

	return &pb.DeleteForecastResponse{}, nil
}

func (s *DataPlatformDataServiceServerImpl) GetLatestForecasts(
	ctx context.Context,
	req *pb.GetLatestForecastsRequest,
) (*pb.GetLatestForecastsResponse, error) {
	l := zerolog.Ctx(ctx)
	querier := db.New(ix.GetTxFromContext(ctx))

	if req.PivotTimestampUtc == nil {
		req.PivotTimestampUtc = timestamppb.New(time.Now().UTC().Truncate(time.Minute))
	}

	glfprms := db.GetLatestForecastsAtHorizonSincePivotParams{
		GeometryUuid:   uuid.MustParse(req.LocationUuid),
		SourceTypeID:   int16(req.EnergySource),
		PivotTimestamp: pgtype.Timestamp{Time: req.PivotTimestampUtc.AsTime(), Valid: true},
	}

	dbListForecasts, err := querier.GetLatestForecastsAtHorizonSincePivot(ctx, glfprms)
	if err != nil {
		return nil, handleDBError(err, "No forecasts found. Ensure location exists and forecasts have been created for it.")
	}

	l.Debug().Str("dp.geometry.uuid", req.LocationUuid).
		Int16("dp.source.type_id", glfprms.SourceTypeID).
		Int("dp.forecasts.count", len(dbListForecasts)).
		Msg("fetched latest forecasts")

	forecasts := make([]*pb.GetLatestForecastsResponse_Forecast, len(dbListForecasts))
	for i, fc := range dbListForecasts {
		forecasts[i] = &pb.GetLatestForecastsResponse_Forecast{
			InitializationTimestampUtc: timestamppb.New(fc.InitTimeUtc.Time),
			Forecaster: &pb.Forecaster{
				ForecasterName:    fc.ForecasterName,
				ForecasterVersion: fc.ForecasterVersion,
			},
			LocationUuid:        fc.GeometryUuid.String(),
			Metadata:            fc.Metadata,
			CreatedTimestampUtc: timestamppb.New(fc.CreatedAtUtc.Time),
		}
	}

	return &pb.GetLatestForecastsResponse{
		Forecasts: forecasts,
	}, nil
}

func (s *DataPlatformDataServiceServerImpl) CreateForecaster(
	ctx context.Context,
	req *pb.CreateForecasterRequest,
) (*pb.CreateForecasterResponse, error) {
	l := zerolog.Ctx(ctx)

	querier := db.New(ix.GetTxFromContext(ctx))

	// Check if the forecaster already exists and error out if so
	gpprms := db.GetForecasterElseLatestParams{
		ForecasterName:    req.Name,
		ForecasterVersion: req.Version,
	}

	dbExistingForecaster, err := querier.GetForecasterElseLatest(ctx, gpprms)
	if err == nil {
		return nil, status.Errorf(
			codes.AlreadyExists,
			"Forecaster with already exists (at version '%s'). "+
				"Use the update method to add a new version, or create a new forecaster.",
			dbExistingForecaster.ForecasterVersion,
		)
	}

	// Create a new forecaster
	cfprms := db.CreateForecasterParams{ForecasterName: req.Name, ForecasterVersion: req.Version}

	dbForecaster, err := querier.CreateForecaster(ctx, cfprms)
	if err != nil {
		return nil, handleDBError(err, "Invalid forecaster. Ensure name and version are not empty and are lowercase")
	}

	l.Debug().Int32("dp.forecaster.id", dbForecaster.ForecasterID).
		Str("dp.forecaster.name", dbForecaster.ForecasterName).
		Str("dp.forecaster.version", dbForecaster.ForecasterVersion).
		Msg("created forecaster")

	return &pb.CreateForecasterResponse{
		Forecaster: &pb.Forecaster{ForecasterName: req.Name, ForecasterVersion: req.Version},
	}, nil
}

func (s *DataPlatformDataServiceServerImpl) UpdateForecaster(
	ctx context.Context,
	req *pb.UpdateForecasterRequest,
) (*pb.UpdateForecasterResponse, error) {
	l := zerolog.Ctx(ctx)

	querier := db.New(ix.GetTxFromContext(ctx))

	// Check if the forecaster already exists and error out if not
	gpprms := db.GetForecasterElseLatestParams{
		ForecasterName: req.Name,
	}

	dbExistingForecaster, err := querier.GetForecasterElseLatest(ctx, gpprms)
	if err != nil {
		return nil, handleDBError(err, "No such forecaster. Use the create method to add it.")
	}

	// Update the forecaster
	cfprms := db.CreateForecasterParams{
		ForecasterName:    dbExistingForecaster.ForecasterName,
		ForecasterVersion: req.NewVersion,
	}

	dbForecaster, err := querier.CreateForecaster(ctx, cfprms)
	if err != nil {
		return nil, handleDBError(err, "Invalid forecaster. Ensure name and version are not empty and are lowercase, and name is unique.")
	}

	l.Debug().Int32("dp.forecaster.id", dbForecaster.ForecasterID).
		Str("dp.forecaster.name", dbForecaster.ForecasterName).
		Str("dp.forecaster.version_new", dbForecaster.ForecasterVersion).
		Str("dp.forecaster.version_old", dbExistingForecaster.ForecasterVersion).
		Msg("updated forecaster")

	return &pb.UpdateForecasterResponse{
		Forecaster: &pb.Forecaster{ForecasterName: req.Name, ForecasterVersion: req.NewVersion},
	}, nil
}

func (s *DataPlatformDataServiceServerImpl) ListForecasters(
	ctx context.Context,
	req *pb.ListForecastersRequest,
) (*pb.ListForecastersResponse, error) {
	querier := db.New(ix.GetTxFromContext(ctx))

	lfprms := db.GetForecastersByFiltersParams{
		ForecasterNames:   req.ForecasterNamesFilter,
		LatestVersionOnly: req.LatestVersionsOnly,
	}

	dbListForecasters, err := querier.GetForecastersByFilters(ctx, lfprms)
	if err != nil {
		return nil, handleDBError(err, "No forecasters found with the specified filters")
	}

	forecasters := make([]*pb.Forecaster, len(dbListForecasters))
	for i, fc := range dbListForecasters {
		forecasters[i] = &pb.Forecaster{
			ForecasterName:    fc.ForecasterName,
			ForecasterVersion: fc.ForecasterVersion,
		}
	}

	return &pb.ListForecastersResponse{
		Forecasters: forecasters,
	}, nil
}

func (s *DataPlatformDataServiceServerImpl) StreamForecastData(
	req *pb.StreamForecastDataRequest,
	stream grpc.ServerStreamingServer[pb.StreamForecastDataResponse],
) error {
	l := zerolog.Ctx(stream.Context())
	pool := ix.GetPoolFromContext(stream.Context())

	var (
		fNames    []string
		fVersions []string
	)

	const batchSize = 200

	for _, fc := range req.Forecasters {
		fNames = append(fNames, fc.ForecasterName)
		fVersions = append(fVersions, fc.ForecasterVersion)
	}

	// Instantiate a worker pool of a limited size to handle the query.
	// Handy blog on errgroups: https://oneuptime.com/blog/post/2026-01-07-go-errgroup
	eg, ctx := errgroup.WithContext(stream.Context())
	eg.SetLimit(2)
	resChan := make(chan *pb.StreamForecastDataResponse, 100)

	// Fan out queries for each location to the database.
	go func() {
		for _, locStr := range req.LocationUuids {
			eg.Go(func() error {
				// Check for errors prior to running query.
				if ctx.Err() != nil {
					return ctx.Err()
				}

				l.Debug().Str("loc", locStr).Msg("STARTING database query")

				locationUuid, err := uuid.Parse(locStr)
				if err != nil {
					return status.Errorf(codes.InvalidArgument, "Invalid location UUID: %v", err)
				}

				// Query with the pool directly so each concurrent request gets a fresh connection.
				// This is to avoid very large data requests choking the memory of the API.
				// Normally I woudn't want to bypass SQLC's type safety - but this RPC is specifically for
				// ML debugging and isn't on any hot path, so I don't mind here.
				rows, err := pool.Query(
					stream.Context(),
					db.ListPredictionsForForecasts,
					pgtype.Timestamp{
						Time:  req.TimeWindow.StartTimestampUtc.AsTime(),
						Valid: true,
					},
					pgtype.Timestamp{
						Time:  req.TimeWindow.EndTimestampUtc.AsTime(),
						Valid: true,
					},
					locationUuid,
					int16(req.EnergySource.Number()),
					fNames,
					fVersions,
				)
				if err != nil {
					return handleDBError(err, "Failed to stream predictions")
				}
				defer rows.Close()

				batch := make([]*pb.ForecastDatum, 0, batchSize)

				for rows.Next() {
					var row db.ListPredictionsForForecastsRow

					err := rows.Scan(
						&row.ForecasterName,
						&row.ForecasterVersion,
						&row.CreatedAtUtc,
						&row.HorizonMins,
						&row.P02Sip,
						&row.P10Sip,
						&row.P25Sip,
						&row.P50Sip,
						&row.P75Sip,
						&row.P90Sip,
						&row.P98Sip,
						&row.CapacityWatts,
						&row.Metadata,
						&row.InitTimeUtc,
						&row.TargetTimeUtc,
					)
					if err != nil {
						return status.Errorf(codes.Internal, "Error reading prediction stream: %v", err)
					}

					otherStatistics := make(map[string]float32)
					if row.P02Sip != nil {
						otherStatistics["p02"] = float32(*row.P02Sip) / 30000.0
					}

					if row.P10Sip != nil {
						otherStatistics["p10"] = float32(*row.P10Sip) / 30000.0
					}

					if row.P25Sip != nil {
						otherStatistics["p25"] = float32(*row.P25Sip) / 30000.0
					}

					if row.P75Sip != nil {
						otherStatistics["p75"] = float32(*row.P75Sip) / 30000.0
					}

					if row.P90Sip != nil {
						otherStatistics["p90"] = float32(*row.P90Sip) / 30000.0
					}

					if row.P98Sip != nil {
						otherStatistics["p98"] = float32(*row.P98Sip) / 30000.0
					}

					metadata := make(map[string]string)
					if req.IncludeMetadata && row.Metadata != nil {
						for k, v := range row.Metadata.AsMap() {
							metadata[k] = v.(string)
						}
					}

					batch = append(batch, &pb.ForecastDatum{
						InitTimestamp: timestamppb.New(row.InitTimeUtc.Time),
						LocationUuid:  locationUuid.String(),
						ForecasterFullname: fmt.Sprintf(
							"%s:%s",
							row.ForecasterName,
							row.ForecasterVersion,
						),
						HorizonMins:              uint32(row.HorizonMins),
						P50Fraction:              float32(row.P50Sip) / 30000.0,
						OtherStatisticsFractions: otherStatistics,
						CreatedTimestampUtc:      timestamppb.New(row.CreatedAtUtc.Time),
						EffectiveCapacityWatts:   uint64(row.CapacityWatts),
						Metadata:                 metadata,
					})
					if len(batch) == batchSize {
						select {
						case resChan <- &pb.StreamForecastDataResponse{Values: batch}:
						case <-ctx.Done():
							return ctx.Err()
						}

						batch = make([]*pb.ForecastDatum, 0, batchSize)
					}
				}

				l.Debug().
					Str("dp.geometry.uuid", locationUuid.String()).
					Msg("streamed forecasts for location")

				err = rows.Err()
				if err != nil {
					return err
				}

				if len(batch) > 0 {
					select {
					case resChan <- &pb.StreamForecastDataResponse{Values: batch}:
					case <-ctx.Done():
						return ctx.Err()
					}
				}

				return nil
			})
		}

		_ = eg.Wait()

		close(resChan)
	}()

	// Consume the channel and stream to the client as results come in.
	// If the client disconnects, the context is cancelled, which terminates the workers.
	for res := range resChan {
		err := stream.Send(res)
		if err != nil {
			return err
		}
	}

	// Finally, retrieve the actual error (if any) from the errgroup.
	err := eg.Wait()
	if err != nil {
		if errors.Is(err, context.Canceled) || status.Code(err) == codes.Canceled {
			l.Info().Msg("Stream gracefully cancelled by client")
			return err
		}

		return err
	}

	return nil
}

func (s *DataPlatformDataServiceServerImpl) GetWeekAverageDeltas(
	ctx context.Context,
	req *pb.GetWeekAverageDeltasRequest,
) (*pb.GetWeekAverageDeltasResponse, error) {
	querier := db.New(ix.GetTxFromContext(ctx))

	// Get the location and source
	locationUuid, err := uuid.Parse(req.LocationUuid)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid location UUID: %v", err)
	}

	gstprms := db.GetSourceAtTimestampParams{
		GeometryUuid:   locationUuid,
		SourceTypeID:   int16(req.EnergySource.Number()),
		AtTimestampUtc: pgtype.Timestamp{Time: req.PivotTimestampUtc.AsTime(), Valid: true},
	}

	dbSource, err := querier.GetSourceAtTimestamp(ctx, gstprms)
	if err != nil {
		return nil, handleDBError(err, fmt.Sprintf("No location source found for name '%s' with source type '%s'", req.LocationUuid, req.EnergySource))
	}

	// Get the relevant forecaster
	pctprms := db.GetForecasterElseLatestParams{
		ForecasterName:    req.Forecaster.ForecasterName,
		ForecasterVersion: req.Forecaster.ForecasterVersion,
	}

	dbExistingForecaster, err := querier.GetForecasterElseLatest(ctx, pctprms)
	if err != nil {
		return nil, handleDBError(err, fmt.Sprintf("No forecaster found for name '%s' and version '%s'", req.Forecaster.ForecasterName, req.Forecaster.ForecasterVersion))
	}

	// Get the observer
	obprms := db.GetObserverByNameParams{ObserverName: req.ObserverName}

	dbObserver, err := querier.GetObserverByName(ctx, obprms)
	if err != nil {
		return nil, handleDBError(err, fmt.Sprintf("No observer of name '%s' found. Choose an existing observer or create a new one.", req.ObserverName))
	}

	// Get the deltas
	avgprms := db.GetWeekAverageDeltasForLocationsParams{
		SourceTypeID:   dbSource.SourceTypeID,
		ForecasterID:   dbExistingForecaster.ForecasterID,
		ObserverUuid:   dbObserver.ObserverUuid,
		PivotTimestamp: pgtype.Timestamp{Time: req.PivotTimestampUtc.AsTime(), Valid: true},
		GeometryUuid:   locationUuid,
	}

	dbDeltas, err := querier.GetWeekAverageDeltasForLocations(ctx, avgprms)
	if err != nil {
		return nil, handleDBError(err, fmt.Sprintf("No deltas found for location '%s' with source type '%s' and observer ID '%s'", req.LocationUuid, req.EnergySource, dbObserver.ObserverUuid.String()))
	}

	// Convert the deltas to the response format
	deltas := make([]*pb.GetWeekAverageDeltasResponse_AverageDelta, len(dbDeltas))
	for i, delta := range dbDeltas {
		deltas[i] = &pb.GetWeekAverageDeltasResponse_AverageDelta{
			DeltaFraction: float32(delta.AvgDeltaSip) / 30000.0,
			HorizonMins:   uint32(delta.HorizonMins),
			EffectiveCapacityWatts: uint64(
				dbSource.CapacityWatts,
			), // Should this be done over time?
		}
	}

	return &pb.GetWeekAverageDeltasResponse{
		Deltas:        deltas,
		InitTimeOfDay: req.PivotTimestampUtc.AsTime().Format("03:04"),
	}, nil
}

func (s *DataPlatformDataServiceServerImpl) GetObservationsAsTimeseries(
	ctx context.Context,
	req *pb.GetObservationsAsTimeseriesRequest,
) (*pb.GetObservationsAsTimeseriesResponse, error) {
	querier := db.New(ix.GetTxFromContext(ctx))
	locationUuid := uuid.MustParse(req.LocationUuid)

	// Get the observer
	obprms := db.GetObserverByNameParams{ObserverName: req.ObserverName}

	observerResp, err := querier.GetObserverByName(ctx, obprms)
	if err != nil {
		return nil, handleDBError(err, fmt.Sprintf("No observer of name '%s' found. Choose an existing observer or create a new one.", req.ObserverName))
	}

	start, end, err := timeWindowToPgWindow(req.TimeWindow)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid time window: %v", err)
	}

	goprms := db.GetObservationsBetweenParams{
		GeometryUuid: locationUuid,
		SourceTypeID: int16(req.EnergySource),
		ObserverUuid: observerResp.ObserverUuid,
		StartTimeUtc: start,
		EndTimeUtc:   end,
	}

	dbObs, err := querier.GetObservationsBetween(ctx, goprms)
	if err != nil {
		return nil, handleDBError(err, fmt.Sprintf("No observations found for location '%s'", req.LocationUuid))
	}

	values := make([]*pb.GetObservationsAsTimeseriesResponse_Value, len(dbObs))
	for i, obs := range dbObs {
		values[i] = &pb.GetObservationsAsTimeseriesResponse_Value{
			ValueFraction:          float32(obs.ValueSip) / 30000.0,
			TimestampUtc:           timestamppb.New(obs.ObservationTimestampUtc.Time),
			EffectiveCapacityWatts: uint64(obs.CapacityWatts),
		}
	}

	return &pb.GetObservationsAsTimeseriesResponse{
		LocationUuid: locationUuid.String(),
		Values:       values,
	}, nil
}

func (s *DataPlatformDataServiceServerImpl) CreateObservations(
	ctx context.Context,
	req *pb.CreateObservationsRequest,
) (*pb.CreateObservationsResponse, error) {
	l := zerolog.Ctx(ctx)

	querier := db.New(ix.GetTxFromContext(ctx))

	// Get the location and source
	locationUuid, err := uuid.Parse(req.LocationUuid)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid location UUID: %v", err)
	}

	cfprms := db.GetSourceAtTimestampParams{
		GeometryUuid:   locationUuid,
		SourceTypeID:   int16(req.EnergySource.Number()),
		AtTimestampUtc: pgtype.Timestamp{Time: req.Values[0].TimestampUtc.AsTime(), Valid: true},
	}

	dbSource, err := querier.GetSourceAtTimestamp(ctx, cfprms)
	if err != nil {
		return nil, handleDBError(err, fmt.Sprintf("No location found for name '%s' with source type '%s'", req.LocationUuid, req.EnergySource))
	}

	// Get the observer ID
	obprms := db.GetObserverByNameParams{ObserverName: req.ObserverName}

	dbObserver, err := querier.GetObserverByName(ctx, obprms)
	if err != nil {
		return nil, handleDBError(err, fmt.Sprintf("No observer of name '%s', found. Choose an existing observer or create a new one.", req.ObserverName))
	}

	// Insert the observations
	coprms := make([]db.CreateObservationsBatchParams, len(req.Values))
	for i, v := range req.Values {
		coprms[i] = db.CreateObservationsBatchParams{
			GeometryUuid: locationUuid,
			ObserverUuid: dbObserver.ObserverUuid,
			ObservationTimestampUtc: pgtype.Timestamp{
				Time:  v.TimestampUtc.AsTime(),
				Valid: true,
			},
			SourceTypeID: dbSource.SourceTypeID,
			ValueWatts:   int64(v.ValueWatts),
		}
	}

	batch := querier.CreateObservationsBatch(ctx, coprms)

	err = batch.Close()
	if err != nil {
		return nil, handleDBError(err, "Invalid observation values. Ensure the values are positive, and less than 110% of capacity.")
	}

	l.Debug().Int16("dp.source.type_id", dbSource.SourceTypeID).
		Str("dp.geometry.uuid", dbSource.GeometryUuid.String()).
		Str("dp.observer.uuid", dbObserver.ObserverUuid.String()).
		Str("dp.observer.name", dbObserver.ObserverName).
		Str("dp.observations.target_period", fmt.Sprintf(
			"%s - %s",
			coprms[0].ObservationTimestampUtc.Time.String(),
			coprms[len(coprms)-1].ObservationTimestampUtc.Time.String(),
		)).
		Int("dp.observations.count", len(coprms)).
		Msg("created observations")

	return &pb.CreateObservationsResponse{}, nil
}

func (s *DataPlatformDataServiceServerImpl) GetLatestObservations(
	ctx context.Context,
	req *pb.GetLatestObservationsRequest,
) (*pb.GetLatestObservationsResponse, error) {
	l := zerolog.Ctx(ctx)
	querier := db.New(ix.GetTxFromContext(ctx))

	// Set the pivot time to now if not provided
	if req.PivotTimestampUtc == nil {
		req.PivotTimestampUtc = timestamppb.New(time.Now().UTC().Truncate(time.Minute))
	}

	locUuids := make([]uuid.UUID, len(req.LocationUuids))
	for i, locStr := range req.LocationUuids {
		locUuids[i] = uuid.MustParse(locStr)
	}

	goprms := db.GetLatestObservationsParams{
		GeometryUuids: locUuids,
		SourceTypeID:  int16(req.EnergySource),
		ObserverName:  req.ObserverName,
		PivotTimeUtc:  pgtype.Timestamp{Time: req.PivotTimestampUtc.AsTime(), Valid: true},
	}

	dbObs, err := querier.GetLatestObservations(ctx, goprms)
	if err != nil {
		return nil, handleDBError(err, "Backend communication error")
	}

	observations := make([]*pb.GetLatestObservationsResponse_Observation, len(dbObs))
	for i, obs := range dbObs {
		observations[i] = &pb.GetLatestObservationsResponse_Observation{
			LocationUuid:           obs.GeometryUuid.String(),
			TimestampUtc:           timestamppb.New(obs.ObservationTimestampUtc.Time),
			ValueFraction:          float32(obs.ValueSip) / 30000.0,
			EffectiveCapacityWatts: uint64(obs.CapacityWatts),
		}
	}

	l.Debug().
		Int16("dp.source.type_id", goprms.SourceTypeID).
		Int("dp.geometry.count", len(req.LocationUuids)).
		Int("dp.observations.count", len(observations)).
		Msg("found observations")

	return &pb.GetLatestObservationsResponse{
		Observations: observations,
	}, nil
}

func (s *DataPlatformDataServiceServerImpl) CreateObserver(
	ctx context.Context,
	req *pb.CreateObserverRequest,
) (*pb.CreateObserverResponse, error) {
	querier := db.New(ix.GetTxFromContext(ctx))

	obprms := db.CreateObserverParams{ObserverName: req.Name}

	dbObserver, err := querier.CreateObserver(ctx, obprms)
	if err != nil {
		return nil, handleDBError(err, "Invalid observer name. Ensure it is not empty, unique, and lowercase")
	}

	return &pb.CreateObserverResponse{
		ObserverUuid: dbObserver.ObserverUuid.String(),
		ObserverName: dbObserver.ObserverName,
	}, nil
}

func (s *DataPlatformDataServiceServerImpl) ListObservers(
	ctx context.Context,
	req *pb.ListObserversRequest,
) (*pb.ListObserversResponse, error) {
	l := zerolog.Ctx(ctx)
	querier := db.New(ix.GetTxFromContext(ctx))

	loPrms := db.GetObserversByFiltersParams{
		ObserverNames: req.ObserverNamesFilter,
	}

	dbListObservers, err := querier.GetObserversByFilters(ctx, loPrms)
	if err != nil {
		return nil, handleDBError(err, "Backend communication error")
	}

	l.Debug().
		Int("dp.observers.count", len(dbListObservers)).
		Msg("found observers")

	observers := make([]*pb.ListObserversResponse_ObserverSummary, len(dbListObservers))
	for i, ob := range dbListObservers {
		observers[i] = &pb.ListObserversResponse_ObserverSummary{
			ObserverUuid: ob.ObserverUuid.String(),
			ObserverName: ob.ObserverName,
		}
	}

	return &pb.ListObserversResponse{
		Observers: observers,
	}, nil
}

func (s *DataPlatformDataServiceServerImpl) GetForecastAtTimestamp(
	ctx context.Context,
	req *pb.GetForecastAtTimestampRequest,
) (*pb.GetForecastAtTimestampResponse, error) {
	l := zerolog.Ctx(ctx)

	querier := db.New(ix.GetTxFromContext(ctx))

	// Set default timestamp to now if not provided
	if req.TimestampUtc == nil {
		req.TimestampUtc = timestamppb.New(time.Now().UTC().Truncate(time.Minute))
	}

	// Get the relevant forecaster
	cfprms := db.GetForecasterElseLatestParams{
		ForecasterName:    req.Forecaster.ForecasterName,
		ForecasterVersion: req.Forecaster.ForecasterVersion,
	}

	dbForecaster, err := querier.GetForecasterElseLatest(ctx, cfprms)
	if err != nil {
		return nil, handleDBError(err, fmt.Sprintf("No forecaster found for name '%s' and version '%s'", req.Forecaster.ForecasterName, req.Forecaster.ForecasterVersion))
	}

	l.Debug().
		Int32("dp.forecaster.id", dbForecaster.ForecasterID).
		Str("dp.forecaster.name", dbForecaster.ForecasterName).
		Str("dp.forecaster.version", dbForecaster.ForecasterVersion).
		Msg("found forecaster")

	locUuids := make([]uuid.UUID, len(req.LocationUuids))
	for i, locStr := range req.LocationUuids {
		locUuids[i] = uuid.MustParse(locStr)
	}

	lpprms := db.ListPredictionsAtTimeForLocationsParams{
		GeometryUuids:      locUuids,
		SourceTypeID:       int16(req.EnergySource),
		ForecasterID:       dbForecaster.ForecasterID,
		TargetTimestampUtc: pgtype.Timestamp{Time: req.TimestampUtc.AsTime(), Valid: true},
		HorizonMins:        0, // NOTE: May want to make this available to the RPC message
	}

	dbPredictions, err := querier.ListPredictionsAtTimeForLocations(ctx, lpprms)
	if err != nil {
		return nil, handleDBError(err, "No predicted values found for the specified locations at the given time.")
	}

	values := make([]*pb.GetForecastAtTimestampResponse_Value, len(dbPredictions))
	for i, value := range dbPredictions {
		otherStats := make(map[string]float32)
		if value.P02Sip != nil {
			otherStats["p02"] = float32(*value.P02Sip) / 30000.0
		}

		if value.P10Sip != nil {
			otherStats["p10"] = float32(*value.P10Sip) / 30000.0
		}

		if value.P25Sip != nil {
			otherStats["p25"] = float32(*value.P25Sip) / 30000.0
		}

		if value.P75Sip != nil {
			otherStats["p75"] = float32(*value.P75Sip) / 30000.0
		}

		if value.P90Sip != nil {
			otherStats["p90"] = float32(*value.P90Sip) / 30000.0
		}

		if value.P98Sip != nil {
			otherStats["p98"] = float32(*value.P98Sip) / 30000.0
		}

		values[i] = &pb.GetForecastAtTimestampResponse_Value{
			ValueFraction:          float32(value.P50Sip) / 30000.0,
			EffectiveCapacityWatts: uint64(value.CapacityWatts),
			LocationUuid:           value.GeometryUuid.String(),
			LocationName:           value.GeometryName,
			Latlng: &pb.LatLng{
				Latitude:  value.Latitude,
				Longitude: value.Longitude,
			},
			Metadata:                   value.Metadata,
			InitializationTimestampUtc: timestamppb.New(value.InitTimeUtc.Time),
			CreatedTimestampUtc:        timestamppb.New(value.CreatedAtUtc.Time),
			OtherStatisticsFractions:   otherStats,
		}
	}

	return &pb.GetForecastAtTimestampResponse{
		TimestampUtc: req.TimestampUtc,
		Values:       values,
	}, nil
}

func (s *DataPlatformDataServiceServerImpl) GetObservationsAtTimestamp(
	ctx context.Context,
	req *pb.GetObservationsAtTimestampRequest,
) (*pb.GetObservationsAtTimestampResponse, error) {
	querier := db.New(ix.GetTxFromContext(ctx))

	// Set default timestamp to now if not provided
	if req.TimestampUtc == nil {
		req.TimestampUtc = timestamppb.New(time.Now().UTC().Truncate(time.Minute))
	}

	locUuids := make([]uuid.UUID, len(req.LocationUuids))
	for i, locStr := range req.LocationUuids {
		locUuids[i] = uuid.MustParse(locStr)
	}

	// Check that the observer exists
	obprms := db.GetObserverByNameParams{ObserverName: req.ObserverName}

	dbObserver, err := querier.GetObserverByName(ctx, obprms)
	if err != nil {
		return nil, handleDBError(err, fmt.Sprintf("No observer of name '%s' found. Choose an existing observer or create a new one.", req.ObserverName))
	}

	loprms := db.ListObservationsAtTimeForLocationsParams{
		GeometryUuids:      locUuids,
		SourceTypeID:       int16(req.EnergySource),
		ObserverUuid:       dbObserver.ObserverUuid,
		TargetTimestampUtc: pgtype.Timestamp{Time: req.TimestampUtc.AsTime(), Valid: true},
	}

	dbObs, err := querier.ListObservationsAtTimeForLocations(ctx, loprms)
	if err != nil {
		return nil, handleDBError(err, "No observations found for the specified locations at the given time. Ensure the observer exists, and that the location is operational.")
	}

	observations := make([]*pb.GetObservationsAtTimestampResponse_Value, len(dbObs))
	for i, obs := range dbObs {
		observations[i] = &pb.GetObservationsAtTimestampResponse_Value{
			ValueFraction:          float32(obs.ValueSip) / 30000.0,
			EffectiveCapacityWatts: uint64(obs.CapacityWatts),
			LocationUuid:           obs.GeometryUuid.String(),
			Latlng: &pb.LatLng{
				Latitude:  obs.Latitude,
				Longitude: obs.Longitude,
			},
		}
	}

	return &pb.GetObservationsAtTimestampResponse{
		TimestampUtc: req.TimestampUtc,
		Values:       observations,
	}, nil
}

func (s *DataPlatformDataServiceServerImpl) GetLocation(
	ctx context.Context,
	req *pb.GetLocationRequest,
) (*pb.GetLocationResponse, error) {
	l := zerolog.Ctx(ctx)

	validTime := time.Now().UTC().Truncate(time.Minute)
	if req.PivotTimestampUtc != nil {
		validTime = req.PivotTimestampUtc.AsTime()
	}

	querier := db.New(ix.GetTxFromContext(ctx))

	cfprms := db.GetSourceAtTimestampParams{
		GeometryUuid:   uuid.MustParse(req.LocationUuid),
		SourceTypeID:   int16(req.EnergySource.Number()),
		AtTimestampUtc: pgtype.Timestamp{Time: validTime, Valid: true},
	}

	dbSource, err := querier.GetSourceAtTimestamp(ctx, cfprms)
	if err != nil {
		return nil, handleDBError(err, "No such location. Ensure the location exists and is currently operational.")
	}

	l.Debug().
		Str("dp.source.geometry_uuid", dbSource.GeometryUuid.String()).
		Int16("dp.source.type_id", dbSource.SourceTypeID).
		Int64("dp.source.capacity", int64(dbSource.CapacityWatts)).
		Str("dp.source.valid_from_utc", dbSource.SysPeriod.Lower.Time.String()).
		Msg("found source")

	var geometry []byte
	if req.IncludeGeometry {
		gwkbprms := db.GetGeometryWKBParams{
			GeometryUuids: []uuid.UUID{dbSource.GeometryUuid},
		}

		dbGeometry, err := querier.GetGeometryWKB(ctx, gwkbprms)
		if err != nil {
			return nil, handleDBError(err, "Failed to retrieve geometry for location")
		}

		geometry = dbGeometry.GeomWkb
	}

	return &pb.GetLocationResponse{
		LocationUuid: dbSource.GeometryUuid.String(),
		LocationName: dbSource.GeometryName,
		Latlng: &pb.LatLng{
			Latitude:  dbSource.Latitude,
			Longitude: dbSource.Longitude,
		},
		EffectiveCapacityWatts: uint64(dbSource.CapacityWatts),
		Metadata:               dbSource.MetadataJsonb,
		GeometryWkb:            geometry,
	}, nil
}

func (s *DataPlatformDataServiceServerImpl) GetLocationAsTimeseries(
	ctx context.Context,
	req *pb.GetLocationAsTimeseriesRequest,
) (*pb.GetLocationAsTimeseriesResponse, error) {
	querier := db.New(ix.GetTxFromContext(ctx))

	locationUuid := uuid.MustParse(req.LocationUuid)

	gprms := db.GetSourceHistoryParams{
		GeometryUuid: locationUuid,
		SourceTypeID: int16(req.EnergySource.Number()),
		StartTimestampUtc: pgtype.Timestamp{
			Time:  req.TimeWindow.StartTimestampUtc.AsTime(),
			Valid: true,
		},
		EndTimestampUtc: pgtype.Timestamp{
			Time:  req.TimeWindow.EndTimestampUtc.AsTime(),
			Valid: true,
		},
	}

	dbValues, err := querier.GetSourceHistory(ctx, gprms)
	if err != nil {
		return nil, handleDBError(err, "No such location or no history for location in the specified time window. Ensure the location exists and has source entries in the given time window.")
	}

	values := make([]*pb.GetLocationAsTimeseriesResponse_LocationSnapshot, len(dbValues))
	for i, v := range dbValues {
		values[i] = &pb.GetLocationAsTimeseriesResponse_LocationSnapshot{
			EffectiveCapacityWatts: uint64(v.CapacityWatts),
			TimestampUtc:           timestamppb.New(v.ValidFromUtc.Time),
			Metadata:               v.Metadata,
		}
	}

	return &pb.GetLocationAsTimeseriesResponse{
		Values: values,
	}, nil
}

func (s *DataPlatformDataServiceServerImpl) CreateLocation(
	ctx context.Context,
	req *pb.CreateLocationRequest,
) (*pb.CreateLocationResponse, error) {
	l := zerolog.Ctx(ctx)

	querier := db.New(ix.GetTxFromContext(ctx))

	var associated_point *string
	if req.AssociatedLatlng != nil {
		point := fmt.Sprintf(
			"POINT(%f %f)",
			req.AssociatedLatlng.Longitude,
			req.AssociatedLatlng.Latitude,
		)
		associated_point = &point
	}

	cgprms := db.CreateGeometryParams{
		GeometryName:    req.LocationName,
		Geom:            req.GeometryWkt,
		GeometryTypeID:  int16(req.LocationType),
		AssociatedPoint: associated_point,
	}

	dbLocation, err := querier.CreateGeometry(ctx, cgprms)
	if err != nil {
		return nil, handleDBError(err, "Invalid location. Ensure name is not empty, and that geometry is valid, closed, unique WGS84.")
	}

	l.Debug().
		Str("dp.geometry.uuid", dbLocation.GeometryUuid.String()).
		Str("dp.geometry.name", dbLocation.GeometryName).
		Float32("dp.geometry.longitude", dbLocation.Longitude).
		Float32("dp.geometry.latitude", dbLocation.Latitude).
		Msgf("created geometry")

	// Create a source associated with the location

	// Set valid from time to now if not provided
	if req.ValidFromUtc == nil {
		req.ValidFromUtc = timestamppb.New(time.Now().UTC().Truncate(time.Minute))
	}

	csprms := db.CreateSourceEntryParams{
		GeometryUuid:  dbLocation.GeometryUuid,
		SourceTypeID:  int16(req.EnergySource),
		CapacityWatts: int64(req.EffectiveCapacityWatts),
		Metadata:      req.Metadata,
		ValidFromUtc:  pgtype.Timestamp{Time: req.ValidFromUtc.AsTime(), Valid: true},
	}

	dbSource, err := querier.CreateSourceEntry(ctx, csprms)
	if err != nil {
		return nil, handleDBError(err, "Invalid location. Ensure metadata is NULL or a non-empty JSON object.")
	}

	l.Debug().
		Str("dp.source.geometry_uuid", dbSource.GeometryUuid.String()).
		Int16("dp.source.type_id", dbSource.SourceTypeID).
		Int64("dp.source.capacity", int64(dbSource.CapacityWatts)).
		Str("dp.source.valid_from_utc", dbSource.ValidFromUtc.Time.String()).
		Msg("created source entry for location")

	err = querier.RefreshSourcesMaterializedView(ctx)
	if err != nil {
		return nil, handleDBError(err, "Failed to update sources materialised view")
	}

	l.Debug().Msg("refreshed sources materialised view")

	return &pb.CreateLocationResponse{
		LocationUuid:           dbLocation.GeometryUuid.String(),
		LocationName:           dbLocation.GeometryName,
		EffectiveCapacityWatts: uint64(dbSource.CapacityWatts),
	}, nil
}

func (s *DataPlatformDataServiceServerImpl) UpdateLocation(
	ctx context.Context,
	req *pb.UpdateLocationRequest,
) (*pb.UpdateLocationResponse, error) {
	l := zerolog.Ctx(ctx)
	querier := db.New(ix.GetTxFromContext(ctx))

	if req.NewEffectiveCapacityWatts == nil &&
		req.NewLocationName == nil &&
		req.NewMetadata == nil {
		return nil, status.Error(
			codes.InvalidArgument,
			"At least one of new effective capacity, new location name, or new metadata must be provided.",
		)
	}

	// Set the valid from time to now if not provided
	validFrom := time.Now().UTC().Truncate(time.Minute)
	if req.ValidFromUtc != nil {
		validFrom = req.ValidFromUtc.AsTime().UTC()
	}

	// Get the location source as it stands at the valid time
	lsprms := db.GetSourceAtTimestampParams{
		GeometryUuid:   uuid.MustParse(req.LocationUuid),
		SourceTypeID:   int16(req.EnergySource.Number()),
		AtTimestampUtc: pgtype.Timestamp{Time: validFrom, Valid: true},
	}

	dbSource, err := querier.GetSourceAtTimestamp(ctx, lsprms)
	if err != nil {
		return nil, handleDBError(err, "Location does not exist. Create the location before attempting to update capacity.")
	}

	l.Debug().
		Str("dp.source.geometry_uuid", dbSource.GeometryUuid.String()).
		Int16("dp.source.type_id", dbSource.SourceTypeID).
		Int64("dp.source.capacity", int64(dbSource.CapacityWatts)).
		Str("dp.source.valid_from_utc", dbSource.SysPeriod.Lower.Time.String()).
		Msg("fetched source")

	capacity := dbSource.CapacityWatts
	if req.NewEffectiveCapacityWatts != nil {
		capacity = int64(*req.NewEffectiveCapacityWatts)
	}

	// Use existing metadata, unless new metadata is provided
	metadata := dbSource.MetadataJsonb
	if req.NewMetadata != nil {
		metadata = req.NewMetadata
	}

	// Update the location name, if provided
	if req.NewLocationName != nil {
		rgprms := db.RenameGeometryParams{
			GeometryUuid:    dbSource.GeometryUuid,
			NewGeometryName: req.GetNewLocationName(),
		}

		_, err = querier.RenameGeometry(ctx, rgprms)
		if err != nil {
			return nil, handleDBError(err, "Invalid location name. Ensure new name is not empty and unique.")
		}
	}

	refreshIsRequired := req.NewLocationName != nil

	// Update the source history with a new entry
	csprms := db.CreateSourceEntryParams{
		GeometryUuid:     dbSource.GeometryUuid,
		SourceTypeID:     dbSource.SourceTypeID,
		CapacityWatts:    capacity,
		CapacityLimitSip: dbSource.CapacityLimitSip, // TODO: Enable updating this
		ValidFromUtc:     pgtype.Timestamp{Time: validFrom, Valid: true},
		Metadata:         metadata,
	}

	dbNewSource, err := querier.CreateSourceEntry(ctx, csprms)
	switch {
	case err == nil:
		refreshIsRequired = true

	case errors.Is(err, pgx.ErrNoRows):
		l.Debug().Msg("capacity and metadata unchanged; skipping insert")
		dbNewSource = db.CreateSourceEntryRow{
			CapacityWatts: dbSource.CapacityWatts,
			ValidFromUtc:  csprms.ValidFromUtc,
		}

	default:
		return nil, handleDBError(err, "Invalid location source. Ensure metadata is NULL or a non-empty JSON object.")
	}

	l.Debug().
		Str("dp.source.geometry_uuid", dbSource.GeometryUuid.String()).
		Int16("dp.source.type_id", dbSource.SourceTypeID).
		Int64("dp.source.new_capacity", int64(dbNewSource.CapacityWatts)).
		Str("dp.source.valid_from_utc", dbNewSource.ValidFromUtc.Time.String()).
		Msg("updated source")

	if refreshIsRequired {
		err = querier.RefreshSourcesMaterializedView(ctx)
		if err != nil {
			return nil, handleDBError(err, "Failed to update sources materialised view")
		}

		l.Debug().Msg("refreshed sources materialised view")
	}

	return &pb.UpdateLocationResponse{
		LocationUuid:           req.LocationUuid,
		LocationName:           dbSource.GeometryName,
		EffectiveCapacityWatts: uint64(dbNewSource.CapacityWatts),
	}, nil
}

func (s *DataPlatformDataServiceServerImpl) UpdateLocationOwner(
	ctx context.Context,
	req *pb.UpdateLocationOwnerRequest,
) (resp *pb.UpdateLocationOwnerResponse, err error) {
	l := zerolog.Ctx(ctx)

	querier := db.New(ix.GetTxFromContext(ctx))

	params := db.ReownGeometryParams{
		GeometryUuid:              uuid.MustParse(req.LocationUuid),
		NewOwningEntityExternalID: req.NewOrganisationId,
	}

	dbGeom, err := querier.ReownGeometry(ctx, params)
	if err != nil {
		return nil, handleDBError(err, "Invalid location UUID")
	}

	l.Debug().
		Str("dp.geometry.uuid", dbGeom.GeometryUuid.String()).
		Str("dp.geometry.new_owner_org_id", req.NewOrganisationId).
		Msg("updated location owner")

	err = querier.RefreshSourcesMaterializedView(ctx)
	if err != nil {
		return nil, handleDBError(err, "Failed to update sources materialised view")
	}

	l.Debug().Msg("refreshed sources materialised view")

	return &pb.UpdateLocationOwnerResponse{
		LocationUuid:   dbGeom.GeometryUuid.String(),
		OrganisationId: req.NewOrganisationId,
	}, nil
}

func (s *DataPlatformDataServiceServerImpl) GetLocationsAsGeoJSON(
	ctx context.Context,
	req *pb.GetLocationsAsGeoJSONRequest,
) (resp *pb.GetLocationsAsGeoJSONResponse, err error) {
	querier := db.New(ix.GetTxFromContext(ctx))

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
			return nil, status.Errorf(codes.InvalidArgument, "Invalid location UUID: %v", err)
		}
	}

	ggprms := db.GetGeometryGeoJSONParams{
		SimplificationLevel: simplificationLevel,
		GeometryUuids:       locationUuids,
	}

	geojson, err := querier.GetGeometryGeoJSON(ctx, ggprms)
	if err != nil {
		return nil, handleDBError(err, "No locations found for input IDs")
	}

	return &pb.GetLocationsAsGeoJSONResponse{Geojson: string(geojson)}, nil
}

func (s *DataPlatformDataServiceServerImpl) GetForecastAsTimeseries(
	ctx context.Context,
	req *pb.GetForecastAsTimeseriesRequest,
) (*pb.GetForecastAsTimeseriesResponse, error) {
	l := zerolog.Ctx(ctx)

	querier := db.New(ix.GetTxFromContext(ctx))

	gsprms := db.GetSourceAtTimestampParams{
		GeometryUuid: uuid.MustParse(req.LocationUuid),
		SourceTypeID: int16(req.EnergySource.Number()),
		AtTimestampUtc: pgtype.Timestamp{
			Time:  req.TimeWindow.StartTimestampUtc.AsTime(),
			Valid: true,
		},
	}

	dbSource, err := querier.GetSourceAtTimestamp(ctx, gsprms)
	if err != nil {
		return nil, handleDBError(err, "No such location. Ensure the location exists, and is operational at the start of the requested time window.")
	}

	// If in init time has been requested, only return the values for that single forecast.
	if req.InitializationTimestampUtc != nil {
		llprms := db.ListPredictionsForForecastsParams{
			GeometryUuid:       uuid.MustParse(req.LocationUuid),
			SourceTypeID:       int16(req.EnergySource.Number()),
			ForecasterNames:    []string{req.Forecaster.ForecasterName},
			ForecasterVersions: []string{req.Forecaster.ForecasterVersion},
			StartTimestamp: pgtype.Timestamp{
				Time:  req.InitializationTimestampUtc.AsTime(),
				Valid: true,
			},
			EndTimestamp: pgtype.Timestamp{
				Time:  req.InitializationTimestampUtc.AsTime(),
				Valid: true,
			},
		}

		dbPreds, err := querier.ListPredictionsForForecasts(ctx, llprms)
		if err != nil {
			return nil, handleDBError(err, "No forecasts found for the given parameters.")
		}

		out := make([]*pb.GetForecastAsTimeseriesResponse_Value, len(dbPreds))
		for i, pred := range dbPreds {
			otherStats := make(map[string]float32)
			if pred.P02Sip != nil {
				otherStats["p02"] = float32(*pred.P02Sip) / 30000.0
			}

			if pred.P10Sip != nil {
				otherStats["p10"] = float32(*pred.P10Sip) / 30000.0
			}

			if pred.P25Sip != nil {
				otherStats["p25"] = float32(*pred.P25Sip) / 30000.0
			}

			if pred.P75Sip != nil {
				otherStats["p75"] = float32(*pred.P75Sip) / 30000.0
			}

			if pred.P90Sip != nil {
				otherStats["p90"] = float32(*pred.P90Sip) / 30000.0
			}

			if pred.P98Sip != nil {
				otherStats["p98"] = float32(*pred.P98Sip) / 30000.0
			}

			out[i] = &pb.GetForecastAsTimeseriesResponse_Value{
				TargetTimestampUtc: timestamppb.New(
					pred.InitTimeUtc.Time.Add(time.Duration(pred.HorizonMins) * time.Minute),
				),
				P50ValueFraction:           float32(pred.P50Sip) / 30000.0,
				EffectiveCapacityWatts:     uint64(pred.CapacityWatts),
				InitializationTimestampUtc: timestamppb.New(pred.InitTimeUtc.Time),
				CreatedTimestampUtc:        timestamppb.New(pred.CreatedAtUtc.Time),
				OtherStatisticsFractions:   otherStats,
				Metadata:                   pred.Metadata,
			}
		}

		return &pb.GetForecastAsTimeseriesResponse{
			LocationUuid: req.LocationUuid,
			LocationName: dbSource.GeometryName,
			Values:       out,
		}, nil
	}

	// Otherwise, return the collapsed timeseries.
	// Get the relevant forecaster
	gpprms := db.GetForecasterElseLatestParams{
		ForecasterName:    req.Forecaster.ForecasterName,
		ForecasterVersion: req.Forecaster.ForecasterVersion,
	}

	dbExistingForecaster, err := querier.GetForecasterElseLatest(ctx, gpprms)
	if err != nil {
		return nil, handleDBError(err, "No such forecaster.")
	}

	// Get the predictions for the given location source
	start, end, err := timeWindowToPgWindow(req.TimeWindow)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid time window: %v", err)
	}

	pivotTime := pgtype.Timestamp{Valid: false}
	if req.PivotTimestampUtc != nil {
		pivotTime = pgtype.Timestamp{Time: req.PivotTimestampUtc.AsTime(), Valid: true}
	}

	lpprms := db.ListPredictionsForLocationParams{
		GeometryUuid:      dbSource.GeometryUuid,
		ForecasterID:      dbExistingForecaster.ForecasterID,
		SourceTypeID:      dbSource.SourceTypeID,
		HorizonMins:       int32(req.HorizonMins),
		StartTimestampUtc: start,
		EndTimestampUtc:   end,
		PivotTimestamp:    pivotTime,
	}

	dbValues, err := querier.ListPredictionsForLocation(ctx, lpprms)
	if err != nil {
		return nil, handleDBError(err, "Error communicating with backend.")
	}

	if len(dbValues) == 0 {
		l.Debug().
			Str("dp.geometry.uuid", dbSource.GeometryUuid.String()).
			Int16("dp.source.type_id", dbSource.SourceTypeID).
			Int32("dp.forecaster.id", dbExistingForecaster.ForecasterID).
			Str("dp.time_window", fmt.Sprintf("%s - %s", start.Time.String(), end.Time.String())).
			Msg("no predictions found")
	} else {
		l.Debug().
			Str("dp.geometry.uuid", dbSource.GeometryUuid.String()).
			Int16("dp.source.type_id", dbSource.SourceTypeID).
			Int32("dp.forecaster.id", dbExistingForecaster.ForecasterID).
			Int32("dp.predictions.count", int32(len(dbValues))).
			Str("dp.predictions.target_period", fmt.Sprintf(
				"%s - %s",
				dbValues[0].TargetTimeUtc.Time.String(),
				dbValues[len(dbValues)-1].TargetTimeUtc.Time.String(),
			)).
			Msg(fmt.Sprintf("found %d predictions", len(dbValues)))
	}

	values := make([]*pb.GetForecastAsTimeseriesResponse_Value, len(dbValues))
	for i, value := range dbValues {
		otherStats := make(map[string]float32)
		if value.P02Sip != nil {
			otherStats["p02"] = float32(*value.P02Sip) / 30000.0
		}

		if value.P10Sip != nil {
			otherStats["p10"] = float32(*value.P10Sip) / 30000.0
		}

		if value.P25Sip != nil {
			otherStats["p25"] = float32(*value.P25Sip) / 30000.0
		}

		if value.P75Sip != nil {
			otherStats["p75"] = float32(*value.P75Sip) / 30000.0
		}

		if value.P90Sip != nil {
			otherStats["p90"] = float32(*value.P90Sip) / 30000.0
		}

		if value.P98Sip != nil {
			otherStats["p98"] = float32(*value.P98Sip) / 30000.0
		}

		values[i] = &pb.GetForecastAsTimeseriesResponse_Value{
			TargetTimestampUtc:         timestamppb.New(value.TargetTimeUtc.Time),
			P50ValueFraction:           float32(value.P50Sip) / 30000.0,
			OtherStatisticsFractions:   otherStats,
			EffectiveCapacityWatts:     uint64(value.CapacityWatts),
			InitializationTimestampUtc: timestamppb.New(value.InitTimeUtc.Time),
			CreatedTimestampUtc:        timestamppb.New(value.CreatedAtUtc.Time),
			Metadata:                   value.Metadata,
		}
	}

	return &pb.GetForecastAsTimeseriesResponse{
		LocationUuid: dbSource.GeometryUuid.String(),
		LocationName: dbSource.GeometryName,
		Values:       values,
	}, nil
}

// ListLocations implements dp.DataPlatformDataServiceServer.
func (s *DataPlatformDataServiceServerImpl) ListLocations(
	ctx context.Context,
	req *pb.ListLocationsRequest,
) (*pb.ListLocationsResponse, error) {
	l := zerolog.Ctx(ctx)
	querier := db.New(ix.GetTxFromContext(ctx))

	parsedUuids := make([]uuid.UUID, len(req.LocationUuidsFilter))
	for i, id := range req.LocationUuidsFilter {
		parsedUuids[i] = uuid.MustParse(id)
	}

	var sourceTypeId *int16
	if req.EnergySourceFilter != nil {
		stid := int16(req.EnergySourceFilter.Number())
		sourceTypeId = &stid
	}

	var locationTypeId *int16
	if req.LocationTypeFilter != nil {
		ltid := int16(req.LocationTypeFilter.Number())
		locationTypeId = &ltid
	}

	var locations []*pb.ListLocationsResponse_LocationSummary

	if req.EnclosingLocationUuidFilter != nil {
		llprms := db.ListSourcesAtTimestampWithinParams{
			OuterGeometryUuid:      uuid.MustParse(*req.EnclosingLocationUuidFilter),
			AtTimestampUtc:         pgtype.Timestamp{Time: time.Now().UTC(), Valid: true},
			OwningEntityExternalID: req.OrganisationIdFilter,
			GeometryUuids:          parsedUuids,
			SourceTypeID:           sourceTypeId,
			GeometryTypeID:         locationTypeId,
		}

		glResp, err := querier.ListSourcesAtTimestampWithin(ctx, llprms)
		if err != nil {
			return nil, handleDBError(err, "Failed to list enclosed locations")
		}

		for _, loc := range glResp {
			locations = append(locations, &pb.ListLocationsResponse_LocationSummary{
				LocationUuid: loc.GeometryUuid.String(),
				LocationName: loc.GeometryName,
				Latlng: &pb.LatLng{
					Latitude:  loc.Latitude,
					Longitude: loc.Longitude,
				},
				EffectiveCapacityWatts: uint64(loc.CapacityWatts),
				EnergySource:           pb.EnergySource(loc.SourceTypeID),
				LocationType:           pb.LocationType(loc.GeometryTypeID),
				Metadata:               loc.MetadataJsonb,
			})
		}
	} else if req.EnclosedLocationUuidFilter != nil {
		llprms := db.ListSourcesAtTimestampWithoutParams{
			InnerGeometryUuid:      uuid.MustParse(*req.EnclosedLocationUuidFilter),
			AtTimestampUtc:         pgtype.Timestamp{Time: time.Now().UTC(), Valid: true},
			OwningEntityExternalID: req.OrganisationIdFilter,
			GeometryUuids:          parsedUuids,
			SourceTypeID:           sourceTypeId,
			GeometryTypeID:         locationTypeId,
		}

		glResp, err := querier.ListSourcesAtTimestampWithout(ctx, llprms)
		if err != nil {
			return nil, handleDBError(err, "Failed to list enclosing locations")
		}

		for _, loc := range glResp {
			locations = append(locations, &pb.ListLocationsResponse_LocationSummary{
				LocationUuid: loc.GeometryUuid.String(),
				LocationName: loc.GeometryName,
				Latlng: &pb.LatLng{
					Latitude:  loc.Latitude,
					Longitude: loc.Longitude,
				},
				EffectiveCapacityWatts: uint64(loc.CapacityWatts),
				EnergySource:           pb.EnergySource(loc.SourceTypeID),
				LocationType:           pb.LocationType(loc.GeometryTypeID),
				Metadata:               loc.MetadataJsonb,
			})
		}
	} else {
		lsprms := db.ListSourcesAtTimestampParams{
			OwningEntityExternalID: req.OrganisationIdFilter,
			GeometryUuids:          parsedUuids,
			AtTimestampUtc:         pgtype.Timestamp{Time: time.Now().UTC(), Valid: true},
			SourceTypeID:           sourceTypeId,
			GeometryTypeID:         locationTypeId,
			GeometryNames:          req.LocationNamesFilter,
		}

		glResp, err := querier.ListSourcesAtTimestamp(ctx, lsprms)
		if err != nil {
			return nil, handleDBError(err, "Failed to list locations")
		}

		for _, loc := range glResp {
			locations = append(locations, &pb.ListLocationsResponse_LocationSummary{
				LocationUuid: loc.GeometryUuid.String(),
				LocationName: loc.GeometryName,
				Latlng: &pb.LatLng{
					Latitude:  loc.Latitude,
					Longitude: loc.Longitude,
				},
				EffectiveCapacityWatts: uint64(loc.CapacityWatts),
				EnergySource:           pb.EnergySource(loc.SourceTypeID),
				LocationType:           pb.LocationType(loc.GeometryTypeID),
				Metadata:               loc.MetadataJsonb,
			})
		}
	}

	l.Debug().
		Int("dp.locations.count", len(locations)).
		Msg("found locations")

	return &pb.ListLocationsResponse{
		Locations: locations,
	}, nil
}

// Compile-time check to ensure the interface is implemented fully.
var _ pb.DataPlatformDataServiceServer = (*DataPlatformDataServiceServerImpl)(nil)
