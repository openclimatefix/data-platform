// Package postgres defines server implementations for the DataPlatform Services that
// are backed by a PostgreSQL database.
//
// Functions and structs for connecting to the database are generated from SQL using
// the sqlc library, whilst the Server interface that is being implemented comes from
// the top-level proto definitions.
package postgres

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/rs/zerolog"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/openclimatefix/data-platform/internal/gen/ocf/dp"
	ix "github.com/openclimatefix/data-platform/internal/interceptors"
	db "github.com/openclimatefix/data-platform/internal/server/postgres/gen"
)

func NewDataPlatformDataServiceServerImpl() *DataPlatformDataServiceServerImpl {
	return &DataPlatformDataServiceServerImpl{}
}

// DataPlatformDataServiceServerImpl implements the pb.DataPlatformDataServiceServer interface.
// It requires the database transaction for the request to be set in the context.
type DataPlatformDataServiceServerImpl struct{}

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
		return nil, fmt.Errorf("no location found: %w", err)
	}

	l.Debug().Str("dp.geometry.uuid", dbSource.GeometryUuid.String()).
		Int16("dp.source.type_id", dbSource.SourceTypeID).
		Msg("found source")

	// Check the forecast values have monotonically increasing horizons
	err = validateForecastValues(req.Values)
	if err != nil {
		return nil, status.Error(
			codes.InvalidArgument,
			fmt.Sprintf("invalid forecast values: %v", err),
		)
	}

	// Check the forecaster exists
	pctprms := db.GetForecasterElseLatestParams{
		ForecasterName:    req.Forecaster.ForecasterName,
		ForecasterVersion: req.Forecaster.ForecasterVersion,
	}

	dbForecaster, err := querier.GetForecasterElseLatest(ctx, pctprms)
	if err != nil {
		return nil, fmt.Errorf("no such forecaster: %w", err)
	}

	l.Debug().
		Int32("dp.forecaster.id", dbForecaster.ForecasterID).
		Str("dp.forecaster.name", dbForecaster.ForecasterName).
		Str("dp.forecaster.version", dbForecaster.ForecasterVersion).
		Msg("found forecaster")

	// Create a new forecast
	fParams, err := mapCreateForecast(
		req,
		uuid.MustParse(req.LocationUuid),
		dbSource.SourceTypeID,
		dbForecaster.ForecasterID,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to map forecast params: %w", err)
	}

	countF, err := querier.CreateForecasts(ctx, []db.CreateForecastsParams{fParams})
	if err != nil || countF < 1 {
		if err == nil {
			err = errors.New("inserted forecasts count less than requested")
		}

		return nil, fmt.Errorf("invalid forecast: %w", err)
	}

	l.Debug().
		Str("dp.forecast.uuid", fParams.ForecastUuid.String()).
		Str("dp.geometry.uuid", fParams.GeometryUuid.String()).
		Str("dp.forecast.init_time", fParams.InitTimeUtc.Time.String()).
		Str("dp.forecast.target_period", fmt.Sprintf(
			"%s - %s",
			fParams.TargetPeriod.Lower.Time.String(),
			fParams.TargetPeriod.Upper.Time.String(),
		)).Msgf("created forecast")

	return &pb.CreateForecastResponse{
		ForecastUuid: fParams.ForecastUuid.String(),
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
		return nil, fmt.Errorf("no such forecaster: %w", err)
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
		return nil, fmt.Errorf("could not delete forecast: %w", err)
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
		return nil, fmt.Errorf("no forecasts found: %w", err)
	}

	l.Debug().Str("dp.geometry.uuid", req.LocationUuid).
		Int16("dp.source.type_id", glfprms.SourceTypeID).
		Int("dp.forecasts.count", len(dbListForecasts)).
		Msg("fetched latest forecasts")

	forecasts := MapSlice(dbListForecasts, mapLatestForecast)

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
			"Forecaster already exists (at version '%s'). "+
				"Use the update method to add a new version, or create a new forecaster.",
			dbExistingForecaster.ForecasterVersion,
		)
	}

	// Create a new forecaster
	cfprms := db.CreateForecasterParams{ForecasterName: req.Name, ForecasterVersion: req.Version}

	dbForecaster, err := querier.CreateForecaster(ctx, cfprms)
	if err != nil {
		return nil, fmt.Errorf("invalid forecaster: %w", err)
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
		return nil, fmt.Errorf("no such forecaster: %w", err)
	}

	// Update the forecaster
	cfprms := db.CreateForecasterParams{
		ForecasterName:    dbExistingForecaster.ForecasterName,
		ForecasterVersion: req.NewVersion,
	}

	dbForecaster, err := querier.CreateForecaster(ctx, cfprms)
	if err != nil {
		return nil, fmt.Errorf("invalid forecaster: %w", err)
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
		return nil, fmt.Errorf("no forecasters found with the specified filters: %w", err)
	}

	forecasters := MapSlice(dbListForecasters, mapForecaster)

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

				locationUuid := uuid.MustParse(locStr)

				// Query with the pool directly so each concurrent request gets a fresh connection.
				// This is to avoid very large data requests choking the memory of the API.
				// Normally I woudn't want to bypass SQLC's type safety - but this RPC is specifically for
				// ML debugging and isn't on any hot path, so I don't mind here.
				rows, err := pool.Query(
					stream.Context(),
					db.ListPredictionsForForecasts,
					fNames,
					fVersions,
					locationUuid,
					int16(req.EnergySource.Number()),
					pgtype.Timestamp{
						Time:  req.TimeWindow.StartTimestampUtc.AsTime(),
						Valid: true,
					},
					pgtype.Timestamp{
						Time:  req.TimeWindow.EndTimestampUtc.AsTime(),
						Valid: true,
					},
				)
				if err != nil {
					return fmt.Errorf("failed to stream predictions: %w", err)
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
						return status.Errorf(
							codes.Internal,
							"Error reading prediction stream: %v",
							err,
						)
					}

					batch = append(
						batch,
						mapStreamedForecastDatum(row, locationUuid, req.IncludeMetadata),
					)
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
	locationUuid := uuid.MustParse(req.LocationUuid)

	gstprms := db.GetSourceAtTimestampParams{
		GeometryUuid:   locationUuid,
		SourceTypeID:   int16(req.EnergySource.Number()),
		AtTimestampUtc: pgtype.Timestamp{Time: req.PivotTimestampUtc.AsTime(), Valid: true},
	}

	dbSource, err := querier.GetSourceAtTimestamp(ctx, gstprms)
	if err != nil {
		return nil, fmt.Errorf(
			"no location source found for name '%s' with source type '%s': %w",
			req.LocationUuid,
			req.EnergySource,
			err,
		)
	}

	// Get the relevant forecaster
	pctprms := db.GetForecasterElseLatestParams{
		ForecasterName:    req.Forecaster.ForecasterName,
		ForecasterVersion: req.Forecaster.ForecasterVersion,
	}

	dbExistingForecaster, err := querier.GetForecasterElseLatest(ctx, pctprms)
	if err != nil {
		return nil, fmt.Errorf(
			"no forecaster found for name '%s' and version '%s': %w",
			req.Forecaster.ForecasterName,
			req.Forecaster.ForecasterVersion,
			err,
		)
	}

	// Get the observer
	obprms := db.GetObserverByNameParams{ObserverName: req.ObserverName}

	dbObserver, err := querier.GetObserverByName(ctx, obprms)
	if err != nil {
		return nil, fmt.Errorf(
			"no observer of name '%s' found: %w",
			req.ObserverName,
			err,
		)
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
		return nil, fmt.Errorf(
			"no deltas found for location '%s' with source type '%s' and observer ID '%s': %w",
			req.LocationUuid,
			req.EnergySource,
			dbObserver.ObserverUuid.String(),
			err,
		)
	}

	// Convert the deltas to the response format
	deltas := MapSlice(
		dbDeltas,
		func(row db.GetWeekAverageDeltasForLocationsRow) *pb.GetWeekAverageDeltasResponse_AverageDelta {
			return mapWeekAverageDelta(row, dbSource.CapacityWatts)
		},
	)

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
		return nil, fmt.Errorf(
			"no observer of name '%s' found: %w",
			req.ObserverName,
			err,
		)
	}

	start, end := timeWindowToPgWindow(req.TimeWindow)

	goprms := db.GetObservationsBetweenParams{
		GeometryUuid: locationUuid,
		SourceTypeID: int16(req.EnergySource),
		ObserverUuid: observerResp.ObserverUuid,
		StartTimeUtc: start,
		EndTimeUtc:   end,
	}

	dbObs, err := querier.GetObservationsBetween(ctx, goprms)
	if err != nil {
		return nil, fmt.Errorf(
			"no observations found for location '%s': %w",
			req.LocationUuid,
			err,
		)
	}

	values := MapSlice(dbObs, mapObservationAsTimeseries)

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
	locationUuid := uuid.MustParse(req.LocationUuid)

	cfprms := db.GetSourceAtTimestampParams{
		GeometryUuid:   locationUuid,
		SourceTypeID:   int16(req.EnergySource.Number()),
		AtTimestampUtc: pgtype.Timestamp{Time: req.Values[0].TimestampUtc.AsTime(), Valid: true},
	}

	dbSource, err := querier.GetSourceAtTimestamp(ctx, cfprms)
	if err != nil {
		return nil, fmt.Errorf(
			"no location found for name '%s' with source type '%s': %w",
			req.LocationUuid,
			req.EnergySource,
			err,
		)
	}

	// Get the observer ID
	obprms := db.GetObserverByNameParams{ObserverName: req.ObserverName}

	dbObserver, err := querier.GetObserverByName(ctx, obprms)
	if err != nil {
		return nil, fmt.Errorf(
			"no observer of name '%s' found: %w",
			req.ObserverName,
			err,
		)
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
		return nil, fmt.Errorf("invalid observation values: %w", err)
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
		return nil, fmt.Errorf("backend communication error: %w", err)
	}

	observations := MapSlice(dbObs, mapLatestObservation)

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
		return nil, fmt.Errorf("invalid observer name: %w", err)
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
		return nil, fmt.Errorf("backend communication error: %w", err)
	}

	l.Debug().
		Int("dp.observers.count", len(dbListObservers)).
		Msg("found observers")

	observers := MapSlice(dbListObservers, mapObserver)

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
		return nil, fmt.Errorf(
			"no forecaster found for name '%s' and version '%s': %w",
			req.Forecaster.ForecasterName,
			req.Forecaster.ForecasterVersion,
			err,
		)
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
		return nil, fmt.Errorf(
			"no predicted values found for the specified locations at the given time.,: %w",
			err,
		)
	}

	values := MapSlice(dbPredictions, mapPredictionAtTime)

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
		return nil, fmt.Errorf(
			"no observer of name '%s' found: %w",
			req.ObserverName,
			err,
		)
	}

	loprms := db.ListObservationsAtTimeForLocationsParams{
		GeometryUuids:      locUuids,
		SourceTypeID:       int16(req.EnergySource),
		ObserverUuid:       dbObserver.ObserverUuid,
		TargetTimestampUtc: pgtype.Timestamp{Time: req.TimestampUtc.AsTime(), Valid: true},
	}

	dbObs, err := querier.ListObservationsAtTimeForLocations(ctx, loprms)
	if err != nil {
		return nil, fmt.Errorf(
			"no observations found for the specified locations at the given time: %w",
			err,
		)
	}

	observations := MapSlice(dbObs, mapObservationAtTimestamp)

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
		return nil, fmt.Errorf("no such location: %w", err)
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
			return nil, fmt.Errorf("failed to retrieve geometry for location: %w", err)
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
		return nil, fmt.Errorf(
			"no such location or no history for location in the specified time window: %w",
			err,
		)
	}

	values := MapSlice(dbValues, mapLocationSnapshot)

	return &pb.GetLocationAsTimeseriesResponse{
		Values: values,
	}, nil
}

// createLocationSource inserts a source entry for a geometry.
// Importantly, this also refreshes the sources materialised view.
func createLocationSource(
	ctx context.Context,
	querier *db.Queries,
	geometryUuid uuid.UUID,
	sourceTypeID int16,
	capacityWatts uint64,
	metadata *structpb.Struct,
	validFrom time.Time,
) (db.CreateSourceEntryRow, error) {
	csprms := db.CreateSourceEntryParams{
		GeometryUuid:  geometryUuid,
		SourceTypeID:  sourceTypeID,
		CapacityWatts: int64(capacityWatts),
		Metadata:      metadata,
		ValidFromUtc:  pgtype.Timestamp{Time: validFrom, Valid: true},
	}

	dbSource, err := querier.CreateSourceEntry(ctx, csprms)
	if err != nil {
		return db.CreateSourceEntryRow{}, fmt.Errorf("invalid location source: %w", err)
	}

	if err := querier.RefreshSourcesMaterializedView(ctx); err != nil {
		return db.CreateSourceEntryRow{}, fmt.Errorf(
			"failed to update sources materialised view: %w",
			err,
		)
	}

	return dbSource, nil
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
		return nil, fmt.Errorf("invalid location: %w", err)
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

	dbSource, err := createLocationSource(
		ctx, querier, dbLocation.GeometryUuid, int16(req.EnergySource.Number()),
		req.EffectiveCapacityWatts, req.Metadata, req.ValidFromUtc.AsTime(),
	)
	if err != nil {
		return nil, err
	}

	l.Debug().
		Str("dp.source.geometry_uuid", dbSource.GeometryUuid.String()).
		Int16("dp.source.type_id", dbSource.SourceTypeID).
		Int64("dp.source.capacity", int64(dbSource.CapacityWatts)).
		Str("dp.source.valid_from_utc", dbSource.ValidFromUtc.Time.String()).
		Msg("created source entry for location")

	return &pb.CreateLocationResponse{
		LocationUuid:           dbLocation.GeometryUuid.String(),
		LocationName:           dbLocation.GeometryName,
		EffectiveCapacityWatts: uint64(dbSource.CapacityWatts),
	}, nil
}

func (s *DataPlatformDataServiceServerImpl) CreateLocationEnergySource(
	ctx context.Context,
	req *pb.CreateLocationEnergySourceRequest,
) (*pb.CreateLocationEnergySourceResponse, error) {
	l := zerolog.Ctx(ctx)
	querier := db.New(ix.GetTxFromContext(ctx))

	locationUuid := uuid.MustParse(req.LocationUuid)

	if req.ValidFromUtc == nil {
		req.ValidFromUtc = timestamppb.New(time.Now().UTC().Truncate(time.Minute))
	}

	// Reject if this energy source already exists for the location.
	cseprms := db.CheckSourceExistsParams{
		GeometryUuid: locationUuid,
		SourceTypeID: int16(req.EnergySource.Number()),
	}

	exists, err := querier.CheckSourceExists(ctx, cseprms)
	if err != nil {
		return nil, fmt.Errorf("failed to check source existence: %w", err)
	}

	if exists {
		return nil, status.Errorf(
			codes.AlreadyExists,
			"energy source '%s' has already been created for location '%s'. New inserts to an existing energy source must go through the UpdateLocation RPC.",
			req.EnergySource,
			req.LocationUuid,
		)
	}

	dbSource, err := createLocationSource(
		ctx, querier, locationUuid, int16(req.EnergySource.Number()),
		req.EffectiveCapacityWatts, req.Metadata, req.ValidFromUtc.AsTime(),
	)
	if err != nil {
		return nil, err
	}

	l.Debug().
		Str("dp.source.geometry_uuid", locationUuid.String()).
		Int16("dp.source.type_id", int16(req.EnergySource.Number())).
		Int64("dp.source.capacity", int64(dbSource.CapacityWatts)).
		Msg("created new energy source for location")

	return &pb.CreateLocationEnergySourceResponse{
		LocationUuid:           locationUuid.String(),
		EnergySource:           req.EnergySource,
		EffectiveCapacityWatts: uint64(dbSource.CapacityWatts),
	}, nil
}

func (s *DataPlatformDataServiceServerImpl) UpdateLocation(
	ctx context.Context,
	req *pb.UpdateLocationRequest,
) (*pb.UpdateLocationResponse, error) {
	l := zerolog.Ctx(ctx)
	querier := db.New(ix.GetTxFromContext(ctx))

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
		return nil, fmt.Errorf("location does not exist: %w", err)
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
			return nil, fmt.Errorf("invalid location name: %w", err)
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
		return nil, fmt.Errorf("invalid location source: %w", err)
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
			return nil, fmt.Errorf("failed to update sources materialised view: %w", err)
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
		return nil, fmt.Errorf("invalid location UUID: %w", err)
	}

	l.Debug().
		Str("dp.geometry.uuid", dbGeom.GeometryUuid.String()).
		Str("dp.geometry.new_owner_org_id", req.NewOrganisationId).
		Msg("updated location owner")

	err = querier.RefreshSourcesMaterializedView(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to update sources materialised view: %w", err)
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
		locationUuids[i] = uuid.MustParse(id)
	}

	ggprms := db.GetGeometryGeoJSONParams{
		SimplificationLevel: simplificationLevel,
		GeometryUuids:       locationUuids,
	}

	geojson, err := querier.GetGeometryGeoJSON(ctx, ggprms)
	if err != nil {
		return nil, fmt.Errorf("no locations found for input IDs: %w", err)
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
		return nil, fmt.Errorf("no such location: %w", err)
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
			return nil, fmt.Errorf("no forecasts found for the given parameters: %w", err)
		}

		out := MapSlice(dbPreds, mapForecastAsTimeseriesFromForecastValue)

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
		return nil, fmt.Errorf("no such forecaster: %w", err)
	}

	// Get the predictions for the given location source
	start, end := timeWindowToPgWindow(req.TimeWindow)

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
		return nil, fmt.Errorf("error communicating with backend: %w", err)
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

	values := MapSlice(dbValues, mapForecastAsTimeseriesFromLocationValue)

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
			return nil, fmt.Errorf("failed to list enclosed locations: %w", err)
		}

		for _, loc := range glResp {
			locations = append(locations, mapLocationSummary(
				loc.GeometryUuid, loc.GeometryName, loc.Latitude, loc.Longitude,
				loc.CapacityWatts, loc.SourceTypeID, loc.GeometryTypeID, loc.MetadataJsonb,
			))
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
			return nil, fmt.Errorf("failed to list enclosing locations: %w", err)
		}

		for _, loc := range glResp {
			locations = append(locations, mapLocationSummary(
				loc.GeometryUuid, loc.GeometryName, loc.Latitude, loc.Longitude,
				loc.CapacityWatts, loc.SourceTypeID, loc.GeometryTypeID, loc.MetadataJsonb,
			))
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
			return nil, fmt.Errorf("failed to list locations: %w", err)
		}

		for _, loc := range glResp {
			locations = append(locations, mapLocationSummary(
				loc.GeometryUuid, loc.GeometryName, loc.Latitude, loc.Longitude,
				loc.CapacityWatts, loc.SourceTypeID, loc.GeometryTypeID, loc.MetadataJsonb,
			))
		}
	}

	l.Debug().
		Int("dp.locations.count", len(locations)).
		Msg("found locations")

	return &pb.ListLocationsResponse{
		Locations: locations,
	}, nil
}

// StreamCreateForecasts efficiently creates multiple forecasts and their predictions via copyfrom batching.
func (s *DataPlatformDataServiceServerImpl) StreamCreateForecasts(
	stream grpc.ClientStreamingServer[pb.CreateForecastRequest, pb.StreamCreateForecastsResponse],
) error {
	ctx := stream.Context()
	pool := ix.GetPoolFromContext(ctx)

	tx, err := pool.Begin(ctx)
	querier := db.New(tx)

	if err != nil {
		return status.Errorf(codes.Internal, "failed to begin transaction: %v", err)
	}

	defer func() { _ = tx.Rollback(ctx) }()

	const (
		batchSize  = 500
		maxBatches = 10
	)
	batchesProcessed := 0

	var (
		forecastParams []db.CreateForecastsParams
		createdUuids   []string
		batchUuids     []string
	)

	// In-memory caches to avoid hammering the database for repeated forecaster/source lookups
	type sourceKey struct {
		locationUuid string
		sourceTypeId int16
	}

	type sourceInfo struct {
		capacityWatts int64
		geometryUuid  uuid.UUID
	}

	type forecasterKey struct {
		name    string
		version string
	}

	sourceCache := make(map[sourceKey]sourceInfo)
	forecasterCache := make(map[forecasterKey]int32)

	flushBatch := func() error {
		if len(forecastParams) == 0 {
			return nil
		}

		// Sort the batch to match the index to improve clustering in the database.
		slices.SortFunc(forecastParams, func(a, b db.CreateForecastsParams) int {
			if c := bytes.Compare(a.GeometryUuid[:], b.GeometryUuid[:]); c != 0 {
				return c
			}

			if c := cmp.Compare(a.SourceTypeID, b.SourceTypeID); c != 0 {
				return c
			}

			if c := cmp.Compare(a.ForecasterID, b.ForecasterID); c != 0 {
				return c
			}

			return bytes.Compare(b.ForecastUuid[:], a.ForecastUuid[:])
		})

		countF, err := querier.CreateForecasts(ctx, forecastParams)
		if err != nil || countF < int64(len(forecastParams)) {
			if err == nil {
				err = errors.New("inserted forecasts count less than requested")
			}

			return fmt.Errorf("failed to insert forecasts batch: %w", err)
		}

		createdUuids = append(createdUuids, batchUuids...)

		// Reset batch buffers
		forecastParams = forecastParams[:0]
		batchUuids = batchUuids[:0]

		batchesProcessed++
		if batchesProcessed > maxBatches {
			return status.Error(
				codes.InvalidArgument,
				fmt.Sprintf(
					"maximum number of forecasts per stream exceeded (%d)",
					maxBatches*batchSize,
				),
			)
		}

		return nil
	}

	for {
		req, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			return fmt.Errorf("error receiving from stream: %w", err)
		}

		if err := validateForecastValues(req.Values); err != nil {
			return status.Error(
				codes.InvalidArgument,
				fmt.Sprintf("invalid forecast values: %v", err),
			)
		}

		fKey := forecasterKey{
			name:    req.Forecaster.ForecasterName,
			version: req.Forecaster.ForecasterVersion,
		}

		fId, ok := forecasterCache[fKey]
		if !ok {
			pctprms := db.GetForecasterElseLatestParams{
				ForecasterName:    fKey.name,
				ForecasterVersion: fKey.version,
			}

			dbForecaster, err := querier.GetForecasterElseLatest(ctx, pctprms)
			if err != nil {
				return fmt.Errorf(
					"no forecaster found for name '%s' and version '%s': %w",
					fKey.name,
					fKey.version,
					err,
				)
			}

			fId = dbForecaster.ForecasterID
			forecasterCache[fKey] = fId
		}

		sKey := sourceKey{
			locationUuid: req.LocationUuid,
			sourceTypeId: int16(req.EnergySource.Number()),
		}

		sInfo, ok := sourceCache[sKey]
		if !ok {
			gsprms := db.GetSourceAtTimestampParams{
				GeometryUuid:   uuid.MustParse(req.LocationUuid),
				SourceTypeID:   sKey.sourceTypeId,
				AtTimestampUtc: timeptrToPgTimestamp(req.InitTimeUtc),
			}

			dbSource, err := querier.GetSourceAtTimestamp(ctx, gsprms)
			if err != nil {
				return fmt.Errorf(
					"no location source found for name '%s' with source type '%s': %w",
					req.LocationUuid,
					req.EnergySource,
					err,
				)
			}

			sInfo = sourceInfo{
				capacityWatts: dbSource.CapacityWatts,
				geometryUuid:  dbSource.GeometryUuid,
			}
			sourceCache[sKey] = sInfo
		}

		fParams, err := mapCreateForecast(
			req,
			sInfo.geometryUuid,
			sKey.sourceTypeId,
			fId,
		)
		if err != nil {
			return fmt.Errorf("failed to prepare forecast params: %w", err)
		}

		forecastParams = append(forecastParams, fParams)
		batchUuids = append(batchUuids, fParams.ForecastUuid.String())

		// Flush if we hit the batch size limit
		if len(forecastParams) >= batchSize {
			if err := flushBatch(); err != nil {
				return err
			}
		}
	}

	// Flush any remaining requests
	if err := flushBatch(); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return status.Errorf(codes.Internal, "failed to commit transaction: %v", err)
	}

	return stream.SendAndClose(&pb.StreamCreateForecastsResponse{
		ForecastUuids: createdUuids,
	})
}

// Compile-time check to ensure the interface is implemented fully.
var _ pb.DataPlatformDataServiceServer = (*DataPlatformDataServiceServerImpl)(nil)
