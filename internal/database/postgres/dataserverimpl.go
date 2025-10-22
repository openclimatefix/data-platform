// Package postgres defines server implementations for the DataPlatform Services that
// are backed by a PostgreSQL database.
//
// Functions and structs for connecting to the database are generated from SQL using
// the sqlc library, whilst the Server interface that is being implemented comes from
// the top-level proto definitions.
package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	db "github.com/openclimatefix/data-platform/internal/database/postgres/gen"
	pb "github.com/openclimatefix/data-platform/internal/gen/ocf/dp"
)

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

// jsonbToStruct converts a JSONB byte array to a protobuf Struct.
func jsonbToStruct(data []byte) (*structpb.Struct, error) {
	if len(data) == 0 {
		return &structpb.Struct{}, nil
	}

	var m map[string]interface{}
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("json.Unmarshal: %w", err)
	}

	s, err := structpb.NewStruct(m)
	if err != nil {
		return nil, fmt.Errorf("structpb.NewStruct: %w", err)
	}

	return s, nil
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
	l := log.With().Str("method", "CreateForecast").Logger()

	if len(req.Values) == 0 {
		return nil, status.Error(codes.InvalidArgument, "No forecast values provided")
	}

	querier := db.New(GetTxFromContext(ctx))

	// Get the location and source
	locationUuid, err := uuid.Parse(req.LocationUuid)
	if err != nil {
		l.Err(err).Msgf("uuid.Parse(%s)", req.LocationUuid)
		return nil, status.Errorf(codes.InvalidArgument, "Invalid location UUID: %v", err)
	}

	gsParams := db.GetLocationSourceAtTimestampParams{
		LocationUuid:   locationUuid,
		SourceTypeID:   int16(req.EnergySource.Number()),
		AtTimestampUtc: pgtype.Timestamp{Time: req.InitTimeUtc.AsTime(), Valid: true},
	}

	dbSource, err := querier.GetLocationSourceAtTimestamp(ctx, gsParams)
	if err != nil {
		l.Err(err).Msgf("querier.GetLocationSourceAtTimestamp(%+v)", gsParams)

		return nil, status.Errorf(
			codes.NotFound, "No location found for id '%s' with source type '%s'.",
			req.LocationUuid, req.EnergySource,
		)
	}

	resolution_mins := req.Values[1].HorizonMins - req.Values[0].HorizonMins // TODO: Check they are all the same

	// Check the forecaster exists
	pctParams := db.GetForecasterElseLatestParams{
		ForecasterName:    req.Forecaster.ForecasterName,
		ForecasterVersion: req.Forecaster.ForecasterVersion,
	}

	_, err = querier.GetForecasterElseLatest(ctx, pctParams)
	if err != nil {
		l.Err(err).Msgf("querier.GetForecasterElseLatest(%+v)", pctParams)

		return nil, status.Errorf(
			codes.NotFound, "No forecaster found for name '%s' and version '%s'."+
				"Create the forecaster before submitting a forecast.",
			req.Forecaster.ForecasterName, req.Forecaster.ForecasterVersion,
		)
	}

	// Create a new forecast
	params2 := db.CreateForecastParams{
		LocationUuid:        locationUuid,
		SourceTypeID:        dbSource.SourceTypeID,
		ForecasterName:      req.Forecaster.ForecasterName,
		ForecasterVersion:   req.Forecaster.ForecasterVersion,
		ValueResolutionMins: int16(resolution_mins),
		InitTimeUtc: pgtype.Timestamp{
			Time:  req.InitTimeUtc.AsTime(),
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
		p10sip := int16(value.P10Fraction * 30000.0)
		p90sip := int16(value.P90Fraction * 30000.0)

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
			P50Sip:       int16(value.P50Fraction * 30000.0),
			ForecastUuid: dbForecast.ForecastUuid,
			TargetTimeUtc: pgtype.Timestamp{
				Time: req.InitTimeUtc.AsTime().Add(
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

	return &pb.CreateForecastResponse{}, nil
}

func (s *DataPlatformDataServiceServerImpl) GetLatestForecasts(
	context.Context,
	*pb.GetLatestForecastsRequest,
) (*pb.GetLatestForecastsResponse, error) {
	_ = log.With().Str("method", "GetLatestForecasts").Logger()

	panic("unimplemented")
}

func (s *DataPlatformDataServiceServerImpl) CreateForecaster(
	ctx context.Context,
	req *pb.CreateForecasterRequest,
) (*pb.CreateForecasterResponse, error) {
	l := log.With().Str("method", "CreateForecaster").Logger()

	querier := db.New(GetTxFromContext(ctx))

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
	}, nil
}

func (s *DataPlatformDataServiceServerImpl) UpdateForecaster(
	ctx context.Context,
	req *pb.UpdateForecasterRequest,
) (*pb.UpdateForecasterResponse, error) {
	l := log.With().Str("method", "UpdateForecaster").Logger()

	querier := db.New(GetTxFromContext(ctx))

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
	}, nil
}

func (s *DataPlatformDataServiceServerImpl) StreamForecastData(
	req *pb.StreamForecastDataRequest,
	stream grpc.ServerStreamingServer[pb.StreamForecastDataResponse],
) error {
	l := log.With().Str("method", "StreamForecastData").Logger()

	querier := db.New(GetTxFromContext(stream.Context()))

	locationUuid, err := uuid.Parse(req.LocationUuid)
	if err != nil {
		l.Err(err).Msgf("uuid.Parse(%s)", req.LocationUuid)
		return status.Errorf(codes.InvalidArgument, "Invalid location UUID: %v", err)
	}
	// Get the source as it was at the initial time of the time window
	srcParams := db.GetLocationSourceAtTimestampParams{
		LocationUuid: locationUuid,
		SourceTypeID: int16(req.EnergySource.Number()),
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
				p90val := float32(*dbPreds[i].P90Sip) / 30000.0
				p90 = &p90val
			}

			var p10 *float32
			if dbPreds[i].P10Sip != nil {
				p10val := float32(*dbPreds[i].P10Sip) / 30000.0

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
				P50Fraction: float32(dbPreds[i].P50Sip) / 30000.0,
				P10Fraction: p10,
				P90Fraction: p90,
				CreatedTimestampUtc: timestamppb.New(forecast.CreatedAtUtc.Time),
			})
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (s *DataPlatformDataServiceServerImpl) ListLocationsWithin(
	ctx context.Context,
	req *pb.ListLocationsWithinRequest,
) (*pb.ListLocationsWithinResponse, error) {
	l := log.With().Str("method", "ListLocationsWithin").Logger()

	querier := db.New(GetTxFromContext(ctx))

	lwprms := db.GetLocationsWithinParams{
		LocationUuid: uuid.MustParse(req.EnclosingLocationUuid),
	}

	dbLocations, err := querier.GetLocationsWithin(ctx, lwprms)
	if err != nil {
		l.Err(err).Msgf("querier.GetLocationsWithin(%+v)", lwprms)

		return nil, status.Errorf(
			codes.NotFound,
			"No locations found within the specified location '%s'", req.EnclosingLocationUuid,
		)
	}

	locations := make([]*pb.ListLocationsWithinResponse_LocationData, len(dbLocations))
	for i := range dbLocations {
		locations[i] = &pb.ListLocationsWithinResponse_LocationData{
			LocationUuid: dbLocations[i].LocationUuid.String(),
			LocationName: strings.ToUpper(dbLocations[i].LocationName),
		}
	}

	return &pb.ListLocationsWithinResponse{
		Locations: locations,
	}, nil
}

func (s *DataPlatformDataServiceServerImpl) GetWeekAverageDeltas(
	ctx context.Context,
	req *pb.GetWeekAverageDeltasRequest,
) (*pb.GetWeekAverageDeltasResponse, error) {
	l := log.With().Str("method", "GetWeekAverageDeltas").Logger()

	querier := db.New(GetTxFromContext(ctx))

	// Get the location and source
	locationUuid, err := uuid.Parse(req.LocationUuid)
	if err != nil {
		l.Err(err).Msgf("uuid.Parse(%s)", req.LocationUuid)
		return nil, status.Errorf(codes.InvalidArgument, "Invalid location UUID: %v", err)
	}

	gstParams := db.GetLocationSourceAtTimestampParams{
		LocationUuid:   locationUuid,
		SourceTypeID:   int16(req.EnergySource.Number()),
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
			DeltaFraction: float32(delta.AvgDeltaSip) / 30000.0,
			HorizonMins:   uint32(delta.HorizonMins),
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

func (s *DataPlatformDataServiceServerImpl) GetObservationsAsTimeseries(
	ctx context.Context,
	req *pb.GetObservationsAsTimeseriesRequest,
) (*pb.GetObservationsAsTimeseriesResponse, error) {
	l := log.With().Str("method", "GetObservationsAsTimeseries").Logger()

	querier := db.New(GetTxFromContext(ctx))
	locationUuid := uuid.MustParse(req.LocationUuid)

	stprms := db.GetSourceTypeByNameParams{SourceTypeName: req.EnergySource.String()}

	sourceTypeResp, err := querier.GetSourceTypeByName(ctx, stprms)
	if err != nil {
		l.Err(err).Msgf("querier.GetSourceTypeByName(%+v)", stprms)

		return nil, status.Errorf(
			codes.NotFound, "No source type found for name '%s'.",
			req.EnergySource,
		)
	}

	// Get the observer
	obParams := db.GetObserverByNameParams{ObserverName: req.ObserverName}

	observerResp, err := querier.GetObserverByName(ctx, obParams)
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
		SourceTypeID: sourceTypeResp.SourceTypeID,
		ObserverID:   observerResp.ObserverID,
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
			ValueFraction: float32(obs.ValueSip) / 30000.0,
			TimestampUtc:  timestamppb.New(obs.ObservationTimestampUtc.Time),
			EffectiveCapacityWatts: uint64(
				float64(obs.EffectiveCapacity) * math.Pow10(int(obs.CapacityUnitPrefixFactor)),
			),
		}
	}

	return &pb.GetObservationsAsTimeseriesResponse{
		LocationUuid: locationUuid.String(),
		LocationName: "", // TODO
		Values:       values,
	}, nil
}

func (s *DataPlatformDataServiceServerImpl) CreateObservations(
	ctx context.Context,
	req *pb.CreateObservationsRequest,
) (*pb.CreateObservationsResponse, error) {
	l := log.With().Str("method", "CreateObservations").Logger()

	querier := db.New(GetTxFromContext(ctx))

	// Get the location and source
	locationUuid, err := uuid.Parse(req.LocationUuid)
	if err != nil {
		l.Err(err).Msgf("uuid.Parse(%s)", req.LocationUuid)
		return nil, status.Errorf(codes.InvalidArgument, "Invalid location UUID: %v", err)
	}

	params := db.GetLocationSourceAtTimestampParams{
		LocationUuid:   locationUuid,
		SourceTypeID:   int16(req.EnergySource.Number()),
		AtTimestampUtc: pgtype.Timestamp{Time: req.Values[0].TimestampUtc.AsTime(), Valid: true},
	}

	dbSource, err := querier.GetLocationSourceAtTimestamp(ctx, params)
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
			ValueSip:     int16(v.ValueFraction * 30000.0),
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

	return &pb.CreateObservationsResponse{}, nil
}

func (s *DataPlatformDataServiceServerImpl) CreateObserver(
	ctx context.Context,
	req *pb.CreateObserverRequest,
) (*pb.CreateObserverResponse, error) {
	l := log.With().Str("method", "CreateObserver").Logger()

	querier := db.New(GetTxFromContext(ctx))

	obParams := db.CreateObserverParams{ObserverName: req.Name}

	dbObserver, err := querier.CreateObserver(ctx, obParams)
	if err != nil {
		l.Err(err).Msgf("querier.CreateObserver(%+v)", obParams)

		return nil, status.Error(
			codes.InvalidArgument,
			"Invalid observer name. Ensure it is not empty and is lowercase",
		)
	}

	return &pb.CreateObserverResponse{
		ObserverId:   dbObserver.ObserverID,
		ObserverName: dbObserver.ObserverName,
	}, nil
}

func (s *DataPlatformDataServiceServerImpl) GetForecastAtTimestamp(
	ctx context.Context,
	req *pb.GetForecastAtTimestampRequest,
) (*pb.GetForecastAtTimestampResponse, error) {
	l := log.With().Str("method", "GetForecastAtTimestamp").Logger()

	querier := db.New(GetTxFromContext(ctx))

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

	lsParams := db.ListSourcesAtTimestampParams{
		SourceTypeName: req.EnergySource.String(),
		LocationUuids:  locationUuids,
		AtTimestampUtc: pgtype.Timestamp{Time: req.TimestampUtc.AsTime(), Valid: true},
	}

	dbSources, err := querier.ListSourcesAtTimestamp(ctx, lsParams)
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
				ValueFraction: float32(dbCrossSection[idx].P50Sip) / 30000.0,
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

func (s *DataPlatformDataServiceServerImpl) GetLocation(
	ctx context.Context,
	req *pb.GetLocationRequest,
) (*pb.GetLocationResponse, error) {
	l := log.With().Str("method", "GetLocation").Logger()

	querier := db.New(GetTxFromContext(ctx))

	// Get the location and source
	locationUuid, err := uuid.Parse(req.LocationUuid)
	if err != nil {
		l.Err(err).Msgf("uuid.Parse(%s)", req.LocationUuid)
		return nil, status.Errorf(codes.InvalidArgument, "Invalid location UUID: %v", err)
	}

	params := db.GetLocationSourceAtTimestampParams{
		LocationUuid:   locationUuid,
		SourceTypeID:   int16(req.EnergySource.Number()),
		AtTimestampUtc: pgtype.Timestamp{Time: time.Now().UTC(), Valid: true},
	}

	dbSource, err := querier.GetLocationSourceAtTimestamp(ctx, params)
	if err != nil {
		l.Err(err).Msgf("querier.GetLocationSourceAtTimestamp(%+v)", params)

		return nil, status.Errorf(
			codes.NotFound,
			"No location source found for name '%s' with source type '%s'. Ensure the location has an associated source and it is not decommissioned.",
			req.LocationUuid,
			req.EnergySource,
		)
	}

	metadata, err := jsonbToStruct(dbSource.MetadataJsonb)
	if err != nil {
		l.Err(err).Msgf("jsonToStruct(%s)", dbSource.MetadataJsonb)

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
		EffectiveCapacityWatts: uint64(
			dbSource.Capacity,
		) * uint64(
			math.Pow10(int(dbSource.CapacityUnitPrefixFactor)),
		),
		Metadata: metadata,
	}, nil
}

func (s *DataPlatformDataServiceServerImpl) CreateLocation(
	ctx context.Context,
	req *pb.CreateLocationRequest,
) (*pb.CreateLocationResponse, error) {
	l := log.With().Str("method", "CreateLocation").Logger()

	querier := db.New(GetTxFromContext(ctx))

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

	l.Debug().
		Msgf("Created location with UUID '%s' and name '%s'", dbLocation.LocationUuid, dbLocation.LocationName)

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

	cp, ex, err := capacityToValueMultiplier(req.EffectiveCapacityWatts)
	if err != nil {
		l.Err(err).Msgf("capacityMwToValueMultiplier(%d)", req.EffectiveCapacityWatts)

		return nil, status.Error(
			codes.InvalidArgument,
			"Invalid capacity. Ensure capacity is non-negative.",
		)
	}

	validFrom := time.Now().UTC()
	if req.ValidFromUtc != nil {
		validFrom = req.ValidFromUtc.AsTime()
	}

	csParams := db.CreateLocationSourceEntryParams{
		LocationUuid:             dbLocation.LocationUuid,
		SourceTypeID:             dbSourceType.SourceTypeID,
		Capacity:                 cp,
		CapacityUnitPrefixFactor: ex,
		Metadata:                 metadata,
		ValidFromUtc:             pgtype.Timestamp{Time: validFrom, Valid: true},
	}

	dbSource, err := querier.CreateLocationSourceEntry(ctx, csParams)
	if err != nil {
		l.Err(err).Msgf("querier.CreateLocationSourceEntry(%+v)", csParams)

		return nil, status.Error(
			codes.InvalidArgument,
			"Invalid location. Ensure metadata is NULL or a non-empty JSON object.",
		)
	}

	l.Debug().
		Msgf("Created source for location UUID '%s' with source type '%s'", dbLocation.LocationUuid, dbSourceType.SourceTypeName)

	err = querier.RefreshSourcesMaterializedView(ctx)
	if err != nil {
		l.Err(err).Msg("querier.RefreshSourcesMaterializedView()")
		return nil, status.Error(codes.Internal, "Failed to update sources materialised view")
	}

	l.Debug().Msg("Refreshed sources materialised view")

	return &pb.CreateLocationResponse{
		LocationUuid: dbLocation.LocationUuid.String(),
		LocationName: strings.ToUpper(dbLocation.LocationName),
		EffectiveCapacityWatts: uint64(
			dbSource.Capacity,
		) * uint64(
			math.Pow10(int(dbSource.CapacityUnitPrefixFactor)),
		),
	}, nil
}

func (s *DataPlatformDataServiceServerImpl) GetLocationsAsGeoJSON(
	ctx context.Context,
	req *pb.GetLocationsAsGeoJSONRequest,
) (resp *pb.GetLocationsAsGeoJSONResponse, err error) {
	l := log.With().Str("method", "GetLocationsAsGeoJSON").Logger()

	querier := db.New(GetTxFromContext(ctx))

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

	return &pb.GetLocationsAsGeoJSONResponse{Geojson: string(geojson)}, nil
}

func (s *DataPlatformDataServiceServerImpl) GetForecastAsTimeseries(
	ctx context.Context,
	req *pb.GetForecastAsTimeseriesRequest,
) (*pb.GetForecastAsTimeseriesResponse, error) {
	l := log.With().Str("method", "GetForecastAsTimeseries").Logger()

	querier := db.New(GetTxFromContext(ctx))

	// Get the location and source
	gsParams := db.GetLocationSourceAtTimestampParams{
		LocationUuid: uuid.MustParse(req.LocationUuid),
		SourceTypeID: int16(req.EnergySource.Number()),
		AtTimestampUtc: pgtype.Timestamp{
			Time:  req.TimeWindow.StartTimestampUtc.AsTime(),
			Valid: true,
		},
	}

	dbSource, err := querier.GetLocationSourceAtTimestamp(ctx, gsParams)
	if err != nil {
		l.Err(err).Msgf("querier.GetLocationSourceAtTimestamp(%+v)", gsParams)

		return nil, status.Errorf(
			codes.NotFound, "No location found for uuid '%s' with source type '%s'.",
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
			p10 = float32(*value.P10Sip) / 30000.0
		}

		var p90 float32
		if value.P90Sip == nil {
			p90 = float32(math.NaN())
		} else {
			p90 = float32(*value.P90Sip) / 30000.0
		}

		values[i] = &pb.GetForecastAsTimeseriesResponse_Value{
			TargetTimestampUtc:     timestamppb.New(value.TargetTimeUtc.Time),
			P50ValueFraction: float32(value.P50Sip) / 30000.0,
			P10ValueFraction: p10,
			P90ValueFraction: p90,
			EffectiveCapacityWatts: uint64(
				dbSource.Capacity,
			) * uint64(
				math.Pow10(int(dbSource.CapacityUnitPrefixFactor)),
			), // TODO: Capacity over time
			InitializationTimestampUtc: timestamppb.New(value.InitTimeUtc.Time),
			CreatedTimestampUtc:        timestamppb.New(value.CreatedAtUtc.Time),
		}
	}

	return &pb.GetForecastAsTimeseriesResponse{
		LocationUuid: dbSource.LocationUuid.String(),
		LocationName: strings.ToUpper(dbSource.LocationName),
		Values:       values,
	}, nil
}

// Compile-time check to ensure the interface is implemented fully.
var _ pb.DataPlatformDataServiceServer = (*DataPlatformDataServiceServerImpl)(nil)
