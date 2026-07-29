package postgres

import (
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/openclimatefix/data-platform/internal/gen/ocf/dp"
	db "github.com/openclimatefix/data-platform/internal/server/postgres/gen"
)

// MapSlice transforms a slice of type T into a slice of type U using a mapping function.
func MapSlice[T, U any](input []T, mapper func(T) U) []U {
	if input == nil {
		return nil
	}

	out := make([]U, len(input))
	for i, v := range input {
		out[i] = mapper(v)
	}

	return out
}

// timeWindowToPgWindow converts a TimeWindow protobuf message to a pair of pgtype.Timestamp values.
// If the TimeWindow is nil or its StartTimestampUtc is nil, it defaults to a window from 48 hours ago to 36 hours in the future. Protovalidate ensures at the boundary that the start is always before the end, so we don't need to check that here.
func timeWindowToPgWindow(
	window *pb.TimeWindow,
) (start pgtype.Timestamp, end pgtype.Timestamp) {
	currentTime := time.Now().UTC()
	if window == nil || window.StartTimestampUtc == nil {
		start = pgtype.Timestamp{Time: currentTime.Add(-48 * time.Hour), Valid: true}
		end = pgtype.Timestamp{Time: currentTime.Add(36 * time.Hour), Valid: true}
	} else {
		start = pgtype.Timestamp{Time: window.StartTimestampUtc.AsTime(), Valid: true}
		end = pgtype.Timestamp{Time: window.EndTimestampUtc.AsTime(), Valid: true}
	}

	return start, end
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

// sipToFraction converts a SIP value to a fraction.
func sipToFraction(sip int16) float32 {
	return float32(sip) / 30000.0
}

// buildOtherStatsMap constructs a map of other statistics from optional SIP pointers.
// Only keys that are not nil will be included in the returned map.
func buildOtherStatsMap(p02, p10, p25, p75, p90, p98 *int16) map[string]float32 {
	otherStats := make(map[string]float32)
	if p02 != nil {
		otherStats["p02"] = sipToFraction(*p02)
	}

	if p10 != nil {
		otherStats["p10"] = sipToFraction(*p10)
	}

	if p25 != nil {
		otherStats["p25"] = sipToFraction(*p25)
	}

	if p75 != nil {
		otherStats["p75"] = sipToFraction(*p75)
	}

	if p90 != nil {
		otherStats["p90"] = sipToFraction(*p90)
	}

	if p98 != nil {
		otherStats["p98"] = sipToFraction(*p98)
	}

	return otherStats
}

// mapCreateForecast generates the database parameters for a single forecast from a gRPC request.
func mapCreateForecast(
	req *pb.CreateForecastRequest,
	geometryUuid uuid.UUID,
	sourceTypeId int16,
	forecasterId int32,
) (db.CreateForecastsParams, error) {
	initTime := req.InitTimeUtc.AsTime().Truncate(time.Minute)

	fUuid, err := uuid.NewV7()
	if err != nil {
		return db.CreateForecastsParams{}, fmt.Errorf("failed to generate uuidv7: %w", err)
	}

	// Manually overwrite the 48-bit timestamp with the initTime milliseconds
	ms := uint64(initTime.UnixMilli())
	fUuid[0] = byte(ms >> 40)
	fUuid[1] = byte(ms >> 32)
	fUuid[2] = byte(ms >> 24)
	fUuid[3] = byte(ms >> 16)
	fUuid[4] = byte(ms >> 8)
	fUuid[5] = byte(ms)

	firstHorizon := int32(req.Values[0].HorizonMins)
	lastHorizon := int32(req.Values[len(req.Values)-1].HorizonMins)

	periodStart := initTime.Add(time.Duration(firstHorizon) * time.Minute)
	periodEnd := initTime.Add(time.Duration(lastHorizon) * time.Minute)

	targetPeriod := pgtype.Range[pgtype.Timestamp]{
		Lower:     pgtype.Timestamp{Time: periodStart, Valid: true},
		Upper:     pgtype.Timestamp{Time: periodEnd, Valid: true},
		LowerType: pgtype.Inclusive,
		UpperType: pgtype.Inclusive,
		Valid:     true,
	}

	var createdTime pgtype.Timestamp
	if req.CreatedTimestampUtc != nil {
		createdTime = pgtype.Timestamp{Time: req.CreatedTimestampUtc.AsTime(), Valid: true}
	} else {
		createdTime = pgtype.Timestamp{
			Time:  time.Now().UTC().Truncate(time.Minute),
			Valid: true,
		}
	}

	return db.CreateForecastsParams{
		ForecastUuid:        fUuid,
		GeometryUuid:        geometryUuid,
		SourceTypeID:        sourceTypeId,
		ForecasterID:        forecasterId,
		InitTimeUtc:         pgtype.Timestamp{Time: initTime, Valid: true},
		ValueResolutionMins: int16(req.Values[1].HorizonMins - req.Values[0].HorizonMins),
		TargetPeriod:        targetPeriod,
		Metadata:            req.Metadata,
		CreatedAtUtc:        createdTime,
	}, nil
}

func mapLatestForecast(
	fc db.GetLatestForecastsAtHorizonSincePivotRow,
) *pb.GetLatestForecastsResponse_Forecast {
	return &pb.GetLatestForecastsResponse_Forecast{
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

func mapForecaster(fc db.GetForecastersByFiltersRow) *pb.Forecaster {
	return &pb.Forecaster{
		ForecasterName:    fc.ForecasterName,
		ForecasterVersion: fc.ForecasterVersion,
	}
}

func mapWeekAverageDelta(
	delta db.GetWeekAverageDeltasForLocationsRow,
	capacityWatts int64,
) *pb.GetWeekAverageDeltasResponse_AverageDelta {
	return &pb.GetWeekAverageDeltasResponse_AverageDelta{
		DeltaFraction:          float32(delta.AvgDeltaSip) / 30000.0,
		HorizonMins:            uint32(delta.HorizonMins),
		EffectiveCapacityWatts: uint64(capacityWatts),
	}
}

func mapObservationAsTimeseries(
	obs db.GetObservationsBetweenRow,
) *pb.GetObservationsAsTimeseriesResponse_Value {
	return &pb.GetObservationsAsTimeseriesResponse_Value{
		ValueFraction:          sipToFraction(obs.ValueSip),
		TimestampUtc:           timestamppb.New(obs.ObservationTimestampUtc.Time),
		EffectiveCapacityWatts: uint64(obs.CapacityWatts),
	}
}

func mapLatestObservation(
	obs db.GetLatestObservationsRow,
) *pb.GetLatestObservationsResponse_Observation {
	return &pb.GetLatestObservationsResponse_Observation{
		LocationUuid:           obs.GeometryUuid.String(),
		TimestampUtc:           timestamppb.New(obs.ObservationTimestampUtc.Time),
		ValueFraction:          sipToFraction(obs.ValueSip),
		EffectiveCapacityWatts: uint64(obs.CapacityWatts),
	}
}

func mapObserver(ob db.ObsObserver) *pb.ListObserversResponse_ObserverSummary {
	return &pb.ListObserversResponse_ObserverSummary{
		ObserverUuid: ob.ObserverUuid.String(),
		ObserverName: ob.ObserverName,
	}
}

func mapPredictionAtTime(
	value db.ListPredictionsAtTimeForLocationsRow,
) *pb.GetForecastAtTimestampResponse_Value {
	return &pb.GetForecastAtTimestampResponse_Value{
		ValueFraction:          sipToFraction(value.P50Sip),
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
		OtherStatisticsFractions: buildOtherStatsMap(
			value.P02Sip,
			value.P10Sip,
			value.P25Sip,
			value.P75Sip,
			value.P90Sip,
			value.P98Sip,
		),
	}
}

func mapObservationAtTimestamp(
	obs db.ListObservationsAtTimeForLocationsRow,
) *pb.GetObservationsAtTimestampResponse_Value {
	return &pb.GetObservationsAtTimestampResponse_Value{
		ValueFraction:          sipToFraction(obs.ValueSip),
		EffectiveCapacityWatts: uint64(obs.CapacityWatts),
		LocationUuid:           obs.GeometryUuid.String(),
		Latlng: &pb.LatLng{
			Latitude:  obs.Latitude,
			Longitude: obs.Longitude,
		},
	}
}

func mapLocationSnapshot(
	v db.GetSourceHistoryRow,
) *pb.GetLocationAsTimeseriesResponse_LocationSnapshot {
	return &pb.GetLocationAsTimeseriesResponse_LocationSnapshot{
		EffectiveCapacityWatts: uint64(v.CapacityWatts),
		TimestampUtc:           timestamppb.New(v.ValidFromUtc.Time),
		Metadata:               v.Metadata,
	}
}

func mapForecastAsTimeseriesFromForecastValue(
	pred db.ListPredictionsForForecastsRow,
) *pb.GetForecastAsTimeseriesResponse_Value {
	return &pb.GetForecastAsTimeseriesResponse_Value{
		TargetTimestampUtc: timestamppb.New(
			pred.InitTimeUtc.Time.Add(time.Duration(pred.HorizonMins) * time.Minute),
		),
		P50ValueFraction:           sipToFraction(pred.P50Sip),
		EffectiveCapacityWatts:     uint64(pred.CapacityWatts),
		InitializationTimestampUtc: timestamppb.New(pred.InitTimeUtc.Time),
		CreatedTimestampUtc:        timestamppb.New(pred.CreatedAtUtc.Time),
		OtherStatisticsFractions: buildOtherStatsMap(
			pred.P02Sip,
			pred.P10Sip,
			pred.P25Sip,
			pred.P75Sip,
			pred.P90Sip,
			pred.P98Sip,
		),
		Metadata: pred.Metadata,
	}
}

func mapForecastAsTimeseriesFromLocationValue(
	value db.ListPredictionsForLocationRow,
) *pb.GetForecastAsTimeseriesResponse_Value {
	return &pb.GetForecastAsTimeseriesResponse_Value{
		TargetTimestampUtc: timestamppb.New(value.TargetTimeUtc.Time),
		P50ValueFraction:   sipToFraction(value.P50Sip),
		OtherStatisticsFractions: buildOtherStatsMap(
			value.P02Sip,
			value.P10Sip,
			value.P25Sip,
			value.P75Sip,
			value.P90Sip,
			value.P98Sip,
		),
		EffectiveCapacityWatts:     uint64(value.CapacityWatts),
		InitializationTimestampUtc: timestamppb.New(value.InitTimeUtc.Time),
		CreatedTimestampUtc:        timestamppb.New(value.CreatedAtUtc.Time),
		Metadata:                   value.Metadata,
	}
}

func mapLocationSummary(
	geomUuid uuid.UUID,
	geomName string,
	lat, lon float32,
	cap int64,
	srcType, geomType int16,
	meta *structpb.Struct,
) *pb.ListLocationsResponse_LocationSummary {
	return &pb.ListLocationsResponse_LocationSummary{
		LocationUuid: geomUuid.String(),
		LocationName: geomName,
		Latlng: &pb.LatLng{
			Latitude:  lat,
			Longitude: lon,
		},
		EffectiveCapacityWatts: uint64(cap),
		EnergySource:           pb.EnergySource(srcType),
		LocationType:           pb.LocationType(geomType),
		Metadata:               meta,
	}
}

func mapStreamedForecastDatum(
	row db.ListPredictionsForForecastsRow,
	locUuid uuid.UUID,
	includeMetadata bool,
) *pb.ForecastDatum {
	metadata := make(map[string]string)
	if includeMetadata && row.Metadata != nil {
		for k, v := range row.Metadata.AsMap() {
			metadata[k] = v.(string)
		}
	}

	return &pb.ForecastDatum{
		InitTimestamp: timestamppb.New(row.InitTimeUtc.Time),
		LocationUuid:  locUuid.String(),
		ForecasterFullname: fmt.Sprintf(
			"%s:%s",
			row.ForecasterName,
			row.ForecasterVersion,
		),
		HorizonMins: uint32(row.HorizonMins),
		P50Fraction: sipToFraction(row.P50Sip),
		OtherStatisticsFractions: buildOtherStatsMap(
			row.P02Sip,
			row.P10Sip,
			row.P25Sip,
			row.P75Sip,
			row.P90Sip,
			row.P98Sip,
		),
		CreatedTimestampUtc:    timestamppb.New(row.CreatedAtUtc.Time),
		EffectiveCapacityWatts: uint64(row.CapacityWatts),
		Metadata:               metadata,
	}
}
