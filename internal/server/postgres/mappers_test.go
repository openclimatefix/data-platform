package postgres

import (
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/openclimatefix/data-platform/internal/gen/ocf/dp"
	db "github.com/openclimatefix/data-platform/internal/server/postgres/gen"
)

func Test_MapSlice(t *testing.T) {
	tests := []struct {
		name     string
		input    []int
		expected []string
	}{
		{
			name:     "nil input returns nil",
			input:    nil,
			expected: nil,
		},
		{
			name:     "empty slice returns empty slice",
			input:    []int{},
			expected: []string{},
		},
		{
			name:     "populated slice maps correctly",
			input:    []int{1, 2, 3},
			expected: []string{"1", "2", "3"},
		},
	}

	mapper := func(i int) string { return strconv.Itoa(i) }

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res := MapSlice(tt.input, mapper)
			require.Equal(t, tt.expected, res)
		})
	}
}

func Test_timeptrToPgTimestamp(t *testing.T) {
	now := time.Now().UTC()
	tests := []struct {
		name           string
		input          *timestamppb.Timestamp
		validateResult func(*testing.T, pgtype.Timestamp)
	}{
		{
			name:  "nil input returns truncated current time",
			input: nil,
			validateResult: func(t *testing.T, res pgtype.Timestamp) {
				require.True(t, res.Valid)
				// It should be within a couple of seconds of Now().Truncate(time.Minute)
				expected := time.Now().UTC().Truncate(time.Minute)
				require.WithinDuration(t, expected, res.Time, 2*time.Second)
			},
		},
		{
			name:  "valid input maps exactly",
			input: timestamppb.New(now),
			validateResult: func(t *testing.T, res pgtype.Timestamp) {
				require.True(t, res.Valid)
				require.Equal(t, now, res.Time)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res := timeptrToPgTimestamp(tt.input)
			tt.validateResult(t, res)
		})
	}
}

func Test_sipToFraction(t *testing.T) {
	tests := []struct {
		name     string
		input    int16
		expected float32
	}{
		{
			name:     "zero",
			input:    0,
			expected: 0.0,
		},
		{
			name:     "max positive",
			input:    30000,
			expected: 1.0,
		},
		{
			name:     "max negative",
			input:    -30000,
			expected: -1.0,
		},
		{
			name:     "half",
			input:    15000,
			expected: 0.5,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res := sipToFraction(tt.input)
			require.InDelta(t, tt.expected, res, 0.0001)
		})
	}
}

func Test_buildOtherStatsMap(t *testing.T) {
	v10 := int16(3000)
	v90 := int16(27000)
	v25 := int16(7500)

	tests := []struct {
		name     string
		p02      *int16
		p10      *int16
		p25      *int16
		p75      *int16
		p90      *int16
		p98      *int16
		expected map[string]float32
	}{
		{
			name:     "all nil returns empty map",
			expected: map[string]float32{},
		},
		{
			name: "partially populated",
			p10:  &v10,
			p90:  &v90,
			expected: map[string]float32{
				"p10": 0.1,
				"p90": 0.9,
			},
		},
		{
			name: "fully populated",
			p02:  &v10, // Just using v10 for convenience
			p10:  &v10,
			p25:  &v25,
			p75:  &v25,
			p90:  &v90,
			p98:  &v90,
			expected: map[string]float32{
				"p02": 0.1,
				"p10": 0.1,
				"p25": 0.25,
				"p75": 0.25,
				"p90": 0.9,
				"p98": 0.9,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res := buildOtherStatsMap(tt.p02, tt.p10, tt.p25, tt.p75, tt.p90, tt.p98)
			require.Equal(t, tt.expected, res)
		})
	}
}

func Test_timeWindowToPgWindow(t *testing.T) {
	now := time.Now().UTC()
	startTs := timestamppb.New(now.Add(-2 * time.Hour))
	endTs := timestamppb.New(now.Add(2 * time.Hour))

	tests := []struct {
		name           string
		input          *pb.TimeWindow
		validateResult func(*testing.T, pgtype.Timestamp, pgtype.Timestamp)
	}{
		{
			name:  "nil window applies defaults",
			input: nil,
			validateResult: func(t *testing.T, start pgtype.Timestamp, end pgtype.Timestamp) {
				require.True(t, start.Valid)
				require.True(t, end.Valid)
				require.WithinDuration(t, now.Add(-48*time.Hour), start.Time, 5*time.Second)
				require.WithinDuration(t, now.Add(36*time.Hour), end.Time, 5*time.Second)
			},
		},
		{
			name: "start timestamp nil applies defaults",
			input: &pb.TimeWindow{
				EndTimestampUtc: endTs,
			}, // Assuming protovalidate let this slip somehow
			validateResult: func(t *testing.T, start pgtype.Timestamp, end pgtype.Timestamp) {
				require.True(t, start.Valid)
				require.True(t, end.Valid)
				require.WithinDuration(t, now.Add(-48*time.Hour), start.Time, 5*time.Second)
				require.WithinDuration(t, now.Add(36*time.Hour), end.Time, 5*time.Second)
			},
		},
		{
			name: "perfectly populated window",
			input: &pb.TimeWindow{
				StartTimestampUtc: startTs,
				EndTimestampUtc:   endTs,
			},
			validateResult: func(t *testing.T, start pgtype.Timestamp, end pgtype.Timestamp) {
				require.True(t, start.Valid)
				require.True(t, end.Valid)
				require.Equal(t, startTs.AsTime(), start.Time)
				require.Equal(t, endTs.AsTime(), end.Time)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			start, end := timeWindowToPgWindow(tt.input)
			tt.validateResult(t, start, end)
		})
	}
}

func Test_mapLocationSummary(t *testing.T) {
	id := uuid.New()
	meta, _ := structpb.NewStruct(map[string]interface{}{"key": "value"})

	tests := []struct {
		name           string
		metadata       *structpb.Struct
		validateResult func(*testing.T, *pb.ListLocationsResponse_LocationSummary)
	}{
		{
			name:     "valid metadata",
			metadata: meta,
			validateResult: func(t *testing.T, res *pb.ListLocationsResponse_LocationSummary) {
				require.NotNil(t, res.Metadata)
				require.Equal(t, "value", res.Metadata.Fields["key"].GetStringValue())
			},
		},
		{
			name:     "nil metadata",
			metadata: nil,
			validateResult: func(t *testing.T, res *pb.ListLocationsResponse_LocationSummary) {
				require.Nil(t, res.Metadata)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res := mapLocationSummary(
				id,
				"Test Location",
				51.5,
				-0.1,
				1000,
				1,
				1,
				tt.metadata,
			)

			require.Equal(t, id.String(), res.LocationUuid)
			require.Equal(t, "Test Location", res.LocationName)
			require.Equal(t, float32(51.5), res.Latlng.Latitude)
			require.Equal(t, float32(-0.1), res.Latlng.Longitude)
			require.Equal(t, uint64(1000), res.EffectiveCapacityWatts)
			require.Equal(t, pb.EnergySource(1), res.EnergySource)
			require.Equal(t, pb.LocationType(1), res.LocationType)

			tt.validateResult(t, res)
		})
	}
}

func Test_mapStreamedForecastDatum(t *testing.T) {
	id := uuid.New()
	meta, _ := structpb.NewStruct(map[string]interface{}{"key": "value"})

	baseRow := db.ListPredictionsForForecastsRow{
		ForecasterName:    "test",
		ForecasterVersion: "1.0",
		HorizonMins:       60,
		P50Sip:            15000,
		CapacityWatts:     1000,
		InitTimeUtc:       pgtype.Timestamp{Time: time.Now().UTC(), Valid: true},
		CreatedAtUtc:      pgtype.Timestamp{Time: time.Now().UTC(), Valid: true},
		Metadata:          meta,
	}

	tests := []struct {
		name            string
		includeMetadata bool
		row             db.ListPredictionsForForecastsRow
		validateResult  func(*testing.T, *pb.ForecastDatum)
	}{
		{
			name:            "includeMetadata false, DB has metadata",
			includeMetadata: false,
			row:             baseRow,
			validateResult: func(t *testing.T, res *pb.ForecastDatum) {
				require.NotNil(t, res.Metadata)
				require.Empty(t, res.Metadata)
			},
		},
		{
			name:            "includeMetadata true, DB metadata is nil",
			includeMetadata: true,
			row: func() db.ListPredictionsForForecastsRow {
				r := baseRow
				r.Metadata = nil
				return r
			}(),
			validateResult: func(t *testing.T, res *pb.ForecastDatum) {
				require.NotNil(t, res.Metadata)
				require.Empty(t, res.Metadata)
			},
		},
		{
			name:            "includeMetadata true, DB has metadata",
			includeMetadata: true,
			row:             baseRow,
			validateResult: func(t *testing.T, res *pb.ForecastDatum) {
				require.NotNil(t, res.Metadata)
				require.Len(t, res.Metadata, 1)
				require.Equal(t, "value", res.Metadata["key"])
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res := mapStreamedForecastDatum(tt.row, id, tt.includeMetadata)

			require.Equal(t, id.String(), res.LocationUuid)
			require.Equal(t, "test:1.0", res.ForecasterFullname)
			require.Equal(t, uint32(60), res.HorizonMins)
			require.InDelta(t, 0.5, res.P50Fraction, 0.0001)

			tt.validateResult(t, res)
		})
	}
}

func Test_mapForecastAsTimeseriesFromLocationValue(t *testing.T) {
	p50 := int16(15000)
	p10 := int16(3000)
	p90 := int16(27000)

	initTime := time.Now().UTC()
	targetTime := initTime.Add(time.Hour)

	baseRow := db.ListPredictionsForLocationRow{
		P50Sip:        p50,
		CapacityWatts: 10000,
		TargetTimeUtc: pgtype.Timestamp{Time: targetTime, Valid: true},
		InitTimeUtc:   pgtype.Timestamp{Time: initTime, Valid: true},
		CreatedAtUtc:  pgtype.Timestamp{Time: initTime, Valid: true},
	}

	tests := []struct {
		name           string
		row            db.ListPredictionsForLocationRow
		validateResult func(*testing.T, *pb.GetForecastAsTimeseriesResponse_Value)
	}{
		{
			name: "sparse row only p50",
			row:  baseRow,
			validateResult: func(t *testing.T, v *pb.GetForecastAsTimeseriesResponse_Value) {
				require.InDelta(t, 0.5, v.P50ValueFraction, 0.0001)
				require.Empty(t, v.OtherStatisticsFractions)
			},
		},
		{
			name: "fully populated row",
			row: func() db.ListPredictionsForLocationRow {
				r := baseRow
				r.P10Sip = &p10
				r.P90Sip = &p90
				return r
			}(),
			validateResult: func(t *testing.T, v *pb.GetForecastAsTimeseriesResponse_Value) {
				require.InDelta(t, 0.5, v.P50ValueFraction, 0.0001)
				require.Len(t, v.OtherStatisticsFractions, 2)
				require.InDelta(t, 0.1, v.OtherStatisticsFractions["p10"], 0.0001)
				require.InDelta(t, 0.9, v.OtherStatisticsFractions["p90"], 0.0001)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res := mapForecastAsTimeseriesFromLocationValue(tt.row)

			require.Equal(t, uint64(10000), res.EffectiveCapacityWatts)
			require.Equal(t, targetTime, res.TargetTimestampUtc.AsTime())
			require.Equal(t, initTime, res.InitializationTimestampUtc.AsTime())

			tt.validateResult(t, res)
		})
	}
}
