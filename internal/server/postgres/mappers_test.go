package postgres

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	pb "github.com/openclimatefix/data-platform/internal/gen/ocf/dp"
	db "github.com/openclimatefix/data-platform/internal/server/postgres/gen"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func Test_sipToFraction(t *testing.T) {
	assert.InDelta(t, 0.5, sipToFraction(15000), 0.0001)
	assert.InDelta(t, 1.0, sipToFraction(30000), 0.0001)
	assert.InDelta(t, 0.0, sipToFraction(0), 0.0001)
	assert.InDelta(t, -0.5, sipToFraction(-15000), 0.0001)
}

func Test_buildOtherStatsMap(t *testing.T) {
	v10 := int16(3000)
	v90 := int16(27000)

	// Partial array of pointers
	m := buildOtherStatsMap(nil, &v10, nil, nil, &v90, nil)

	assert.Len(t, m, 2)
	assert.InDelta(t, 0.1, m["p10"], 0.0001)
	assert.InDelta(t, 0.9, m["p90"], 0.0001)

	// Empty
	mEmpty := buildOtherStatsMap(nil, nil, nil, nil, nil, nil)
	assert.Len(t, mEmpty, 0)
}

func Test_mapLocationSummary(t *testing.T) {
	id := uuid.New()
	meta, _ := structpb.NewStruct(map[string]interface{}{"key": "value"})

	res := mapLocationSummary(
		id,
		"Test Location",
		51.5,
		-0.1,
		1000,
		1,
		1,
		meta,
	)

	assert.Equal(t, id.String(), res.LocationUuid)
	assert.Equal(t, "Test Location", res.LocationName)
	assert.Equal(t, float32(51.5), res.Latlng.Latitude)
	assert.Equal(t, float32(-0.1), res.Latlng.Longitude)
	assert.Equal(t, uint64(1000), res.EffectiveCapacityWatts)
	assert.Equal(t, pb.EnergySource(1), res.EnergySource)
	assert.Equal(t, pb.LocationType(1), res.LocationType)
	assert.Equal(t, "value", res.Metadata.Fields["key"].GetStringValue())
}

func Test_timeWindowToPgWindow(t *testing.T) {
	now := time.Now().UTC()
	startTs := timestamppb.New(now.Add(-2 * time.Hour))
	endTs := timestamppb.New(now.Add(2 * time.Hour))

	window := &pb.TimeWindow{
		StartTimestampUtc: startTs,
		EndTimestampUtc:   endTs,
	}

	start, end, err := timeWindowToPgWindow(window)
	assert.NoError(t, err)
	assert.True(t, start.Valid)
	assert.True(t, end.Valid)
	assert.Equal(t, startTs.AsTime(), start.Time)
	assert.Equal(t, endTs.AsTime(), end.Time)

	// Nil window
	start, end, err = timeWindowToPgWindow(nil)
	assert.NoError(t, err)
	assert.True(t, start.Valid)
	assert.True(t, end.Valid)
	// Should be -48h and +36h roughly
	assert.WithinDuration(t, now.Add(-48*time.Hour), start.Time, 5*time.Second)
	assert.WithinDuration(t, now.Add(36*time.Hour), end.Time, 5*time.Second)

	// Invalid window
	invalidWindow := &pb.TimeWindow{
		StartTimestampUtc: startTs,
	}
	_, _, err = timeWindowToPgWindow(invalidWindow)
	assert.Error(t, err)
}

func Test_mapForecastAsTimeseriesFromLocation(t *testing.T) {
	p50 := int16(15000)
	p10 := int16(3000)
	p90 := int16(27000)

	initTime := time.Now().UTC()
	targetTime := initTime.Add(time.Hour)

	rows := []db.ListPredictionsForLocationRow{
		{
			P50Sip:        p50,
			P10Sip:        &p10,
			P90Sip:        &p90,
			CapacityWatts: 10000,
			TargetTimeUtc: pgtype.Timestamp{Time: targetTime, Valid: true},
			InitTimeUtc:   pgtype.Timestamp{Time: initTime, Valid: true},
			CreatedAtUtc:  pgtype.Timestamp{Time: initTime, Valid: true},
		},
	}

	res := MapSlice(rows, mapForecastAsTimeseriesFromLocationValue)
	assert.Len(t, res, 1)

	v := res[0]
	assert.InDelta(t, 0.5, v.P50ValueFraction, 0.0001)
	assert.Equal(t, uint64(10000), v.EffectiveCapacityWatts)
	assert.InDelta(t, 0.1, v.OtherStatisticsFractions["p10"], 0.0001)
	assert.InDelta(t, 0.9, v.OtherStatisticsFractions["p90"], 0.0001)
	assert.Equal(t, targetTime, v.TargetTimestampUtc.AsTime())
	assert.Equal(t, initTime, v.InitializationTimestampUtc.AsTime())
}
