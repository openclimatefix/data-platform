package repository

import (
	"math"
	"math/rand"
	"time"

	internal "github.com/openclimatefix/api/src/internal"
)

type DummyClient struct{}

// GetActualYieldForLocations implements main.DatabaseService.
func (*DummyClient) GetActualYieldForLocations(locIDs []string, timeUnix int32) ([]internal.DBActualLocalisedYield, error) {
	yields := make([]internal.DBActualLocalisedYield, len(locIDs))
	for i, id := range locIDs {
		yields[i] = internal.DBActualLocalisedYield{
			LocationID: id,
			YieldKW:    rand.Int63n(10000),
		}
	}
	return yields, nil
}

// GetActualYieldsForLocation implements main.DatabaseService.
func (*DummyClient) GetActualYieldsForLocation(locID string) ([]internal.DBActualYield, error) {
	windowStart := time.Now().Add(-time.Hour * 48).Truncate(time.Hour * 24)
	windowEnd := time.Now().Add(time.Hour * 48).Truncate(time.Hour * 24)
	numHours := windowEnd.Sub(windowStart).Hours()

	yields := make([]internal.DBActualYield, int(numHours))
	for i, _ := range yields {
		yields[i] = internal.DBActualYield{
			TimeUnix: int32(windowStart.Add(time.Hour * time.Duration(i)).Unix()),
			YieldKW:  int64(10000 * math.Cos(float64(i)/24.0*math.Pi*2.0)),
		}
	}

	return yields, nil

}

// GetPredictedYieldForLocations implements main.DatabaseService.
func (*DummyClient) GetPredictedYieldForLocations(locIDs []string, timeUnix int32) ([]internal.DBPredictedLocalisedYield, error) {
	yields := make([]internal.DBPredictedLocalisedYield, len(locIDs))
	for i, id := range locIDs {
		yield := rand.Int63n(10000)
		yields[i] = internal.DBPredictedLocalisedYield{
			LocationID: id,
			YieldKW:    yield,
			ErrLow:     yield - rand.Int63n(1000),
			ErrHigh:    yield + rand.Int63n(1000),
		}
	}

	return yields, nil
}

// GetPredictedYieldsForLocation implements main.DatabaseService.
func (*DummyClient) GetPredictedYieldsForLocation(locID string) ([]internal.DBPredictedYield, error) {
	windowStart := time.Now().Add(-time.Hour * 48).Truncate(time.Hour * 24)
	windowEnd := time.Now().Add(time.Hour * 48).Truncate(time.Hour * 24)
	numHours := windowEnd.Sub(windowStart).Hours()

	yields := make([]internal.DBPredictedYield, int(numHours))
	for i, _ := range yields {
		yield := int64(10000 * math.Cos(float64(i)/24.0*math.Pi*2.0))
		yields[i] = internal.DBPredictedYield{
			TimeUnix: int32(windowStart.Add(time.Hour * time.Duration(i)).Unix()),
			YieldKW:  yield,
			ErrLow:   yield - rand.Int63n(1000),
			ErrHigh:  yield + rand.Int63n(1000),
		}
	}

	return yields, nil
}

var _ internal.DatabaseService = &DummyClient{}
