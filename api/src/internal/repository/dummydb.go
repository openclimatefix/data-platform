package repository

import (
	"math"
	"math/rand"
	"time"

	"github.com/openclimatefix/api/src/internal"
)

func BasicYieldFunc(timeUnix int64) int64 {
	t := time.Unix(int64(timeUnix), 0)
	hour := float64(t.Hour()) + float64(t.Minute())/60.0

	// scaleX makes the period of the function 24 hours
	scaleX := math.Pi / 12.0
	// translateX moves the minimum of the function to 0 hours
	translateX := -math.Pi / 2.0

	// translateY modulates the function based on the month.
	// The function ranges between -0.5 and + 0.5, with 0.5 at the summer solstice
	// and -0.5 at the winter solstice
	month := float64(t.Month())
	translateY := math.Sin(((math.Pi/6)*month)+translateX) / 2.0

	// baseFunc ranges between -1 and +1, peaking at 12 hours, with a period of 24 hours
	// higher in the summer months and lower in the winter months. TranslateY changes
	// the min and max to range between 1.5 and -1.5
	baseFunc := math.Sin(((scaleX * hour) + translateX) + translateY)

	// Remove negative values
	baseFunc = math.Max(baseFunc, 0)

	// Steepen the curve slightly. The divisor is based on the max value
	baseFunc = math.Pow(baseFunc, 4) / math.Pow(1.5, 4)

	// Instead of applying noise randomly, apply based on the following process:
	// * Create a base function that is the product of short and long wavelength sines
	// * The resultant funtion modulates with very small amplitude around 1
	baseNoise := ((math.Sin(math.Pi*hour) / 20) * (math.Sin(hour / 3))) + 1
	// * Finally, add a small amount of random noise
	noise := baseNoise

	// Scale the function and apply noise
	scaleFactor := 10.0

	outputVal := int64(baseFunc * scaleFactor * noise)

	return outputVal

}

type DummyClient struct{}

// GetActualYieldForLocations implements main.DatabaseService.
func (*DummyClient) GetActualYieldForLocations(locIDs []string, timeUnix int32) ([]internal.DBActualLocalisedYield, error) {
	yields := make([]internal.DBActualLocalisedYield, len(locIDs))
	for i, id := range locIDs {
		yields[i] = internal.DBActualLocalisedYield{
			LocationID: id,
			YieldKW:    BasicYieldFunc(int64(timeUnix)),
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
			YieldKW:  BasicYieldFunc(windowStart.Add(time.Hour * time.Duration(i)).Unix()),
		}
	}

	return yields, nil

}

// GetPredictedYieldForLocations implements main.DatabaseService.
func (*DummyClient) GetPredictedYieldForLocations(locIDs []string, timeUnix int32) ([]internal.DBPredictedLocalisedYield, error) {
	yields := make([]internal.DBPredictedLocalisedYield, len(locIDs))
	for i, id := range locIDs {
		yield := BasicYieldFunc(int64(timeUnix))
		yields[i] = internal.DBPredictedLocalisedYield{
			LocationID: id,
			YieldKW:    yield,
			ErrLow:     yield - rand.Int63n(int64(yield/10)),
			ErrHigh:    yield + rand.Int63n(int64(yield/10)),
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
		yield := BasicYieldFunc(windowStart.Add(time.Hour * time.Duration(i)).Unix())
		yields[i] = internal.DBPredictedYield{
			TimeUnix: int32(windowStart.Add(time.Hour * time.Duration(i)).Unix()),
			YieldKW:  yield,
			ErrLow:   yield - rand.Int63n(int64(yield/10)),
			ErrHigh:  yield + rand.Int63n(int64(yield/10)),
		}
	}

	return yields, nil
}

var _ internal.DatabaseService = &DummyClient{}
