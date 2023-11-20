package repository

import (
	"math"
	"math/rand"
	"time"

	"github.com/openclimatefix/api/src/internal"
)

type FakeYield struct {
	Yield float64
	ErrLow float64
	ErrHigh float64
}

func BasicYieldFunc(timeUnix int64, scaleFactor float64) FakeYield {
	ti := time.Unix(timeUnix, 0)
	hour := (float64(ti.Day()) * 24.0) + float64(ti.Hour()) + (float64(ti.Minute()) / 60.0)

	// scaleX makes the period of the function 24 hours
	scaleX := math.Pi / 12.0
	// translateX moves the minimum of the function to 0 hours
	translateX := -math.Pi / 2.0
	// translateY modulates the function based on the month.
	// The function ranges between -0.5 and + 0.5, with 0.5 at the summer solstice
	// and -0.5 at the winter solstice
	month := float64(ti.Month())
	translateY := math.Sin(((math.Pi/6)*month)+translateX) / 2.0

	// baseFunc ranges between -1 and +1, peaking at 12 hours, with a period of 24 hours
	// TranslateY changes the min and max to range between 1.5 and -1.5 depending on
	// the month
	baseFunc := math.Sin((scaleX*hour)+translateX) + translateY
	// Remove negative values
	baseFunc = math.Max(baseFunc, 0.0)
	// Steepen the curve slightly. The divisor is based on the max value
	baseFunc = math.Pow(baseFunc, 4.0) / math.Pow(1.5, 4.0)

	// Instead of completely random noise, apply based on the following process:
	// * Create a base function that is the product of short and long wavelength sines
	// * The resultant funtion modulates with very small amplitude around 1
	noise := (math.Sin(math.Pi*hour)/20.0)*(math.Sin(hour/3.0)) + 1.0
	noise = noise * (rand.Float64()/20.0 + 0.97)

	outputVal := baseFunc * noise * scaleFactor

	errLow, errHigh := 0.0, 0.0
	if outputVal > 0.0 {
		errLow = outputVal - float64(rand.Int63n(int64(outputVal/10.0)))
		errHigh = outputVal + float64(rand.Int63n(int64(outputVal/10.0)))
	}

	return FakeYield{
		Yield:   outputVal,
		ErrLow:  errLow,
		ErrHigh: errHigh,
	}
}

type DummyClient struct{}

// GetActualYieldForLocations implements main.DatabaseService.
func (*DummyClient) GetActualYieldForLocations(locIDs []string, timeUnix int32) ([]internal.DBActualLocalisedYield, error) {
	yields := make([]internal.DBActualLocalisedYield, len(locIDs))
	for i, id := range locIDs {
		yields[i] = internal.DBActualLocalisedYield{
			LocationID: id,
			YieldKW:    int64(BasicYieldFunc(int64(timeUnix), 10000.0).Yield),
		}
	}
	return yields, nil
}

// GetActualYieldsForLocation implements main.DatabaseService.
func (*DummyClient) GetActualYieldsForLocation(locID string) ([]internal.DBActualYield, error) {
	windowStart := time.Now().Add(-time.Hour * 48).Truncate(time.Hour * 24)
	windowEnd := time.Now().Add(time.Hour * 48).Truncate(time.Hour * 24)
	numMins := windowEnd.Sub(windowStart).Hours() * 60

	yields := make([]internal.DBActualYield, int(numMins))
	for i := range yields {
		ti := windowStart.Add(((time.Hour * 60) + time.Minute) * time.Duration(i))
		yields[i] = internal.DBActualYield{
			TimeUnix: int32(ti.Unix()),
			YieldKW:  int64(BasicYieldFunc(ti.Unix(), 10000.0).Yield),
		}
	}

	return yields, nil

}

// GetPredictedYieldForLocations implements main.DatabaseService.
func (*DummyClient) GetPredictedYieldForLocations(locIDs []string, timeUnix int32) ([]internal.DBPredictedLocalisedYield, error) {
	yields := make([]internal.DBPredictedLocalisedYield, len(locIDs))
	for i, id := range locIDs {
		yield := BasicYieldFunc(int64(timeUnix), 10000.0)
		yields[i] = internal.DBPredictedLocalisedYield{
			LocationID: id,
			YieldKW:    int64(yield.Yield),
			ErrLow:     int64(yield.ErrLow),
			ErrHigh:    int64(yield.ErrHigh),
		}
	}

	return yields, nil
}

func (*DummyClient) GetPredictedYieldsForLocation(locID string) ([]internal.DBPredictedYield, error) {
	windowStart := time.Now().Add(-time.Hour * 48).Truncate(time.Hour * 24)
	windowEnd := time.Now().Add(time.Hour * 48).Truncate(time.Hour * 24)
	numHours := windowEnd.Sub(windowStart).Hours()

	yields := make([]internal.DBPredictedYield, int(numHours))
	for i := range yields {
		ti := windowStart.Add(((time.Hour * 60) + time.Minute) * time.Duration(i))
		yield := BasicYieldFunc(ti.Unix(), 10000.0)
		yields[i] = internal.DBPredictedYield{
			TimeUnix: int32(windowStart.Add(time.Hour * time.Duration(i)).Unix()),
			YieldKW:  int64(yield.Yield),
			ErrLow:   int64(yield.ErrLow),
			ErrHigh:  int64(yield.ErrHigh),
		}
	}

	return yields, nil
}

var _ internal.DatabaseService = &DummyClient{}
