package dummy

import (
	"math"
	"math/rand"
	"time"

	"github.com/devsjc/fcfs/api/src/internal"
)

// step defines the time step of the timeseries data
const step = time.Duration(5 * time.Minute)

type FakeYield struct {
	Yield   float64
	ErrLow  float64
	ErrHigh float64
}

// fx defines a type alias for a real mathematical function
type fx func(float64) float64

// / getWindow returns the start and end of the window for the timeseries data.
func getWindow() (time.Time, time.Time) {
	windowStart := time.Now().UTC().Add(-time.Hour * 48).Truncate(time.Hour * 24)
	windowEnd := time.Now().UTC().Add(time.Hour * 48).Truncate(time.Hour * 24)
	return windowStart, windowEnd
}

// basicYieldFunc returns a fake yield value for a given time and scale factor.
// The scale factor is used to scale the output value. A scale factor of 10000
// will result in a yield of 10kW at the peak of the curve.
// The output value is a function of the time of day and the time of year.
// The base sin function has a period of 24 hours, and peaks at 12 hours.
func basicYieldFunc(timeUnix int64, scaleFactor float64) FakeYield {

	// Convert the time to a time.Time object
	ti := time.Unix(timeUnix, 0)
	// Since the function's x values are hours, convert the time to hours, with
	// minutes being a fraction of an hour
	hour := (float64(ti.Day()) * 24.0) + float64(ti.Hour()) + (float64(ti.Minute()) / 60.0)

	// Create orbital intensity function with period 24, min/max at 0,12
	timeOfDayFunc := -1 * math.Cos(hour*math.Pi/12)
	// seasonalShift modulates the orbital intensity based on the month
	// with min/max of -0.5 and + 0.5 at the winter/summer solstices
	seasonalShift := -1 * math.Cos((math.Pi/6)*float64(ti.Month())) / 2.0

	// solarOrbitFunc ranges between -1 and +1, peaking at 12 hours, with a period of 24 hours
	// TranslateY changes the min and max to range between 1.5 and -1.5 depending on
	// the month
	solarOrbitFunc := timeOfDayFunc + seasonalShift
	// Remove negative values
	solarOrbitFunc = math.Max(solarOrbitFunc, 0.0)
	// Steepen the curve slightly. The divisor is based on the max value
	solarOrbitFunc = math.Pow(solarOrbitFunc, 4.0) / math.Pow(1.5, 4.0)

	// Instead of completely random noise, apply based on the following process:
	// * Create a base function that is the product of short and long wavelength sines
	// * The resultant function modulates with very small amplitude around 1
	noise := (math.Sin(math.Pi*hour)/20.0)*(math.Sin(hour/3.0)) + 1.0
	noise = noise * (rand.Float64()/20.0 + 0.97)

	// Create the output value from the base function, noise, and scale factor
	outputVal := solarOrbitFunc * noise * scaleFactor

	// Add some random error
	// * Error is not added if the output value is 0
	errLow, errHigh := 0.0, 0.0
	if outputVal > 0.0 {
		errLow = outputVal - (rand.Float64() * outputVal / 10.0)
		errHigh = outputVal + (rand.Float64() * outputVal / 10.0)
	}

	return FakeYield{
		Yield:   outputVal,
		ErrLow:  errLow,
		ErrHigh: errHigh,
	}
}

type DummyClient struct{}

func NewDummyClient() *DummyClient {
	return &DummyClient{}
}

func (*DummyClient) Migrate() error {
	// No-op
	return nil
}

// GetActualYieldForLocations implements main.DatabaseService.
func (*DummyClient) GetActualYieldForLocations(locIDs []string, timeUnix int64) ([]internal.DBActualLocalisedYield, error) {
	yields := make([]internal.DBActualLocalisedYield, len(locIDs))
	for i, id := range locIDs {
		yields[i] = internal.DBActualLocalisedYield{
			LocationID: id,
			YieldKW:    int(basicYieldFunc(int64(timeUnix), 10000.0).Yield),
		}
	}
	return yields, nil
}

// GetActualYieldsForLocation implements main.DatabaseService.
func (*DummyClient) GetActualYieldsForLocation(locID string) ([]internal.DBActualYield, error) {
	windowStart, windowEnd := getWindow()
	numSteps := int(math.Floor(float64(windowEnd.Sub(windowStart) / step)))

	yields := make([]internal.DBActualYield, numSteps)
	for i := range yields {
		// Note that this is not a mathematical multiplication of two durations,
		// but rather a conversion of the integer i to a duration type in order
		// for Go to allow it to be multiplied by step, which is a duration type.
		ti := windowStart.Add(time.Duration(i) * step)
		yields[i] = internal.DBActualYield{
			TimeUnix: ti.Unix(),
			YieldKW:  int(basicYieldFunc(ti.Unix(), 10000.0).Yield),
		}
	}

	return yields, nil

}

// GetPredictedYieldForLocations implements main.DatabaseService.
func (*DummyClient) GetPredictedYieldForLocations(locIDs []string, timeUnix int64) ([]internal.DBPredictedLocalisedYield, error) {
	yields := make([]internal.DBPredictedLocalisedYield, len(locIDs))
	for i, id := range locIDs {
		yield := basicYieldFunc(int64(timeUnix), 10000.0)
		yields[i] = internal.DBPredictedLocalisedYield{
			LocationID: id,
			YieldKW:    int(yield.Yield),
			ErrLow:     int(yield.ErrLow),
			ErrHigh:    int(yield.ErrHigh),
		}
	}

	return yields, nil
}

func (*DummyClient) GetPredictedYieldsForLocation(locID string) ([]internal.DBPredictedYield, error) {
	windowStart, windowEnd := getWindow()
	numSteps := int(math.Floor(float64(windowEnd.Sub(windowStart) / step)))

	yields := make([]internal.DBPredictedYield, int(numSteps))
	for i := range yields {
		// Note that this is not a mathematical multiplication of two durations,
		// but rather a conversion of the integer i to a duration type in order
		// for Go to allow it to be multiplied by step, which is a duration type.
		ti := windowStart.Add(time.Duration(i) * step)
		yield := basicYieldFunc(ti.Unix(), 10000.0)
		yields[i] = internal.DBPredictedYield{
			TimeUnix: ti.Unix(),
			YieldKW:  int(yield.Yield),
			ErrLow:   int(yield.ErrLow),
			ErrHigh:  int(yield.ErrHigh),
		}
	}

	return yields, nil
}

// Compile check to ensure that the DummyClient implements the DatabaseService interface.
var _ internal.DatabaseRepository = &DummyClient{}
