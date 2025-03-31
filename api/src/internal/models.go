package internal

// --- Structs and interfaces for connecting to a database service --- //

type DBPredictedYield struct {
	YieldKW  int
	TimeUnix int64
	ErrLow   int
	ErrHigh  int
}

type DBActualYield struct {
	YieldKW  int
	TimeUnix int64
}

type DBPredictedLocalisedYield struct {
	LocationID string
	YieldKW    int
	ErrLow     int
	ErrHigh    int
}

type DBActualLocalisedYield struct {
	LocationID string
	YieldKW    int
}

type DatabaseRepository interface {
	GetPredictedYieldsForLocation(locID string) ([]DBPredictedYield, error)
	GetActualYieldsForLocation(locID string) ([]DBActualYield, error)
	GetPredictedYieldForLocations(locIDs []string, timeUnix int64) ([]DBPredictedLocalisedYield, error)
	GetActualYieldForLocations(locIDs []string, timeUnix int64) ([]DBActualLocalisedYield, error)
}
