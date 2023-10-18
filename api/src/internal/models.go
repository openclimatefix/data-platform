package internal

type DBPredictedYield struct {
	YieldKW  int64
	TimeUnix int32
	ErrLow   int64
	ErrHigh  int64
}

type DBActualYield struct {
	YieldKW  int64
	TimeUnix int32
}

type DBPredictedLocalisedYield struct {
	LocationID string
	YieldKW    int64
	ErrLow     int64
	ErrHigh    int64
}

type DBActualLocalisedYield struct {
	LocationID string
	YieldKW    int64
}

type DatabaseService interface {
	GetPredictedYieldsForLocation(locID string) ([]DBPredictedYield, error)
	GetActualYieldsForLocation(locID string) ([]DBActualYield, error)
	GetPredictedYieldForLocations(locIDs []string, timeUnix int32) ([]DBPredictedLocalisedYield, error)
	GetActualYieldForLocations(locIDs []string, timeUnix int32) ([]DBActualLocalisedYield, error)
}
