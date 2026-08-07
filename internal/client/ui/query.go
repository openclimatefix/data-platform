package ui

import (
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type forecastParams struct {
	LocUUID      string
	EnergySource int
	StartTs      time.Time
	EndTs        time.Time
	Forecasters  []ForecasterInput
	Observers    []ObserverInput
	SkipMap      bool

	RawEnergySource string
	RawTimeWindow   string
}

type ForecasterInput struct {
	Raw         string
	Name        string
	Version     string
	HorizonMins int
}

type ObserverInput struct {
	Raw  string
	Name string
}

func parseForecastRequest(r *http.Request) (*forecastParams, error) {
	params := &forecastParams{
		LocUUID:         r.URL.Query().Get("location_uuid"),
		RawEnergySource: r.URL.Query().Get("energy_source"),
		RawTimeWindow:   r.URL.Query().Get("time_window"),
		SkipMap:         r.URL.Query().Get("skip_map") == "true",
	}

	forecastersRaw := r.URL.Query()["forecaster"]
	observersRaw := r.URL.Query()["observer"]

	if params.LocUUID == "" || (len(forecastersRaw) == 0 && len(observersRaw) == 0) ||
		params.RawEnergySource == "" || params.RawTimeWindow == "" {
		return nil, errors.New("missing required query parameters")
	}

	var err error
	if params.EnergySource, err = strconv.Atoi(params.RawEnergySource); err != nil {
		return nil, errors.New("invalid energy_source format")
	}

	parts := strings.Split(params.RawTimeWindow, " to ")
	if len(parts) != 2 {
		return nil, errors.New("invalid time window format, expected 'start to end'")
	}

	if params.StartTs, err = time.ParseInLocation(
		"2006-01-02 15:04",
		parts[0],
		time.UTC,
	); err != nil {
		return nil, errors.New("invalid start time format")
	}

	if params.EndTs, err = time.ParseInLocation(
		"2006-01-02 15:04",
		parts[1],
		time.UTC,
	); err != nil {
		return nil, errors.New("invalid end time format")
	}

	for _, fRaw := range forecastersRaw {
		parts := strings.Split(fRaw, "|")
		if len(parts) == 3 {
			horizonMins, err := strconv.Atoi(parts[2])
			if err == nil {
				params.Forecasters = append(params.Forecasters, ForecasterInput{
					Raw:         fRaw,
					Name:        parts[0],
					Version:     parts[1],
					HorizonMins: horizonMins,
				})
			}
		}
	}

	for _, oRaw := range observersRaw {
		params.Observers = append(params.Observers, ObserverInput{Raw: oRaw, Name: oRaw})
	}

	return params, nil
}
