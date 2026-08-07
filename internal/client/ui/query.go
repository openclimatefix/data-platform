package ui

import (
	"errors"
	"net/http"
	"net/url"
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
	q := r.URL.Query()

	params := &forecastParams{
		LocUUID: q.Get("location_uuid"),
		SkipMap: q.Get("skip_map") == "true",
	}

	forecastersRaw := q["forecaster"]
	observersRaw := q["observer"]

	esRaw := q.Get("energy_source")
	startRaw := q.Get("start")
	endRaw := q.Get("end")

	// Support legacy time_window parameter
	if startRaw == "" && endRaw == "" {
		tw := q.Get("time_window")
		if parts := strings.Split(tw, " to "); len(parts) == 2 {
			startRaw = parts[0]
			endRaw = parts[1]
		}
	}

	if params.LocUUID == "" || (len(forecastersRaw) == 0 && len(observersRaw) == 0) ||
		esRaw == "" || startRaw == "" || endRaw == "" {
		return nil, errors.New("missing required query parameters")
	}

	var err error
	if params.EnergySource, err = strconv.Atoi(esRaw); err != nil {
		return nil, errors.New("invalid energy_source format")
	}

	if params.StartTs, err = time.ParseInLocation("2006-01-02 15:04", startRaw, time.UTC); err != nil {
		return nil, errors.New("invalid start time format")
	}

	if params.EndTs, err = time.ParseInLocation("2006-01-02 15:04", endRaw, time.UTC); err != nil {
		return nil, errors.New("invalid end time format")
	}

	for _, fRaw := range forecastersRaw {
		parts := strings.Split(fRaw, "|")
		if len(parts) == 3 {
			if horizonMins, err := strconv.Atoi(parts[2]); err == nil {
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

	if len(params.Forecasters) == 0 && len(params.Observers) == 0 {
		return nil, errors.New("no valid forecasters or observers provided")
	}

	return params, nil
}

func (p *forecastParams) URLValues() url.Values {
	v := url.Values{}
	v.Set("location_uuid", p.LocUUID)
	v.Set("energy_source", strconv.Itoa(p.EnergySource))
	v.Set("start", p.StartTs.Format("2006-01-02 15:04"))
	v.Set("end", p.EndTs.Format("2006-01-02 15:04"))
	
	for _, f := range p.Forecasters {
		v.Add("forecaster", f.Raw)
	}
	for _, o := range p.Observers {
		v.Add("observer", o.Raw)
	}
	return v
}
