package ui

import (
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const timeLayout = "2006-01-02 15:04"

type forecastQuery struct {
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

// defaultForecastQuery fills in the parts of a query that don't depend on a location or
// forecaster/observer lookup: the default energy source, and a rolling window around now.
func defaultForecastQuery(now time.Time) forecastQuery {
	return forecastQuery{
		EnergySource: int(getEnergySourceOptions()[0].Value),
		StartTs:      now.Add(-48 * time.Hour),
		EndTs:        now.Add(36 * time.Hour),
	}
}

// Complete reports whether q carries enough information to run a forecast query.
func (q forecastQuery) Complete() bool {
	return q.LocUUID != "" &&
		q.EnergySource != 0 &&
		!q.StartTs.IsZero() &&
		!q.EndTs.IsZero() &&
		(len(q.Forecasters) > 0 || len(q.Observers) > 0)
}

// parseForecastQuery reads a forecastQuery from r's query parameters. Any parameter that is
// present is validated; if requireComplete is true, a missing parameter is also an error. This
// lets /components/forecast reject an incomplete query outright, while / can accept a partial
// one (e.g. just a location_uuid) to prefill the form without erroring.
func parseForecastQuery(r *http.Request, requireComplete bool) (*forecastQuery, error) {
	q := r.URL.Query()

	out := &forecastQuery{
		LocUUID: q.Get("location_uuid"),
		SkipMap: q.Get("skip_map") == "true",
	}

	forecastersRaw := q["forecaster"]
	observersRaw := q["observer"]

	esRaw := q.Get("energy_source")
	startRaw := q.Get("start")
	endRaw := q.Get("end")

	// Support legacy time_window parameter; this is still the only format the form submits.
	if startRaw == "" && endRaw == "" {
		if tw := q.Get("time_window"); tw != "" {
			parts := strings.Split(tw, " to ")
			if len(parts) != 2 {
				return nil, errors.New("invalid time_window format")
			}

			startRaw, endRaw = parts[0], parts[1]
		}
	}

	if requireComplete && (out.LocUUID == "" || (len(forecastersRaw) == 0 && len(observersRaw) == 0) ||
		esRaw == "" || startRaw == "" || endRaw == "") {
		return nil, errors.New("missing required query parameters")
	}

	var err error

	if esRaw != "" {
		if out.EnergySource, err = strconv.Atoi(esRaw); err != nil {
			return nil, errors.New("invalid energy_source format")
		}
	}

	if startRaw != "" {
		if out.StartTs, err = time.ParseInLocation(timeLayout, startRaw, time.UTC); err != nil {
			return nil, errors.New("invalid start time format")
		}
	}

	if endRaw != "" {
		if out.EndTs, err = time.ParseInLocation(timeLayout, endRaw, time.UTC); err != nil {
			return nil, errors.New("invalid end time format")
		}
	}

	for _, fRaw := range forecastersRaw {
		parts := strings.Split(fRaw, "|")
		if len(parts) == 3 {
			if horizonMins, err := strconv.Atoi(parts[2]); err == nil {
				out.Forecasters = append(out.Forecasters, ForecasterInput{
					Raw:         fRaw,
					Name:        parts[0],
					Version:     parts[1],
					HorizonMins: horizonMins,
				})
			}
		}
	}

	for _, oRaw := range observersRaw {
		out.Observers = append(out.Observers, ObserverInput{Raw: oRaw, Name: oRaw})
	}

	if requireComplete && len(out.Forecasters) == 0 && len(out.Observers) == 0 {
		return nil, errors.New("no valid forecasters or observers provided")
	}

	return out, nil
}

func (q *forecastQuery) Values() url.Values {
	v := url.Values{}
	v.Set("location_uuid", q.LocUUID)
	v.Set("energy_source", strconv.Itoa(q.EnergySource))
	v.Set("start", q.StartTs.Format(timeLayout))
	v.Set("end", q.EndTs.Format(timeLayout))

	for _, f := range q.Forecasters {
		v.Add("forecaster", f.Raw)
	}

	for _, o := range q.Observers {
		v.Add("observer", o.Raw)
	}

	return v
}
