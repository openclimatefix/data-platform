package ui

import (
	"context"
	"errors"
	"fmt"
	"html/template"
	"math"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/openclimatefix/data-platform/internal/gen/ocf/dp"
)

type BandLayer struct {
	Level     int        `json:"level"`
	LowerName string     `json:"lowerName"`
	UpperName string     `json:"upperName"`
	Lower     []*float32 `json:"lower"`
	Diff      []*float32 `json:"diff"`
}

type SeriesData struct {
	Name          string                `json:"name"`
	Data          []*float32            `json:"data"`
	HasBands      bool                  `json:"hasBands"`
	Bands         []BandLayer           `json:"bands"`
	BandMap       []map[string]*float32 `json:"bandMap"`
	IsObservation bool                  `json:"isObservation"`
}

type forecastParams struct {
	LocUUID      string
	EnergySource int
	HorizonMins  int
	StartTs      time.Time
	EndTs        time.Time
	Forecasters  []ForecasterInput
	Observers    []ObserverInput
	SkipMap      bool

	RawEnergySource string
	RawHorizonMins  string
	RawTimeWindow   string
}

type ForecasterInput struct {
	Raw     string
	Name    string
	Version string
}

type ObserverInput struct {
	Raw  string
	Name string
}

type ForecasterResult struct {
	Raw         string
	SeriesMap   map[int64]float32
	StatsMap    map[int64]map[string]float32
	UniqueT     map[int64]time.Time
	FractionSum float32
	Count       int
}

func parseForecastRequest(r *http.Request) (*forecastParams, error) {
	params := &forecastParams{
		LocUUID:         r.URL.Query().Get("location_uuid"),
		RawEnergySource: r.URL.Query().Get("energy_source"),
		RawHorizonMins:  r.URL.Query().Get("horizon_mins"),
		RawTimeWindow:   r.URL.Query().Get("time_window"),
		SkipMap:         r.URL.Query().Get("skip_map") == "true",
	}

	forecastersRaw := r.URL.Query()["forecaster"]
	observersRaw := r.URL.Query()["observer"]

	if params.LocUUID == "" || (len(forecastersRaw) == 0 && len(observersRaw) == 0) ||
		params.RawEnergySource == "" || params.RawHorizonMins == "" || params.RawTimeWindow == "" {
		return nil, errors.New("missing required query parameters")
	}

	var err error
	if params.EnergySource, err = strconv.Atoi(params.RawEnergySource); err != nil {
		return nil, errors.New("invalid energy_source format")
	}

	if params.HorizonMins, err = strconv.Atoi(params.RawHorizonMins); err != nil {
		return nil, errors.New("invalid horizon_mins format")
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
		if len(parts) == 2 {
			params.Forecasters = append(params.Forecasters, ForecasterInput{
				Raw:     fRaw,
				Name:    parts[0],
				Version: parts[1],
			})
		}
	}

	for _, oRaw := range observersRaw {
		params.Observers = append(params.Observers, ObserverInput{Raw: oRaw, Name: oRaw})
	}

	return params, nil
}

func fetchForecasterData(
	ctx context.Context,
	client pb.DataPlatformDataServiceClient,
	p *forecastParams,
) []ForecasterResult {
	results := make([]ForecasterResult, len(p.Forecasters))
	g, gCtx := errgroup.WithContext(ctx)

	for i, f := range p.Forecasters {
		i, f := i, f
		g.Go(func() error {
			req := &pb.GetForecastAsTimeseriesRequest{
				LocationUuid: p.LocUUID,
				EnergySource: pb.EnergySource(p.EnergySource),
				HorizonMins:  uint32(p.HorizonMins),
				TimeWindow: &pb.TimeWindow{
					StartTimestampUtc: timestamppb.New(p.StartTs),
					EndTimestampUtc:   timestamppb.New(p.EndTs),
				},
				Forecaster: &pb.Forecaster{ForecasterName: f.Name, ForecasterVersion: f.Version},
			}

			resp, err := client.GetForecastAsTimeseries(gCtx, req)
			if err != nil {
				log.Warn().Err(err).Msgf("Failed to get forecast for %s", f.Raw)
				return nil
			}

			res := ForecasterResult{
				Raw:       f.Raw,
				SeriesMap: make(map[int64]float32),
				StatsMap:  make(map[int64]map[string]float32),
				UniqueT:   make(map[int64]time.Time),
			}

			for _, v := range resp.GetValues() {
				t := v.GetTargetTimestampUtc().AsTime()
				unix := t.Unix()
				res.UniqueT[unix] = t
				res.SeriesMap[unix] = v.GetP50ValueFraction()

				if stats := v.GetOtherStatisticsFractions(); stats != nil {
					res.StatsMap[unix] = make(map[string]float32)
					for k, val := range stats {
						if strings.HasPrefix(k, "p") {
							res.StatsMap[unix][k] = val
						}
					}
				}

				res.FractionSum += v.GetP50ValueFraction()
				res.Count++
			}

			results[i] = res

			return nil
		})
	}

	_ = g.Wait()

	return results
}

func fetchObserverData(
	ctx context.Context,
	client pb.DataPlatformDataServiceClient,
	p *forecastParams,
) []ForecasterResult {
	results := make([]ForecasterResult, len(p.Observers))
	g, gCtx := errgroup.WithContext(ctx)

	for i, o := range p.Observers {
		i, o := i, o
		g.Go(func() error {
			req := &pb.GetObservationsAsTimeseriesRequest{
				LocationUuid: p.LocUUID,
				ObserverName: o.Name,
				EnergySource: pb.EnergySource(p.EnergySource),
				TimeWindow: &pb.TimeWindow{
					StartTimestampUtc: timestamppb.New(p.StartTs),
					EndTimestampUtc:   timestamppb.New(p.EndTs),
				},
			}

			resp, err := client.GetObservationsAsTimeseries(gCtx, req)
			if err != nil {
				log.Warn().Err(err).Msgf("Failed to get observations for %s", o.Raw)
				return nil
			}

			res := ForecasterResult{
				Raw:       o.Raw,
				SeriesMap: make(map[int64]float32),
				UniqueT:   make(map[int64]time.Time),
			}

			for _, v := range resp.GetValues() {
				t := v.GetTimestampUtc().AsTime()
				unix := t.Unix()
				res.UniqueT[unix] = t
				res.SeriesMap[unix] = v.GetValueFraction()
			}

			results[i] = res

			return nil
		})
	}

	_ = g.Wait()

	return results
}

func buildChartSeries(
	results []ForecasterResult,
	obsResults []ForecasterResult,
	forecasters []ForecasterInput,
	observers []ObserverInput,
	capacity float32,
) ([]SeriesData, []string, []int64, float32) {
	var allSeries []SeriesData
	uniqueTimes := make(map[int64]time.Time)
	forecasterResults := make(map[string]map[int64]float32)
	forecasterStats := make(map[string]map[int64]map[string]float32)

	var (
		totalFraction float32
		count         int
	)

	for _, res := range results {
		if res.SeriesMap == nil {
			continue
		}

		forecasterResults[res.Raw] = res.SeriesMap
		forecasterStats[res.Raw] = res.StatsMap

		for unix, t := range res.UniqueT {
			uniqueTimes[unix] = t
		}

		totalFraction += res.FractionSum
		count += res.Count
	}

	for _, res := range obsResults {
		if res.SeriesMap == nil {
			continue
		}

		forecasterResults[res.Raw] = res.SeriesMap
		for unix, t := range res.UniqueT {
			uniqueTimes[unix] = t
		}
	}

	var timeKeys []int64
	for k := range uniqueTimes {
		timeKeys = append(timeKeys, k)
	}

	slices.Sort(timeKeys)

	var labels []string
	for _, k := range timeKeys {
		labels = append(labels, uniqueTimes[k].Format("Jan 02 15:04"))
	}

	for _, f := range forecasters {
		sd := SeriesData{
			Name:    fmt.Sprintf("%s (v%s)", f.Name, f.Version),
			BandMap: make([]map[string]*float32, len(timeKeys)),
		}

		// Discover all complimentary p-levels (e.g. p10 + p90 = 100)
		pPairs := make(map[string]struct{})
		for _, k := range timeKeys {
			if stats, ok := forecasterStats[f.Raw][k]; ok {
				for pKey := range stats {
					if len(pKey) > 1 {
						pVal, err := strconv.Atoi(pKey[1:])
						if err == nil && pVal < 50 {
							complement := fmt.Sprintf("p%d", 100-pVal)
							if _, hasComplement := stats[complement]; hasComplement {
								pPairs[fmt.Sprintf("%d", pVal)] = struct{}{}
							}
						}
					}
				}
			}
		}

		type PLevelPair struct {
			Level     int
			LowerName string
			UpperName string
		}

		var activePairs []PLevelPair
		for k := range pPairs {
			pVal, _ := strconv.Atoi(k)
			activePairs = append(activePairs, PLevelPair{
				Level:     100 - (pVal * 2), // e.g. p10+p90 = 80 level
				LowerName: fmt.Sprintf("p%d", pVal),
				UpperName: fmt.Sprintf("p%d", 100-pVal),
			})
		}

		slices.SortFunc(activePairs, func(a, b PLevelPair) int {
			return b.Level - a.Level // Widest first
		})

		if len(activePairs) > 0 {
			sd.HasBands = true
			for _, pair := range activePairs {
				sd.Bands = append(sd.Bands, BandLayer{
					Level:     pair.Level,
					LowerName: pair.LowerName,
					UpperName: pair.UpperName,
					Lower:     make([]*float32, len(timeKeys)),
					Diff:      make([]*float32, len(timeKeys)),
				})
			}
		}

		for idx, k := range timeKeys {
			sd.BandMap[idx] = make(map[string]*float32)

			if val, ok := forecasterResults[f.Raw][k]; ok {
				v := float32(math.Round(float64(val*capacity)*100) / 100)
				sd.Data = append(sd.Data, &v)
			} else {
				sd.Data = append(sd.Data, nil)
			}

			if stats, ok := forecasterStats[f.Raw][k]; ok {
				for sKey, sVal := range stats {
					v := float32(math.Round(float64(sVal*capacity)*100) / 100)
					sd.BandMap[idx][sKey] = &v
				}

				if sd.HasBands {
					for bIdx, pair := range activePairs {
						if bLow, hasLow := sd.BandMap[idx][pair.LowerName]; hasLow {
							if bUp, hasUp := sd.BandMap[idx][pair.UpperName]; hasUp {
								
								// Echarts Stack logic:
								// When stacking lines, the new 'Lower' line starts from 0 (if it is the first in the stack)
								// or it starts from the previous layer's 'Upper' line if stacked. 
								// But since we want concentric overlapping areas, we DO NOT stack them against each other!
								// By explicitly not linking their stacks together (stack: "band_i_bIndex"), they render independently.
								
								sd.Bands[bIdx].Lower[idx] = bLow
								diff := float32(math.Round(float64(*bUp-*bLow)*100) / 100)
								sd.Bands[bIdx].Diff[idx] = &diff
							}
						}
					}
				}
			}
		}

		allSeries = append(allSeries, sd)
	}

	for _, o := range observers {
		sd := SeriesData{Name: o.Name + " [Obs]", IsObservation: true}
		for _, k := range timeKeys {
			if val, ok := forecasterResults[o.Raw][k]; ok {
				v := float32(math.Round(float64(val*capacity)*100) / 100)
				sd.Data = append(sd.Data, &v)
			} else {
				sd.Data = append(sd.Data, nil)
			}
		}

		allSeries = append(allSeries, sd)
	}

	var avgFraction float32
	if count > 0 {
		avgFraction = totalFraction / float32(count)
	}

	return allSeries, labels, timeKeys, avgFraction
}

func (ui *UIClient) handleForecast(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	p, err := parseForecastRequest(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var (
		locResp    *pb.GetLocationResponse
		locErr     error
		results    []ForecasterResult
		obsResults []ForecasterResult

		geoJSONStr       template.JS = "null"
		isInteractiveMap bool
	)

	g, gCtx := errgroup.WithContext(ctx)

	// Fetch Location
	g.Go(func() error {
		locReq := &pb.GetLocationRequest{
			LocationUuid: p.LocUUID,
			EnergySource: pb.EnergySource(p.EnergySource),
		}
		locResp, locErr = ui.grpcClient.GetLocation(gCtx, locReq)
		if locErr != nil {
			log.Error().Err(locErr).Msg("Failed to get location")
			return fmt.Errorf("failed to get location: %w", locErr)
		}
		return nil
	})

	// Fetch Forecasters
	g.Go(func() error {
		results = fetchForecasterData(gCtx, ui.grpcClient, p)
		return nil
	})

	// Fetch Observers
	g.Go(func() error {
		obsResults = fetchObserverData(gCtx, ui.grpcClient, p)
		return nil
	})

	// Fetch Map GeoJSON concurrently if not skipped
	if !p.SkipMap {
		g.Go(func() error {
			locTypeGSP := pb.LocationType_LOCATION_TYPE_GSP
			gspResp, err := ui.grpcClient.ListLocations(gCtx, &pb.ListLocationsRequest{
				EnclosingLocationUuidFilter: &p.LocUUID,
				LocationTypeFilter:          &locTypeGSP,
			})

			var fetchUUIDs []string
			if err == nil && len(gspResp.GetLocations()) > 0 {
				isInteractiveMap = true
				for _, l := range gspResp.GetLocations() {
					fetchUUIDs = append(fetchUUIDs, l.GetLocationUuid())
				}
			} else {
				fetchUUIDs = []string{p.LocUUID}
			}

			geoResp, err := ui.grpcClient.GetLocationsAsGeoJSON(gCtx, &pb.GetLocationsAsGeoJSONRequest{
				LocationUuids: fetchUUIDs,
				Unsimplified:  false,
			})
			if err == nil && geoResp != nil && geoResp.GetGeojson() != "" {
				geoJSONStr = template.JS(geoResp.GetGeojson())
			} else if err != nil {
				log.Warn().Err(err).Msg("Failed to get GeoJSON for map")
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	capacity := float32(locResp.GetEffectiveCapacityWatts())

	allSeries, labels, timeKeys, avgFraction := buildChartSeries(
		results,
		obsResults,
		p.Forecasters,
		p.Observers,
		capacity,
	)

	firstForecaster := ""
	if len(p.Forecasters) > 0 {
		firstForecaster = p.Forecasters[0].Name + "|" + p.Forecasters[0].Version
	}

	data := struct {
		Location         *pb.GetLocationResponse
		GeoJSON          template.JS
		AvgFraction      float32
		Labels           []string
		Timestamps       []int64
		Series           []SeriesData
		SkipMap          bool
		IsInteractiveMap bool
		IsPolygon        bool
		EnergySource     string
		FirstForecaster  string
		TimeWindow       string
		HorizonMins      string
	}{
		Location:         locResp,
		GeoJSON:          geoJSONStr,
		AvgFraction:      avgFraction,
		Labels:           labels,
		Timestamps:       timeKeys,
		Series:           allSeries,
		SkipMap:          p.SkipMap,
		IsInteractiveMap: isInteractiveMap,
		IsPolygon: strings.Contains(string(geoJSONStr), `"Polygon"`) ||
			strings.Contains(string(geoJSONStr), `"MultiPolygon"`),
		EnergySource:    p.RawEnergySource,
		FirstForecaster: firstForecaster,
		TimeWindow:      p.RawTimeWindow,
		HorizonMins:     p.RawHorizonMins,
	}

	if err := tpl.ExecuteTemplate(w, "forecast_results.html", data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
