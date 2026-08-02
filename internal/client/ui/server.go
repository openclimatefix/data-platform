package ui

import (
	"compress/gzip"
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"html/template"
	"io"
	"math"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/openclimatefix/data-platform/internal/gen/ocf/dp"
)

//go:embed templates/*.html static/*
var templateFiles embed.FS

var tpl *template.Template

func init() {
	funcs := template.FuncMap{
		"calcWatts": func(frac float32, cap uint64) float32 {
			return frac * float32(cap)
		},
		"formatCapacity": func(cap uint64) string {
			if cap >= 1_000_000 {
				return fmt.Sprintf("%.1fMW", float64(cap)/1_000_000.0)
			} else if cap >= 1_000 {
				return fmt.Sprintf("%.1fkW", float64(cap)/1_000.0)
			}

			return fmt.Sprintf("%dW", cap)
		},
		"toJSON": func(v interface{}) template.JS {
			b, _ := json.Marshal(v)
			return template.JS(b)
		},
	}

	var err error

	tpl, err = template.New("").Funcs(funcs).ParseFS(templateFiles, "templates/*.html")
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to parse UI templates")
	}
}

type UIClient struct {
	grpcClient pb.DataPlatformDataServiceClient
}

func NewUIClient(grpcTarget string) (*UIClient, error) {
	conn, err := grpc.NewClient(
		grpcTarget,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to dial gRPC target %s: %w", grpcTarget, err)
	}

	client := pb.NewDataPlatformDataServiceClient(conn)

	return &UIClient{
		grpcClient: client,
	}, nil
}

type gzipResponseWriter struct {
	io.Writer
	http.ResponseWriter
}

func (w gzipResponseWriter) Write(b []byte) (int, error) {
	return w.Writer.Write(b)
}

func withGzip(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
			next.ServeHTTP(w, r)
			return
		}

		w.Header().Set("Content-Encoding", "gzip")
		w.Header().Set("Vary", "Accept-Encoding")

		gz := gzip.NewWriter(w)
		defer gz.Close()
		gzw := gzipResponseWriter{Writer: gz, ResponseWriter: w}
		next.ServeHTTP(gzw, r)
	})
}

func (ui *UIClient) Start(port string) error {
	mux := http.NewServeMux()

	mux.Handle("/static/", http.FileServer(http.FS(templateFiles)))
	mux.HandleFunc("/", ui.handleIndex)
	mux.HandleFunc("/components/selectors", ui.handleSelectors)
	mux.HandleFunc("/components/forecast", ui.handleForecast)

	mux.HandleFunc("/dashboard/", ui.handleDashboardCountry)
	mux.HandleFunc("/api/dashboard/map-snapshot", ui.handleDashboardMapSnapshot)

	return http.ListenAndServe(port, withGzip(mux))
}

func (ui *UIClient) handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}

	err := tpl.ExecuteTemplate(w, "analysis.html", nil)
	if err != nil {
		log.Error().Err(err).Msg("Failed to execute analysis template")
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (ui *UIClient) handleDashboardCountry(w http.ResponseWriter, r *http.Request) {
	country := strings.TrimPrefix(r.URL.Path, "/dashboard/")
	if country == "" {
		http.Redirect(w, r, "/dashboard/uk", http.StatusFound)
		return
	}

	data := struct {
		Country string
	}{
		Country: country,
	}

	err := tpl.ExecuteTemplate(w, "dashboard.html", data)
	if err != nil {
		log.Error().Err(err).Msg("Failed to execute dashboard template")
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (ui *UIClient) handleSelectors(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	mode := r.URL.Query().Get("mode")

	var (
		locResp *pb.ListLocationsResponse
		fcResp  *pb.ListForecastersResponse
		obsResp *pb.ListObserversResponse
	)

	g, gCtx := errgroup.WithContext(ctx)

	g.Go(func() error {
		var locErr error

		locResp, locErr = ui.grpcClient.ListLocations(gCtx, &pb.ListLocationsRequest{})
		if locErr != nil {
			log.Error().Err(locErr).Msg("Failed to list locations")
		}

		return locErr
	})

	g.Go(func() error {
		var fcErr error

		fcResp, fcErr = ui.grpcClient.ListForecasters(gCtx, &pb.ListForecastersRequest{})
		if fcErr != nil {
			log.Error().Err(fcErr).Msg("Failed to list forecasters")
		}

		return fcErr
	})

	g.Go(func() error {
		var obsErr error

		obsResp, obsErr = ui.grpcClient.ListObservers(gCtx, &pb.ListObserversRequest{})
		if obsErr != nil {
			log.Error().Err(obsErr).Msg("Failed to list observers")
		}

		return obsErr
	})

	if err := g.Wait(); err != nil {
		log.Error().Err(err).Msg("Failed to list required resources for selectors")
		http.Error(
			w,
			fmt.Sprintf("Failed to list required resources: %v", err),
			http.StatusInternalServerError,
		)

		return
	}

	dashboardMode := false

	var dashboardCountry, defaultLocationUUID string
	if mode != "" {
		dashboardMode = true
		dashboardCountry = mode
		// find location uuid for country
		for _, loc := range locResp.GetLocations() {
			if strings.EqualFold(loc.GetLocationName(), dashboardCountry) {
				defaultLocationUUID = loc.GetLocationUuid()
				break
			}
		}
	}

	data := struct {
		Locations           []*pb.ListLocationsResponse_LocationSummary
		Forecasters         []*pb.Forecaster
		Observers           []*pb.ListObserversResponse_ObserverSummary
		DefaultTimeWindow   string
		DashboardMode       bool
		DashboardCountry    string
		DefaultLocationUUID string
	}{
		Locations:   locResp.GetLocations(),
		Forecasters: fcResp.GetForecasters(),
		Observers:   obsResp.GetObservers(),
		DefaultTimeWindow: fmt.Sprintf("%s to %s",
			time.Now().UTC().Add(-48*time.Hour).Format("2006-01-02 15:04"),
			time.Now().UTC().Add(36*time.Hour).Format("2006-01-02 15:04"),
		),
		DashboardMode:       dashboardMode,
		DashboardCountry:    dashboardCountry,
		DefaultLocationUUID: defaultLocationUUID,
	}

	err := tpl.ExecuteTemplate(w, "selectors.html", data)
	if err != nil {
		log.Error().Err(err).Msg("Failed to execute selectors template")
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

type SeriesData struct {
	Name          string     `json:"name"`
	Data          []*float32 `json:"data"`
	HasBands      bool       `json:"hasBands"`
	BandLower     []*float32 `json:"bandLower"`
	BandDiff      []*float32 `json:"bandDiff"`
	IsObservation bool       `json:"isObservation"`
}

func (ui *UIClient) handleForecast(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	locUUID := r.URL.Query().Get("location_uuid")
	forecastersRaw := r.URL.Query()["forecaster"]
	observersRaw := r.URL.Query()["observer"]
	energySourceRaw := r.URL.Query().Get("energy_source")
	horizonMinsRaw := r.URL.Query().Get("horizon_mins")
	timeWindowRaw := r.URL.Query().Get("time_window")
	skipMapRaw := r.URL.Query().Get("skip_map")
	skipMap := skipMapRaw == "true"

	if locUUID == "" || (len(forecastersRaw) == 0 && len(observersRaw) == 0) ||
		energySourceRaw == "" ||
		horizonMinsRaw == "" ||
		timeWindowRaw == "" {
		http.Error(w, "Missing required query parameters", http.StatusBadRequest)
		return
	}

	energySource, err := strconv.Atoi(energySourceRaw)
	if err != nil {
		http.Error(w, "Invalid energy_source format", http.StatusBadRequest)
		return
	}

	horizonMins, err := strconv.Atoi(horizonMinsRaw)
	if err != nil {
		http.Error(w, "Invalid horizon_mins format", http.StatusBadRequest)
		return
	}

	parts := strings.Split(timeWindowRaw, " to ")
	if len(parts) != 2 {
		http.Error(w, "Invalid time window format, expected 'start to end'", http.StatusBadRequest)
		return
	}

	startTsObj, err := time.ParseInLocation("2006-01-02 15:04", parts[0], time.UTC)
	if err != nil {
		http.Error(w, "Invalid start time format", http.StatusBadRequest)
		return
	}

	endTsObj, err := time.ParseInLocation("2006-01-02 15:04", parts[1], time.UTC)
	if err != nil {
		http.Error(w, "Invalid end time format", http.StatusBadRequest)
		return
	}

	locReq := &pb.GetLocationRequest{
		LocationUuid: locUUID,
		EnergySource: pb.EnergySource(energySource),
	}

	locResp, err := ui.grpcClient.GetLocation(ctx, locReq)
	if err != nil {
		log.Error().Err(err).Msg("Failed to get location for map")
		http.Error(
			w,
			fmt.Sprintf("Failed to get location: %v", err),
			http.StatusInternalServerError,
		)

		return
	}

	capacity := float32(locResp.GetEffectiveCapacityWatts())

	type ForecasterInput struct {
		Raw     string
		Name    string
		Version string
	}

	var forecasters []ForecasterInput
	for _, fRaw := range forecastersRaw {
		parts := strings.Split(fRaw, "|")
		if len(parts) == 2 {
			forecasters = append(forecasters, ForecasterInput{
				Raw:     fRaw,
				Name:    parts[0],
				Version: parts[1],
			})
		}
	}

	type ObserverInput struct {
		Raw  string
		Name string
	}

	var observers []ObserverInput
	for _, oRaw := range observersRaw {
		observers = append(observers, ObserverInput{
			Raw:  oRaw,
			Name: oRaw,
		})
	}

	type ForecasterResult struct {
		Raw          string
		SeriesMap    map[int64]float32
		BandLowerMap map[int64]float32
		BandUpperMap map[int64]float32
		UniqueT      map[int64]time.Time
		FractionSum  float32
		Count        int
	}

	results := make([]ForecasterResult, len(forecasters))
	g, gCtx := errgroup.WithContext(ctx)

	for i, f := range forecasters {
		i, f := i, f
		g.Go(func() error {
			req := &pb.GetForecastAsTimeseriesRequest{
				LocationUuid: locUUID,
				EnergySource: pb.EnergySource(energySource),
				HorizonMins:  uint32(horizonMins),
				TimeWindow: &pb.TimeWindow{
					StartTimestampUtc: timestamppb.New(startTsObj),
					EndTimestampUtc:   timestamppb.New(endTsObj),
				},
				Forecaster: &pb.Forecaster{
					ForecasterName:    f.Name,
					ForecasterVersion: f.Version,
				},
			}

			resp, err := ui.grpcClient.GetForecastAsTimeseries(gCtx, req)
			if err != nil {
				log.Warn().Err(err).Msgf("Failed to get forecast for %s", f.Raw)
				return nil
			}

			seriesMap := make(map[int64]float32)
			bandLowerMap := make(map[int64]float32)
			bandUpperMap := make(map[int64]float32)
			uniqueT := make(map[int64]time.Time)

			var (
				sum   float32
				count int
			)

			for _, v := range resp.GetValues() {
				t := v.GetTargetTimestampUtc().AsTime()
				unix := t.Unix()
				uniqueT[unix] = t
				seriesMap[unix] = float32(math.Round(float64(v.GetP50ValueFraction()*capacity)*100) / 100)

				stats := v.GetOtherStatisticsFractions()
				if stats != nil {
					if p10, ok := stats["p10"]; ok {
						if p90, ok := stats["p90"]; ok {
							bandLowerMap[unix] = float32(math.Round(float64(p10*capacity)*100) / 100)
							bandUpperMap[unix] = float32(math.Round(float64(p90*capacity)*100) / 100)
						}
					}
				}

				sum += v.GetP50ValueFraction()
				count++
			}

			results[i] = ForecasterResult{
				Raw:          f.Raw,
				SeriesMap:    seriesMap,
				BandLowerMap: bandLowerMap,
				BandUpperMap: bandUpperMap,
				UniqueT:      uniqueT,
				FractionSum:  sum,
				Count:        count,
			}

			return nil
		})
	}

	obsResults := make([]ForecasterResult, len(observers))
	for i, o := range observers {
		i, o := i, o
		g.Go(func() error {
			req := &pb.GetObservationsAsTimeseriesRequest{
				LocationUuid: locUUID,
				ObserverName: o.Name,
				EnergySource: pb.EnergySource(energySource),
				TimeWindow: &pb.TimeWindow{
					StartTimestampUtc: timestamppb.New(startTsObj),
					EndTimestampUtc:   timestamppb.New(endTsObj),
				},
			}

			resp, err := ui.grpcClient.GetObservationsAsTimeseries(gCtx, req)
			if err != nil {
				log.Warn().Err(err).Msgf("Failed to get observations for %s", o.Raw)
				return nil
			}

			seriesMap := make(map[int64]float32)
			uniqueT := make(map[int64]time.Time)

			for _, v := range resp.GetValues() {
				t := v.GetTimestampUtc().AsTime()
				unix := t.Unix()
				uniqueT[unix] = t
				seriesMap[unix] = float32(math.Round(float64(v.GetValueFraction()*capacity)*100) / 100)
			}

			obsResults[i] = ForecasterResult{
				Raw:       o.Raw,
				SeriesMap: seriesMap,
				UniqueT:   uniqueT,
			}

			return nil
		})
	}

	var (
		geoJSONStr       template.JS = "null"
		isInteractiveMap bool
	)

	if !skipMap {
		g.Go(func() error {
			locTypeGSP := pb.LocationType_LOCATION_TYPE_GSP

			gspResp, err := ui.grpcClient.ListLocations(gCtx, &pb.ListLocationsRequest{
				EnclosingLocationUuidFilter: &locUUID,
				LocationTypeFilter:          &locTypeGSP,
			})
			if err != nil {
				return err
			}

			var fetchUUIDs []string
			if len(gspResp.GetLocations()) > 0 {
				isInteractiveMap = true

				for _, l := range gspResp.GetLocations() {
					fetchUUIDs = append(fetchUUIDs, l.GetLocationUuid())
				}
			} else {
				fetchUUIDs = []string{locUUID}
			}

			geoReq := &pb.GetLocationsAsGeoJSONRequest{
				LocationUuids: fetchUUIDs,
				Unsimplified:  false,
			}

			geoResp, err := ui.grpcClient.GetLocationsAsGeoJSON(gCtx, geoReq)
			if err == nil && geoResp != nil && geoResp.GetGeojson() != "" {
				geoJSONStr = template.JS(geoResp.GetGeojson())
			} else {
				if err != nil {
					log.Warn().Err(err).Msg("Failed to get GeoJSON for map")
				}
			}

			return nil
		})
	}

	_ = g.Wait()

	var allSeries []SeriesData
	uniqueTimes := make(map[int64]time.Time)
	forecasterResults := make(map[string]map[int64]float32)
	forecasterBandsLower := make(map[string]map[int64]float32)
	forecasterBandsUpper := make(map[string]map[int64]float32)

	var (
		totalFraction float32
		count         int
	)

	for _, res := range results {
		if res.SeriesMap == nil {
			continue
		}

		forecasterResults[res.Raw] = res.SeriesMap
		forecasterBandsLower[res.Raw] = res.BandLowerMap

		forecasterBandsUpper[res.Raw] = res.BandUpperMap
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

	// Sort times to align X-axis labels
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
		sd := SeriesData{Name: fmt.Sprintf("%s (v%s)", f.Name, f.Version)}
		hasBands := len(forecasterBandsLower[f.Raw]) > 0

		sd.HasBands = hasBands
		for _, k := range timeKeys {
			if val, ok := forecasterResults[f.Raw][k]; ok {
				v := val
				sd.Data = append(sd.Data, &v)
			} else {
				sd.Data = append(sd.Data, nil)
			}

			if hasBands {
				var (
					bLow, bUp     float32
					hasLow, hasUp bool
				)

				if val, ok := forecasterBandsLower[f.Raw][k]; ok {
					bLow = val
					hasLow = true

					sd.BandLower = append(sd.BandLower, &bLow)
				} else {
					sd.BandLower = append(sd.BandLower, nil)
				}

				if val, ok := forecasterBandsUpper[f.Raw][k]; ok {
					bUp = val
					hasUp = true
				}

				if hasLow && hasUp {
					diff := float32(math.Round(float64(bUp-bLow)*100) / 100)
					sd.BandDiff = append(sd.BandDiff, &diff)
				} else {
					sd.BandDiff = append(sd.BandDiff, nil)
				}
			}
		}

		allSeries = append(allSeries, sd)
	}

	for _, o := range observers {
		sd := SeriesData{
			Name:          o.Name + " [Obs]",
			IsObservation: true,
		}
		for _, k := range timeKeys {
			if val, ok := forecasterResults[o.Raw][k]; ok {
				v := val
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

	firstForecaster := ""
	if len(forecasters) > 0 {
		firstForecaster = forecasters[0].Name + "|" + forecasters[0].Version
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
		SkipMap:          skipMap,
		IsInteractiveMap: isInteractiveMap,
		EnergySource:     energySourceRaw,
		FirstForecaster:  firstForecaster,
		TimeWindow:       timeWindowRaw,
		HorizonMins:      horizonMinsRaw,
	}

	err = tpl.ExecuteTemplate(w, "forecast_results.html", data)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
