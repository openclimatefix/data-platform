package ui

import (
	"context"
	"embed"
	"fmt"
	"html/template"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	pb "github.com/openclimatefix/data-platform/internal/gen/ocf/dp"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"
)

//go:embed templates/*.html
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
	}
	tpl = template.Must(template.New("").Funcs(funcs).ParseFS(templateFiles, "templates/*.html"))
}

type UIClient struct {
	grpcClient pb.DataPlatformDataServiceClient
}

func NewUIClient(grpcTarget string) (*UIClient, error) {
	conn, err := grpc.Dial(grpcTarget, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to dial gRPC target %s: %w", grpcTarget, err)
	}

	client := pb.NewDataPlatformDataServiceClient(conn)

	return &UIClient{
		grpcClient: client,
	}, nil
}

func (ui *UIClient) Start(port string) error {
	mux := http.NewServeMux()

	mux.HandleFunc("/", ui.handleIndex)
	mux.HandleFunc("/components/selectors", ui.handleSelectors)
	mux.HandleFunc("/components/forecast", ui.handleForecast)

	return http.ListenAndServe(port, mux)
}

func (ui *UIClient) handleIndex(w http.ResponseWriter, r *http.Request) {
	err := tpl.ExecuteTemplate(w, "index.html", nil)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (ui *UIClient) handleSelectors(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	locResp, err := ui.grpcClient.ListLocations(ctx, &pb.ListLocationsRequest{})
	if err != nil {
		log.Error().Err(err).Msg("Failed to list locations")
		http.Error(w, fmt.Sprintf("Failed to list locations: %v", err), http.StatusInternalServerError)
		return
	}

	fcResp, err := ui.grpcClient.ListForecasters(ctx, &pb.ListForecastersRequest{})
	if err != nil {
		log.Error().Err(err).Msg("Failed to list forecasters")
		http.Error(w, fmt.Sprintf("Failed to list forecasters: %v", err), http.StatusInternalServerError)
		return
	}

	data := struct {
		Locations   []*pb.ListLocationsResponse_LocationSummary
		Forecasters []*pb.Forecaster
		DefaultTimeWindow string
	}{
		Locations:   locResp.GetLocations(),
		Forecasters: fcResp.GetForecasters(),
		DefaultTimeWindow: fmt.Sprintf("%s to %s", 
			time.Now().UTC().Add(-48 * time.Hour).Format("2006-01-02 15:04"),
			time.Now().UTC().Add(36 * time.Hour).Format("2006-01-02 15:04"),
		),
	}

	err = tpl.ExecuteTemplate(w, "selectors.html", data)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (ui *UIClient) handleForecast(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	locUUID := r.URL.Query().Get("location_uuid")
	forecastersRaw := r.URL.Query()["forecaster"]
	energySourceRaw := r.URL.Query().Get("energy_source")
	horizonMinsRaw := r.URL.Query().Get("horizon_mins")
	timeWindowRaw := r.URL.Query().Get("time_window")

	if locUUID == "" || len(forecastersRaw) == 0 || energySourceRaw == "" || horizonMinsRaw == "" || timeWindowRaw == "" {
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
		http.Error(w, fmt.Sprintf("Failed to get location: %v", err), http.StatusInternalServerError)
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

	type SeriesData struct {
		Name string
		Data []string
	}

	type ForecasterResult struct {
		Raw         string
		SeriesMap   map[int64]float32
		UniqueT     map[int64]time.Time
		FractionSum float32
		Count       int
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
			uniqueT := make(map[int64]time.Time)
			var sum float32
			var count int

			for _, v := range resp.GetValues() {
				t := v.GetTargetTimestampUtc().AsTime()
				unix := t.Unix()
				uniqueT[unix] = t
				seriesMap[unix] = v.GetP50ValueFraction() * capacity

				sum += v.GetP50ValueFraction()
				count++
			}

			results[i] = ForecasterResult{
				Raw:         f.Raw,
				SeriesMap:   seriesMap,
				UniqueT:     uniqueT,
				FractionSum: sum,
				Count:       count,
			}
			return nil
		})
	}

	_ = g.Wait()

	var allSeries []SeriesData
	uniqueTimes := make(map[int64]time.Time)
	forecasterResults := make(map[string]map[int64]float32)
	var totalFraction float32
	var count int

	for _, res := range results {
		if res.SeriesMap == nil {
			continue
		}
		forecasterResults[res.Raw] = res.SeriesMap
		for unix, t := range res.UniqueT {
			uniqueTimes[unix] = t
		}
		totalFraction += res.FractionSum
		count += res.Count
	}

	// Sort times to align X-axis labels
	var timeKeys []int64
	for k := range uniqueTimes {
		timeKeys = append(timeKeys, k)
	}
	slices.Sort(timeKeys)

	var labels []string
	for _, k := range timeKeys {
		labels = append(labels, uniqueTimes[k].Format("01-02 15:04"))
	}

	for _, f := range forecasters {
		sd := SeriesData{Name: fmt.Sprintf("%s (v%s)", f.Name, f.Version)}
		for _, k := range timeKeys {
			if val, ok := forecasterResults[f.Raw][k]; ok {
				sd.Data = append(sd.Data, fmt.Sprintf("%.2f", val))
			} else {
				sd.Data = append(sd.Data, "null")
			}
		}
		allSeries = append(allSeries, sd)
	}

	geoReq := &pb.GetLocationsAsGeoJSONRequest{
		LocationUuids: []string{locUUID},
	}
	geoResp, err := ui.grpcClient.GetLocationsAsGeoJSON(ctx, geoReq)
	var geoJSONStr string
	if err == nil && geoResp != nil {
		geoJSONStr = geoResp.GetGeojson()
	} else {
		log.Warn().Err(err).Msg("Failed to get GeoJSON for map")
	}

	var avgFraction float32
	if count > 0 {
		avgFraction = totalFraction / float32(count)
	}

	data := struct {
		Location    *pb.GetLocationResponse
		GeoJSON     string
		AvgFraction float32
		Labels      []string
		Series      []SeriesData
	}{
		Location:    locResp,
		GeoJSON:     geoJSONStr,
		AvgFraction: avgFraction,
		Labels:      labels,
		Series:      allSeries,
	}

	err = tpl.ExecuteTemplate(w, "forecast_results.html", data)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
