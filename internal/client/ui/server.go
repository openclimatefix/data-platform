package ui

import (
	"context"
	"embed"
	"fmt"
	"html/template"
	"net/http"
	"strconv"
	"strings"
	"time"

	pb "github.com/openclimatefix/data-platform/internal/gen/ocf/dp"
	"github.com/rs/zerolog/log"
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
		DefaultStart string
		DefaultEnd   string
	}{
		Locations:   locResp.GetLocations(),
		Forecasters: fcResp.GetForecasters(),
		DefaultStart: time.Now().UTC().Add(-48 * time.Hour).Format("2006-01-02T15:04"),
		DefaultEnd:   time.Now().UTC().Add(36 * time.Hour).Format("2006-01-02T15:04"),
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
	forecasterRaw := r.URL.Query().Get("forecaster")
	energySourceRaw := r.URL.Query().Get("energy_source")
	horizonMinsRaw := r.URL.Query().Get("horizon_mins")
	startTimeRaw := r.URL.Query().Get("start_time")
	endTimeRaw := r.URL.Query().Get("end_time")

	if locUUID == "" || forecasterRaw == "" || energySourceRaw == "" || horizonMinsRaw == "" || startTimeRaw == "" || endTimeRaw == "" {
		http.Error(w, "Missing required query parameters", http.StatusBadRequest)
		return
	}

	parts := strings.Split(forecasterRaw, "|")
	if len(parts) != 2 {
		http.Error(w, "Invalid forecaster format", http.StatusBadRequest)
		return
	}
	fName, fVersion := parts[0], parts[1]

	energySource, _ := strconv.Atoi(energySourceRaw)
	horizonMins, _ := strconv.Atoi(horizonMinsRaw)

	startTsObj, err := time.ParseInLocation("2006-01-02T15:04", startTimeRaw, time.UTC)
	if err != nil {
		http.Error(w, "Invalid start time format", http.StatusBadRequest)
		return
	}

	endTsObj, err := time.ParseInLocation("2006-01-02T15:04", endTimeRaw, time.UTC)
	if err != nil {
		http.Error(w, "Invalid end time format", http.StatusBadRequest)
		return
	}

	req := &pb.GetForecastAsTimeseriesRequest{
		LocationUuid: locUUID,
		EnergySource: pb.EnergySource(energySource),
		HorizonMins:  uint32(horizonMins),
		TimeWindow: &pb.TimeWindow{
			StartTimestampUtc: timestamppb.New(startTsObj),
			EndTimestampUtc:   timestamppb.New(endTsObj),
		},
		Forecaster: &pb.Forecaster{
			ForecasterName:    fName,
			ForecasterVersion: fVersion,
		},
	}

	resp, err := ui.grpcClient.GetForecastAsTimeseries(ctx, req)
	if err != nil {
		log.Error().Err(err).Msg("Failed to get forecast as timeseries")
		http.Error(w, fmt.Sprintf("Failed to get forecast: %v", err), http.StatusInternalServerError)
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

	var totalFraction float32
	var count int
	for _, v := range resp.GetValues() {
		totalFraction += v.GetP50ValueFraction()
		count++
	}
	var avgFraction float32
	if count > 0 {
		avgFraction = totalFraction / float32(count)
	}

	data := struct {
		Forecast    *pb.GetForecastAsTimeseriesResponse
		Location    *pb.GetLocationResponse
		GeoJSON     string
		AvgFraction float32
	}{
		Forecast:    resp,
		Location:    locResp,
		GeoJSON:     geoJSONStr,
		AvgFraction: avgFraction,
	}

	err = tpl.ExecuteTemplate(w, "forecast_results.html", data)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
