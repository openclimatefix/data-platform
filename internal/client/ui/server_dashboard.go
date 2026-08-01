package ui

import (
	"context"
	"encoding/json"
	"fmt"
	"html/template"
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

func (ui *UIClient) handleDashboardSelectors(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	locType := pb.LocationType_LOCATION_TYPE_NATION
	locResp, err := ui.grpcClient.ListLocations(ctx, &pb.ListLocationsRequest{
		LocationTypeFilter: &locType,
	})
	if err != nil {
		log.Error().Err(err).Msg("Dashboard: Failed to list locations")
		http.Error(w, fmt.Sprintf("Failed to list locations: %v", err), http.StatusInternalServerError)
		return
	}

	fcResp, err := ui.grpcClient.ListForecasters(ctx, &pb.ListForecastersRequest{})
	if err != nil {
		log.Error().Err(err).Msg("Dashboard: Failed to list forecasters")
		http.Error(w, fmt.Sprintf("Failed to list forecasters: %v", err), http.StatusInternalServerError)
		return
	}

	data := struct {
		Locations         []*pb.ListLocationsResponse_LocationSummary
		Forecasters       []*pb.Forecaster
		DefaultTimeWindow string
	}{
		Locations:   locResp.GetLocations(),
		Forecasters: fcResp.GetForecasters(),
		DefaultTimeWindow: fmt.Sprintf("%s to %s",
			time.Now().UTC().Add(-48*time.Hour).Format("2006-01-02 15:04"),
			time.Now().UTC().Add(36*time.Hour).Format("2006-01-02 15:04"),
		),
	}

	if err := tpl.ExecuteTemplate(w, "dashboard_selectors.html", data); err != nil {
		log.Error().Err(err).Msg("Failed to execute dashboard_selectors template")
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (ui *UIClient) handleDashboardForecast(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	locUUID := r.URL.Query().Get("location_uuid")
	fRaw := r.URL.Query().Get("forecaster")
	energySourceRaw := r.URL.Query().Get("energy_source")
	horizonMinsRaw := r.URL.Query().Get("horizon_mins")
	timeWindowRaw := r.URL.Query().Get("time_window")

	if locUUID == "" || fRaw == "" || energySourceRaw == "" || horizonMinsRaw == "" || timeWindowRaw == "" {
		http.Error(w, "Missing required query parameters", http.StatusBadRequest)
		return
	}

	energySource, _ := strconv.Atoi(energySourceRaw)
	horizonMins, _ := strconv.Atoi(horizonMinsRaw)
	parts := strings.Split(timeWindowRaw, " to ")
	startTsObj, _ := time.ParseInLocation("2006-01-02 15:04", parts[0], time.UTC)
	endTsObj, _ := time.ParseInLocation("2006-01-02 15:04", parts[1], time.UTC)
	fParts := strings.Split(fRaw, "|")
	fName, fVer := fParts[0], ""
	if len(fParts) > 1 {
		fVer = fParts[1]
	}

	locReq := &pb.GetLocationRequest{
		LocationUuid: locUUID,
		EnergySource: pb.EnergySource(energySource),
	}
	locResp, err := ui.grpcClient.GetLocation(ctx, locReq)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to get location: %v", err), http.StatusInternalServerError)
		return
	}

	capacity := float32(locResp.GetEffectiveCapacityWatts())

	var timeseriesResp *pb.GetForecastAsTimeseriesResponse
	var geoJSONStr string
	type GSPValue struct {
		Name     string  `json:"name"`
		Title    string  `json:"title"`
		Value    float32 `json:"value"`
		Capacity uint64  `json:"capacity"`
	}
	initialMapData := []GSPValue{}

	g, gCtx := errgroup.WithContext(ctx)

	// Fetch Nation Timeseries
	g.Go(func() error {
		req := &pb.GetForecastAsTimeseriesRequest{
			LocationUuid: locUUID,
			EnergySource: pb.EnergySource(energySource),
			HorizonMins:  uint32(horizonMins),
			TimeWindow: &pb.TimeWindow{
				StartTimestampUtc: timestamppb.New(startTsObj),
				EndTimestampUtc:   timestamppb.New(endTsObj),
			},
			Forecaster: &pb.Forecaster{ForecasterName: fName, ForecasterVersion: fVer},
		}
		resp, err := ui.grpcClient.GetForecastAsTimeseries(gCtx, req)
		if err == nil {
			timeseriesResp = resp
		}
		return err
	})

	// Fetch GSP Geometries and Initial map state
	g.Go(func() error {
		locTypeGSP := pb.LocationType_LOCATION_TYPE_GSP
		gspResp, err := ui.grpcClient.ListLocations(gCtx, &pb.ListLocationsRequest{
			EnclosingLocationUuidFilter: &locUUID,
			LocationTypeFilter:          &locTypeGSP,
		})
		if err != nil {
			return err
		}

		var gspUUIDs []string
		for _, l := range gspResp.GetLocations() {
			gspUUIDs = append(gspUUIDs, l.GetLocationUuid())
		}
		if len(gspUUIDs) == 0 {
			return nil
		}

		geoReq := &pb.GetLocationsAsGeoJSONRequest{
			LocationUuids: gspUUIDs,
			Unsimplified:  false,
		}
		geoResp, err := ui.grpcClient.GetLocationsAsGeoJSON(gCtx, geoReq)
		if err == nil && geoResp != nil {
			geoJSONStr = geoResp.GetGeojson()
		}

		// Let JavaScript load the initial map data once it aligns the timestamps.
		// initialMapData is left as []

		return nil
	})

	if err := g.Wait(); err != nil {
		log.Warn().Err(err).Msg("Partial failure in dashboard forecast fetch")
	}

	uniqueTimes := make(map[int64]time.Time)
	seriesMap := make(map[int64]float32)
	bandLowerMap := make(map[int64]float32)
	bandUpperMap := make(map[int64]float32)

	if timeseriesResp != nil {
		for _, v := range timeseriesResp.GetValues() {
			t := v.GetTargetTimestampUtc().AsTime()
			unix := t.Unix()
			uniqueTimes[unix] = t
			seriesMap[unix] = v.GetP50ValueFraction() * capacity

			stats := v.GetOtherStatisticsFractions()
			if stats != nil {
				if p10, ok := stats["p10"]; ok {
					if p90, ok := stats["p90"]; ok {
						bandLowerMap[unix] = p10 * capacity
						bandUpperMap[unix] = p90 * capacity
					}
				}
			}
		}
	}

	var timeKeys []int64
	for k := range uniqueTimes {
		timeKeys = append(timeKeys, k)
	}
	slices.Sort(timeKeys)

	var labels []string
	var timestamps []int64
	for _, k := range timeKeys {
		labels = append(labels, uniqueTimes[k].Format("Jan 02 15:04"))
		timestamps = append(timestamps, k)
	}

	sd := SeriesData{Name: fmt.Sprintf("%s (v%s)", fName, fVer)}
	hasBands := len(bandLowerMap) > 0
	sd.HasBands = hasBands

	for _, k := range timeKeys {
		if val, ok := seriesMap[k]; ok {
			v := val
			sd.Data = append(sd.Data, &v)
		} else {
			sd.Data = append(sd.Data, nil)
		}
		if hasBands {
			var bLow, bUp float32
			var hasLow, hasUp bool

			if val, ok := bandLowerMap[k]; ok {
				bLow = val
				hasLow = true
				sd.BandLower = append(sd.BandLower, &bLow)
			} else {
				sd.BandLower = append(sd.BandLower, nil)
			}
			if val, ok := bandUpperMap[k]; ok {
				bUp = val
				hasUp = true
			}
			if hasLow && hasUp {
				diff := bUp - bLow
				sd.BandDiff = append(sd.BandDiff, &diff)
			} else {
				sd.BandDiff = append(sd.BandDiff, nil)
			}
		}
	}

	var jsGeoJSON template.JS = "null"
	if geoJSONStr != "" {
		jsGeoJSON = template.JS(geoJSONStr)
	}

	data := struct {
		Location       *pb.GetLocationResponse
		Labels         []string
		Timestamps     []int64
		Series         []SeriesData
		EnergySource   string
		Forecaster     string
		TimeWindow     string
		Horizon        string
		GSPGeoJSON     template.JS
		InitialMapData []GSPValue
	}{
		Location:       locResp,
		Labels:         labels,
		Timestamps:     timestamps,
		Series:         []SeriesData{sd},
		EnergySource:   energySourceRaw,
		Forecaster:     fRaw,
		TimeWindow:     timeWindowRaw,
		Horizon:        horizonMinsRaw,
		GSPGeoJSON:     jsGeoJSON,
		InitialMapData: initialMapData,
	}

	if err := tpl.ExecuteTemplate(w, "dashboard_forecast.html", data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (ui *UIClient) handleDashboardGSPTimeseries(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	locUUID := r.URL.Query().Get("location_uuid")
	fRaw := r.URL.Query().Get("forecaster")
	energySourceRaw := r.URL.Query().Get("energy_source")
	horizonMinsRaw := r.URL.Query().Get("horizon_mins")
	timeWindowRaw := r.URL.Query().Get("time_window")

	if locUUID == "" || fRaw == "" || energySourceRaw == "" || horizonMinsRaw == "" || timeWindowRaw == "" {
		http.Error(w, "Missing required query parameters", http.StatusBadRequest)
		return
	}

	energySource, _ := strconv.Atoi(energySourceRaw)
	horizonMins, _ := strconv.Atoi(horizonMinsRaw)
	parts := strings.Split(timeWindowRaw, " to ")
	startTsObj, _ := time.ParseInLocation("2006-01-02 15:04", parts[0], time.UTC)
	endTsObj, _ := time.ParseInLocation("2006-01-02 15:04", parts[1], time.UTC)
	fParts := strings.Split(fRaw, "|")
	fName, fVer := fParts[0], ""
	if len(fParts) > 1 {
		fVer = fParts[1]
	}

	locReq := &pb.GetLocationRequest{
		LocationUuid: locUUID,
		EnergySource: pb.EnergySource(energySource),
	}
	locResp, err := ui.grpcClient.GetLocation(ctx, locReq)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to get location: %v", err), http.StatusInternalServerError)
		return
	}

	capacity := float32(locResp.GetEffectiveCapacityWatts())

	req := &pb.GetForecastAsTimeseriesRequest{
		LocationUuid: locUUID,
		EnergySource: pb.EnergySource(energySource),
		HorizonMins:  uint32(horizonMins),
		TimeWindow: &pb.TimeWindow{
			StartTimestampUtc: timestamppb.New(startTsObj),
			EndTimestampUtc:   timestamppb.New(endTsObj),
		},
		Forecaster: &pb.Forecaster{ForecasterName: fName, ForecasterVersion: fVer},
	}
	resp, err := ui.grpcClient.GetForecastAsTimeseries(ctx, req)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to get forecast: %v", err), http.StatusInternalServerError)
		return
	}

	uniqueTimes := make(map[int64]time.Time)
	seriesMap := make(map[int64]float32)
	bandLowerMap := make(map[int64]float32)
	bandUpperMap := make(map[int64]float32)

	for _, v := range resp.GetValues() {
		t := v.GetTargetTimestampUtc().AsTime()
		unix := t.Unix()
		uniqueTimes[unix] = t
		seriesMap[unix] = v.GetP50ValueFraction() * capacity

		stats := v.GetOtherStatisticsFractions()
		if stats != nil {
			if p10, ok := stats["p10"]; ok {
				if p90, ok := stats["p90"]; ok {
					bandLowerMap[unix] = p10 * capacity
					bandUpperMap[unix] = p90 * capacity
				}
			}
		}
	}

	var timeKeys []int64
	for k := range uniqueTimes {
		timeKeys = append(timeKeys, k)
	}
	slices.Sort(timeKeys)

	var labels []string
	var timestamps []int64
	for _, k := range timeKeys {
		labels = append(labels, uniqueTimes[k].Format("Jan 02 15:04"))
		timestamps = append(timestamps, k)
	}

	sd := SeriesData{Name: fmt.Sprintf("%s (v%s)", fName, fVer)}
	hasBands := len(bandLowerMap) > 0
	sd.HasBands = hasBands

	for _, k := range timeKeys {
		if val, ok := seriesMap[k]; ok {
			v := val
			sd.Data = append(sd.Data, &v)
		} else {
			sd.Data = append(sd.Data, nil)
		}
		if hasBands {
			var bLow, bUp float32
			var hasLow, hasUp bool

			if val, ok := bandLowerMap[k]; ok {
				bLow = val
				hasLow = true
				sd.BandLower = append(sd.BandLower, &bLow)
			} else {
				sd.BandLower = append(sd.BandLower, nil)
			}
			if val, ok := bandUpperMap[k]; ok {
				bUp = val
				hasUp = true
			}
			if hasLow && hasUp {
				diff := bUp - bLow
				sd.BandDiff = append(sd.BandDiff, &diff)
			} else {
				sd.BandDiff = append(sd.BandDiff, nil)
			}
		}
	}

	data := struct {
		Location   *pb.GetLocationResponse
		Labels     []string
		Timestamps []int64
		Series     []SeriesData
	}{
		Location:   locResp,
		Labels:     labels,
		Timestamps: timestamps,
		Series:     []SeriesData{sd},
	}

	if err := tpl.ExecuteTemplate(w, "dashboard_chart_partial.html", data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (ui *UIClient) handleDashboardMapSnapshot(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	tsRaw := r.URL.Query().Get("timestamp")
	nationUUID := r.URL.Query().Get("nation_uuid")
	fRaw := r.URL.Query().Get("forecaster")
	energySourceRaw := r.URL.Query().Get("energy_source")

	if tsRaw == "" || fRaw == "" || energySourceRaw == "" || nationUUID == "" {
		http.Error(w, "Missing required query parameters", http.StatusBadRequest)
		return
	}

	tsUnix, err := strconv.ParseInt(tsRaw, 10, 64)
	if err != nil {
		http.Error(w, "Invalid timestamp format", http.StatusBadRequest)
		return
	}
	tsObj := time.Unix(tsUnix, 0).UTC()

	energySource, _ := strconv.Atoi(energySourceRaw)
	
	fParts := strings.Split(fRaw, "|")
	fName, fVer := fParts[0], ""
	if len(fParts) > 1 {
		fVer = fParts[1]
	}

	// We need GSPs within the specific Nation.
	locTypeGSP := pb.LocationType_LOCATION_TYPE_GSP
	gspResp, err := ui.grpcClient.ListLocations(ctx, &pb.ListLocationsRequest{
		EnclosingLocationUuidFilter: &nationUUID,
		LocationTypeFilter:          &locTypeGSP,
	})
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to list GSPs: %v", err), http.StatusInternalServerError)
		return
	}

	var gspUUIDs []string
	for _, l := range gspResp.GetLocations() {
		gspUUIDs = append(gspUUIDs, l.GetLocationUuid())
	}

	type GSPValue struct {
		Name     string  `json:"name"`
		Value    float32 `json:"value"`
		Capacity uint64  `json:"capacity"`
	}
	mapData := []GSPValue{}

	if len(gspUUIDs) > 0 {
		mapReq := &pb.GetForecastAtTimestampRequest{
			LocationUuids: gspUUIDs,
			EnergySource:  pb.EnergySource(energySource),
			TimestampUtc:  timestamppb.New(tsObj),
			Forecaster:    &pb.Forecaster{ForecasterName: fName, ForecasterVersion: fVer},
		}
		mapResp, err := ui.grpcClient.GetForecastAtTimestamp(ctx, mapReq)
		if err == nil && mapResp != nil {
			for _, v := range mapResp.GetValues() {
				mapData = append(mapData, GSPValue{
					Name:     v.GetLocationUuid(),
					Value:    v.GetValueFraction(),
					Capacity: v.GetEffectiveCapacityWatts(),
				})
			}
		} else {
			log.Warn().Err(err).Msg("Failed to get forecast at timestamp")
		}
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(mapData); err != nil {
		log.Error().Err(err).Msg("Failed to encode map snapshot JSON")
	}
}
