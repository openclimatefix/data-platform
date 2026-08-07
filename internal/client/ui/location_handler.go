package ui

import (
	"context"
	"html/template"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/rs/zerolog/log"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/openclimatefix/data-platform/internal/gen/ocf/dp"
)

func (ui *UIClient) handleLocations(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	locResp, err := ui.grpcClient.ListLocations(ctx, &pb.ListLocationsRequest{})
	if err != nil {
		log.Error().Err(err).Msg("Failed to list locations")
		http.Error(w, "Failed to load locations", http.StatusInternalServerError)
		return
	}

	defaultLocUUID := r.URL.Query().Get("location_uuid")
	
	defaultEnergy := r.URL.Query().Get("energy_source")
	if defaultEnergy == "" {
		defaultEnergy = "1"
	}

	data := struct {
		Locations           []*pb.ListLocationsResponse_LocationSummary
		DefaultLocationUUID string
		DefaultEnergySource string
	}{
		Locations:           locResp.GetLocations(),
		DefaultLocationUUID: defaultLocUUID,
		DefaultEnergySource: defaultEnergy,
	}

	if err := tpl.ExecuteTemplate(w, "locations.html", data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (ui *UIClient) handleLocationDetails(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	locUUID := r.URL.Query().Get("location_uuid")
	if locUUID == "" {
		http.Error(w, "Missing location_uuid", http.StatusBadRequest)
		return
	}

	esRaw := r.URL.Query().Get("energy_source")
	es, err := strconv.Atoi(esRaw)
	if err != nil {
		es = int(pb.EnergySource_ENERGY_SOURCE_SOLAR)
	}
	energySource := pb.EnergySource(es)

	locReq := &pb.GetLocationRequest{
		LocationUuid: locUUID,
		EnergySource: energySource,
	}

	locResp, err := ui.grpcClient.GetLocation(ctx, locReq)
	if err != nil {
		log.Error().Err(err).Msg("Failed to get location")
		http.Error(w, "Failed to load location details", http.StatusInternalServerError)
		return
	}

	// Fetch geojson
	geoResp, _ := ui.grpcClient.GetLocationsAsGeoJSON(ctx, &pb.GetLocationsAsGeoJSONRequest{
		LocationUuids: []string{locUUID},
		Unsimplified:  true,
	})

	var geoJSONStr template.JS = "null"
	if geoResp != nil && geoResp.GetGeojson() != "" {
		geoJSONStr = template.JS(geoResp.GetGeojson())
	}

	// Fetch timeseries history (capacity schedule)
	// Query back exactly 31 days to avoid exceeding the 1-year constraint.
	tsReq := &pb.GetLocationAsTimeseriesRequest{
		LocationUuid: locUUID,
		EnergySource: energySource,
		TimeWindow: &pb.TimeWindow{
			StartTimestampUtc: timestamppb.New(time.Now().UTC().AddDate(0, 0, -31)),
			EndTimestampUtc:   timestamppb.New(time.Now().UTC().AddDate(0, 0, 31)),
		},
	}
	tsResp, err := ui.grpcClient.GetLocationAsTimeseries(ctx, tsReq)

	var historyLabels []string
	var historyValues []float64

	if err == nil && tsResp != nil && len(tsResp.GetValues()) > 0 {
		for _, v := range tsResp.GetValues() {
			historyLabels = append(historyLabels, v.GetTimestampUtc().AsTime().Format(time.RFC3339))
			historyValues = append(historyValues, float64(v.GetEffectiveCapacityWatts()))
		}
	} else {
		if err != nil {
			log.Warn().Err(err).Msg("Failed to fetch location timeseries history")
		}
		
		// If history is empty, fallback to current capacity so ECharts doesn't crash on empty arrays
		historyLabels = []string{time.Now().UTC().AddDate(0, 0, -31).Format(time.RFC3339)}
		historyValues = []float64{float64(locResp.GetEffectiveCapacityWatts())}
	}

	esName := "Solar"
	if energySource == pb.EnergySource_ENERGY_SOURCE_WIND {
		esName = "Wind"
	}

	data := struct {
		Location           *pb.GetLocationResponse
		LocationTypeString string
		EnergySource       int32
		EnergySourceName   string
		GeoJSON            template.JS
		IsPolygon          bool
		IsInteractiveMap   bool
		AvgFraction        float32
		Labels             []string
		Timestamps         []int64
		MapID              string
		HistoryLabels      []string
		HistoryValues      []float64
	}{
		Location:           locResp,
		LocationTypeString: locResp.GetLocationType().String(),
		EnergySource:       int32(energySource.Number()),
		EnergySourceName:   esName,
		GeoJSON:            geoJSONStr,
		IsPolygon:          strings.Contains(string(geoJSONStr), `"Polygon"`) || strings.Contains(string(geoJSONStr), `"MultiPolygon"`),
		IsInteractiveMap:   false,
		AvgFraction:        1.0,
		Labels:             nil,
		Timestamps:         nil,
		MapID:              "map_locations",
		HistoryLabels:      historyLabels,
		HistoryValues:      historyValues,
	}

	if err := tpl.ExecuteTemplate(w, "location_details.html", data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (ui *UIClient) handleLocationEdit(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	if err := r.ParseForm(); err != nil {
		http.Error(w, "Failed to parse form", http.StatusBadRequest)
		return
	}

	uuid := r.FormValue("location_uuid")
	energySourceStr := r.FormValue("energy_source")
	validFromStr := r.FormValue("valid_from_utc")
	capacityStr := r.FormValue("capacity")
	
	// We do not have geometry update in the proto right now, but we will leave the template 
	// for future implementation or for use in another method. For now, we update capacity.
	
	energySource, _ := strconv.Atoi(energySourceStr)

	validFrom, err := time.Parse("2006-01-02 15:04", validFromStr)
	if err != nil {
		validFrom = time.Now().UTC()
	}

	req := &pb.UpdateLocationRequest{
		LocationUuid: uuid,
		EnergySource: pb.EnergySource(energySource),
		ValidFromUtc: timestamppb.New(validFrom.UTC()),
	}

	var hasUpdate bool
	if capacityStr != "" {
		if capWatts, err := strconv.ParseUint(capacityStr, 10, 64); err == nil {
			req.NewEffectiveCapacityWatts = &capWatts
			hasUpdate = true
		}
	}

	if !hasUpdate {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	_, err = ui.grpcClient.UpdateLocation(ctx, req)
	if err != nil {
		log.Error().Err(err).Msg("Failed to update location")
		http.Error(w, "Failed to update location: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("HX-Trigger", "locationUpdated")
	w.WriteHeader(http.StatusNoContent)
}
