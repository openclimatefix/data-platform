package ui

import (
	"context"
	"encoding/json"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/rs/zerolog/log"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/openclimatefix/data-platform/internal/gen/ocf/dp"
)

func (ui *UIClient) handleDashboardMapSnapshot(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	tsRaw := r.URL.Query().Get("timestamp")
	nationUUID := r.URL.Query().Get("nation_uuid")
	fRaw := r.URL.Query().Get("forecaster")
	energySourceRaw := r.URL.Query().Get("energy_source")

	if tsRaw == "" || fRaw == "" || energySourceRaw == "" || nationUUID == "" {
		httpError(w, r, "Missing required query parameters", http.StatusBadRequest, nil)
		return
	}

	tsUnix, err := strconv.ParseInt(tsRaw, 10, 64)
	if err != nil {
		httpError(w, r, "Invalid timestamp format", http.StatusBadRequest, err)
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
		httpError(w, r, "Failed to list GSPs", http.StatusInternalServerError, err)
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
					Value:    float32(math.Round(float64(v.GetValueFraction())*1000) / 1000),
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
