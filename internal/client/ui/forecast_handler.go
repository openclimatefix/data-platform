package ui

import (
	"context"
	"fmt"
	"html/template"
	"net/http"
	"strconv"
	"time"

	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"

	pb "github.com/openclimatefix/data-platform/internal/gen/ocf/dp"
)

func (ui *UIClient) handleForecast(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	p, err := parseForecastRequest(r)
	if err != nil {
		httpError(w, r, err.Error(), http.StatusBadRequest, nil)
		return
	}

	var (
		locResp    *pb.GetLocationResponse
		locErr     error
		results    []seriesResult
		obsResults []seriesResult

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
			return fmt.Errorf("failed to get location: %w", locErr)
		}
		return nil
	})

	// Fetch Forecasters
	g.Go(func() error {
		results = fetchForecasters(gCtx, ui.grpcClient, p)
		return nil
	})

	// Fetch Observers
	g.Go(func() error {
		obsResults = fetchObservers(gCtx, ui.grpcClient, p)
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
		httpError(w, r, "Failed to load forecast data", http.StatusInternalServerError, err)
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

	data := forecastView{
		mapView: mapView{
			Location:         locResp,
			GeoJSON:          geoJSONStr,
			AvgFraction:      avgFraction,
			Labels:           labels,
			Timestamps:       timeKeys,
			IsInteractiveMap: isInteractiveMap,
			IsPolygon:        isPolygon(geoJSONStr),
			EnergySource:     strconv.Itoa(int(p.EnergySource)),
			FirstForecaster:  firstForecaster,
			TimeWindow:       p.StartTs.Format("2006-01-02 15:04") + " to " + p.EndTs.Format("2006-01-02 15:04"),
			MapID:            "map",
		},
		Series:  allSeries,
		SkipMap: p.SkipMap,
	}

	w.Header().Set("HX-Push-URL", "/?"+p.URLValues().Encode())
	render(w, r, "forecast_results.html", data)
}
