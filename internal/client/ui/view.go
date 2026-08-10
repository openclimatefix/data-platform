package ui

import (
	"fmt"
	"html/template"

	pb "github.com/openclimatefix/data-platform/internal/gen/ocf/dp"
)

type mapView struct {
	Location         *pb.GetLocationResponse
	GeoJSON          template.JS
	IsInteractiveMap bool
	AvgFraction      float32
	MapID            string
	Labels           []string
	Timestamps       []int64
	EnergySource     string
	FirstForecaster  string
	// TimeWindow is not read by any Go/template code; it is passed through to map.js's
	// choropleth config, which round-trips it into the GSP drill-down request URL.
	TimeWindow string
}

type forecastView struct {
	mapView
	Series  []SeriesData
	SkipMap bool
}

type SeriesData struct {
	Name          string                `json:"name"`
	Data          []*float32            `json:"data"`
	HasBands      bool                  `json:"hasBands"`
	Bands         []BandLayer           `json:"bands"`
	BandMap       []map[string]*float32 `json:"bandMap"`
	IsObservation bool                  `json:"isObservation"`
}

type BandLayer struct {
	Level     int        `json:"level"`
	LowerName string     `json:"lowerName"`
	UpperName string     `json:"upperName"`
	Lower     []*float32 `json:"lower"`
	Diff      []*float32 `json:"diff"`
}

type energySourceOption struct {
	Value int32
	Name  string
}

// formatCapacityString renders a watt value with the same units used throughout the UI
// (MW/kW/W). Shared between the "formatCapacity" template func and handleSelectors' location
// label, which need identical formatting in Go rather than a template-only helper.
func formatCapacityString(watts uint64) string {
	if watts >= 1_000_000 {
		return fmt.Sprintf("%.1fMW", float64(watts)/1_000_000.0)
	} else if watts >= 1_000 {
		return fmt.Sprintf("%.1fkW", float64(watts)/1_000.0)
	}

	return fmt.Sprintf("%dW", watts)
}

func getEnergySourceOptions() []energySourceOption {
	return []energySourceOption{
		{Value: int32(pb.EnergySource_ENERGY_SOURCE_SOLAR), Name: "Solar"},
		{Value: int32(pb.EnergySource_ENERGY_SOURCE_WIND), Name: "Wind"},
	}
}

func energySourceName(es int32) string {
	for _, o := range getEnergySourceOptions() {
		if o.Value == es {
			return o.Name
		}
	}

	return "Unknown"
}

type locationView struct {
	mapView
	LocationTypeString string
	EnergySourceName   string
	HistoryLabels      []string
	HistoryValues      []float64
}
