package ui

import (
	"bytes"
	"html/template"
	"testing"

	pb "github.com/openclimatefix/data-platform/internal/gen/ocf/dp"
)

// Exercises the templates touched by the section-3 cleanup (mount-script extraction,
// IsPolygon removal) against realistic data, since template parsing alone (done once in
// init()) doesn't catch field-access errors that only surface at execution time.
func TestTemplatesExecute(t *testing.T) {
	loc := &pb.GetLocationResponse{
		LocationUuid:           "loc-1",
		LocationName:           "Test GSP",
		LocationType:           pb.LocationType_LOCATION_TYPE_GSP,
		EffectiveCapacityWatts: 5_000_000,
		Latlng:                 &pb.LatLng{Latitude: 51.5, Longitude: -0.1},
	}

	cases := []struct {
		name string
		tmpl string
		data any
	}{
		{
			name: "forecast_results interactive map",
			tmpl: "forecast_results.html",
			data: forecastView{
				mapView: mapView{
					Location:         loc,
					GeoJSON:          template.JS(`{"type":"FeatureCollection","features":[]}`),
					IsInteractiveMap: true,
					AvgFraction:      0.5,
					MapID:            "map",
					Labels:           []string{"Jan 01 00:00"},
					Timestamps:       []int64{1234567890},
					EnergySource:     "1",
					FirstForecaster:  "blend|v1",
					TimeWindow:       "2026-08-05 00:00 to 2026-08-07 00:00",
				},
				Series: []SeriesData{{Name: "blend (v1) @ 0m", Data: []*float32{}}},
			},
		},
		{
			name: "forecast_results location map, no series",
			tmpl: "forecast_results.html",
			data: forecastView{
				mapView: mapView{
					Location: loc,
					GeoJSON: template.JS(
						`{"type":"FeatureCollection","features":[{"geometry":{"type":"Polygon"}}]}`,
					),
					IsInteractiveMap: false,
					MapID:            "map",
					EnergySource:     "1",
				},
				Series: nil,
			},
		},
		{
			name: "location_details",
			tmpl: "location_details.html",
			data: locationView{
				mapView: mapView{
					Location:     loc,
					GeoJSON:      template.JS(`{"type":"FeatureCollection","features":[]}`),
					MapID:        "map_locations",
					EnergySource: "1",
				},
				LocationTypeString: "LOCATION_TYPE_GSP",
				EnergySourceName:   "Solar",
				HistoryLabels:      []string{"Jan 01 00:00"},
				HistoryValues:      []float64{5_000_000},
			},
		},
		{
			name: "selectors",
			tmpl: "selectors.html",
			data: struct {
				Locations       []*pb.ListLocationsResponse_LocationSummary
				Forecasters     []*pb.Forecaster
				Observers       []*pb.ListObserversResponse_ObserverSummary
				EnergySources   []energySourceOption
				SelectedEnergy  string
				TimeWindow      string
				LocationUUID    string
				LocationLabel   string
				SelectedSources []struct{ Type, Value, Label string }
			}{
				Locations: []*pb.ListLocationsResponse_LocationSummary{
					{LocationUuid: "loc-1", LocationName: "Test GSP"},
				},
				Forecasters: []*pb.Forecaster{
					{ForecasterName: "blend", ForecasterVersion: "v1"},
				},
				Observers: []*pb.ListObserversResponse_ObserverSummary{
					{ObserverName: "pvlive_in_day"},
				},
				EnergySources:  getEnergySourceOptions(),
				SelectedEnergy: "1",
				TimeWindow:     "2026-08-05 00:00 to 2026-08-07 00:00",
				LocationUUID:   "loc-1",
				LocationLabel:  "Test GSP (LOCATION_TYPE_GSP, 5.0MW)",
				SelectedSources: []struct{ Type, Value, Label string }{
					{Type: "forecaster", Value: "blend|v1|0", Label: "blend (v1) @ 0m"},
					{Type: "observer", Value: "pvlive_in_day", Label: "pvlive_in_day [Observer]"},
				},
			},
		},
		{
			name: "locations",
			tmpl: "locations.html",
			data: struct {
				Locations           []*pb.ListLocationsResponse_LocationSummary
				DefaultLocationUUID string
				DefaultEnergySource string
			}{
				Locations: []*pb.ListLocationsResponse_LocationSummary{
					{LocationUuid: "loc-1", LocationName: "Test GSP"},
				},
				DefaultLocationUUID: "loc-1",
				DefaultEnergySource: "1",
			},
		},
		{
			name: "forecasts",
			tmpl: "forecasts.html",
			data: struct{ Query template.URL }{Query: template.URL("location_uuid=loc-1")},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			if err := tpl.ExecuteTemplate(&buf, tc.tmpl, tc.data); err != nil {
				t.Fatalf("ExecuteTemplate(%s) failed: %v", tc.tmpl, err)
			}

			if buf.Len() == 0 {
				t.Fatalf("ExecuteTemplate(%s) produced no output", tc.tmpl)
			}
		})
	}
}
