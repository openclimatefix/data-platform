package ui

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestForecastQueryRoundTrip(t *testing.T) {
	now := time.Date(2026, 8, 7, 12, 0, 0, 0, time.UTC)

	q := defaultForecastQuery(now)
	q.LocUUID = "loc-1"
	q.Forecasters = []ForecasterInput{
		{Raw: "blend|v1|0", Name: "blend", Version: "v1", HorizonMins: 0},
	}
	q.Observers = []ObserverInput{{Raw: "pvlive_in_day", Name: "pvlive_in_day"}}

	if !q.Complete() {
		t.Fatalf("expected default query with location/sources set to be complete")
	}

	r := httptest.NewRequest(http.MethodGet, "/?"+q.Values().Encode(), nil)

	parsed, err := parseForecastQuery(r, true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if parsed.LocUUID != q.LocUUID || parsed.EnergySource != q.EnergySource {
		t.Errorf("round-trip mismatch: got %+v, want %+v", parsed, q)
	}

	if !parsed.StartTs.Equal(q.StartTs) || !parsed.EndTs.Equal(q.EndTs) {
		t.Errorf(
			"time round-trip mismatch: got %v/%v, want %v/%v",
			parsed.StartTs,
			parsed.EndTs,
			q.StartTs,
			q.EndTs,
		)
	}
}

func TestParseForecastQueryLenientAllowsPartial(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "/?location_uuid=loc-1", nil)

	q, err := parseForecastQuery(r, false)
	if err != nil {
		t.Fatalf("unexpected error for partial query in lenient mode: %v", err)
	}

	if q.Complete() {
		t.Errorf("expected partial query to be incomplete")
	}
}

func TestParseForecastQueryRejectsMalformedValues(t *testing.T) {
	cases := []string{
		"/?location_uuid=loc-1&energy_source=notanumber",
		"/?location_uuid=loc-1&start=not-a-date",
		"/?location_uuid=loc-1&time_window=notvalid",
	}

	for _, target := range cases {
		r := httptest.NewRequest(http.MethodGet, target, nil)
		if _, err := parseForecastQuery(r, false); err == nil {
			t.Errorf("expected error for malformed query %q, got nil", target)
		}
	}
}

func TestParseForecastQueryStrictRequiresCompleteness(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "/?location_uuid=loc-1", nil)

	if _, err := parseForecastQuery(r, true); err == nil {
		t.Errorf("expected error for incomplete query in strict mode")
	}
}
