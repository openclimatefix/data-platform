package ui

import (
	"testing"
	"time"
)

func TestBuildChartSeries(t *testing.T) {
	t.Run("basic observation with gaps", func(t *testing.T) {
		t1 := time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC)
		t2 := t1.Add(time.Hour)

		obsResults := []seriesResult{
			{
				Raw: "obs1",
				SeriesMap: map[int64]float32{
					t1.Unix(): 0.5,
				},
				UniqueT: map[int64]time.Time{
					t1.Unix(): t1,
					t2.Unix(): t2, // Note: t2 is missing data
				},
			},
		}

		allSeries, labels, timeKeys, avgFraction := buildChartSeries(
			nil,
			obsResults,
			nil,
			[]ObserverInput{{Raw: "obs1", Name: "Observer 1"}},
			1000,
		)

		if len(allSeries) != 1 {
			t.Fatalf("expected 1 series, got %d", len(allSeries))
		}

		if len(labels) != 2 || len(timeKeys) != 2 {
			t.Fatalf("expected 2 labels/timeKeys")
		}

		if avgFraction != 0 {
			t.Fatalf("expected 0 avgFraction for no forecasters")
		}

		// First data point is 0.5 * 1000 = 500
		if allSeries[0].Data[0] == nil || *allSeries[0].Data[0] != 500.0 {
			t.Errorf("expected 500.0, got %v", allSeries[0].Data[0])
		}

		// Second data point is nil
		if allSeries[0].Data[1] != nil {
			t.Errorf("expected nil for gap, got %v", allSeries[0].Data[1])
		}
	})

	t.Run("band pairing", func(t *testing.T) {
		t1 := time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC)

		fcResults := []seriesResult{
			{
				Raw: "fc1",
				SeriesMap: map[int64]float32{
					t1.Unix(): 0.5,
				},
				StatsMap: map[int64]map[string]float32{
					t1.Unix(): {
						"p10": 0.2,
						"p90": 0.8,
					},
				},
				UniqueT: map[int64]time.Time{
					t1.Unix(): t1,
				},
				FractionSum: 0.5,
				Count:       1,
			},
		}

		allSeries, labels, timeKeys, avgFraction := buildChartSeries(
			fcResults,
			nil,
			[]ForecasterInput{{Raw: "fc1", Name: "FC1", Version: "v1"}},
			nil,
			1000,
		)

		if avgFraction != 0.5 {
			t.Errorf("expected avgFraction 0.5, got %v", avgFraction)
		}

		if len(labels) != 1 || len(timeKeys) != 1 {
			t.Fatalf("expected 1 label/timeKey")
		}

		if !allSeries[0].HasBands {
			t.Errorf("expected HasBands to be true")
		}

		if len(allSeries[0].Bands) != 1 {
			t.Fatalf("expected 1 band, got %d", len(allSeries[0].Bands))
		}

		band := allSeries[0].Bands[0]
		if band.Level != 80 {
			t.Errorf("expected band level 80, got %d", band.Level)
		}

		if band.LowerName != "p10" || band.UpperName != "p90" {
			t.Errorf("expected p10/p90 pair, got %s/%s", band.LowerName, band.UpperName)
		}

		if *band.Lower[0] != 200.0 {
			t.Errorf("expected lower 200.0, got %v", *band.Lower[0])
		}

		if *band.Diff[0] != 600.0 {
			t.Errorf("expected diff 600.0, got %v", *band.Diff[0])
		}
	})
}
