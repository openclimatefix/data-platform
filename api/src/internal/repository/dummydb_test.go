package repository

import (
	"log"
	"testing"
	"time"

	"gonum.org/v1/plot"
	"gonum.org/v1/plot/plotter"
	"gonum.org/v1/plot/vg"
	"github.com/stretchr/testify/require"

)

var testClient = DummyClient{}

func Plot(pts plotter.XYs) {
	p := plot.New()
	// xticks defines how we convert and display time.Time values.
	xticks := plot.TimeTicks{Format: "2006-01-02\n15:04"}
	p.Title.Text = "Time"
	p.X.Tick.Marker = xticks
	p.Y.Label.Text = "kW"
	p.Add(plotter.NewGrid())

	line, err := plotter.NewLine(pts)
	if err != nil {
		log.Panic(err)
	}
	p.Add(line)

	err = p.Save(40*vg.Centimeter, 20*vg.Centimeter, "timeseries.png")
	if err != nil {
		log.Panic(err)
	}

}

func TestBasicYieldFunc(t *testing.T) {
	windowStart := time.Now().Add(-time.Hour * 48).Truncate(time.Hour * 24)
	windowEnd := time.Now().Add(time.Hour * 48).Truncate(time.Hour * 24)
	numMins := windowEnd.Sub(windowStart).Hours() * 60

	pts := make(plotter.XYs, int(numMins))
	for i := range pts {
		ti := windowStart.Add(time.Minute * time.Duration(i))

		pts[i].X = float64(ti.Unix())
		pts[i].Y = BasicYieldFunc(ti.Unix(), 10000.0).Yield
	}
}

func TestGetPredictedYieldsForLocation(t *testing.T) {

	locID := "test-id"
	out, err := testClient.GetPredictedYieldsForLocation(locID)
    require.NoError(t, err)
	require.NotNil(t, out)
}

func TestGetPredictedYieldForLocations(t *testing.T) {

	locIDs := []string{"test-id", "test-id2"}
	ti := time.Now().Unix()
	out, err := testClient.GetPredictedYieldForLocations(locIDs, ti)
	require.NoError(t, err)
	require.NotNil(t, out)
}
