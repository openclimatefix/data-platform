package repository

import (
	"log"
	"testing"
	"time"

	"gonum.org/v1/plot"
	"gonum.org/v1/plot/plotter"
	"gonum.org/v1/plot/vg"
)

func TestBasicYieldFunc(t *testing.T) {
	windowStart := time.Now().Add(-time.Hour * 48).Truncate(time.Hour * 24)
	windowEnd := time.Now().Add(time.Hour * 48).Truncate(time.Hour * 24)
	numMins := windowEnd.Sub(windowStart).Hours() * 60
	t.Log("numMins")

	pts := make(plotter.XYs, int(numMins))
	for i := range pts {
		ti := windowStart.Add((60 * time.Hour) + time.Minute * time.Duration(i))
		pts[i].X = float64(ti.Unix())
		pts[i].Y = float64(BasicYieldFunc(ti.Unix()))
	}

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
