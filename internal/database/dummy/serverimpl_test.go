package dummy

import (
	// "os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	// "gonum.org/v1/plot"
	// "gonum.org/v1/plot/plotter"
	// "gonum.org/v1/plot/vg"
	// "gonum.org/v1/plot/vg/draw"
	// "gonum.org/v1/plot/vg/vgimg"
)

func TestDetermineIrradience(t *testing.T) {
	t.Parallel()
	// Test the function with a known date and location
	date := time.Date(2019, time.January, 1, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name     string
		date     time.Time
		lngLat   lnglat
		expected SolarData
	}{
		{
			"Equator has expected solar properties",
			date,
			lnglat{5, 0},
			SolarData{
				sunriseTimeTst: date.Truncate(24 * time.Hour).Add(6 * time.Hour),
				sunsetTimeTst:  date.Truncate(24 * time.Hour).Add(18 * time.Hour),
				daylengthHours: 12,
			},
		},
		{
			"Equator is invarient of longitude and date",
			date.AddDate(3452, 15, 242),
			lnglat{60, 0},
			SolarData{
				sunriseTimeTst: date.AddDate(3452, 15, 242).Truncate(24 * time.Hour).Add(6 * time.Hour),
				sunsetTimeTst:  date.AddDate(3452, 15, 242).Truncate(24 * time.Hour).Add(18 * time.Hour),
				daylengthHours: 12,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sd := determineIrradiance(tt.date, tt.lngLat)
			require.Equal(t, tt.expected.sunriseTimeTst, sd.sunriseTimeTst)
			require.Equal(t, tt.expected.sunsetTimeTst, sd.sunsetTimeTst)
			require.Equal(t, tt.expected.daylengthHours, sd.daylengthHours)
		})
	}
}

/*
func plotHelper(tb testing.TB, filename string) {
	tb.Helper()
	p := plot.New()
	// xticks defines how we convert and display time.Time values.
	// xticks := plot.TimeTicks{Format: "2006-01-02\n15:04"}
	p.Title.Text = filename
	// p.X.Tick.Marker = xticks
	p.Add(plotter.NewGrid())

	tb.Cleanup(func() {})

	err := p.Save(40*vg.Centimeter, 20*vg.Centimeter, filename)
	require.NoError(tb, err)
}

func TestPlots(t *testing.T) {
	const rows, cols = 4, 2
	plots := make([][]*plot.Plot, rows)
	for j := range rows {
		plots[j] = make([]*plot.Plot, cols)
	}

	startDate := time.Date(2023, time.January, 1, 0, 0, 0, 0, time.UTC)

	yearPts := make(plotter.XYs, 365)
	yearSds := make([]SolarData, 365)
	for i := range yearPts {
		t := startDate.AddDate(0, 0, i)
		yearPts[i].X = float64(t.YearDay())
		yearSds[i] = determineIrradience(t, lnglat{5, 0})
	}

	dayPts := make(plotter.XYs, 24*60) // 3 days, 60 points per hour
	daySds := make([]SolarData, 24*60)
	for i := range dayPts {
		t := startDate.Add(time.Duration(float64(i) * float64(time.Minute)))
		dayPts[i].X = float64(i) / 60.0
		daySds[i] = determineIrradience(t, lnglat{5, 0})
	}

	// Plot the solar declination over a year
	solarDeclinationPlot := plot.New()
	solarDeclinationPlot.Title.Text = "Solar Declination over a Year"
	solarDeclinationPlot.Add(plotter.NewGrid())
	for i, sd := range yearSds {
		yearPts[i].Y = sd.declinationRadians
	}
	line, err := plotter.NewLine(yearPts)
	require.NoError(t, err)
	solarDeclinationPlot.Add(line)

	// Plot the equation of time over a year
	equationOfTimePlot := plot.New()
	equationOfTimePlot.Title.Text = "Equation of Time over a Year"
	equationOfTimePlot.Add(plotter.NewGrid())
	for i, sd := range yearSds {
		yearPts[i].Y = sd.eotCorrection.Hours()
	}
	line, err = plotter.NewLine(yearPts)
	require.NoError(t, err)
	equationOfTimePlot.Add(line)

	// Plot the solar zenith angle over a year
	solarZenithAnglePlot := plot.New()
	solarZenithAnglePlot.Title.Text = "Solar Zenith Angle over a Year (equator)"
	solarZenithAnglePlot.Add(plotter.NewGrid())
	for i, sd := range yearSds {
		yearPts[i].Y = sd.zenithRadians
	}
	line, err = plotter.NewLine(yearPts)
	require.NoError(t, err)
	solarZenithAnglePlot.Add(line)

	// Plot the sunrise time in TST over a year
	sunrisePlot := plot.New()
	sunrisePlot.Title.Text = "Sunrise Time in MST over a Year"
	sunrisePlot.Add(plotter.NewGrid())
	sunrisePlot.X.Label.Text = "Day of Year"
	for i, sd := range yearSds {
		yearPts[i].Y = float64(sd.sunriseTimeMst.Hour()) + float64(sd.sunriseTimeMst.Minute())/60.0
	}
	line, err = plotter.NewLine(yearPts)
	require.NoError(t, err)
	sunrisePlot.Add(line)

	// Plot the Extraterrestrial Irradiance over a day
	// Do this for the solstices and summer
	extraterrestrialPlot := plot.New()
	extraterrestrialPlot.Title.Text = "Extraterrestrial Irradiation over a day"
	extraterrestrialPlot.Add(plotter.NewGrid())
	extraterrestrialPlot.X.Label.Text = "Time of Day (hours)"
	for i, sd := range daySds {
		dayPts[i].Y = sd.extraterrestrialIrradiance
	}
	line, err = plotter.NewLine(dayPts)
	require.NoError(t, err)
	extraterrestrialPlot.Add(line)

	// Align the plots on the same image and save
	plots[0][0] = solarDeclinationPlot
	plots[1][0] = equationOfTimePlot
	plots[2][0] = solarZenithAnglePlot
	plots[3][0] = sunrisePlot
	plots[0][1] = extraterrestrialPlot
	img := vgimg.New(vg.Points(cols*300), vg.Points(rows*300))
	dc := draw.New(img)
	canvases := plot.Align(plots, draw.Tiles{Rows: rows, Cols: cols}, dc)

	for j := range rows {
		for i := range cols {
			if plots[j][i] != nil {
				plots[j][i].Draw(canvases[j][i])
			}
		}
	}

	w, err := os.Create("test.png")
	require.NoError(t, err)
	defer w.Close()
	png := vgimg.PngCanvas{Canvas: img}
	_, err = png.WriteTo(w)
	require.NoError(t, err)
}
*/
