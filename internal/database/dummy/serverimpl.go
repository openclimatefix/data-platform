// Package dummy provides a fake implementation of the QuartzAPIClient.
// All data returned is simulated on the fly and has no logical bearing between requests.
// Useful for quickly building new clients.
//
// The generated irradience data is based on the formulas and concepts from
// the lecture notes "Basics In Solar Radiation At Earth Surface" [1] and the book
// "Fundamentals Of Solar Radiation" (ISBN 978-0-367-72592-1) [2], both by Lucian Wald.
//
// Functions requiring the True Solar Time will have `tst` as a parameter.
package dummy

import (
	"math"
	"time"
)

const (
	step = time.Duration(5 * time.Minute)
)

type lnglat struct {
	lonDegs float64
	latDegs float64
}

func (l lnglat) lonRads() float64 {
	return l.lonDegs * math.Pi / 180.0
}

func (l lnglat) latRads() float64 {
	return l.latDegs * math.Pi / 180.0
}

type SolarData struct {
	timeUtc time.Time
	timeMst time.Time
	timeTst time.Time
	// eotCorrection is the equation of time in hours.
	eotCorrection time.Duration
	// angleDayRadians is the angle formed by the sun/earth line
	// on the given day of the year, and on the 1st of January of the same year.
	angleDayRadians float64
	// hourAngleRadians is the angluar arc definiting the position of the sun in it's
	// apparent path across the sky. It is zero at solar noon, negative in the morning,
	// and positive in the afternoon.
	hourAngleRadians float64
	// declinationRadians is the solar declination angle in radians.
	declinationRadians               float64
	zenithRadians                    float64
	azimuthRadians                   float64
	extraterrestrialIrradianceNormal float64
	extraterrestrialIrradiance       float64
	sunriseTimeTst                   time.Time
	sunsetTimeTst                    time.Time
	sunriseTimeMst                   time.Time
	sunsetTimeMst                    time.Time
	sunriseTimeUtc                   time.Time
	sunsetTimeUtc                    time.Time
	daylengthHours                   float64
}

func determineIrradience(t time.Time, p lnglat) SolarData {
	sd := SolarData{timeUtc: t}
	yearDay := float64(t.YearDay()) + float64(t.Hour())/24.0 + float64(t.Minute())/1440.0

	// 1. Determine True Solar Time (T_TST) at the given longitude.
	//
	// True Solar Time is defined as being 12:00PM when the sun is at it's highest point in the sky.
	// This depends on the longitude, and differs from the Mean Solar Time because of the change in
	// orbital speed of the earth in its elliptical orbit around the sun. It is also referred to as
	// the Local Apparent Time [2](section 2.5,2.6).
	//
	// It is calculated by correcting the UTC time for the longitude to find the Mean Solar Time (T_MST),
	// and then correcting that for the equation of time (EOT) to find the True Solar Time (T_TST).
	sd.angleDayRadians = (2 * math.Pi / 365.2422) * yearDay
	lonCorrection := time.Duration((p.lonDegs * 24.0 / 360.0) * float64(time.Hour))
	sd.timeMst = t.UTC().Add(lonCorrection)
	sd.eotCorrection = time.Duration((-0.128*math.Sin(sd.angleDayRadians-0.04887) -
		0.165*math.Sin(2*sd.angleDayRadians+0.34383)) *
		float64(time.Hour))
	sd.timeTst = sd.timeMst.Add(sd.eotCorrection)

	// 2. Determine solar declination for the given day of the year.
	//
	// Solar declination is the angle between the equatorial plane and the direction to the sun.
	// It is positive between the equinoxes of March and September, and negative elsewise [2](section 1.3).
	//
	// It is calculated via the angle formed by the sun–Earth line for a given day and that
	// for the day of the March equinox.
	num_0 := 79.3946 + (0.2422 * float64(t.Year()-1957)) - float64((t.Year()-1957)/4)
	ω_day := (2 * math.Pi / 365.2422) * (yearDay - num_0)
	sd.declinationRadians = 0.0064979 + 0.4059059*math.Sin(ω_day) + 0.0020054*math.Sin(2*ω_day) +
		-0.0029880*math.Sin(3*ω_day) + -0.0132296*math.Cos(ω_day) + 0.0063809*math.Cos(2*ω_day) + 0.0003508*math.Cos(3*ω_day)

	// 3. Determine the solar zenith and azimuthal angles at the True Solar Time.
	//
	// The solar zenithal angle is the angle formed by the direction of the sun and the local vertical.
	// The solar azimuthal angle defines the angle formed by the projection of the direction of the sun
	// on the horizontal plane and the north. [2](section 2.1).
	//
	// They are calculated based on the solar declination on the given day, and the sun's position
	// along it's apparent path across the sky (the hour angle).
	// The solar azimuth is unknown - but set to pi by convention - when the declination is 0.
	tstHour := float64(sd.timeTst.Hour()) + float64(sd.timeTst.Minute())/60.0 + float64(sd.timeTst.Second())/3600.0
	sd.hourAngleRadians = (math.Pi / 12) * (tstHour - 12.0)
	sd.zenithRadians = math.Acos(
		(math.Sin(p.latRads()) * math.Sin(sd.declinationRadians)) +
			(math.Cos(p.latRads()) * math.Cos(sd.declinationRadians) * math.Cos(sd.hourAngleRadians)),
	)
	theta_prime := (math.Sin(sd.declinationRadians)*math.Cos(p.latRads()) -
		math.Cos(sd.declinationRadians)*math.Sin(p.latRads())*math.Cos(sd.hourAngleRadians)) / math.Sin(sd.zenithRadians)
	if math.Sin(sd.hourAngleRadians) <= 0 { // Morning
		sd.azimuthRadians = math.Acos(theta_prime)
	} else { // Evening
		sd.azimuthRadians = 2*math.Pi - math.Acos(theta_prime)
	}

	// 4. Determine the local daylength and the sunrise/sunset times.
	//
	// These are calculated based on finding the hour angle at sunset,
	// when the solar declination is 0 and the solar zenithal angle is pi/2.
	var sunsetHourAngle float64
	switch {
	case p.latRads() == math.Pi/2 && sd.declinationRadians > 0:
		sunsetHourAngle = math.Pi
	case p.latRads() == math.Pi/2 && sd.declinationRadians <= 0:
		sunsetHourAngle = 0
	case p.latRads() == -math.Pi/2 && sd.declinationRadians > 0:
		sunsetHourAngle = 0
	case p.latRads() == -math.Pi/2 && sd.declinationRadians <= 0:
		sunsetHourAngle = math.Pi
	case -1*math.Tan(p.latRads())*math.Tan(sd.declinationRadians) >= 1:
		sunsetHourAngle = 0
	case -1*math.Tan(p.latRads())*math.Tan(sd.declinationRadians) <= -1:
		sunsetHourAngle = math.Pi
	default:
		// There is actually an error in my edition of the book here,
		// it should be acos, but the book has cos printed.
		sunsetHourAngle = math.Acos(-1 * math.Tan(p.latRads()) * math.Tan(sd.declinationRadians))
	}
	sunriseHour := 12.0 * (1.0 - (sunsetHourAngle / math.Pi))
	sunsetHour := 12.0 * (1.0 + (sunsetHourAngle / math.Pi))
	sd.sunriseTimeTst = sd.timeTst.Truncate(24 * time.Hour).Add(time.Duration(sunriseHour * float64(time.Hour)))
	sd.sunsetTimeTst = sd.timeTst.Truncate(24 * time.Hour).Add(time.Duration(sunsetHour * float64(time.Hour)))
	sd.sunriseTimeMst = sd.sunriseTimeTst.Add(-sd.eotCorrection)
	sd.sunsetTimeMst = sd.sunsetTimeTst.Add(-sd.eotCorrection)
	sd.sunriseTimeUtc = sd.sunriseTimeMst.Add(-lonCorrection)
	sd.sunsetTimeUtc = sd.sunsetTimeMst.Add(-lonCorrection)
	sd.daylengthHours = (sunsetHour - sunriseHour)

	// 5. Determine the Extraterrestrial Irradiation.
	//
	// Extraterrestrial irradiation is the irradiation on a horizontal plane
	// at the top of the atmosphere for a given True Solar Time.
	//
	// It is calculated via the solar constant E_TSI - the annual average solar irradiance
	// at the top of the atmosphere. This is modulated according to the eccentricity of the earth's orbit
	// on the given day and the solar zenithal angle at the given time. [2](section 3.2).
	ε := 0.03344 * math.Cos(sd.angleDayRadians-0.049)
	E_TSI := 1361.0
	E_0N := E_TSI * (1 + ε)
	sd.extraterrestrialIrradianceNormal = E_0N
	sd.extraterrestrialIrradiance = max(E_0N*math.Cos(sd.zenithRadians), 0.0)

	return sd
}

func cloudCoverFactor(t time.Time, p lnglat) float64 {
	// Handy blog on fake clouds https://nullprogram.com/blog/2007/11/20/

	// 1. Generate FBM noise.
	//
	// This is a mix of spline-smoothed Gaussian noise on a few different scales,
	// with more weight given to the lower frequencies.
	return 0.5
}

// / getWindow returns the start and end of the window for the timeseries data.
func getWindow() (time.Time, time.Time) {
	windowStart := time.Now().UTC().Add(-time.Hour * 48).Truncate(time.Hour * 24)
	windowEnd := time.Now().UTC().Add(time.Hour * 48).Truncate(time.Hour * 24)
	return windowStart, windowEnd
}

