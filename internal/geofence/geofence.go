package geofence

import "math"

const earthRadiusM = 6371000.0

func DistanceM(lat1, lon1, lat2, lon2 float64) float64 {
	const rad = math.Pi / 180
	dLat := (lat2 - lat1) * rad
	dLon := (lon2 - lon1) * rad
	a := math.Sin(dLat/2)*math.Sin(dLat/2) +
		math.Cos(lat1*rad)*math.Cos(lat2*rad)*math.Sin(dLon/2)*math.Sin(dLon/2)

	return 2 * earthRadiusM * math.Asin(math.Min(1, math.Sqrt(a)))
}

type Fence struct {
	CenterLat, CenterLon, RadiusM float64
}

func (f Fence) Contains(lat, lon float64) bool {
	return DistanceM(f.CenterLat, f.CenterLon, lat, lon) <= f.RadiusM
}

type State struct {
	Inside        bool
	LastTimestamp int64
}

func (f Fence) Evaluate(prev State, lat, lon float64, timestamp int64) (next State, entered bool) {
	if timestamp <= prev.LastTimestamp {
		return prev, false
	}
	inside := f.Contains(lat, lon)
	return State{Inside: inside, LastTimestamp: timestamp}, inside && !prev.Inside
}

func (f Fence) PointNorth(metres float64) (lat, lon float64) {
	return f.CenterLat + metres/earthRadiusM*180/math.Pi, f.CenterLon
}
