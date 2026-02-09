package generator

import (
	"fmt"
	"math"
	"math/rand"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// GeoPoint represents a latitude and longitude coordinate
type GeoPoint struct {
	Lat float64
	Lon float64
}

// Predefined cities for geospatial testing
// Mirrors the CITIES array from the reference Java code
var Cities = []GeoPoint{
	{Lat: 6.46, Lon: 3.38},     // Lagos
	{Lat: 30.04, Lon: 31.24},   // Cairo
	{Lat: 35.69, Lon: 139.69},  // Tokyo
	{Lat: 28.61, Lon: 77.21},   // Delhi
	{Lat: 41.01, Lon: 28.98},   // Istanbul
	{Lat: 52.52, Lon: 13.40},   // Berlin
	{Lat: 19.43, Lon: -99.13},  // Mexico City
	{Lat: 40.71, Lon: -74.01},  // New York
	{Lat: -23.55, Lon: -46.64}, // São Paulo
	{Lat: -34.60, Lon: -58.38}, // Buenos Aires
	{Lat: -33.87, Lon: 151.21}, // Sydney
	{Lat: -37.81, Lon: 144.96}, // Melbourne
}

// Distance constants (in meters)
const (
	SearchMinGeoDistanceMeters = 1
	SearchMaxGeoDistanceMeters = 1_000_000 // 1000 km

	// Safety margins for distance calculations
	DistanceSafetyMargin = 0.08 * (SearchMaxGeoDistanceMeters - SearchMinGeoDistanceMeters)
	RandomMinDistance    = SearchMinGeoDistanceMeters + DistanceSafetyMargin
	RandomMaxDistance    = SearchMaxGeoDistanceMeters - DistanceSafetyMargin

	// Min/Max difference bounds when both are used
	MinMaxDifferenceLowerBound = 0.2 * (RandomMaxDistance - RandomMinDistance)
	MinMaxDifferenceUpperBound = 0.9 * (RandomMaxDistance - RandomMinDistance)

	// Earth radius in meters
	EarthRadiusMeters = 6371000.0
)

// ToGeoJSON converts a GeoPoint to a MongoDB GeoJSON Point
func (p GeoPoint) ToGeoJSON() bson.M {
	return bson.M{
		"type":        "Point",
		"coordinates": []float64{p.Lon, p.Lat},
	}
}

// MakePointOnCircle generates a point on a circle around a center
// This is used to create random locations within a certain distance from a city
func MakePointOnCircle(center GeoPoint, bearingRadians, distanceMeters float64) GeoPoint {
	// Angular distance in radians
	angularDistance := distanceMeters / EarthRadiusMeters

	lat1 := center.Lat * math.Pi / 180.0
	lon1 := center.Lon * math.Pi / 180.0

	lat2 := math.Asin(math.Sin(lat1)*math.Cos(angularDistance) +
		math.Cos(lat1)*math.Sin(angularDistance)*math.Cos(bearingRadians))

	lon2 := lon1 + math.Atan2(math.Sin(bearingRadians)*math.Sin(angularDistance)*math.Cos(lat1),
		math.Cos(angularDistance)-math.Sin(lat1)*math.Sin(lat2))

	return GeoPoint{
		Lat: lat2 * 180.0 / math.Pi,
		Lon: lon2 * 180.0 / math.Pi,
	}
}

// GeoGenerator generates geospatial documents and queries
type GeoGenerator struct {
	rand *rand.Rand
	seed int64
}

// NewGeoGenerator creates a new geospatial generator
func NewGeoGenerator(seed int64) *GeoGenerator {
	return &GeoGenerator{
		rand: rand.New(rand.NewSource(seed)),
		seed: seed,
	}
}

// GenerateGeoDocument creates a document with a GeoJSON Point location
// The location is randomly offset from one of the predefined cities
// Returns a Document compatible with the database interface
func (g *GeoGenerator) GenerateGeoDocument(id string) interface{} {
	// Pick a random city
	city := Cities[g.rand.Intn(len(Cities))]

	// Generate random bearing (direction) and distance
	bearing := g.rand.Float64() * 2 * math.Pi
	distanceMeters := SearchMinGeoDistanceMeters +
		g.rand.Float64()*(SearchMaxGeoDistanceMeters-SearchMinGeoDistanceMeters)

	// Create a point on the circle around the city
	location := MakePointOnCircle(city, bearing, distanceMeters)

	return bson.M{
		"_id":      id,
		"location": location.ToGeoJSON(),
	}
}

// GeoQueryParams defines parameters for a geospatial query
type GeoQueryParams struct {
	WithMinDistance bool
	WithMaxDistance bool
	Limit           int
}

// GenerateGeoQuery creates a MongoDB $nearSphere query
// The query center is randomly selected from the predefined cities
func (g *GeoGenerator) GenerateGeoQuery(params GeoQueryParams) bson.M {
	// Pick a random city as the query center
	city := Cities[g.rand.Intn(len(Cities))]
	centerGeoJSON := city.ToGeoJSON()

	var minDistance, maxDistance *float64

	if params.WithMinDistance && params.WithMaxDistance {
		// Generate both min and max distance
		minMaxDifference := MinMaxDifferenceLowerBound +
			g.rand.Float64()*(MinMaxDifferenceUpperBound-MinMaxDifferenceLowerBound)
		minDist := RandomMinDistance +
			g.rand.Float64()*(RandomMaxDistance-minMaxDifference-RandomMinDistance)
		maxDist := minDist + minMaxDifference
		minDistance = &minDist
		maxDistance = &maxDist
	} else if params.WithMinDistance {
		// Generate only min distance
		minDist := RandomMinDistance + g.rand.Float64()*(RandomMaxDistance-RandomMinDistance)
		minDistance = &minDist
	} else if params.WithMaxDistance {
		// Generate only max distance
		maxDist := RandomMinDistance + g.rand.Float64()*(RandomMaxDistance-RandomMinDistance)
		maxDistance = &maxDist
	}

	// Build $nearSphere query structure
	nearSphereFilter := bson.M{
		"$geometry": centerGeoJSON,
	}
	if maxDistance != nil {
		nearSphereFilter["$maxDistance"] = *maxDistance
	}
	if minDistance != nil {
		nearSphereFilter["$minDistance"] = *minDistance
	}

	return bson.M{
		"location": bson.M{"$nearSphere": nearSphereFilter},
	}
}

// FormatGeoQuery returns a human-readable string representation of the query
func (g *GeoGenerator) FormatGeoQuery(params GeoQueryParams) string {
	distanceDesc := "no-limits"
	if params.WithMinDistance && params.WithMaxDistance {
		distanceDesc = "min-max"
	} else if params.WithMinDistance {
		distanceDesc = "min-only"
	} else if params.WithMaxDistance {
		distanceDesc = "max-only"
	}
	return fmt.Sprintf("geo-%s-limit-%d", distanceDesc, params.Limit)
}
