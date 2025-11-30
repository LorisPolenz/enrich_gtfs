package helpers

import (
	"database/sql"
	"time"
)

type StopCSV struct {
	StopID        string  `json:"stop_id"`
	StopName      string  `json:"stop_name"`
	Lat           float64 `json:"stop_lat"`
	Lon           float64 `json:"stop_lon"`
	LocationType  string  `json:"location_type"`
	ParentStation string  `json:"parent_station"`
	PlatformCode  string  `json:"platform_code"`
}

type StopTimeCSV struct {
	TripID        string `json:"trip_id"`
	ArrivalTime   string `json:"arrival_time"`
	DepartureTime string `json:"departure_time"`
	StopID        string `json:"stop_id"`
	StopSequence  string `json:"stop_sequence"`
	PickupType    string `json:"pickup_type"`
	DropOffType   string `json:"drop_off_type"`
}

type RouteCSV struct {
	RouteID        string `json:"route_id"`
	AgencyID       string `json:"agency_id"`
	RouteShortName string `json:"route_short_name"`
	RouteLongName  string `json:"route_long_name"`
	RouteDesc      string `json:"route_desc"`
	RouteType      string `json:"route_type"`
}

type TripCSV struct {
	RouteID        string `json:"route_id"`
	ServiceID      string `json:"service_id"`
	TripID         string `json:"trip_id"`
	TripHeadsign   string `json:"trip_headsign"`
	TripShortName  string `json:"trip_short_name"`
	DirectionID    string `json:"direction_id"`
	BlockID        string `json:"block_id"`
	OriginalTripID string `json:"original_trip_id"`
	Hints          string `json:"hints"`
}

type StopCache struct {
	Stop       StopCSV
	ValidUntil time.Time
}

type StopTimeCache struct {
	StopTime   StopTimeCSV
	ValidUntil time.Time
}

type RouteCache struct {
	Route      RouteCSV
	ValidUntil time.Time
}

type TripCache struct {
	Trip       TripCSV
	ValidUntil time.Time
}

func GetCachedStop(db *sql.DB, sourceStopID string) (*StopCSV, error) {
	row := db.QueryRow("SELECT stop_id, stop_name, stop_lat, stop_lon, location_type, parent_station, platform_code FROM stops WHERE stop_id = 'Parent" + sourceStopID + "' LIMIT 1;")

	var (
		stopID        string
		stopName      string
		stopLat       float64
		stopLon       float64
		locationType  string
		parentStation string
		platformCode  string
	)

	err := row.Scan(&stopID, &stopName, &stopLat, &stopLon, &locationType, &parentStation, &platformCode)

	if err != nil {
		return &StopCSV{}, err
	}

	stop := &StopCSV{
		StopID:        stopID,
		StopName:      stopName,
		Lat:           stopLat,
		Lon:           stopLon,
		LocationType:  locationType,
		ParentStation: parentStation,
		PlatformCode:  platformCode,
	}

	return stop, nil
}

func GetCachedStopTimesByTripID(db *sql.DB, tripID string, stopID string) (*StopTimeCSV, error) {
	row := db.QueryRow("SELECT trip_id, arrival_time, departure_time, stop_id, stop_sequence, pickup_type, drop_off_type FROM stop_times WHERE trip_id = '" + tripID + "' AND starts_with(stop_id, '" + stopID + "') LIMIT 1;")

	var (
		dbTripID      string
		arrivalTime   string
		departureTime string
		dbStopID      string
		stopSequence  string
		pickupType    string
		dropOffType   string
	)

	err := row.Scan(&dbTripID, &arrivalTime, &departureTime, &dbStopID, &stopSequence, &pickupType, &dropOffType)

	if err != nil {
		return &StopTimeCSV{}, err
	}

	stopTime := StopTimeCSV{
		TripID:        dbTripID,
		ArrivalTime:   arrivalTime,
		DepartureTime: departureTime,
		StopID:        dbStopID,
		StopSequence:  stopSequence,
		PickupType:    pickupType,
		DropOffType:   dropOffType,
	}

	return &stopTime, nil

}

func GetCachedRoute(db *sql.DB, routeID string) (*RouteCSV, error) {
	row := db.QueryRow("SELECT route_id, agency_id, route_short_name, route_long_name, route_desc, route_type FROM routes WHERE route_id = '" + routeID + "' LIMIT 1;")

	var (
		dbRouteID      string
		agencyID       string
		routeShortName string
		routeLongName  string
		routeDesc      string
		routeType      string
	)

	err := row.Scan(&dbRouteID, &agencyID, &routeShortName, &routeLongName, &routeDesc, &routeType)

	if err != nil {
		return &RouteCSV{}, err
	}

	route := &RouteCSV{
		RouteID:        dbRouteID,
		AgencyID:       agencyID,
		RouteShortName: routeShortName,
		RouteLongName:  routeLongName,
		RouteDesc:      routeDesc,
		RouteType:      routeType,
	}

	return route, nil

}

func GetCachedTrip(db *sql.DB, tripID string) (*TripCSV, error) {
	row := db.QueryRow("SELECT route_id, service_id, trip_id, trip_headsign, trip_short_name, direction_id, block_id, original_trip_id, hints FROM trips WHERE trip_id = '" + tripID + "' LIMIT 1;")

	var (
		routeID        string
		serviceID      string
		dbTripID       string
		tripHeadsign   string
		tripShortName  string
		directionID    string
		blockID        string
		originalTripID string
		hints          string
	)

	err := row.Scan(&routeID, &serviceID, &dbTripID, &tripHeadsign, &tripShortName, &directionID, &blockID, &originalTripID, &hints)

	if err != nil {
		return &TripCSV{}, err
	}

	trip := &TripCSV{
		RouteID:        routeID,
		ServiceID:      serviceID,
		TripID:         dbTripID,
		TripHeadsign:   tripHeadsign,
		TripShortName:  tripShortName,
		DirectionID:    directionID,
		BlockID:        blockID,
		OriginalTripID: originalTripID,
		Hints:          hints,
	}

	return trip, nil
}
