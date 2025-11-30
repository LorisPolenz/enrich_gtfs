package transformer

import (
	"database/sql"
	"go-etl/duckdb"
	"go-etl/helpers"
)

type EnrichStopNameByID struct {
	db     *sql.DB
	StopID string
	Stop   *helpers.StopCSV
}

type EnrichStopTimesByTripID struct {
	db        *sql.DB
	TripID    string
	StopID    string
	StopTimes *helpers.StopTimeCSV
}

type EnrichRouteByID struct {
	db      *sql.DB
	RouteID string
	Route   *helpers.RouteCSV
}

type EnrichTripByID struct {
	db     *sql.DB
	TripID string
	Trip   *helpers.TripCSV
}

// Contruct type
func NewEnrichStopByID(stopID string, feedVersion string) *EnrichStopNameByID {
	return &EnrichStopNameByID{StopID: stopID, db: duckdb.GetDuckDB(feedVersion)}
}

func NewEnrichStopTimeByTripID(tripID string, stopID string, feedVersion string) *EnrichStopTimesByTripID {
	return &EnrichStopTimesByTripID{TripID: tripID, StopID: stopID, db: duckdb.GetDuckDB(feedVersion)}
}

func NewEnrichRouteByID(routeID string, feedVersion string) *EnrichRouteByID {
	return &EnrichRouteByID{RouteID: routeID, db: duckdb.GetDuckDB(feedVersion)}
}

func NewEnrichTripByID(tripID string, feedVersion string) *EnrichTripByID {
	return &EnrichTripByID{TripID: tripID, db: duckdb.GetDuckDB(feedVersion)}
}

// Func implementing Transformer interface
func (e *EnrichStopNameByID) Transform() {
	stop, err := helpers.GetCachedStop(e.db, e.StopID)

	if err != nil {
		e.Stop = &helpers.StopCSV{StopID: e.StopID, StopName: "UNKNOWN"}
	} else {
		e.Stop = stop
	}
}

func (e *EnrichStopTimesByTripID) Transform() {
	stopTime, err := helpers.GetCachedStopTimesByTripID(e.db, e.TripID, e.StopID)

	if err != nil {
		e.StopTimes = &helpers.StopTimeCSV{}
	} else {
		e.StopTimes = stopTime
	}
}

func (e *EnrichRouteByID) Transform() {
	route, err := helpers.GetCachedRoute(e.db, e.RouteID)

	if err != nil {
		e.Route = &helpers.RouteCSV{RouteID: e.RouteID, RouteShortName: "UNKNOWN"}
	} else {
		e.Route = route
	}
}

func (e *EnrichTripByID) Transform() {
	trip, err := helpers.GetCachedTrip(e.db, e.TripID)

	if err != nil {
		e.Trip = &helpers.TripCSV{TripID: e.TripID, RouteID: "UNKNOWN"}
	} else {
		e.Trip = trip
	}
}
