package helpers

import (
	"fmt"
	"go-etl/duckdb"
	"log/slog"
	"os"
	"slices"
	"sort"
	"sync"
)

var (
	routeIDs   []string
	tripIDs    []string
	routesOnce sync.Once
	tripsOnce  sync.Once
)

func RouteExists(routeID string, feedVersion string) bool {
	routeIDs := *loadRoutes(feedVersion)

	idx := sort.SearchStrings(routeIDs, routeID)

	return idx < len(routeIDs) && routeIDs[idx] == routeID
}

func TripExists(tripID string, feedVersion string) bool {
	tripIDs := *loadTrips(feedVersion)

	idx := sort.SearchStrings(tripIDs, tripID)

	return idx < len(tripIDs) && tripIDs[idx] == tripID
}

func loadTrips(feedVersion string) *[]string {

	tripsOnce.Do(func() {
		db := duckdb.GetDuckDB(feedVersion)

		trips, err := db.Query("SELECT DISTINCT trip_id FROM trips;")

		if err != nil {
			slog.Error("Error found", "err", err)
			os.Exit(1)
		}

		for trips.Next() {
			var tripID string
			trips.Scan(&tripID)
			tripIDs = append(tripIDs, tripID)
		}

		slices.Sort(tripIDs)
	})

	return &tripIDs
}

func loadRoutes(feedVersion string) *[]string {

	routesOnce.Do(func() {
		fmt.Println("Loading Routes")
		db := duckdb.GetDuckDB(feedVersion)

		routes, err := db.Query("SELECT DISTINCT route_id FROM routes WHERE route_type > 100 AND route_type < 200;")

		if err != nil {
			slog.Error("Error found", "err", err)
			os.Exit(1)
		}

		defer routes.Close()

		for routes.Next() {
			var tripID string
			routes.Scan(&tripID)
			routeIDs = append(routeIDs, tripID)
		}

		slices.Sort(routeIDs)

	})

	return &routeIDs
}
