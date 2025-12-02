package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"go-etl/data"
	"go-etl/elastic"
	"go-etl/helpers"
	"go-etl/logging"
	"go-etl/pipeline"
	"go-etl/transformer"
	"strconv"
	"strings"
)

var logger = logging.GetLogger()

func main() {
	var objectName string

	flag.StringVar(&objectName, "p", "data.pb.gz", "ObjectName of compressed protobuf GTFS-RT file")

	flag.Parse()

	flag.Usage = func() {
		fmt.Println("Usage of script:")
		flag.PrintDefaults()
	}

	// Initialize environment variables
	helpers.InitEnvVars()

	// load pb data
	feedMessage, err := helpers.LoadFeedMessage(objectName)

	if err != nil {
		panic(err)
	}

	feedVersion := feedMessage.Header.GetFeedVersion()

	logger.Info("Feed Message Stats:", "feedEntities", len(feedMessage.Entity), "version", feedVersion)

	enrichedFeedEntities := []data.EnrichedFeedEntity{}

	for _, entity := range feedMessage.Entity {
		if entity.TripUpdate == nil {
			continue
		}

		if entity.TripUpdate.StopTimeUpdate == nil {
			continue
		}

		if !helpers.RouteExists(*entity.TripUpdate.Trip.RouteId, feedVersion) {
			logger.Warn("Skipping Trip Update - Route ID not found in GTFS data", "routeID", *entity.TripUpdate.Trip.RouteId)
			continue
		}

		if !helpers.TripExists(*entity.TripUpdate.Trip.TripId, feedVersion) {
			logger.Warn("Skipping Trip Update - Trip ID not found in GTFS data", "tripID", *entity.TripUpdate.Trip.TripId)
			continue
		}

		enrichedStopTimeUpdates := []data.EnrichedStopTimeUpdate{}

		p1 := pipeline.NewPipeline("Enrich Route Pipeline")

		enrichRoute := transformer.NewEnrichRouteByID(*entity.TripUpdate.Trip.RouteId, feedVersion)
		enrichTrip := transformer.NewEnrichTripByID(*entity.TripUpdate.Trip.TripId, feedVersion)

		p1.
			AddStage("enrich Route by ID", enrichRoute).
			AddStage("enrich Trip by ID", enrichTrip)

		p1.Run()

		routeType, err := strconv.Atoi(enrichRoute.Route.RouteType)
		if err != nil {
			fmt.Printf("Route: %v\n", enrichRoute.Route)
			panic(err)
		}

		if (routeType < 100) || (routeType > 130) {
			// Skip non-rail routes
			logger.Warn("Skipping non-rail route: %s of type %d\n", *entity.TripUpdate.Trip.RouteId, routeType)
			continue
		}

		for _, stu := range entity.TripUpdate.StopTimeUpdate {

			p2 := pipeline.NewPipeline("Process Stop Time Update")

			splitStopID := transformer.NewSplit(*stu.StopId, ":")

			p2.
				AddStage("split Stop ID by ':'", splitStopID)

			p2.Run()

			p3 := pipeline.NewPipeline("Enrich Stop Name Pipeline")

			enrichStopName := transformer.NewEnrichStopByID(splitStopID.Parts[0], feedVersion)
			enrichTripTime := transformer.NewEnrichStopTimeByTripID(*entity.TripUpdate.Trip.TripId, splitStopID.Parts[0], feedVersion)

			p3.
				AddStage("enrich Stop Name by ID", enrichStopName).
				AddStage("enrich Stop Times by Trip ID", enrichTripTime)

			p3.Run()

			enrichedStopTimeUpdate := data.NewEnrichedStopTimeUpdate(stu, *enrichStopName.Stop, *enrichTripTime.StopTimes, stu.ScheduleRelationship.String())

			enrichedStopTimeUpdates = append(enrichedStopTimeUpdates, *enrichedStopTimeUpdate)
		}

		enrichedTripUpdate := data.NewEnrichedTripUpdate(entity.TripUpdate, enrichedStopTimeUpdates, enrichRoute.Route, enrichTrip.Trip)
		enrichedFeedEntity := data.NewEnrichedFeedEntity(entity, *enrichedTripUpdate)
		enrichedFeedEntities = append(enrichedFeedEntities, *enrichedFeedEntity)
	}

	enrichedFeedMessage := data.NewEnrichedFeedMessage(feedMessage, enrichedFeedEntities)

	logger.Info("Enriched Feed Message...", "enrichedFeedEntities", len(enrichedFeedMessage.EnrichedFeedEntities))

	logger.Info("Writing results to json")

	var sb strings.Builder

	e := json.NewEncoder(&sb)
	e.SetEscapeHTML(false)
	e.Encode(enrichedFeedMessage)

	data, err := json.MarshalIndent(enrichedFeedMessage, "", "  ")

	if err != nil {
		logger.Error("Failed to marshal feed message to JSON", "error", err)
		return
	}

	helpers.WriteEnrichedFeedMessageToS3("enriched_"+objectName+".json", data)

	logger.Info("Indexing documents to Elasticsearch\n")

	elastic.IndexDocuments(enrichedFeedEntities)
}
