package data

import "go-etl/helpers"

type EnrichedFeedMessage struct {
	*helpers.FeedMessage
	EnrichedFeedEntities []EnrichedFeedEntity `json:"entity"`
}

type EnrichedFeedEntity struct {
	*helpers.FeedEntity
	EnrichedTripUpdate EnrichedTripUpdate `json:"trip_update"`
}

type EnrichedTripUpdate struct {
	*helpers.TripUpdate
	EnrichedStopTimeUpdates []EnrichedStopTimeUpdate `json:"stop_time_update"`
	EnrichedRoute           helpers.RouteCSV         `json:"route"`
	EnrichedTrip            helpers.TripCSV          `json:"trip_enriched"`
}

type EnrichedStopTimeUpdate struct {
	*helpers.TripUpdate_StopTimeUpdate
	Stop                       helpers.StopCSV     `json:"stop"`
	StopTime                   helpers.StopTimeCSV `json:"stop_time"`
	ScheduleRelationShipString string              `json:"schedule_relationship_string"`
}

func NewEnrichedFeedMessage(fm *helpers.FeedMessage, efe []EnrichedFeedEntity) *EnrichedFeedMessage {
	return &EnrichedFeedMessage{
		FeedMessage:          fm,
		EnrichedFeedEntities: efe,
	}
}

func NewEnrichedFeedEntity(fe *helpers.FeedEntity, etu EnrichedTripUpdate) *EnrichedFeedEntity {
	return &EnrichedFeedEntity{
		FeedEntity:         fe,
		EnrichedTripUpdate: etu,
	}
}

func NewEnrichedTripUpdate(tu *helpers.TripUpdate, estu []EnrichedStopTimeUpdate, eru *helpers.RouteCSV, et *helpers.TripCSV) *EnrichedTripUpdate {
	return &EnrichedTripUpdate{
		TripUpdate:              tu,
		EnrichedStopTimeUpdates: estu,
		EnrichedRoute:           *eru,
		EnrichedTrip:            *et,
	}
}

func NewEnrichedStopTimeUpdate(stu *helpers.TripUpdate_StopTimeUpdate, stop helpers.StopCSV, stopTime helpers.StopTimeCSV, srs string) *EnrichedStopTimeUpdate {
	return &EnrichedStopTimeUpdate{
		TripUpdate_StopTimeUpdate:  stu,
		Stop:                       stop,
		StopTime:                   stopTime,
		ScheduleRelationShipString: srs,
	}
}
