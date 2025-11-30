package data

import "go-etl/helpers"

type StopProcessed struct {
	ProcessedAt int64
	StopName    string
	*helpers.Stop
}

func NewStopProcessed(stop *helpers.Stop, processedAt int64, stopName string) *StopProcessed {
	return &StopProcessed{
		Stop:        stop,
		ProcessedAt: processedAt,
		StopName:    stopName,
	}
}
