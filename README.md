# ETL Processor

## Inputs
- Compressed GTFS-RT Protobuf (loaded from S3 Bucket)
- Duckdb Database (loaded from S3 Bucket, based on feed_version)

## Enrichment
- Trip Information
- Route Information
- Stop Information

## Output
- Elasticsearch (per StopTimeUpdate)
- JSON File (Written to S3 Bucket)


## Usage
> Note: This code has been written with an automation engine such as Argo Workflow in mind. 

Define following env variables:
```
ELASTIC_HOST=
ELASTIC_API_KEY=
ELASTIC_TARGET_INDEX=
S3_ENDPOINT=
S3_ACCESS_KEY_ID=
S3_SECRET_ACCESS_KEY=
```

Execute the binary with the flag containing the object name of the compressed protobuf file. 

## Packages
- DuckDB
- Elasticsearch
- Minio
- Protobuf

## Enrichment Data
The GTFS-RT Message contains the feed_version in the header i.e. `20251002`, this feed_version relates to a specific version of the GTFS Timetable. Thus, a new database file needs to be created for feed_version. 

Currently the following steps need to be executed manually for a new feed_version:
```
CREATE TABLE stops AS SELECT * FROM read_csv('assets/stops.csv', force_not_null = [location_type, parent_station, platform_code]);

CREATE TABLE trips AS SELECT * FROM read_csv('assets/trips.csv', force_not_null = [block_id, original_trip_id, hints], types={'block_id': 'VARCHAR'});

CREATE TABLE routes AS SELECT * FROM read_csv('assets/routes.csv', force_not_null = [route_long_name]);

CREATE TABLE stop_times AS SELECT * FROM read_csv('assets/stop_times.csv');


CREATE INDEX route_id on routes (route_id);
CREATE INDEX trip_id on trips (trip_id);
CREATE INDEX stop_id on stops (stop_id);
CREATE INDEX stop_times_trip_id on stop_times (trip_id);
```


## Sample Result
```json
{
    "header": {
        "gtfs_realtime_version": "2.0",
        "incrementality": 0,
        "timestamp": 1759612780,
        "feed_version": "20251002"
    },
    "entity": [
        {
            "id": "398.TA.91-75-j25-1.103.H",
            "trip_update": {
                "trip": {
                    "trip_id": "398.TA.91-75-j25-1.103.H",
                    "route_id": "91-75-j25-1",
                    "start_time": "22:35:00",
                    "start_date": "20251004",
                    "schedule_relationship": 0
                },
                "stop_time_update": [
                    {
                        "stop_sequence": 1,
                        "stop_id": "8503000:0:7",
                        "departure": {
                            "delay": 78
                        },
                        "schedule_relationship": 0,
                        "stop": {
                            "stop_id": "Parent8503000",
                            "stop_name": "Zürich HB",
                            "stop_lat": 47.3781762,
                            "stop_lon": 8.54021154,
                            "location_type": "1",
                            "parent_station": "",
                            "platform_code": ""
                        },
                        "stop_time": {
                            "trip_id": "398.TA.91-75-j25-1.103.H",
                            "arrival_time": "22:35:00",
                            "departure_time": "22:35:00",
                            "stop_id": "8503000:0:7",
                            "stop_sequence": "1",
                            "pickup_type": "0",
                            "drop_off_type": "0"
                        },
                        "schedule_relationship_string": "SCHEDULED"
                    },
                    {
                        "stop_sequence": 3,
                        "stop_id": "8503202:0:4",
                        "arrival": {
                            "delay": -18
                        },
                        "departure": {
                            "delay": 96
                        },
                        "schedule_relationship": 0,
                        "stop": {
                            "stop_id": "Parent8503202",
                            "stop_name": "Thalwil",
                            "stop_lat": 47.29597958,
                            "stop_lon": 8.56477148,
                            "location_type": "1",
                            "parent_station": "",
                            "platform_code": ""
                        },
                        "stop_time": {
                            "trip_id": "398.TA.91-75-j25-1.103.H",
                            "arrival_time": "22:45:00",
                            "departure_time": "22:45:00",
                            "stop_id": "8503202:0:4",
                            "stop_sequence": "3",
                            "pickup_type": "0",
                            "drop_off_type": "0"
                        },
                        "schedule_relationship_string": "SCHEDULED"
                    },
                    {
                        "stop_sequence": 4,
                        "stop_id": "8502206:0:1",
                        "arrival": {
                            "delay": 6
                        },
                        "departure": {
                            "delay": 102
                        },
                        "schedule_relationship": 0,
                        "stop": {
                            "stop_id": "Parent8502206",
                            "stop_name": "Baar",
                            "stop_lat": 47.19536428,
                            "stop_lon": 8.52326931,
                            "location_type": "1",
                            "parent_station": "",
                            "platform_code": ""
                        },
                        "stop_time": {
                            "trip_id": "398.TA.91-75-j25-1.103.H",
                            "arrival_time": "22:58:00",
                            "departure_time": "22:58:00",
                            "stop_id": "8502206:0:1",
                            "stop_sequence": "4",
                            "pickup_type": "0",
                            "drop_off_type": "0"
                        },
                        "schedule_relationship_string": "SCHEDULED"
                    },
                    {
                        "stop_sequence": 5,
                        "stop_id": "8502204:0:4",
                        "arrival": {
                            "delay": 0
                        },
                        "departure": {
                            "delay": 78
                        },
                        "schedule_relationship": 0,
                        "stop": {
                            "stop_id": "Parent8502204",
                            "stop_name": "Zug",
                            "stop_lat": 47.17370877,
                            "stop_lon": 8.51504973,
                            "location_type": "1",
                            "parent_station": "",
                            "platform_code": ""
                        },
                        "stop_time": {
                            "trip_id": "398.TA.91-75-j25-1.103.H",
                            "arrival_time": "23:02:00",
                            "departure_time": "23:03:00",
                            "stop_id": "8502204:0:4",
                            "stop_sequence": "5",
                            "pickup_type": "0",
                            "drop_off_type": "0"
                        },
                        "schedule_relationship_string": "SCHEDULED"
                    },
                    {
                        "stop_sequence": 6,
                        "stop_id": "8502202:0:2",
                        "arrival": {
                            "delay": -6
                        },
                        "departure": {
                            "delay": 108
                        },
                        "schedule_relationship": 0,
                        "stop": {
                            "stop_id": "Parent8502202",
                            "stop_name": "Rotkreuz",
                            "stop_lat": 47.14176191,
                            "stop_lon": 8.43049131,
                            "location_type": "1",
                            "parent_station": "",
                            "platform_code": ""
                        },
                        "stop_time": {
                            "trip_id": "398.TA.91-75-j25-1.103.H",
                            "arrival_time": "23:11:00",
                            "departure_time": "23:11:00",
                            "stop_id": "8502202:0:2",
                            "stop_sequence": "6",
                            "pickup_type": "0",
                            "drop_off_type": "0"
                        },
                        "schedule_relationship_string": "SCHEDULED"
                    },
                    {
                        "stop_sequence": 7,
                        "stop_id": "8505000:0:4",
                        "arrival": {
                            "delay": 30
                        },
                        "schedule_relationship": 0,
                        "stop": {
                            "stop_id": "Parent8505000",
                            "stop_name": "Luzern",
                            "stop_lat": 47.05017648,
                            "stop_lon": 8.31017995,
                            "location_type": "1",
                            "parent_station": "",
                            "platform_code": ""
                        },
                        "stop_time": {
                            "trip_id": "398.TA.91-75-j25-1.103.H",
                            "arrival_time": "23:25:00",
                            "departure_time": "23:25:00",
                            "stop_id": "8505000:0:4",
                            "stop_sequence": "7",
                            "pickup_type": "0",
                            "drop_off_type": "0"
                        },
                        "schedule_relationship_string": "SCHEDULED"
                    }
                ],
                "route": {
                    "route_id": "91-75-j25-1",
                    "agency_id": "11",
                    "route_short_name": "IR75",
                    "route_long_name": "",
                    "route_desc": "IR",
                    "route_type": "103"
                },
                "trip_enriched": {
                    "route_id": "91-75-j25-1",
                    "service_id": "TA+k5200",
                    "trip_id": "398.TA.91-75-j25-1.103.H",
                    "trip_headsign": "Luzern",
                    "trip_short_name": "2590",
                    "direction_id": "0",
                    "block_id": "",
                    "original_trip_id": "ch:1:sjyid:100001:2590-001",
                    "hints": "BZ FA FS NF RZ"
                }
            }
        }
    ]
}
```


## ToDo
- Optimize enrichment performance
- Add K8s ressources (argo workflow, secrets)
- Write metrics to Elastic
- Automate Database creation from CSVs
- Workflow for Docker Image