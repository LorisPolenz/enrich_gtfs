package duckdb

import (
	"database/sql"
	"go-etl/s3"
	"log"
	"log/slog"
	"os"
	"sync"

	_ "github.com/duckdb/duckdb-go/v2"
)

var (
	duckDBConn *sql.DB
	dbOnce     sync.Once
)

func fetchDBObjectFromS3(feedVersion string) (*string, error) {
	objectName := feedVersion + "_feed.db"
	localPath := "assets/" + objectName

	_, err := os.Stat(localPath)

	if err == nil {
		slog.Info("DuckDB file already exists locally:", "path", localPath)
		return &localPath, nil
	}

	slog.Info("DuckDB file not found locally. Fetching from S3...", "object", objectName)

	s3_client := s3.InitS3Client(
		os.Getenv("S3_ENDPOINT"),
		os.Getenv("S3_ACCESS_KEY_ID"),
		os.Getenv("S3_SECRET_ACCESS_KEY"),
		true,
	)

	data, err := s3.FetchObject(s3_client, "gtfs-fp", objectName)
	if err != nil {
		slog.Error("Error fetching DuckDB file from S3:", "error", err)
		return nil, err
	}

	err = os.WriteFile(localPath, data, 0644)
	if err != nil {
		slog.Error("Error writing DuckDB file locally:", "error", err)
		return nil, err
	}

	return &localPath, nil
}

func openDuckDB(dbPath *string) (*sql.DB, error) {

	slog.Info("Initializing Database...")

	// Load the existing database into memory
	db, err := sql.Open("duckdb", "")
	db.Exec("ATTACH DATABASE '" + *dbPath + "' AS persistent_db;")
	db.Exec("COPY FROM DATABASE persistent_db TO memory;")
	db.Exec("DETACH persistent_db;")

	if err != nil {
		log.Fatal(err)
	}

	slog.Info("Successfully Initialized Database...")

	if err != nil {
		log.Fatal(err)
	}

	return db, nil
}

func GetDuckDB(feedVersion string) *sql.DB {

	dbOnce.Do(func() {
		var err error

		dbPath, err := fetchDBObjectFromS3(feedVersion)

		if err != nil {
			log.Fatal(err)
		}

		duckDBConn, err = openDuckDB(dbPath)

		if err != nil {
			log.Fatal(err)
		}

		duckDBConn.SetMaxOpenConns(1)

		err = duckDBConn.Ping()

		if err != nil {
			log.Fatal(err)
		}
	})

	return duckDBConn
}

func CloseDuckDB(db *sql.DB) {
	if err := db.Close(); err != nil {
		log.Fatal(err)
	}
}
