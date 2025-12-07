package helpers

import (
	"log/slog"
	"os"
)

func validateEnvVars(keys []string) bool {
	for _, key := range keys {
		if os.Getenv(key) == "" {
			return false
		}
	}
	return true
}

func InitEnvVars() {
	requiredVars := []string{
		"ELASTIC_HOST",
		"ELASTIC_API_KEY",
		"ELASTIC_TARGET_INDEX",
		"S3_ENDPOINT",
		"S3_ACCESS_KEY",
		"S3_SECRET_KEY",
	}

	if !validateEnvVars(requiredVars) {
		slog.Error("One or more required environment variables are missing.")
		os.Exit(1)
	}
}
