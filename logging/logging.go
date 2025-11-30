package logging

import (
	"log/slog"
	"os"
)

func GetLogger() *slog.Logger {
	// Custom handler to format output
	handler := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level:     slog.LevelDebug, // minimum level
		AddSource: true,            // include file + line
	})
	logger := slog.New(handler)

	return logger
}
