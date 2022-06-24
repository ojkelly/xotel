package exporter

import (
	"context"
	"os"

	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
)

// TODO: this exporter only allows connecting to a local insecure otel collector
// Ideally you would run this on the same box/container as this app
// If you know a better more configurable way to do this, please let me know.
// I can't seem to get it to configure based on environment variables.
func newExporterClient(ctx context.Context) otlptrace.Client {
	opts := []otlptracegrpc.Option{
		otlptracegrpc.WithEndpoint(os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")),
		otlptracegrpc.WithInsecure(),
	}

	return otlptracegrpc.NewClient(opts...)
}
