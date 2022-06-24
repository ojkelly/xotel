package main

import (
	"context"
	"log"

	"github.com/ojkelly/xray-to-otel/exporter"
)

func main() {
	ctx := context.Background()
	svc, err := exporter.New(ctx)

	if err != nil {
		log.Fatalf("ERROR: %s\n", err)
	}

	err = svc.Run(ctx)

	if err != nil {
		log.Fatalf("ERROR: %s\n", err)
	}
}
