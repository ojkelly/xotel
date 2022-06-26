package exporter

import (
	"context"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/xray"
	"github.com/aws/aws-sdk-go-v2/service/xray/types"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

type Service struct {
	cfg    Config
	xry    *xray.Client
	otlp   otlptrace.Client
	errors chan error

	// a channel with a chunk of 5 trace id's, the max we can query
	// from batch-get-traces
	idChunkChan chan []string

	traceChan chan types.Trace
	otlpChan  chan *tracepb.ResourceSpans // TODO: batching? ring buffer?

	// how far back to look
	maxLookBack time.Duration
	// we need to leave at least T -1 minute because some data in
	// xray might not have loaded into a full trace yet
	// TODO: double check this is correct
	minLookBack time.Duration
}

func (s *Service) Debug(msg string) {
	if s.cfg.Debug {
		log.Println("[DEBUG]", msg)
	}
}

func New(ctx context.Context) (*Service, error) {
	log.Println("Create service")

	cfg := getConfig()

	awscfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("unable to get aws config: %s", err)
	}

	otlp := newExporterClient(ctx)
	err = otlp.Start(ctx)
	if err != nil {
		return nil, err
	}

	svc := Service{
		cfg:         cfg,
		xry:         xray.NewFromConfig(awscfg),
		otlp:        otlp,
		errors:      make(chan error),
		idChunkChan: make(chan []string),
		traceChan:   make(chan types.Trace),
		otlpChan:    make(chan *tracepb.ResourceSpans),
		maxLookBack: cfg.MaxLookBack * -1,
		minLookBack: cfg.MinLookBack * -1,
	}
	return &svc, nil
}

// TODO: concurrency is great, but now need to manage getting rate limited
// TODO: this will ship duplicate traces, can we do better?
func (svc *Service) Run(ctx context.Context) error {
	svc.Debug("Start run")
	ticker := time.NewTicker(svc.maxLookBack * -1)
	updateTicker := time.NewTicker(time.Second * 10)

	var exported uint64

	go func() {
		err := svc.collectAndForwardTraces(ctx, time.Now().Add(svc.maxLookBack), time.Now().Add(svc.minLookBack))

		if err != nil {
			svc.errors <- err
		}
	}()

	go func() {
		for {
			chunk := <-svc.idChunkChan
			// TODO: track these ID's for say 10 minutes?
			// if we get the same ones in, ignore them
			traces, err := svc.processTraceIdChunk(ctx, chunk)
			if err != nil {
				svc.errors <- err
			} else {
				for _, t := range traces {
					svc.traceChan <- t
				}
			}
		}
	}()

	go func() {
		for {
			trace := <-svc.traceChan
			protoSpans, err := parseTrace(trace)
			if err != nil {
				svc.errors <- err
			} else {

				for _, spn := range protoSpans {

					svc.otlpChan <- spn
				}
			}
		}
	}()

	go func() {
		for {
			rspan := <-svc.otlpChan
			err := svc.otlp.UploadTraces(ctx, []*tracepb.ResourceSpans{rspan})
			if err != nil {
				svc.errors <- err
			}

			atomic.AddUint64(&exported, 1)

		}
	}()

	for {
		select {
		case <-updateTicker.C:
			if exported != 0 {
				log.Printf("Exported (%d) spans\n", exported)
				atomic.AddUint64(&exported, -exported) // reset the counter
			} else {
				svc.Debug("didn't export any spans")
			}

		case t := <-ticker.C:
			go func() {
				// TODO: record the time of this check to use as the starting point of the next one
				err := svc.collectAndForwardTraces(ctx, t.Add(svc.maxLookBack), t.Add(svc.minLookBack))

				if err != nil {
					svc.errors <- err
				}
			}()

		case err := <-svc.errors:
			if err != nil {
				log.Println("Error: ", err)
			}
		}
	}
}
