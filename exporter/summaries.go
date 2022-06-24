package exporter

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/xray"
	"github.com/aws/aws-sdk-go-v2/service/xray/types"
)

func (svc *Service) processTraceSummaryOutput(
	ctx context.Context,
	startTime time.Time,
	endTime time.Time,
	output *xray.GetTraceSummariesOutput,
) (err error) {
	var wg sync.WaitGroup
	defer wg.Wait()

	if output == nil {
		return fmt.Errorf("no output from trace summary api call")
	}

	if len(output.TraceSummaries) == 0 {
		log.Println("no trace summaries found", output)
		return nil
	}

	ids := make([]string, len(output.TraceSummaries))
	for i, ts := range output.TraceSummaries {
		ids[i] = *ts.Id
	}
	if svc.cfg.Debug {
		log.Printf("Found (%d) trace ids\n", len(ids))
	}
	chunks := chunkBy(ids, 5) // 5 is the max ids allowed to BatchGetTraces call

	for _, chunk := range chunks {
		svc.idChunkChan <- chunk
	}

	if output.NextToken != nil {
		// TODO: confirm this is actually getting the next page when there's many of them
		err := svc.readTracesNextPage(ctx, startTime, endTime, *output.NextToken)
		if err != nil {
			svc.errors <- err
		}
	}
	return nil
}

func (svc *Service) processTraceIdChunk(ctx context.Context, ids []string) ([]types.Trace, error) {
	batchGetTracesOutput, err := svc.xry.BatchGetTraces(ctx, &xray.BatchGetTracesInput{
		TraceIds: ids,
	})
	if err != nil {
		return nil, err
	}

	return batchGetTracesOutput.Traces, nil
}
