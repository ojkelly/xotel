package exporter

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/xray"
)

func (svc *Service) collectAndForwardTraces(
	ctx context.Context,
	startTime time.Time,
	endTime time.Time,
) error {
	svc.Debug("collectAndForwardTraces")

	output, err := svc.xry.GetTraceSummaries(ctx, &xray.GetTraceSummariesInput{
		StartTime: aws.Time(startTime),
		EndTime:   aws.Time(endTime),
	})
	if err != nil {
		svc.errors <- err
	}

	err = svc.processTraceSummaryOutput(ctx, startTime, endTime, output)

	if err != nil {
		svc.errors <- err
	}

	return nil
}

func (svc *Service) readTracesNextPage(
	ctx context.Context,
	startTime time.Time,
	endTime time.Time,
	nextToken string,
) error {
	svc.Debug("readTracesNextPage")

	output, err := svc.xry.GetTraceSummaries(ctx, &xray.GetTraceSummariesInput{
		StartTime: aws.Time(startTime),
		EndTime:   aws.Time(endTime),
		NextToken: &nextToken,
	})
	if err != nil {
		return err
	}
	err = svc.processTraceSummaryOutput(ctx, startTime, endTime, output)

	if err != nil {
		return err
	}
	return nil
}

func chunkBy(items []string, chunkSize int) (chunks [][]string) {
	for chunkSize < len(items) {
		items, chunks = items[chunkSize:], append(chunks, items[0:chunkSize:chunkSize])
	}

	return append(chunks, items)
}
