package exporter

import (
	"encoding/json"
	"fmt"
	"log"
	"math"
	"strings"
	"time"

	"github.com/ojkelly/xray-to-otel/exporter/awsxray"
	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.10.0"
	"go.opentelemetry.io/otel/trace"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

func parseSegmentDocument(raw string) (*awsxray.Segment, error) {

	seg := &awsxray.Segment{}
	err := json.Unmarshal([]byte(raw), seg)

	if err != nil {
		return nil, fmt.Errorf("unable to parse segment document: %s", err)
	}

	return seg, nil
}

func parseXrayTimestamp(t float64) time.Time {
	sec, dec := math.Modf(t)
	return time.Unix(int64(sec), int64(dec*(1e9)))
}

// Parse the Xray Trace ID into an OTEL Trace ID
// `trace_id` – A unique identifier that connects all segments and subsegments
//  originating from a single client request.
// Trace ID Format
//   A trace_id consists of three numbers separated by hyphens. For example,
//   1-58406520-a006649127e371903a2de979. This includes:
//   The version number, that is, 1.
//   The time of the original request, in Unix epoch time, in 8 hexadecimal digits.
//   For example, 10:00AM December 1st, 2016 PST in epoch time is 1480615200
//    seconds, or 58406520 in hexadecimal digits.
//   A 96-bit identifier for the trace, globally unique, in 24 hexadecimal digits.
// https://docs.aws.amazon.com/xray/latest/devguide/xray-api-segmentdocuments.html
func parseXrayTraceID(tid string) (trace.TraceID, error) {
	s := strings.Split(tid, "-")

	if len(s) != 3 {
		return trace.TraceID{}, fmt.Errorf("unable to parse xray trace id")
	}

	return trace.TraceIDFromHex(fmt.Sprintf("%s%s", s[1], s[2]))
}

func getAttributesFromXraySegment(seg awsxray.Segment) []attribute.KeyValue {
	attrs := []attribute.KeyValue{
		semconv.CloudProviderAWS,
	}

	if seg.Origin != nil {
		switch *seg.Origin {
		case "AWS::Lambda::Function":
			attrs = append(attrs, semconv.CloudPlatformAWSLambda)

		default: // TODO: these
			if false {
				attrs = append(attrs, semconv.CloudPlatformAWSEC2)
			}
			if false {
				attrs = append(attrs, semconv.CloudPlatformAWSECS)
			}
			if false {
				attrs = append(attrs, semconv.CloudPlatformAWSEKS)
			}
			if false {
				attrs = append(attrs, semconv.CloudPlatformAWSElasticBeanstalk)
			}
			if false {
				attrs = append(attrs, semconv.CloudPlatformAWSAppRunner)
			}
		}
	}

	if seg.User != nil {
		attrs = append(attrs, attribute.KeyValue{
			Key:   attribute.Key("aws.user"),
			Value: attribute.StringValue(*seg.User),
		})
	}

	if seg.ResourceARN != nil {
		attrs = append(attrs, attribute.KeyValue{
			Key:   attribute.Key("aws.arn"),
			Value: attribute.StringValue(*seg.ResourceARN),
		})
	}

	if seg.HTTP != nil {
		if seg.HTTP.Request != nil {
			if seg.HTTP.Request.URL != nil {
				attrs = append(attrs, attribute.KeyValue{
					Key:   semconv.HTTPURLKey,
					Value: attribute.StringValue(*seg.HTTP.Request.URL),
				})
			}
		}
		if seg.HTTP.Response != nil {
			if seg.HTTP.Response.Status != nil {
				attrs = append(attrs, attribute.KeyValue{
					Key:   semconv.HTTPStatusCodeKey,
					Value: attribute.Int64Value(*seg.HTTP.Response.Status),
				})
			}
			if seg.HTTP.Response.ContentLength != nil {
				attrs = append(attrs, attribute.KeyValue{
					Key:   semconv.HTTPResponseContentLengthKey,
					Value: attribute.Int64Value(*seg.HTTP.Response.ContentLength),
				})
			}
		}
	}

	if seg.Fault != nil && *seg.Fault {
		attrs = append(attrs, attribute.KeyValue{
			Key:   attribute.Key("error"),
			Value: attribute.BoolValue(true),
		})
	}
	if seg.Error != nil && *seg.Error {
		attrs = append(attrs, attribute.KeyValue{
			Key:   attribute.Key("error"),
			Value: attribute.BoolValue(true),
		})
	}
	if seg.Throttle != nil && *seg.Throttle {
		attrs = append(attrs, attribute.KeyValue{
			Key:   attribute.Key("aws.throttle"),
			Value: attribute.BoolValue(true),
		})
	}

	if seg.Cause != nil {
		if seg.Cause.Message != nil {
			attrs = append(attrs, attribute.KeyValue{
				Key:   attribute.Key("aws.xray.cause.exception.message"),
				Value: attribute.StringValue(*seg.Cause.Message),
			})
		}
		if seg.Cause.WorkingDirectory != nil {
			attrs = append(attrs, attribute.KeyValue{
				Key:   attribute.Key("aws.xray.cause.working-directory"),
				Value: attribute.StringValue(*seg.Cause.WorkingDirectory),
			})
		}
		if seg.Cause.Paths != nil {
			attrs = append(attrs, attribute.KeyValue{
				Key:   semconv.ExceptionTypeKey,
				Value: attribute.StringSliceValue(seg.Cause.Paths),
			})
		}

	}

	if seg.AWS != nil {
		if seg.AWS.AccountID != nil {
			attrs = append(attrs, semconv.CloudAccountIDKey.String(*seg.AWS.AccountID))
		}
		if seg.AWS.RemoteRegion != nil {
			attrs = append(attrs, semconv.CloudRegionKey.String(*seg.AWS.RemoteRegion))
		}

		// TODO: ECS, EC2, EKS, XRay metadata

		if seg.AWS.ResourceNames != nil {
			attrs = append(attrs, attribute.KeyValue{
				Key:   attribute.Key("aws.resource-names"),
				Value: attribute.StringSliceValue(*seg.AWS.ResourceNames),
			})
		}
		if seg.AWS.Operation != nil {
			attrs = append(attrs, attribute.KeyValue{
				Key:   attribute.Key("aws.operation"),
				Value: attribute.StringValue(*seg.AWS.Operation),
			})
		}
		if seg.AWS.AccountID != nil {
			attrs = append(attrs, attribute.KeyValue{
				Key:   attribute.Key("aws.account.id"),
				Value: attribute.StringValue(*seg.AWS.AccountID),
			})
		}
		if seg.AWS.RemoteRegion != nil {
			attrs = append(attrs, attribute.KeyValue{
				Key:   attribute.Key("aws.remote-region"),
				Value: attribute.StringValue(*seg.AWS.RemoteRegion),
			})
		}
		if seg.AWS.RequestID != nil {
			attrs = append(attrs, attribute.KeyValue{
				Key:   attribute.Key("aws.request.id"),
				Value: attribute.StringValue(*seg.AWS.RequestID),
			})
		}
		if seg.AWS.QueueURL != nil {
			attrs = append(attrs, attribute.KeyValue{
				Key:   attribute.Key("aws.queue.url"),
				Value: attribute.StringValue(*seg.AWS.QueueURL),
			})
		}
		if seg.AWS.TableName != nil {
			attrs = append(attrs, attribute.KeyValue{
				Key:   attribute.Key("aws.table.name"),
				Value: attribute.StringValue(*seg.AWS.TableName),
			})
		}
		if seg.AWS.Retries != nil {
			attrs = append(attrs, attribute.KeyValue{
				Key:   attribute.Key("aws.retries"),
				Value: attribute.Int64Value(*seg.AWS.Retries),
			})
		}

		if seg.AWS.Beanstalk != nil {
			if seg.AWS.Beanstalk.Environment != nil {
				attrs = append(attrs, attribute.KeyValue{
					Key:   attribute.Key("aws.beanstalk.environment"),
					Value: attribute.StringValue(*seg.AWS.Beanstalk.Environment),
				})
			}
			if seg.AWS.Beanstalk.DeploymentID != nil {
				attrs = append(attrs, attribute.KeyValue{
					Key:   attribute.Key("aws.beanstalk.deployment.id"),
					Value: attribute.Int64Value(*seg.AWS.Beanstalk.DeploymentID),
				})
			}
			if seg.AWS.Beanstalk.VersionLabel != nil {
				attrs = append(attrs, attribute.KeyValue{
					Key:   attribute.Key("aws.beanstalk.version"),
					Value: attribute.StringValue(*seg.AWS.Beanstalk.VersionLabel),
				})
			}
		}

		if seg.AWS.ECS != nil {
			if seg.AWS.ECS.ContainerName != nil {
				attrs = append(attrs, attribute.KeyValue{
					Key:   attribute.Key("aws.ecs.container.name"),
					Value: attribute.StringValue(*seg.AWS.ECS.ContainerName),
				})
			}
			if seg.AWS.ECS.ContainerID != nil {
				attrs = append(attrs, attribute.KeyValue{
					Key:   attribute.Key("aws.ecs.container.id"),
					Value: attribute.StringValue(*seg.AWS.ECS.ContainerID),
				})
			}
			if seg.AWS.ECS.TaskArn != nil {
				attrs = append(attrs, attribute.KeyValue{
					Key:   attribute.Key("aws.ecs.task.arn"),
					Value: attribute.StringValue(*seg.AWS.ECS.TaskArn),
				})
			}
			if seg.AWS.ECS.TaskFamily != nil {
				attrs = append(attrs, attribute.KeyValue{
					Key:   attribute.Key("aws.ecs.task.family"),
					Value: attribute.StringValue(*seg.AWS.ECS.TaskFamily),
				})
			}
			if seg.AWS.ECS.ClusterArn != nil {
				attrs = append(attrs, attribute.KeyValue{
					Key:   attribute.Key("aws.ecs.cluster.arn"),
					Value: attribute.StringValue(*seg.AWS.ECS.ClusterArn),
				})
			}
			if seg.AWS.ECS.ContainerArn != nil {
				attrs = append(attrs, attribute.KeyValue{
					Key:   attribute.Key("aws.ecs.container.arn"),
					Value: attribute.StringValue(*seg.AWS.ECS.ContainerArn),
				})
			}
			if seg.AWS.ECS.AvailabilityZone != nil {
				attrs = append(attrs, attribute.KeyValue{
					Key:   attribute.Key("aws.availability-zone"),
					Value: attribute.StringValue(*seg.AWS.ECS.AvailabilityZone),
				})
			}
			if seg.AWS.ECS.LaunchType != nil {
				attrs = append(attrs, attribute.KeyValue{
					Key:   attribute.Key("aws.ecs.launch-type"),
					Value: attribute.StringValue(*seg.AWS.ECS.LaunchType),
				})
			}
		}

		if seg.AWS.EC2 != nil {
			if seg.AWS.EC2.InstanceID != nil {
				attrs = append(attrs, attribute.KeyValue{
					Key:   attribute.Key("aws.ec2.instance.id"),
					Value: attribute.StringValue(*seg.AWS.EC2.InstanceID),
				})
			}
			if seg.AWS.EC2.AvailabilityZone != nil {
				attrs = append(attrs, attribute.KeyValue{
					Key:   attribute.Key("aws.availability-zone"),
					Value: attribute.StringValue(*seg.AWS.EC2.AvailabilityZone),
				})
			}
			if seg.AWS.EC2.InstanceSize != nil {
				attrs = append(attrs, attribute.KeyValue{
					Key:   attribute.Key("aws.ec2.instance.size"),
					Value: attribute.StringValue(*seg.AWS.EC2.InstanceSize),
				})
			}
			if seg.AWS.EC2.InstanceID != nil {
				attrs = append(attrs, attribute.KeyValue{
					Key:   attribute.Key("aws.ec2.instance.id"),
					Value: attribute.StringValue(*seg.AWS.EC2.InstanceID),
				})
			}
		}

		if seg.AWS.EKS != nil {
			// TODO: are there k8s semconv's for this?
			if seg.AWS.EKS.ClusterName != nil {
				attrs = append(attrs, attribute.KeyValue{
					Key:   attribute.Key("aws.eks.cluster.name"),
					Value: attribute.StringValue(*seg.AWS.EKS.ClusterName),
				})
			}
			if seg.AWS.EKS.ContainerID != nil {
				attrs = append(attrs, attribute.KeyValue{
					Key:   attribute.Key("aws.eks.container.id"),
					Value: attribute.StringValue(*seg.AWS.EKS.ContainerID),
				})
			}
			if seg.AWS.EKS.Pod != nil {
				attrs = append(attrs, attribute.KeyValue{
					Key:   attribute.Key("aws.eks.pod"),
					Value: attribute.StringValue(*seg.AWS.EKS.Pod),
				})
			}
		}
	}

	if len(seg.Annotations) != 0 {
		for k, v := range seg.Annotations {
			d, err := json.Marshal(v)
			if err != nil {
				log.Println("ERROR:", err)
			}
			attrs = append(attrs, attribute.KeyValue{
				Key:   attribute.Key(k),
				Value: attribute.StringValue(string(d)),
			})
		}
	}

	if len(seg.Metadata) != 0 {
		for k, v := range seg.Metadata {
			d, err := json.Marshal(v)
			if err != nil {
				log.Println("ERROR:", err)
			}
			attrs = append(attrs, attribute.KeyValue{
				Key:   attribute.Key(fmt.Sprintf("aws.metadata.%s", k)),
				Value: attribute.StringValue(string(d)),
			})
		}
	}

	if seg.Namespace != nil {
		attrs = append(attrs, attribute.KeyValue{
			Key:   attribute.Key("aws.namespace"),
			Value: attribute.StringValue(*seg.Namespace),
		})
	}
	if seg.Type != nil {
		attrs = append(attrs, attribute.KeyValue{
			Key:   attribute.Key("aws.type"),
			Value: attribute.StringValue(*seg.Type),
		})
	}
	if seg.PrecursorIDs != nil {
		attrs = append(attrs, attribute.KeyValue{
			Key:   attribute.Key("aws.precusor.ids"),
			Value: attribute.StringSliceValue(seg.PrecursorIDs),
		})
	}

	return attrs
}

func getStatusFromXraySegment(seg awsxray.Segment) *tracepb.Status {
	status := tracepb.Status{}
	if seg.Error != nil && *seg.Error {
		status.Code = tracepb.Status_STATUS_CODE_ERROR
	}

	return &status
}

func getEventsFromXraySegment(seg awsxray.Segment) []*tracepb.Span_Event {
	evts := []*tracepb.Span_Event{}

	if seg.Cause != nil && seg.Cause.Exceptions != nil {
		for _, ex := range seg.Cause.Exceptions {
			attrs := []*commonpb.KeyValue{}

			if ex.Message != nil {
				attrs = append(attrs, &commonpb.KeyValue{
					Key:   "aws.exception.message",
					Value: Value(attribute.StringValue(*ex.Message)),
				})
			}
			if ex.Type != nil {
				attrs = append(attrs, &commonpb.KeyValue{
					Key:   "aws.exception.type",
					Value: Value(attribute.StringValue(*ex.Type)),
				})
			}
			if ex.Remote != nil {
				attrs = append(attrs, &commonpb.KeyValue{
					Key:   "aws.exception.remote",
					Value: Value(attribute.BoolValue(*ex.Remote)),
				})
			}
			if ex.Truncated != nil {
				attrs = append(attrs, &commonpb.KeyValue{
					Key:   "aws.exception.trucated",
					Value: Value(attribute.Int64Value(*ex.Truncated)),
				})
			}
			if ex.Skipped != nil {
				attrs = append(attrs, &commonpb.KeyValue{
					Key:   "aws.exception.skipped",
					Value: Value(attribute.Int64Value(*ex.Skipped)),
				})
			}

			evts = append(evts, &tracepb.Span_Event{
				Attributes: attrs,
			})
		}
	}
	return evts
}
