module github.com/ojkelly/xray-to-otel

go 1.18

require (
	github.com/aws/aws-sdk-go-v2 v1.16.5
	github.com/aws/aws-sdk-go-v2/config v1.15.11
	github.com/aws/aws-sdk-go-v2/service/xray v1.13.7
	github.com/kelseyhightower/envconfig v1.4.0
	go.opentelemetry.io/otel v1.7.0
	go.opentelemetry.io/otel/exporters/otlp/otlptrace v1.7.0
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc v1.7.0
	go.opentelemetry.io/otel/sdk v1.7.0
	go.opentelemetry.io/otel/trace v1.7.0
	go.opentelemetry.io/proto/otlp v0.18.0
)

require (
	github.com/aws/aws-sdk-go-v2/credentials v1.12.6 // indirect
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.12.6 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.1.12 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.4.6 // indirect
	github.com/aws/aws-sdk-go-v2/internal/ini v1.3.13 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.9.6 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.11.9 // indirect
	github.com/aws/aws-sdk-go-v2/service/sts v1.16.7 // indirect
	github.com/aws/smithy-go v1.11.3 // indirect
	github.com/cenkalti/backoff/v4 v4.1.3 // indirect
	github.com/go-logr/logr v1.2.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.7.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/internal/retry v1.7.0 // indirect
	golang.org/x/net v0.0.0-20210405180319-a5a99cb37ef4 // indirect
	golang.org/x/sys v0.0.0-20210510120138-977fb7262007 // indirect
	golang.org/x/text v0.3.5 // indirect
	google.golang.org/genproto v0.0.0-20211118181313-81c1377c94b1 // indirect
	google.golang.org/grpc v1.47.0 // indirect
	google.golang.org/protobuf v1.28.0 // indirect
)
