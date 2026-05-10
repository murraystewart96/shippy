package tracing

import (
	"context"
	"fmt"
	"os"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	otelTrace "go.opentelemetry.io/otel/trace"
)

var (
	tracer otelTrace.Tracer
)

// TracingConfig holds tracing configuration
type TracingConfig struct {
	ServiceName  string
	Environment  string // "development", "testing", "production"
	OTLPEndpoint string
}

// InitTracer initializes OpenTelemetry tracing
func InitTracer(config TracingConfig) (func(), error) {
	var exporter trace.SpanExporter
	var err error

	// Choose exporter based on environment
	switch config.Environment {
	case "development", "testing":
		// Use file exporter for development and testing
		file, err := os.Create("traces.json")
		if err != nil {
			return nil, fmt.Errorf("failed to create traces file: %w", err)
		}
		exporter, err = stdouttrace.New(
			stdouttrace.WithPrettyPrint(),
			stdouttrace.WithWriter(file),
		)
	case "production":
		// Use OTLP HTTP exporter for production
		if config.OTLPEndpoint == "" {
			config.OTLPEndpoint = "http://localhost:4318/v1/traces"
		}
		exporter, err = otlptracehttp.New(
			context.Background(),
			otlptracehttp.WithEndpoint(config.OTLPEndpoint),
			otlptracehttp.WithInsecure(), // Use HTTPS in production
		)
	default:
		return nil, fmt.Errorf("unknown environment: %s", config.Environment)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to create exporter: %w", err)
	}

	var sampler trace.Sampler
	switch config.Environment {
	case "development", "testing":
		sampler = trace.AlwaysSample()
	case "production":
		sampler = trace.TraceIDRatioBased(0.1) // 10% sampling
	}

	// Create trace provider
	tp := trace.NewTracerProvider(
		trace.WithBatcher(exporter),
		trace.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String(config.ServiceName),
			semconv.ServiceVersionKey.String("1.0.0"),
			semconv.DeploymentEnvironmentKey.String(config.Environment),
		)),
		trace.WithSampler(sampler),
	)

	// Set global trace provider and propagator
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	// Create global tracer instance
	tracer = otel.Tracer(config.ServiceName)

	// Return shutdown function
	return func() {
		if err := tp.Shutdown(context.Background()); err != nil {
			fmt.Printf("Error shutting down tracer provider: %v\n", err)
		}
	}, nil
}

// GetTracer returns the global tracer instance
func GetTracer() otelTrace.Tracer {
	if tracer == nil {
		// Return a no-op tracer if not initialized
		return otel.Tracer("uninitialized")
	}
	return tracer
}

// StartSpan is a convenience function to start a span
func StartSpan(ctx context.Context, name string) (context.Context, otelTrace.Span) {
	return GetTracer().Start(ctx, name)
}
