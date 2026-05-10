package tracing

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/trace"
)

// InMemoryExporter captures spans in memory for testing
type InMemoryExporter struct {
	mu    sync.RWMutex
	spans []trace.ReadOnlySpan
}

// NewInMemoryExporter creates a new in-memory span exporter for testing
func NewInMemoryExporter() *InMemoryExporter {
	return &InMemoryExporter{
		spans: make([]trace.ReadOnlySpan, 0),
	}
}

// ExportSpans implements trace.SpanExporter
func (e *InMemoryExporter) ExportSpans(ctx context.Context, spans []trace.ReadOnlySpan) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	// Make copies of spans since they might be reused by the SDK
	for _, span := range spans {
		// Debug: Log when spans are actually captured
		fmt.Printf("📊 CAPTURED span '%s' ended at %v (duration: %v)\n",
			span.Name(), span.EndTime(), span.EndTime().Sub(span.StartTime()))
		e.spans = append(e.spans, span)
	}
	return nil
}

// Shutdown implements trace.SpanExporter
func (e *InMemoryExporter) Shutdown(ctx context.Context) error {
	return nil
}

// GetSpans returns all captured spans (thread-safe)
func (e *InMemoryExporter) GetSpans() []trace.ReadOnlySpan {
	e.mu.RLock()
	defer e.mu.RUnlock()

	// Return a copy to avoid race conditions
	result := make([]trace.ReadOnlySpan, len(e.spans))
	copy(result, e.spans)
	return result
}

// Clear removes all captured spans
func (e *InMemoryExporter) Clear() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.spans = e.spans[:0]
}

// GetSpansByName returns spans with the specified name
func (e *InMemoryExporter) GetSpansByName(name string) []trace.ReadOnlySpan {
	spans := e.GetSpans()
	var result []trace.ReadOnlySpan

	for _, span := range spans {
		if span.Name() == name {
			result = append(result, span)
		}
	}
	return result
}

// GetSpansByTraceID returns all spans belonging to a specific trace
func (e *InMemoryExporter) GetSpansByTraceID(traceID string) []trace.ReadOnlySpan {
	spans := e.GetSpans()
	var result []trace.ReadOnlySpan

	for _, span := range spans {
		if span.SpanContext().TraceID().String() == traceID {
			result = append(result, span)
		}
	}
	return result
}

// TraceMetrics contains timing measurements for a complete trace
type TraceMetrics struct {
	TraceID               string
	TotalDuration         time.Duration
	EventProcessingTime   time.Duration
	KafkaProduceTime      time.Duration
	KafkaTransportTime    time.Duration
	WorkerProcessingTime  time.Duration
	DatabaseOperationTime time.Duration
	CacheOperationTime    time.Duration

	// Span details for debugging
	EventListenerSpan trace.ReadOnlySpan
	KafkaProduceSpan  trace.ReadOnlySpan
	KafkaConsumeSpan  trace.ReadOnlySpan
	WorkerSpan        trace.ReadOnlySpan
	DatabaseSpan      trace.ReadOnlySpan
	CacheSpans        []trace.ReadOnlySpan
}

// AnalyzeTrace takes spans from a complete trace and calculates performance metrics
func (e *InMemoryExporter) AnalyzeTrace(traceID string) *TraceMetrics {
	spans := e.GetSpansByTraceID(traceID)
	if len(spans) == 0 {
		return nil
	}

	metrics := &TraceMetrics{
		TraceID:    traceID,
		CacheSpans: make([]trace.ReadOnlySpan, 0),
	}

	var earliestStart, latestEnd time.Time

	// Categorize spans and find timing boundaries
	for _, span := range spans {
		spanName := span.Name()
		startTime := span.StartTime()
		endTime := span.EndTime()
		duration := endTime.Sub(startTime)

		// Track overall trace boundaries
		if earliestStart.IsZero() || startTime.Before(earliestStart) {
			earliestStart = startTime
		}
		if latestEnd.IsZero() || endTime.After(latestEnd) {
			latestEnd = endTime
		}

		// Categorize spans by name
		switch spanName {
		case "events.handleSwapEvent", "events.handleSyncEvent":
			metrics.EventListenerSpan = span
			metrics.EventProcessingTime = duration
		case "kafka.produce":
			metrics.KafkaProduceSpan = span
			metrics.KafkaProduceTime = duration
		case "kafka.consume":
			metrics.KafkaConsumeSpan = span
		case "worker.handleTradeEvent", "worker.handleReserveEvent":
			metrics.WorkerSpan = span
			metrics.WorkerProcessingTime = duration
		case "db.CreateTrade", "db.CreateReserve":
			metrics.DatabaseSpan = span
			metrics.DatabaseOperationTime = duration
		case "cache.SetPrice", "cache.SetReserves":
			metrics.CacheSpans = append(metrics.CacheSpans, span)
			metrics.CacheOperationTime += duration
		}
	}

	// Calculate total trace duration
	if !earliestStart.IsZero() && !latestEnd.IsZero() {
		metrics.TotalDuration = latestEnd.Sub(earliestStart)
	}

	// Calculate Kafka transport time (time between produce end and consume start)
	if metrics.KafkaProduceSpan != nil && metrics.KafkaConsumeSpan != nil {
		produceEnd := metrics.KafkaProduceSpan.EndTime()
		consumeStart := metrics.KafkaConsumeSpan.StartTime()
		if consumeStart.After(produceEnd) {
			metrics.KafkaTransportTime = consumeStart.Sub(produceEnd)
		}
	}

	return metrics
}

// InitTestTracer initializes tracing with in-memory exporter for testing
func InitTestTracer(serviceName string) (*InMemoryExporter, func(), error) {
	exporter := NewInMemoryExporter()

	// Create trace provider with in-memory exporter
	tp := trace.NewTracerProvider(
		trace.WithSyncer(exporter), // SYNC export for immediate span availability
		trace.WithSampler(trace.AlwaysSample()),
	)

	// Set global providers
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	tracer = otel.Tracer(serviceName)

	return exporter, func() {
		tp.Shutdown(context.Background())
	}, nil
}
