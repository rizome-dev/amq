package metrics

import (
	"context"
	"fmt"
	
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.27.0"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// TracingConfig holds tracing configuration
type TracingConfig struct {
	Enabled      bool
	Endpoint     string
	ServiceName  string
	SamplingRate float64
	Insecure     bool
}

// DefaultTracingConfig returns default tracing configuration
func DefaultTracingConfig() TracingConfig {
	return TracingConfig{
		Enabled:      false,
		Endpoint:     "localhost:4317",
		ServiceName:  "amq",
		SamplingRate: 0.1,
		Insecure:     true,
	}
}

// Tracer wraps OpenTelemetry tracer
type Tracer struct {
	tracer   trace.Tracer
	provider *sdktrace.TracerProvider
}

// NewTracer creates a new tracer instance
func NewTracer(config TracingConfig) (*Tracer, error) {
	if !config.Enabled {
		// Return no-op tracer
		return &Tracer{
			tracer: otel.Tracer("amq-noop"),
		}, nil
	}
	
	// Create OTLP exporter
	ctx := context.Background()
	
	opts := []otlptracegrpc.Option{
		otlptracegrpc.WithEndpoint(config.Endpoint),
	}
	
	if config.Insecure {
		opts = append(opts, otlptracegrpc.WithDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())))
	}
	
	client := otlptracegrpc.NewClient(opts...)
	exporter, err := otlptrace.New(ctx, client)
	if err != nil {
		return nil, fmt.Errorf("failed to create trace exporter: %w", err)
	}
	
	// Create resource
	res, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String(config.ServiceName),
			semconv.ServiceVersionKey.String("0.1.0"),
			attribute.String("service.environment", "production"),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create resource: %w", err)
	}
	
	// Create tracer provider
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.TraceIDRatioBased(config.SamplingRate)),
	)
	
	// Set global provider
	otel.SetTracerProvider(provider)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))
	
	return &Tracer{
		tracer:   provider.Tracer("amq"),
		provider: provider,
	}, nil
}

// Shutdown shuts down the tracer
func (t *Tracer) Shutdown(ctx context.Context) error {
	if t.provider != nil {
		return t.provider.Shutdown(ctx)
	}
	return nil
}

// Start starts a new span
func (t *Tracer) Start(ctx context.Context, spanName string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	return t.tracer.Start(ctx, spanName, opts...)
}

// StartSpan starts a new span with common attributes
func (t *Tracer) StartSpan(ctx context.Context, operation string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	opts := []trace.SpanStartOption{
		trace.WithAttributes(attrs...),
		trace.WithSpanKind(trace.SpanKindServer),
	}
	
	return t.tracer.Start(ctx, operation, opts...)
}

// MessageAttributes returns common message attributes for tracing
func MessageAttributes(messageID, queue, msgType string, priority int) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String("message.id", messageID),
		attribute.String("message.queue", queue),
		attribute.String("message.type", msgType),
		attribute.Int("message.priority", priority),
	}
}

// ClientAttributes returns common client attributes for tracing
func ClientAttributes(clientID string, metadata map[string]string) []attribute.KeyValue {
	attrs := []attribute.KeyValue{
		attribute.String("client.id", clientID),
	}
	
	for k, v := range metadata {
		attrs = append(attrs, attribute.String(fmt.Sprintf("client.metadata.%s", k), v))
	}
	
	return attrs
}

// QueueAttributes returns common queue attributes for tracing
func QueueAttributes(queueName string, queueType string) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String("queue.name", queueName),
		attribute.String("queue.type", queueType),
	}
}

// RecordError records an error on the span
func RecordError(span trace.Span, err error) {
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
}

// SetOK sets the span status to OK
func SetOK(span trace.Span) {
	span.SetStatus(codes.Ok, "")
}

// Extract extracts trace context from carrier
func Extract(ctx context.Context, carrier propagation.TextMapCarrier) context.Context {
	return otel.GetTextMapPropagator().Extract(ctx, carrier)
}

// Inject injects trace context into carrier
func Inject(ctx context.Context, carrier propagation.TextMapCarrier) {
	otel.GetTextMapPropagator().Inject(ctx, carrier)
}

// SpanFromContext returns the current span from context
func SpanFromContext(ctx context.Context) trace.Span {
	return trace.SpanFromContext(ctx)
}

// ContextWithSpan returns a new context with the given span
func ContextWithSpan(ctx context.Context, span trace.Span) context.Context {
	return trace.ContextWithSpan(ctx, span)
}

// TracerFromContext retrieves tracer from context
type tracerContextKey string

const tracerKey tracerContextKey = "amq-tracer"

// WithTracer adds tracer to context
func WithTracer(ctx context.Context, tracer *Tracer) context.Context {
	return context.WithValue(ctx, tracerKey, tracer)
}

// TracerFromContext retrieves tracer from context
func TracerFromContext(ctx context.Context) *Tracer {
	if t, ok := ctx.Value(tracerKey).(*Tracer); ok {
		return t
	}
	return nil
}