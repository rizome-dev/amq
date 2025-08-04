package metrics

import (
	"context"
	"time"
	
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics holds all AMQ metrics
type Metrics struct {
	// Message metrics
	MessagesProcessed   *prometheus.CounterVec
	MessageDuration     *prometheus.HistogramVec
	MessagesInFlight    *prometheus.GaugeVec
	MessageSize         *prometheus.HistogramVec
	
	// Queue metrics
	QueueDepth          *prometheus.GaugeVec
	QueueCapacity       *prometheus.GaugeVec
	EnqueueRate         *prometheus.CounterVec
	DequeueRate         *prometheus.CounterVec
	
	// Client metrics
	ClientConnections   *prometheus.GaugeVec
	ClientHeartbeats    *prometheus.CounterVec
	ClientSubscriptions *prometheus.GaugeVec
	
	// Error metrics
	ErrorsTotal         *prometheus.CounterVec
	RetryAttempts       *prometheus.CounterVec
	DeadLetterMessages  *prometheus.CounterVec
	
	// Store metrics
	StoreOperations     *prometheus.CounterVec
	StoreLatency        *prometheus.HistogramVec
	StoreSize           prometheus.Gauge
	
	// System metrics
	WorkerPoolSize      *prometheus.GaugeVec
	ActiveWorkers       *prometheus.GaugeVec
	BatchSize           *prometheus.HistogramVec
	
	// gRPC metrics (if enabled)
	GRPCRequests        *prometheus.CounterVec
	GRPCDuration        *prometheus.HistogramVec
	GRPCStreamActive    *prometheus.GaugeVec
	
	// HTTP metrics
	HTTPRequests        *prometheus.CounterVec
	HTTPDuration        *prometheus.HistogramVec
	HTTPRequestSize     *prometheus.HistogramVec
	HTTPResponseSize    *prometheus.HistogramVec
}

// NewMetrics creates a new metrics instance
func NewMetrics(namespace string) *Metrics {
	return &Metrics{
		// Message metrics
		MessagesProcessed: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "messages_processed_total",
				Help:      "Total number of messages processed",
			},
			[]string{"queue", "type", "status"},
		),
		MessageDuration: promauto.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: namespace,
				Name:      "message_processing_duration_seconds",
				Help:      "Message processing duration in seconds",
				Buckets:   prometheus.DefBuckets,
			},
			[]string{"queue", "type"},
		),
		MessagesInFlight: promauto.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "messages_in_flight",
				Help:      "Number of messages currently being processed",
			},
			[]string{"queue"},
		),
		MessageSize: promauto.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: namespace,
				Name:      "message_size_bytes",
				Help:      "Message size in bytes",
				Buckets:   prometheus.ExponentialBuckets(100, 10, 7), // 100B to 100MB
			},
			[]string{"queue", "type"},
		),
		
		// Queue metrics
		QueueDepth: promauto.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "queue_depth",
				Help:      "Number of messages in queue",
			},
			[]string{"queue", "priority"},
		),
		QueueCapacity: promauto.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "queue_capacity",
				Help:      "Maximum capacity of queue",
			},
			[]string{"queue"},
		),
		EnqueueRate: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "enqueue_total",
				Help:      "Total number of messages enqueued",
			},
			[]string{"queue", "priority"},
		),
		DequeueRate: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "dequeue_total",
				Help:      "Total number of messages dequeued",
			},
			[]string{"queue", "priority"},
		),
		
		// Client metrics
		ClientConnections: promauto.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "client_connections",
				Help:      "Number of active client connections",
			},
			[]string{"type"},
		),
		ClientHeartbeats: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "client_heartbeats_total",
				Help:      "Total number of client heartbeats received",
			},
			[]string{"client_id"},
		),
		ClientSubscriptions: promauto.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "client_subscriptions",
				Help:      "Number of active subscriptions per client",
			},
			[]string{"client_id"},
		),
		
		// Error metrics
		ErrorsTotal: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "errors_total",
				Help:      "Total number of errors",
			},
			[]string{"type", "operation"},
		),
		RetryAttempts: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "retry_attempts_total",
				Help:      "Total number of retry attempts",
			},
			[]string{"queue", "reason"},
		),
		DeadLetterMessages: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "dead_letter_messages_total",
				Help:      "Total number of messages sent to dead letter queue",
			},
			[]string{"queue", "reason"},
		),
		
		// Store metrics
		StoreOperations: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "store_operations_total",
				Help:      "Total number of store operations",
			},
			[]string{"operation", "status"},
		),
		StoreLatency: promauto.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: namespace,
				Name:      "store_operation_duration_seconds",
				Help:      "Store operation duration in seconds",
				Buckets:   prometheus.DefBuckets,
			},
			[]string{"operation"},
		),
		StoreSize: promauto.NewGauge(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "store_size_bytes",
				Help:      "Total size of the store in bytes",
			},
		),
		
		// System metrics
		WorkerPoolSize: promauto.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "worker_pool_size",
				Help:      "Size of the worker pool",
			},
			[]string{"queue"},
		),
		ActiveWorkers: promauto.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "active_workers",
				Help:      "Number of active workers",
			},
			[]string{"queue"},
		),
		BatchSize: promauto.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: namespace,
				Name:      "batch_size",
				Help:      "Number of messages in batch",
				Buckets:   prometheus.LinearBuckets(1, 10, 10),
			},
			[]string{"queue"},
		),
		
		// gRPC metrics
		GRPCRequests: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "grpc_requests_total",
				Help:      "Total number of gRPC requests",
			},
			[]string{"method", "status"},
		),
		GRPCDuration: promauto.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: namespace,
				Name:      "grpc_request_duration_seconds",
				Help:      "gRPC request duration in seconds",
				Buckets:   prometheus.DefBuckets,
			},
			[]string{"method"},
		),
		GRPCStreamActive: promauto.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "grpc_streams_active",
				Help:      "Number of active gRPC streams",
			},
			[]string{"method"},
		),
		
		// HTTP metrics
		HTTPRequests: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "http_requests_total",
				Help:      "Total number of HTTP requests",
			},
			[]string{"method", "endpoint", "status"},
		),
		HTTPDuration: promauto.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: namespace,
				Name:      "http_request_duration_seconds",
				Help:      "HTTP request duration in seconds",
				Buckets:   prometheus.DefBuckets,
			},
			[]string{"method", "endpoint"},
		),
		HTTPRequestSize: promauto.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: namespace,
				Name:      "http_request_size_bytes",
				Help:      "HTTP request size in bytes",
				Buckets:   prometheus.ExponentialBuckets(100, 10, 7),
			},
			[]string{"method", "endpoint"},
		),
		HTTPResponseSize: promauto.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: namespace,
				Name:      "http_response_size_bytes",
				Help:      "HTTP response size in bytes",
				Buckets:   prometheus.ExponentialBuckets(100, 10, 7),
			},
			[]string{"method", "endpoint"},
		),
	}
}

// RecordMessageProcessed records a processed message
func (m *Metrics) RecordMessageProcessed(queue, msgType, status string, duration time.Duration) {
	m.MessagesProcessed.WithLabelValues(queue, msgType, status).Inc()
	m.MessageDuration.WithLabelValues(queue, msgType).Observe(duration.Seconds())
}

// RecordMessageSize records message size
func (m *Metrics) RecordMessageSize(queue, msgType string, size int) {
	m.MessageSize.WithLabelValues(queue, msgType).Observe(float64(size))
}

// UpdateQueueDepth updates queue depth gauge
func (m *Metrics) UpdateQueueDepth(queue string, priority string, depth int) {
	m.QueueDepth.WithLabelValues(queue, priority).Set(float64(depth))
}

// RecordEnqueue records message enqueue
func (m *Metrics) RecordEnqueue(queue, priority string) {
	m.EnqueueRate.WithLabelValues(queue, priority).Inc()
}

// RecordDequeue records message dequeue
func (m *Metrics) RecordDequeue(queue, priority string) {
	m.DequeueRate.WithLabelValues(queue, priority).Inc()
}

// RecordError records an error
func (m *Metrics) RecordError(errorType, operation string) {
	m.ErrorsTotal.WithLabelValues(errorType, operation).Inc()
}

// RecordStoreOperation records a store operation
func (m *Metrics) RecordStoreOperation(operation string, success bool, duration time.Duration) {
	status := "success"
	if !success {
		status = "failure"
	}
	m.StoreOperations.WithLabelValues(operation, status).Inc()
	m.StoreLatency.WithLabelValues(operation).Observe(duration.Seconds())
}

// UpdateClientConnections updates client connection count
func (m *Metrics) UpdateClientConnections(clientType string, delta int) {
	m.ClientConnections.WithLabelValues(clientType).Add(float64(delta))
}

// RecordHTTPRequest records an HTTP request
func (m *Metrics) RecordHTTPRequest(method, endpoint, status string, duration time.Duration, reqSize, respSize int) {
	m.HTTPRequests.WithLabelValues(method, endpoint, status).Inc()
	m.HTTPDuration.WithLabelValues(method, endpoint).Observe(duration.Seconds())
	if reqSize > 0 {
		m.HTTPRequestSize.WithLabelValues(method, endpoint).Observe(float64(reqSize))
	}
	if respSize > 0 {
		m.HTTPResponseSize.WithLabelValues(method, endpoint).Observe(float64(respSize))
	}
}

// RecordGRPCRequest records a gRPC request
func (m *Metrics) RecordGRPCRequest(method, status string, duration time.Duration) {
	m.GRPCRequests.WithLabelValues(method, status).Inc()
	m.GRPCDuration.WithLabelValues(method).Observe(duration.Seconds())
}

// UpdateGRPCStreams updates active gRPC stream count
func (m *Metrics) UpdateGRPCStreams(method string, delta int) {
	m.GRPCStreamActive.WithLabelValues(method).Add(float64(delta))
}

// Timer provides a convenient way to time operations
type Timer struct {
	start time.Time
}

// NewTimer creates a new timer
func NewTimer() *Timer {
	return &Timer{start: time.Now()}
}

// ObserveDuration records the duration since timer creation
func (t *Timer) ObserveDuration() time.Duration {
	return time.Since(t.start)
}

// Context keys for metrics
type contextKey string

const (
	metricsContextKey contextKey = "amq-metrics"
)

// FromContext retrieves metrics from context
func FromContext(ctx context.Context) *Metrics {
	if m, ok := ctx.Value(metricsContextKey).(*Metrics); ok {
		return m
	}
	return nil
}

// WithContext adds metrics to context
func WithContext(ctx context.Context, m *Metrics) context.Context {
	return context.WithValue(ctx, metricsContextKey, m)
}