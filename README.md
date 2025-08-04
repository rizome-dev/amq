# AMQ - Agentic Message Queue

[![GoDoc](https://pkg.go.dev/badge/github.com/rizome-dev/amq)](https://pkg.go.dev/github.com/rizome-dev/amq)
[![Go Report Card](https://goreportcard.com/badge/github.com/rizome-dev/amq)](https://goreportcard.com/report/github.com/rizome-dev/amq)
[![CI](https://github.com/rizome-dev/amq/actions/workflows/ci.yml/badge.svg)](https://github.com/rizome-dev/amq/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

Production-grade message queue designed for AI agent communication at scale. Built by [Rizome Labs](https://rizome.dev) for enterprise deployments supporting thousands of concurrent agents.

## Quick Start

### Docker Deployment
```bash
docker run -d \
  --name amq-server \
  -p 8080:8080 \
  -p 9090:9090 \
  -v /data/amq:/data \
  -e AMQ_STORAGE_PATH=/data \
  -e AMQ_WORKER_POOL_SIZE=50 \
  -e AMQ_METRICS_ENABLED=true \
  rizome/amq:latest
```

### Kubernetes Deployment
```bash
helm repo add rizome https://charts.rizome.dev
helm install amq rizome/amq \
  --namespace amq-system \
  --create-namespace \
  --set persistence.size=500Gi \
  --set replicas=3 \
  --set resources.requests.memory=16Gi \
  --set resources.requests.cpu=8 \
  --set autoscaling.enabled=true \
  --set autoscaling.maxReplicas=10
```

## Integration Options

### Go SDK
```go
import "github.com/rizome-dev/amq"

// Initialize AMQ
config := amq.DefaultConfig()
config.StorePath = "/data/amq"
config.WorkerPoolSize = 100
amq, _ := amq.New(config)
defer amq.Close()

// Create queues
amq.CreateQueue(ctx, "orders", types.QueueTypeTask)
amq.CreateQueue(ctx, "analytics", types.QueueTypeTask)

// Producer client
producer, _ := amq.NewClient("producer-1")
producer.SubmitTask(ctx, "orders", []byte("process order #123"),
    client.WithPriority(9),
    client.WithTTL(1*time.Hour),
    client.WithMaxRetries(5))

// Consumer client  
consumer, _ := amq.NewClient("consumer-1")
consumer.Subscribe(ctx, "orders")
messages, _ := consumer.Receive(ctx, 10)
for _, msg := range messages {
    // Process message
    consumer.Ack(ctx, msg.ID)
}

// Async consumer with handler
async, _ := amq.NewAsyncConsumer("async-1")
async.Subscribe(ctx, "analytics")
async.Start(func(ctx context.Context, msg *types.Message) error {
    // Process message
    return nil // Auto-acks on success
}, client.ConsumerOptions{
    MaxConcurrency: 10,
    BatchSize:      50,
    AutoAck:        true,
})
```

### gRPC API
```protobuf
service AMQ {
  // Client management
  rpc RegisterClient(RegisterClientRequest) returns (RegisterClientResponse);
  rpc Heartbeat(HeartbeatRequest) returns (google.protobuf.Empty);
  
  // Queue operations
  rpc CreateQueue(CreateQueueRequest) returns (google.protobuf.Empty);
  rpc Subscribe(SubscribeRequest) returns (google.protobuf.Empty);
  
  // Messaging
  rpc SubmitTask(SubmitTaskRequest) returns (SubmitTaskResponse);
  rpc SendDirect(SendDirectRequest) returns (SendDirectResponse);
  rpc ReceiveMessages(ReceiveMessagesRequest) returns (stream Message);
  rpc AckMessage(AckMessageRequest) returns (google.protobuf.Empty);
  rpc NackMessage(NackMessageRequest) returns (google.protobuf.Empty);
  
  // Batch operations for performance
  rpc SubmitTaskBatch(SubmitTaskBatchRequest) returns (SubmitTaskBatchResponse);
  rpc AckMessageBatch(AckMessageBatchRequest) returns (google.protobuf.Empty);
}
```

### HTTP REST API
```bash
POST   /v1/clients/register      # Register client
POST   /v1/tasks                 # Submit task
GET    /v1/messages              # Receive messages
POST   /v1/messages/:id/ack      # Acknowledge message
POST   /v1/messages/:id/nack     # Negative acknowledge
GET    /v1/health                # Health check
GET    /metrics                  # Prometheus metrics
```

### Configuration
```bash
# Core settings
AMQ_STORAGE_PATH=/data/amq           # BadgerDB storage directory
AMQ_WORKER_POOL_SIZE=100            # Workers per queue
AMQ_MESSAGE_TIMEOUT=300s            # Message processing timeout
AMQ_MAX_MESSAGE_SIZE=10485760       # 10MB max message size

# Queue management
AMQ_EXPIRY_CHECK_INTERVAL=60s       # How often to check for expired messages
AMQ_RETRY_INTERVAL=30s              # Retry interval for failed messages
AMQ_RETENTION_PERIOD=604800         # 7 days message retention

# Connection pooling
AMQ_POOL_MIN_SIZE=10                # Min connections per pool
AMQ_POOL_MAX_SIZE=100               # Max connections per pool
AMQ_POOL_MAX_IDLE_TIME=300s         # Max idle time before closing

# Circuit breaker
AMQ_CB_FAILURE_THRESHOLD=5          # Failures before opening circuit
AMQ_CB_SUCCESS_THRESHOLD=2          # Successes to close circuit
AMQ_CB_OPEN_DURATION=60s            # Time to wait in open state

# Monitoring & security
AMQ_METRICS_ENABLED=true            # Prometheus metrics
AMQ_TRACING_ENABLED=true            # OpenTelemetry tracing
AMQ_TLS_CERT=/certs/server.crt      # TLS certificate
AMQ_TLS_KEY=/certs/server.key       # TLS private key
```

### Helm Values
```yaml
replicas: 3
resources:
  requests:
    memory: "16Gi"
    cpu: "8"
  limits:
    memory: "32Gi"
    cpu: "16"

persistence:
  enabled: true
  storageClass: "fast-ssd"
  size: "500Gi"

autoscaling:
  enabled: true
  minReplicas: 3
  maxReplicas: 10
  targetCPUUtilizationPercentage: 70

metrics:
  enabled: true
  serviceMonitor:
    enabled: true
```

## Monitoring & Alerting

### Prometheus Metrics
```yaml
amq_messages_processed_total        # Message throughput by queue/status
amq_message_processing_duration_ms  # Processing latency histogram
amq_queue_depth                     # Current queue depth
amq_queue_enqueue_total             # Messages enqueued
amq_queue_dequeue_total             # Messages dequeued
amq_client_connections_active       # Active client connections
amq_client_heartbeats_total         # Client heartbeats received
amq_errors_total                    # Errors by type
amq_circuit_breaker_state           # Circuit breaker state (0=closed, 1=open, 2=half-open)
amq_pool_connections_active         # Active pooled connections
```

### Alert Rules
```yaml
- alert: HighQueueDepth
  expr: amq_queue_depth > 10000
  for: 5m
  annotations:
    summary: "Queue {{ $labels.queue }} has high depth"

- alert: HighErrorRate
  expr: rate(amq_errors_total[5m]) > 0.01
  for: 5m
  annotations:
    summary: "Error rate above 1%"
```

## Performance

### Benchmarks (Single Node)
- **Message Throughput**: 100,000+ msg/sec
- **Queue Operations**: O(1) enqueue/dequeue with BadgerDB
- **Latency**: < 1ms p99 (local network)
- **Concurrent Clients**: 10,000+ per node
- **Message Size**: Up to 10MB per message

---

Built with ❤️ by [Rizome Labs](https://rizome.dev)
