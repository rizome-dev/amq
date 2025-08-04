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

config := amq.Config{
    StorePath:      "/data/amq",
    WorkerPoolSize: 100,
    MessageTimeout: 5 * time.Minute,
}
amq, _ := amq.New(config)
```

### gRPC API
```protobuf
service AMQ {
  rpc SubmitTask(TaskRequest) returns (TaskResponse);
  rpc ReceiveMessages(ReceiveRequest) returns (stream Message);
  rpc AckMessage(AckRequest) returns (AckResponse);
}
```

### HTTP REST API
```bash
POST   /v1/tasks
GET    /v1/messages
POST   /v1/messages/:id/ack
GET    /v1/health
GET    /metrics
```

### Environment Variables
```bash
AMQ_STORAGE_PATH=/data/amq           # Persistent storage location
AMQ_WORKER_POOL_SIZE=100            # Worker threads per queue
AMQ_MAX_MESSAGE_SIZE=10485760       # 10MB max message size
AMQ_RETENTION_PERIOD=604800         # 7 days message retention
AMQ_METRICS_ENABLED=true            # Enable Prometheus metrics
AMQ_TLS_CERT=/certs/server.crt      # TLS certificate
AMQ_TLS_KEY=/certs/server.key       # TLS private key
AMQ_AUTH_TOKEN=<secure-token>       # Authentication token
```

### Helm Values
```yaml
# values-production.yaml
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
# Key metrics to monitor
amq_messages_processed_total        # Message throughput
amq_message_processing_duration_ms  # Processing latency
amq_queue_depth                     # Queue backlog
amq_client_connections             # Active clients
amq_errors_total                   # Error rate
```

### Grafana Dashboard
Pre-built dashboards available at [grafana.com/dashboards/amq](https://grafana.com)

### Alert Rules
```yaml
# Example Prometheus alerts
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

## Benchmarks (Single Node)
- **Message Throughput**: 100,000+ msg/sec
- **Latency**: < 1ms p99 (local network)
- **Concurrent Clients**: 10,000+ per node
- **Message Size**: Up to 10MB per message

## Best Practices

### Topic Naming
```
<domain>.<entity>.<action>

Examples:
orders.payment.process
users.profile.update
analytics.events.aggregate
```

## Roadmap

### Q1 2025
- [ ] Stream processing API
- [ ] GraphQL API support
- [ ] Advanced routing rules
- [ ] Message deduplication

### Q2 2025  
- [ ] WASM plugin system
- [ ] Kafka bridge
- [ ] Time-series message store

### Contact
- **Rizome Labs**: [sam@rizome.dev](mailto:sam@rizome.dev)

---

Built with ❤️ by [Rizome Labs](https://rizome.dev)
