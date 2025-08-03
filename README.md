# AMQ - Agentic Message Queue

[![GoDoc](https://pkg.go.dev/badge/github.com/rizome-dev/amq)](https://pkg.go.dev/github.com/rizome-dev/amq)
[![Go Report Card](https://goreportcard.com/badge/github.com/rizome-dev/amq)](https://goreportcard.com/report/github.com/rizome-dev/amq)
[![CI](https://github.com/rizome-dev/amq/actions/workflows/ci.yml/badge.svg)](https://github.com/rizome-dev/amq/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

A lightweight, high-performance, agent-agnostic message queue built for Rizome Labs' Agentic Development Swarms. AMQ enables seamless communication between containerized workflows, microservices, and agents regardless of language or protocol.

built by [rizome labs](https://rizome.dev) | contact: [hi@rizome.dev](mailto:hi@rizome.dev)

## Overview

AMQ is purpose-built for agentic workloads where multiple autonomous components need to communicate efficiently. Unlike traditional message queues, AMQ is designed from the ground up to support the unique requirements of AI agents and containerized workflows - while remaining completely agnostic to the implementation details of those agents.

### Key Features

- **Agent-Agnostic**: Any client can connect - containers, microservices, or traditional apps
- **Protocol Support**: Native Go SDK, HTTP API, with gRPC and WebSocket planned
- **Language Independent**: Use from Go, Python, JavaScript, or any language with HTTP
- **High Performance**: Built on BadgerDB for fast, persistent message storage
- **Topic-Based Routing**: Flexible routing with topic subscriptions
- **Direct Messaging**: Point-to-point communication between clients
- **Kubernetes Ready**: Includes Helm charts for easy deployment
- **Simple API**: Intuitive producer/consumer patterns

## Quick Start

```bash
# Install the library
go get github.com/rizome-dev/amq

# Run the example
cd examples/basic-client
go run main.go
```

## Basic Usage

### Go SDK

```go
package main

import (
    "context"
    "log"
    
    "github.com/rizome-dev/amq"
    "github.com/rizome-dev/amq/pkg/client"
)

func main() {
    // Create AMQ instance
    config := amq.DefaultConfig()
    amqInstance, err := amq.New(config)
    if err != nil {
        log.Fatal(err)
    }
    defer amqInstance.Close()
    
    ctx := context.Background()
    
    // Create a client
    producer, err := amqInstance.NewClient("producer-1",
        client.WithMetadata("type", "producer"))
    if err != nil {
        log.Fatal(err)
    }
    defer producer.Close()
    
    // Create a consumer
    consumer, err := amqInstance.NewClient("consumer-1",
        client.WithMetadata("type", "consumer"))
    if err != nil {
        log.Fatal(err)
    }
    defer consumer.Close()
    
    // Subscribe to topics
    consumer.Subscribe(ctx, "tasks.process")
    
    // Submit a task
    msg, err := producer.SubmitTask(ctx, "tasks.process", 
        []byte(`{"data": "process this"}`))
    if err != nil {
        log.Fatal(err)
    }
    log.Printf("Submitted task: %s", msg.ID)
    
    // Receive and process messages
    messages, err := consumer.Receive(ctx, 10)
    for _, msg := range messages {
        log.Printf("Processing: %s", string(msg.Payload))
        consumer.Ack(ctx, msg.ID)
    }
}
```

### HTTP API

```bash
# Register a client
curl -X POST http://localhost:8080/v1/clients \
  -H "Content-Type: application/json" \
  -d '{"id": "worker-1", "metadata": {"type": "worker"}}'

# Subscribe to topics
curl -X POST http://localhost:8080/v1/subscriptions \
  -H "X-Client-ID: worker-1" \
  -H "Content-Type: application/json" \
  -d '{"topics": ["tasks.process"]}'

# Submit a task
curl -X POST http://localhost:8080/v1/tasks \
  -H "X-Client-ID: worker-1" \
  -H "Content-Type: application/json" \
  -d '{
    "topic": "tasks.process",
    "payload": {"data": "process this"},
    "priority": 5
  }'

# Receive messages
curl -X GET http://localhost:8080/v1/messages?limit=10 \
  -H "X-Client-ID: worker-1"

# Acknowledge a message
curl -X POST http://localhost:8080/v1/messages/{message-id}/ack \
  -H "X-Client-ID: worker-1"
```

### Python Client

```python
from amq import AMQHTTPClient

# Create client
client = AMQHTTPClient("http://localhost:8080", "python-worker")

# Subscribe to topics
client.subscribe(["tasks.process", "tasks.analyze"])

# Submit a task
result = client.submit_task("tasks.process", 
    {"data": "process this"}, priority=5)

# Receive and process messages
messages = client.receive(limit=10)
for msg in messages:
    process_message(msg['payload'])
    client.ack(msg['id'])
```

## Architecture

AMQ uses a client-agnostic architecture:

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  Go Client      │     │  Python Client  │     │  Container      │
└────────┬────────┘     └────────┬────────┘     └────────┬────────┘
         │                       │                        │
         │                       ▼                        │
         │              ┌─────────────────┐               │
         │              │   HTTP Adapter  │               │
         │              └────────┬────────┘               │
         │                       │                        │
         └───────────┬───────────┴────────────────────────┘
                     │
        ┌────────────▼────────────┐
        │      AMQ Core           │
        ├─────────────────────────┤
        │  • Message Router       │
        │  • Queue Manager        │
        │  • Client Registry      │
        └────────────┬────────────┘
                     │
        ┌────────────▼────────────┐
        │     BadgerDB            │
        │  (Persistence Layer)    │
        └─────────────────────────┘
```

## Client Development

### Creating Clients

```go
// Go SDK
client, err := amqInstance.NewClient("my-client",
    client.WithMetadata("type", "worker"),
    client.WithMetadata("version", "1.0"))

// Async consumer with automatic message handling
consumer, err := amqInstance.NewAsyncConsumer("async-worker")
consumer.Subscribe(ctx, "tasks.process")
consumer.Start(handler, client.ConsumerOptions{
    MaxConcurrency: 5,
    AutoAck: true,
})
```

### Message Patterns

#### Producer/Consumer
```go
// Producer
msg, err := producer.SubmitTask(ctx, "orders.process", orderData,
    client.WithPriority(8),
    client.WithTTL(1*time.Hour))

// Consumer
messages, err := consumer.Receive(ctx, 10)
for _, msg := range messages {
    processOrder(msg.Payload)
    consumer.Ack(ctx, msg.ID)
}
```

#### Direct Messaging
```go
// Send direct message
msg, err := alice.SendDirect(ctx, "bob", payload,
    client.WithPriority(9))

// Bob receives
messages, err := bob.Receive(ctx, 10)
```

## Configuration

```go
config := amq.Config{
    StorePath:         "./amq-data",      // BadgerDB storage path
    WorkerPoolSize:    10,                // Workers per queue
    MessageTimeout:    5 * time.Minute,   // Default message timeout
}

amqInstance, err := amq.New(config)
```

## Message Types

1. **Task Messages**: Topic-based routing
   - Published to topics
   - Any subscribed client can process
   - Support for priorities and retries

2. **Direct Messages**: Point-to-point
   - Sent directly to a specific client
   - Guaranteed delivery to target
   - Used for responses and coordination

## Container Deployment

AMQ is designed to work seamlessly with containerized workflows:

```yaml
# docker-compose.yml
version: '3.8'

services:
  amq-server:
    image: rizome/amq:latest
    ports:
      - "8080:8080"
    volumes:
      - amq-data:/data

  worker:
    image: my-worker:latest
    environment:
      - AMQ_URL=http://amq-server:8080
      - CLIENT_ID=worker-${HOSTNAME}
    deploy:
      replicas: 5
```

## Kubernetes Deployment

```bash
# Install with Helm
helm install amq ./helm/amq \
  --set persistence.size=100Gi \
  --set replicas=3 \
  --set httpAdapter.enabled=true
```

## Protocol Adapters

### HTTP Adapter (Available)
- RESTful API for any language
- WebSocket support for streaming
- Built-in request/response patterns

### gRPC Adapter (Planned)
- High-performance binary protocol
- Bi-directional streaming
- Type-safe client generation

### AMQP Bridge (Planned)
- Compatibility with RabbitMQ clients
- AMQP 1.0 support

## Monitoring

```go
// Get queue statistics
stats, err := amq.GetQueueStats(ctx, "tasks.process")
log.Printf("Queue depth: %d", stats.MessageCount)

// Get client information
client, err := amq.GetClientInfo(ctx, "worker-1")
log.Printf("Last seen: %v", client.LastSeen)

// List clients by metadata
clients, err := amq.ListClients(ctx, amq.ClientFilter{
    MetadataKey: "type",
    MetadataValue: "worker",
})
```

## Advanced Features

### Message Options

```go
// Priority levels (0-9, higher is more important)
client.WithPriority(9)

// Time to live
client.WithTTL(24 * time.Hour)

// Retry configuration
client.WithMaxRetries(5)

// Custom metadata
client.WithMessageMetadata("trace-id", "abc123")
```

### Subscription Management

```go
// Subscribe to multiple topics
client.Subscribe(ctx, "orders.new", "orders.update", "orders.cancel")

// Dynamic subscription
client.Subscribe(ctx, "seasonal.holiday")

// Unsubscribe
client.Unsubscribe(ctx, "seasonal.holiday")
```

## Performance

AMQ is optimized for high-throughput scenarios:

- Concurrent message processing
- Batched operations for efficiency
- Strategic indexing for fast queries
- Connection pooling and reuse
- Zero-copy message passing where possible

## Examples

See the [examples](examples/) directory for:
- Basic producer/consumer patterns
- HTTP client in Python
- Containerized workflow with Docker Compose
- Priority queue processing
- Retry handling
- Direct messaging

## Development

```bash
# Install dependencies
go mod download

# Run tests
go test ./...

# Run benchmarks
go test -bench=. ./...

# Build
go build ./...

# Run with Docker
docker build -t amq:dev .
docker run -p 8080:8080 amq:dev
```

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

MIT License - see the [LICENSE](LICENSE) file for details.

## Support

- Documentation: [https://pkg.go.dev/github.com/rizome-dev/amq](https://pkg.go.dev/github.com/rizome-dev/amq)
- Issues: [https://github.com/rizome-dev/amq/issues](https://github.com/rizome-dev/amq/issues)
- Contact: [hi@rizome.dev](mailto:hi@rizome.dev)