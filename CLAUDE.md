# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

AMQ (Agentic Message Queue) is a production-grade message queue designed specifically for AI agent communication at scale. Built by Rizome Labs, it provides high-throughput message processing with persistent storage, gRPC/HTTP APIs, and native Go SDK support.

Key features:
- 100,000+ msg/sec throughput on a single node
- BadgerDB for persistent storage
- gRPC and HTTP REST APIs
- Built-in Prometheus metrics and OpenTelemetry tracing
- Worker pool architecture for concurrent message processing
- Circuit breakers for reliability

## Build Commands

### Using Make (primary build system):
```bash
# Install development tools
make setup

# Build the server binary
make build

# Run all tests
make test

# Run tests with coverage
make coverage

# Run benchmarks  
make benchmark

# Run linter
make lint

# Format code
make fmt

# Run security scan
make security

# Generate protobuf code
make proto

# Build Docker image
make docker-build

# Run CI pipeline locally
make ci
```

### Using Just (alternative build system):
```bash
# Build for current platform
just build

# Build for Darwin/macOS
just build-darwin-local

# Build for Linux (using Docker for compatibility)
just build-linux-docker

# Run tests
just test

# Run linter
just lint
```

## Running Tests

### Using Make or Just (recommended):
```bash
# Run all tests with race detection using make
make test

# Run all tests with race detection using just
just test
```

### Using Go directly:
```bash
# Run all tests with race detection
go test -v -race ./...

# Run tests for a specific package
go test -v ./pkg/store/...

# Run benchmarks
go test -bench=. -benchmem ./pkg/store/

# Run a single test
go test -v -run TestBadgerStore_BasicOperations ./pkg/store/
```

## Project Architecture

### Core Components

1. **AMQ SDK** (`amq.go`): Main client interface for the message queue
   - Creates and manages queues
   - Provides client connections
   - Handles configuration

2. **Queue Manager** (`pkg/queue/`): Core message routing and processing
   - `manager.go`: Manages queues and client connections
   - `processor.go`: Handles message processing with worker pools

3. **Storage Layer** (`pkg/store/`): BadgerDB-based persistent storage
   - `badger.go`: BadgerDB implementation of the Store interface
   - `store.go`: Storage interface definition

4. **Client Library** (`pkg/client/`): Client implementations
   - `client.go`: Synchronous client
   - `http_client.go`: HTTP client implementation
   - Supports async consumers with callbacks

5. **Adapters** (`pkg/adapters/`): API implementations
   - `grpc/server.go`: gRPC server implementation
   - `http.go`: HTTP REST API handlers

6. **Metrics & Monitoring** (`pkg/metrics/`):
   - Prometheus metrics collection
   - OpenTelemetry tracing integration
   - Middleware for automatic instrumentation

### Message Flow

1. Client submits a task or sends a direct message
2. Queue Manager routes the message to appropriate queue
3. Worker pool processes messages concurrently
4. Messages are persisted in BadgerDB
5. Consumers receive messages and send acknowledgments
6. Metrics and traces are collected throughout

### Key Design Patterns

- **Worker Pool Pattern**: Each queue has a configurable worker pool for concurrent processing
- **Circuit Breaker Pattern**: Protects against cascading failures in client connections
- **Store Interface**: Abstraction allows swapping storage backends
- **Context-based Cancellation**: Proper shutdown and timeout handling throughout

## Environment Variables

```bash
AMQ_STORAGE_PATH=/data/amq          # BadgerDB storage directory
AMQ_WORKER_POOL_SIZE=100           # Workers per queue
AMQ_MAX_MESSAGE_SIZE=10485760      # 10MB max message size
AMQ_RETENTION_PERIOD=604800        # 7 days retention
AMQ_METRICS_ENABLED=true           # Enable Prometheus metrics
```

## Development Tips

1. Always run `make lint` before committing - the project uses golangci-lint with strict rules
2. BadgerDB performance tests are in `badger_performance_test.go` - useful for tuning
3. The project follows standard Go project layout with `cmd/`, `pkg/`, and `internal/`
4. gRPC definitions are in `api/proto/` - run `make proto` after changes
5. For debugging storage issues, BadgerDB data is in the configured storage path

## Testing Strategy

- Unit tests for all packages with mocks for dependencies
- Performance benchmarks for critical paths (storage, message processing)
- Integration tests for API endpoints
- Circuit breaker tests verify failure handling