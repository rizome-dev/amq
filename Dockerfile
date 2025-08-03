# Build stage
FROM golang:1.23.4-alpine AS builder

# Install dependencies
RUN apk add --no-cache git make

# Set working directory
WORKDIR /build

# Copy go mod files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build the binary
RUN CGO_ENABLED=1 GOOS=linux go build -a -installsuffix cgo -ldflags="-w -s" -o amq-server ./cmd/amq-server

# Runtime stage
FROM alpine:3.19

# Install ca-certificates for HTTPS
RUN apk --no-cache add ca-certificates

# Create non-root user
RUN addgroup -g 1000 amq && \
    adduser -u 1000 -G amq -s /bin/sh -D amq

# Create data directory
RUN mkdir -p /data/amq && chown -R amq:amq /data

# Copy binary from builder
COPY --from=builder /build/amq-server /usr/local/bin/amq-server

# Switch to non-root user
USER amq

# Expose port
EXPOSE 8080

# Set data volume
VOLUME ["/data/amq"]

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
  CMD ["/usr/local/bin/amq-server", "health"]

# Run the server
ENTRYPOINT ["/usr/local/bin/amq-server"]
CMD ["serve"]