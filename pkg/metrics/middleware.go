package metrics

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
	
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// HTTPMiddleware provides HTTP metrics and tracing middleware
func HTTPMiddleware(metrics *Metrics, tracer *Tracer) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Start timer
			start := time.Now()
			
			// Start trace span
			ctx := r.Context()
			var span trace.Span
			if tracer != nil {
				ctx, span = tracer.Start(ctx, fmt.Sprintf("%s %s", r.Method, r.URL.Path),
					trace.WithAttributes(
						attribute.String("http.method", r.Method),
						attribute.String("http.url", r.URL.String()),
						attribute.String("http.target", r.URL.Path),
						attribute.String("http.host", r.Host),
						attribute.String("http.scheme", r.URL.Scheme),
						attribute.String("http.user_agent", r.UserAgent()),
						attribute.String("net.peer.ip", r.RemoteAddr),
					),
				)
				defer span.End()
				r = r.WithContext(ctx)
			}
			
			// Capture request size
			reqSize := 0
			if r.ContentLength > 0 {
				reqSize = int(r.ContentLength)
			}
			
			// Wrap response writer to capture status and size
			lrw := &loggingResponseWriter{ResponseWriter: w, statusCode: http.StatusOK}
			
			// Call next handler
			next.ServeHTTP(lrw, r)
			
			// Record metrics
			duration := time.Since(start)
			endpoint := normalizeEndpoint(r.URL.Path)
			status := strconv.Itoa(lrw.statusCode)
			
			if metrics != nil {
				metrics.RecordHTTPRequest(r.Method, endpoint, status, duration, reqSize, lrw.size)
			}
			
			// Update span with response info
			if span != nil {
				span.SetAttributes(
					attribute.Int("http.status_code", lrw.statusCode),
					attribute.Int("http.response_size", lrw.size),
				)
				if lrw.statusCode >= 400 {
					span.SetStatus(codes.Error, http.StatusText(lrw.statusCode))
				}
			}
		})
	}
}

// loggingResponseWriter wraps http.ResponseWriter to capture status code and response size
type loggingResponseWriter struct {
	http.ResponseWriter
	statusCode int
	size       int
}

func (lrw *loggingResponseWriter) WriteHeader(code int) {
	lrw.statusCode = code
	lrw.ResponseWriter.WriteHeader(code)
}

func (lrw *loggingResponseWriter) Write(b []byte) (int, error) {
	size, err := lrw.ResponseWriter.Write(b)
	lrw.size += size
	return size, err
}

// normalizeEndpoint normalizes the endpoint path for metrics
func normalizeEndpoint(path string) string {
	// Remove trailing slash
	path = strings.TrimSuffix(path, "/")
	
	// Normalize common patterns
	parts := strings.Split(path, "/")
	for i, part := range parts {
		// Replace IDs with placeholders
		if isID(part) {
			parts[i] = ":id"
		}
	}
	
	normalized := strings.Join(parts, "/")
	if normalized == "" {
		return "/"
	}
	return normalized
}

// isID checks if a path segment looks like an ID
func isID(s string) bool {
	// Check if it's a UUID
	if len(s) == 36 && strings.Count(s, "-") == 4 {
		return true
	}
	
	// Check if it's a numeric ID
	if _, err := strconv.Atoi(s); err == nil && len(s) > 0 {
		return true
	}
	
	// Check if it's an alphanumeric ID (common patterns)
	if len(s) > 8 && !strings.Contains(s, " ") {
		hasDigit := false
		hasAlpha := false
		for _, r := range s {
			if r >= '0' && r <= '9' {
				hasDigit = true
			}
			if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') {
				hasAlpha = true
			}
		}
		return hasDigit && hasAlpha
	}
	
	return false
}

// RequestLogger provides request logging with metrics
func RequestLogger(metrics *Metrics) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Skip health check endpoints
			if r.URL.Path == "/health" || r.URL.Path == "/ready" {
				next.ServeHTTP(w, r)
				return
			}
			
			// Log request
			clientID := r.Header.Get("X-Client-ID")
			if clientID == "" {
				clientID = "anonymous"
			}
			
			// Continue with request
			next.ServeHTTP(w, r)
		})
	}
}

// MetricsHandler returns an HTTP handler for Prometheus metrics
func MetricsHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// This will be handled by promhttp.Handler() in the main server
		// For now, return a simple response
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)
		fmt.Fprintln(w, "# Metrics endpoint - configure promhttp.Handler()")
	})
}

// TracingPropagator provides HTTP trace propagation
type TracingPropagator struct {
	tracer *Tracer
}

// NewTracingPropagator creates a new tracing propagator
func NewTracingPropagator(tracer *Tracer) *TracingPropagator {
	return &TracingPropagator{tracer: tracer}
}

// Extract extracts trace context from HTTP headers
func (tp *TracingPropagator) Extract(r *http.Request) *http.Request {
	prop := otel.GetTextMapPropagator()
	ctx := prop.Extract(r.Context(), propagation.HeaderCarrier(r.Header))
	return r.WithContext(ctx)
}

// Inject injects trace context into HTTP headers
func (tp *TracingPropagator) Inject(r *http.Request) {
	prop := otel.GetTextMapPropagator()
	prop.Inject(r.Context(), propagation.HeaderCarrier(r.Header))
}