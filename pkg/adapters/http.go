package adapters

import (
	"encoding/json"
	"io"
	"net/http"
	"time"

	"github.com/rizome-dev/amq/pkg/client"
	"github.com/rizome-dev/amq/pkg/errors"
	"github.com/rizome-dev/amq/pkg/queue"
)

// HTTPAdapter provides HTTP API for AMQ
type HTTPAdapter struct {
	manager *queue.Manager
	config  HTTPConfig
	mux     *http.ServeMux
}

// HTTPConfig holds HTTP adapter configuration
type HTTPConfig struct {
	Address        string
	ReadTimeout    time.Duration
	WriteTimeout   time.Duration
	MaxMessageSize int
}

// DefaultHTTPConfig returns default configuration
func DefaultHTTPConfig() HTTPConfig {
	return HTTPConfig{
		Address:        ":8080",
		ReadTimeout:    10 * time.Second,
		WriteTimeout:   10 * time.Second,
		MaxMessageSize: 10 * 1024 * 1024, // 10MB
	}
}

// NewHTTPAdapter creates a new HTTP adapter
func NewHTTPAdapter(manager *queue.Manager, config HTTPConfig) *HTTPAdapter {
	adapter := &HTTPAdapter{
		manager: manager,
		config:  config,
		mux:     http.NewServeMux(),
	}
	
	// Register routes
	adapter.registerRoutes()
	
	return adapter
}

// ServeHTTP implements http.Handler
func (h *HTTPAdapter) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mux.ServeHTTP(w, r)
}

// Server returns a configured HTTP server
func (h *HTTPAdapter) Server() *http.Server {
	return &http.Server{
		Addr:         h.config.Address,
		Handler:      h,
		ReadTimeout:  h.config.ReadTimeout,
		WriteTimeout: h.config.WriteTimeout,
	}
}

// registerRoutes sets up all HTTP routes
func (h *HTTPAdapter) registerRoutes() {
	// Client management
	h.mux.HandleFunc("/v1/clients", h.handleClients)
	h.mux.HandleFunc("/v1/clients/", h.handleClient)
	
	// Queue management
	h.mux.HandleFunc("/v1/queues", h.handleQueues)
	h.mux.HandleFunc("/v1/queues/", h.handleQueue)
	
	// Subscriptions
	h.mux.HandleFunc("/v1/subscriptions", h.handleSubscriptions)
	
	// Messages
	h.mux.HandleFunc("/v1/tasks", h.handleTasks)
	h.mux.HandleFunc("/v1/messages", h.handleMessages)
	h.mux.HandleFunc("/v1/messages/", h.handleMessage)
	
	// Direct messages
	h.mux.HandleFunc("/v1/direct", h.handleDirect)
}

// Client handlers
func (h *HTTPAdapter) handleClients(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		h.createClient(w, r)
	case http.MethodGet:
		h.listClients(w, r)
	default:
		h.respondError(w, errors.NewAPIError(http.StatusMethodNotAllowed, "method not allowed"))
	}
}

func (h *HTTPAdapter) createClient(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ID       string            `json:"id"`
		Metadata map[string]string `json:"metadata"`
	}
	
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.respondError(w, errors.NewAPIError(http.StatusBadRequest, "invalid request body"))
		return
	}
	
	if req.ID == "" {
		h.respondError(w, errors.NewAPIError(http.StatusBadRequest, "client id is required"))
		return
	}
	
	// Create client
	opts := []client.ClientOption{}
	for k, v := range req.Metadata {
		opts = append(opts, client.WithMetadata(k, v))
	}
	
	c, err := client.NewClient(req.ID, h.manager, opts...)
	if err != nil {
		h.respondError(w, errors.NewAPIError(http.StatusInternalServerError, err.Error()))
		return
	}
	
	// Return client info
	h.respondJSON(w, http.StatusCreated, map[string]interface{}{
		"id":          req.ID,
		"metadata":    req.Metadata,
		"created_at":  time.Now(),
	})
	
	// Note: In production, we'd store the client reference
	_ = c
}

// Task handlers
func (h *HTTPAdapter) handleTasks(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		h.respondError(w, errors.NewAPIError(http.StatusMethodNotAllowed, "method not allowed"))
		return
	}
	
	clientID := r.Header.Get("X-Client-ID")
	if clientID == "" {
		h.respondError(w, errors.NewAPIError(http.StatusBadRequest, "X-Client-ID header required"))
		return
	}
	
	var req struct {
		Topic    string          `json:"topic"`
		Payload  json.RawMessage `json:"payload"`
		Priority int             `json:"priority"`
		TTL      string          `json:"ttl"`
	}
	
	if err := json.NewDecoder(io.LimitReader(r.Body, int64(h.config.MaxMessageSize))).Decode(&req); err != nil {
		h.respondError(w, errors.NewAPIError(http.StatusBadRequest, "invalid request body"))
		return
	}
	
	if req.Topic == "" {
		h.respondError(w, errors.NewAPIError(http.StatusBadRequest, "topic is required"))
		return
	}
	
	// Submit task
	msg, err := h.manager.SubmitTask(r.Context(), clientID, req.Topic, req.Payload)
	if err != nil {
		h.respondError(w, errors.NewAPIError(http.StatusInternalServerError, err.Error()))
		return
	}
	
	h.respondJSON(w, http.StatusCreated, map[string]interface{}{
		"message_id":  msg.ID,
		"created_at":  msg.CreatedAt,
	})
}

// Helper methods
func (h *HTTPAdapter) respondJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

func (h *HTTPAdapter) respondError(w http.ResponseWriter, err error) {
	apiErr, ok := err.(*errors.APIError)
	if !ok {
		apiErr = errors.NewAPIError(http.StatusInternalServerError, err.Error())
	}
	
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(apiErr.StatusCode)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"error": apiErr.Message,
		"code":  apiErr.StatusCode,
	})
}

// Placeholder handlers
func (h *HTTPAdapter) handleClient(w http.ResponseWriter, r *http.Request) {
	h.respondError(w, errors.NewAPIError(http.StatusNotImplemented, "not implemented"))
}

func (h *HTTPAdapter) handleQueues(w http.ResponseWriter, r *http.Request) {
	h.respondError(w, errors.NewAPIError(http.StatusNotImplemented, "not implemented"))
}

func (h *HTTPAdapter) handleQueue(w http.ResponseWriter, r *http.Request) {
	h.respondError(w, errors.NewAPIError(http.StatusNotImplemented, "not implemented"))
}

func (h *HTTPAdapter) handleSubscriptions(w http.ResponseWriter, r *http.Request) {
	h.respondError(w, errors.NewAPIError(http.StatusNotImplemented, "not implemented"))
}

func (h *HTTPAdapter) handleMessages(w http.ResponseWriter, r *http.Request) {
	h.respondError(w, errors.NewAPIError(http.StatusNotImplemented, "not implemented"))
}

func (h *HTTPAdapter) handleMessage(w http.ResponseWriter, r *http.Request) {
	h.respondError(w, errors.NewAPIError(http.StatusNotImplemented, "not implemented"))
}

func (h *HTTPAdapter) handleDirect(w http.ResponseWriter, r *http.Request) {
	h.respondError(w, errors.NewAPIError(http.StatusNotImplemented, "not implemented"))
}

func (h *HTTPAdapter) listClients(w http.ResponseWriter, r *http.Request) {
	h.respondError(w, errors.NewAPIError(http.StatusNotImplemented, "not implemented"))
}