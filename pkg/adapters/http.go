package adapters

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"
	
	"github.com/rizome-dev/amq/pkg/client"
	"github.com/rizome-dev/amq/pkg/types"
)

// HTTPAdapter provides HTTP API for AMQ
type HTTPAdapter struct {
	amq        AMQInterface
	clients    map[string]client.Client
	timeout    time.Duration
}

// AMQInterface defines the methods we need from AMQ
type AMQInterface interface {
	NewClient(id string, opts ...client.ClientOption) (client.Client, error)
	CreateQueue(ctx context.Context, name string, qtype types.QueueType) error
	DeleteQueue(ctx context.Context, name string) error
	ListQueues(ctx context.Context) ([]*types.Queue, error)
	GetQueueStats(ctx context.Context, name string) (*types.QueueStats, error)
}

// NewHTTPAdapter creates a new HTTP adapter
func NewHTTPAdapter(amq AMQInterface) *HTTPAdapter {
	return &HTTPAdapter{
		amq:     amq,
		clients: make(map[string]client.Client),
		timeout: 30 * time.Second,
	}
}

// RegisterRoutes registers all HTTP routes
func (h *HTTPAdapter) RegisterRoutes(mux *http.ServeMux) {
	// Client management
	mux.HandleFunc("/v1/clients", h.handleClients)
	mux.HandleFunc("/v1/clients/", h.handleClient)
	
	// Message operations
	mux.HandleFunc("/v1/messages", h.handleMessages)
	mux.HandleFunc("/v1/messages/", h.handleMessage)
	mux.HandleFunc("/v1/tasks", h.handleTasks)
	
	// Queue management
	mux.HandleFunc("/v1/queues", h.handleQueues)
	mux.HandleFunc("/v1/queues/", h.handleQueue)
	
	// Subscriptions
	mux.HandleFunc("/v1/subscriptions", h.handleSubscriptions)
}

// handleClients handles client registration
func (h *HTTPAdapter) handleClients(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		h.createClient(w, r)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// createClient creates a new client
func (h *HTTPAdapter) createClient(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ID       string            `json:"id"`
		Metadata map[string]string `json:"metadata,omitempty"`
	}
	
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	
	if req.ID == "" {
		http.Error(w, "client ID is required", http.StatusBadRequest)
		return
	}
	
	// Create client options
	var opts []client.ClientOption
	for k, v := range req.Metadata {
		opts = append(opts, client.WithMetadata(k, v))
	}
	
	// Create client
	c, err := h.amq.NewClient(req.ID, opts...)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	
	// Store client
	h.clients[req.ID] = c
	
	// Return success
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"id": req.ID,
		"status": "connected",
	})
}

// handleClient handles individual client operations
func (h *HTTPAdapter) handleClient(w http.ResponseWriter, r *http.Request) {
	clientID := r.URL.Path[len("/v1/clients/"):]
	
	switch r.Method {
	case http.MethodDelete:
		h.deleteClient(w, r, clientID)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// deleteClient disconnects a client
func (h *HTTPAdapter) deleteClient(w http.ResponseWriter, r *http.Request, clientID string) {
	c, exists := h.clients[clientID]
	if !exists {
		http.Error(w, "client not found", http.StatusNotFound)
		return
	}
	
	if err := c.Close(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	
	delete(h.clients, clientID)
	w.WriteHeader(http.StatusNoContent)
}

// handleTasks handles task submission
func (h *HTTPAdapter) handleTasks(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	
	clientID := r.Header.Get("X-Client-ID")
	if clientID == "" {
		http.Error(w, "X-Client-ID header is required", http.StatusBadRequest)
		return
	}
	
	c, exists := h.clients[clientID]
	if !exists {
		http.Error(w, "client not found", http.StatusUnauthorized)
		return
	}
	
	var req struct {
		Topic    string          `json:"topic"`
		Payload  json.RawMessage `json:"payload"`
		Priority int             `json:"priority,omitempty"`
		TTL      string          `json:"ttl,omitempty"`
	}
	
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	
	// Build options
	var opts []client.MessageOption
	if req.Priority > 0 {
		opts = append(opts, client.WithPriority(req.Priority))
	}
	if req.TTL != "" {
		ttl, err := time.ParseDuration(req.TTL)
		if err == nil {
			opts = append(opts, client.WithTTL(ttl))
		}
	}
	
	// Submit task
	ctx, cancel := context.WithTimeout(context.Background(), h.timeout)
	defer cancel()
	
	msg, err := c.SubmitTask(ctx, req.Topic, req.Payload, opts...)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	
	// Return message info
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"id": msg.ID,
		"status": msg.Status.String(),
		"created_at": msg.CreatedAt,
	})
}

// handleMessages handles message operations
func (h *HTTPAdapter) handleMessages(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		h.receiveMessages(w, r)
	case http.MethodPost:
		// Handle direct messages
		h.sendDirectMessage(w, r)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// receiveMessages receives messages for a client
func (h *HTTPAdapter) receiveMessages(w http.ResponseWriter, r *http.Request) {
	clientID := r.Header.Get("X-Client-ID")
	if clientID == "" {
		http.Error(w, "X-Client-ID header is required", http.StatusBadRequest)
		return
	}
	
	c, exists := h.clients[clientID]
	if !exists {
		http.Error(w, "client not found", http.StatusUnauthorized)
		return
	}
	
	// Get limit from query params
	limit := 10
	if l := r.URL.Query().Get("limit"); l != "" {
		if parsed, err := strconv.Atoi(l); err == nil && parsed > 0 {
			limit = parsed
		}
	}
	
	// Receive messages
	ctx, cancel := context.WithTimeout(context.Background(), h.timeout)
	defer cancel()
	
	messages, err := c.Receive(ctx, limit)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	
	// Convert messages to response format
	resp := make([]map[string]interface{}, len(messages))
	for i, msg := range messages {
		resp[i] = map[string]interface{}{
			"id":       msg.ID,
			"type":     msg.Type.String(),
			"from":     msg.From,
			"to":       msg.To,
			"topic":    msg.Topic,
			"payload":  json.RawMessage(msg.Payload),
			"priority": msg.Priority,
			"status":   msg.Status.String(),
			"created_at": msg.CreatedAt,
		}
	}
	
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// sendDirectMessage sends a direct message
func (h *HTTPAdapter) sendDirectMessage(w http.ResponseWriter, r *http.Request) {
	clientID := r.Header.Get("X-Client-ID")
	if clientID == "" {
		http.Error(w, "X-Client-ID header is required", http.StatusBadRequest)
		return
	}
	
	c, exists := h.clients[clientID]
	if !exists {
		http.Error(w, "client not found", http.StatusUnauthorized)
		return
	}
	
	var req struct {
		To       string          `json:"to"`
		Payload  json.RawMessage `json:"payload"`
		Priority int             `json:"priority,omitempty"`
	}
	
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	
	// Build options
	var opts []client.MessageOption
	if req.Priority > 0 {
		opts = append(opts, client.WithPriority(req.Priority))
	}
	
	// Send message
	ctx, cancel := context.WithTimeout(context.Background(), h.timeout)
	defer cancel()
	
	msg, err := c.SendDirect(ctx, req.To, req.Payload, opts...)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	
	// Return message info
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"id": msg.ID,
		"status": msg.Status.String(),
		"created_at": msg.CreatedAt,
	})
}

// handleMessage handles individual message operations
func (h *HTTPAdapter) handleMessage(w http.ResponseWriter, r *http.Request) {
	messageID := r.URL.Path[len("/v1/messages/"):]
	action := ""
	
	// Check for action suffix
	if len(messageID) > 4 {
		if messageID[len(messageID)-4:] == "/ack" {
			action = "ack"
			messageID = messageID[:len(messageID)-4]
		} else if messageID[len(messageID)-5:] == "/nack" {
			action = "nack"
			messageID = messageID[:len(messageID)-5]
		}
	}
	
	if action == "" || r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	
	clientID := r.Header.Get("X-Client-ID")
	if clientID == "" {
		http.Error(w, "X-Client-ID header is required", http.StatusBadRequest)
		return
	}
	
	c, exists := h.clients[clientID]
	if !exists {
		http.Error(w, "client not found", http.StatusUnauthorized)
		return
	}
	
	ctx, cancel := context.WithTimeout(context.Background(), h.timeout)
	defer cancel()
	
	switch action {
	case "ack":
		if err := c.Ack(ctx, messageID); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	case "nack":
		var req struct {
			Reason string `json:"reason"`
		}
		json.NewDecoder(r.Body).Decode(&req)
		
		if err := c.Nack(ctx, messageID, req.Reason); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
	
	w.WriteHeader(http.StatusNoContent)
}

// handleQueues handles queue operations
func (h *HTTPAdapter) handleQueues(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		h.listQueues(w, r)
	case http.MethodPost:
		h.createQueue(w, r)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// listQueues lists all queues
func (h *HTTPAdapter) listQueues(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(context.Background(), h.timeout)
	defer cancel()
	
	queues, err := h.amq.ListQueues(ctx)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(queues)
}

// createQueue creates a new queue
func (h *HTTPAdapter) createQueue(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name string `json:"name"`
		Type string `json:"type"`
	}
	
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	
	// Parse queue type
	var qtype types.QueueType
	switch req.Type {
	case "task":
		qtype = types.QueueTypeTask
	case "direct":
		qtype = types.QueueTypeDirect
	default:
		http.Error(w, "invalid queue type", http.StatusBadRequest)
		return
	}
	
	ctx, cancel := context.WithTimeout(context.Background(), h.timeout)
	defer cancel()
	
	if err := h.amq.CreateQueue(ctx, req.Name, qtype); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	
	w.WriteHeader(http.StatusCreated)
}

// handleQueue handles individual queue operations
func (h *HTTPAdapter) handleQueue(w http.ResponseWriter, r *http.Request) {
	queueName := r.URL.Path[len("/v1/queues/"):]
	
	// Check for /stats suffix
	if len(queueName) > 6 && queueName[len(queueName)-6:] == "/stats" {
		queueName = queueName[:len(queueName)-6]
		h.getQueueStats(w, r, queueName)
		return
	}
	
	switch r.Method {
	case http.MethodDelete:
		h.deleteQueue(w, r, queueName)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// deleteQueue deletes a queue
func (h *HTTPAdapter) deleteQueue(w http.ResponseWriter, r *http.Request, queueName string) {
	ctx, cancel := context.WithTimeout(context.Background(), h.timeout)
	defer cancel()
	
	if err := h.amq.DeleteQueue(ctx, queueName); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	
	w.WriteHeader(http.StatusNoContent)
}

// getQueueStats gets queue statistics
func (h *HTTPAdapter) getQueueStats(w http.ResponseWriter, r *http.Request, queueName string) {
	ctx, cancel := context.WithTimeout(context.Background(), h.timeout)
	defer cancel()
	
	stats, err := h.amq.GetQueueStats(ctx, queueName)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(stats)
}

// handleSubscriptions handles subscription operations
func (h *HTTPAdapter) handleSubscriptions(w http.ResponseWriter, r *http.Request) {
	clientID := r.Header.Get("X-Client-ID")
	if clientID == "" {
		http.Error(w, "X-Client-ID header is required", http.StatusBadRequest)
		return
	}
	
	c, exists := h.clients[clientID]
	if !exists {
		http.Error(w, "client not found", http.StatusUnauthorized)
		return
	}
	
	switch r.Method {
	case http.MethodPost:
		h.subscribe(w, r, c)
	case http.MethodDelete:
		h.unsubscribe(w, r, c)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// subscribe subscribes to topics
func (h *HTTPAdapter) subscribe(w http.ResponseWriter, r *http.Request, c client.Client) {
	var req struct {
		Topics []string `json:"topics"`
	}
	
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	
	ctx, cancel := context.WithTimeout(context.Background(), h.timeout)
	defer cancel()
	
	if err := c.Subscribe(ctx, req.Topics...); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	
	w.WriteHeader(http.StatusNoContent)
}

// unsubscribe unsubscribes from topics
func (h *HTTPAdapter) unsubscribe(w http.ResponseWriter, r *http.Request, c client.Client) {
	var req struct {
		Topics []string `json:"topics"`
	}
	
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	
	ctx, cancel := context.WithTimeout(context.Background(), h.timeout)
	defer cancel()
	
	if err := c.Unsubscribe(ctx, req.Topics...); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	
	w.WriteHeader(http.StatusNoContent)
}

// Example server setup
func ExampleHTTPServer(amq AMQInterface) *http.Server {
	adapter := NewHTTPAdapter(amq)
	mux := http.NewServeMux()
	adapter.RegisterRoutes(mux)
	
	return &http.Server{
		Addr:    ":8080",
		Handler: mux,
	}
}