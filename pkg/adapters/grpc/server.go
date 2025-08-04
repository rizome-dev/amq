package grpc

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/rizome-dev/amq/pkg/client"
	"github.com/rizome-dev/amq/pkg/queue"
	"github.com/rizome-dev/amq/pkg/types"
	pb "github.com/rizome-dev/amq/api/proto"
	
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Server implements the gRPC AMQ service
type Server struct {
	pb.UnimplementedAMQServer
	
	manager       *queue.Manager
	server        *grpc.Server
	addr          string
	
	// Client session management
	sessions      map[string]*clientSession
	sessionsMu    sync.RWMutex
	
	// Metrics
	metrics       *serverMetrics
}

type clientSession struct {
	clientID      string
	sessionID     string
	client        client.Client
	registeredAt  time.Time
	lastSeen      time.Time
	capabilities  *pb.ClientCapabilities
}

type serverMetrics struct {
	totalRequests   uint64
	activeClients   int32
	messagesRouted  uint64
}

// Config holds gRPC server configuration
type Config struct {
	Address            string
	MaxConnectionIdle  time.Duration
	MaxConnectionAge   time.Duration
	KeepAliveInterval  time.Duration
	KeepAliveTimeout   time.Duration
}

// DefaultConfig returns default gRPC configuration
func DefaultConfig() Config {
	return Config{
		Address:            ":9090",
		MaxConnectionIdle:  15 * time.Minute,
		MaxConnectionAge:   30 * time.Minute,
		KeepAliveInterval:  5 * time.Minute,
		KeepAliveTimeout:   20 * time.Second,
	}
}

// NewServer creates a new gRPC server
func NewServer(manager *queue.Manager, config Config) *Server {
	return &Server{
		manager:  manager,
		addr:     config.Address,
		sessions: make(map[string]*clientSession),
		metrics:  &serverMetrics{},
	}
}

// Start starts the gRPC server
func (s *Server) Start() error {
	lis, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}
	
	// Configure server options for production
	opts := []grpc.ServerOption{
		grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionIdle:     15 * time.Minute,
			MaxConnectionAge:      30 * time.Minute,
			MaxConnectionAgeGrace: 5 * time.Minute,
			Time:                  5 * time.Minute,
			Timeout:               20 * time.Second,
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             5 * time.Second,
			PermitWithoutStream: true,
		}),
		grpc.MaxRecvMsgSize(10 * 1024 * 1024), // 10MB
		grpc.MaxSendMsgSize(10 * 1024 * 1024), // 10MB
		grpc.MaxConcurrentStreams(1000),
	}
	
	s.server = grpc.NewServer(opts...)
	pb.RegisterAMQServer(s.server, s)
	
	go func() {
		if err := s.server.Serve(lis); err != nil {
			fmt.Printf("gRPC server error: %v\n", err)
		}
	}()
	
	return nil
}

// Stop stops the gRPC server
func (s *Server) Stop() {
	if s.server != nil {
		s.server.GracefulStop()
	}
}

// RegisterClient registers a new client
func (s *Server) RegisterClient(ctx context.Context, req *pb.RegisterClientRequest) (*pb.RegisterClientResponse, error) {
	if req.ClientId == "" {
		return nil, status.Error(codes.InvalidArgument, "client_id is required")
	}
	
	// Create client options
	opts := []client.ClientOption{}
	for k, v := range req.Metadata {
		opts = append(opts, client.WithMetadata(k, v))
	}
	
	// Create client
	c, err := client.NewClient(req.ClientId, s.manager, opts...)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to create client: %v", err)
	}
	
	// Generate session ID
	sessionID := generateSessionID()
	
	// Store session
	s.sessionsMu.Lock()
	s.sessions[req.ClientId] = &clientSession{
		clientID:     req.ClientId,
		sessionID:    sessionID,
		client:       c,
		registeredAt: time.Now(),
		lastSeen:     time.Now(),
		capabilities: req.Capabilities,
	}
	s.sessionsMu.Unlock()
	
	return &pb.RegisterClientResponse{
		SessionId:    sessionID,
		RegisteredAt: timestamppb.Now(),
	}, nil
}

// UnregisterClient unregisters a client
func (s *Server) UnregisterClient(ctx context.Context, req *pb.UnregisterClientRequest) (*emptypb.Empty, error) {
	s.sessionsMu.Lock()
	session, exists := s.sessions[req.ClientId]
	if exists {
		session.client.Close()
		delete(s.sessions, req.ClientId)
	}
	s.sessionsMu.Unlock()
	
	if !exists {
		return nil, status.Error(codes.NotFound, "client not found")
	}
	
	return &emptypb.Empty{}, nil
}

// Heartbeat updates client heartbeat
func (s *Server) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*emptypb.Empty, error) {
	s.sessionsMu.Lock()
	session, exists := s.sessions[req.ClientId]
	if exists {
		session.lastSeen = time.Now()
		// TODO: Store metrics for monitoring
	}
	s.sessionsMu.Unlock()
	
	if !exists {
		return nil, status.Error(codes.NotFound, "client not found")
	}
	
	return &emptypb.Empty{}, nil
}

// CreateQueue creates a new queue
func (s *Server) CreateQueue(ctx context.Context, req *pb.CreateQueueRequest) (*pb.CreateQueueResponse, error) {
	if req.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "queue name is required")
	}
	
	qtype := convertQueueType(req.Type)
	q := types.NewQueue(req.Name, qtype)
	
	// Apply configuration if provided
	if req.Config != nil {
		if req.Config.MaxDepth > 0 {
			q.Settings.MaxSize = int(req.Config.MaxDepth)
		}
		// Note: MaxRetries is not a queue-level setting in this implementation
		// TODO: Apply other config options
	}
	
	if err := s.manager.CreateQueue(ctx, q); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to create queue: %v", err)
	}
	
	return &pb.CreateQueueResponse{
		QueueId:   q.Name, // Using Name as ID since there's no separate ID field
		CreatedAt: timestamppb.New(q.Created),
	}, nil
}

// SubmitTask submits a task message
func (s *Server) SubmitTask(ctx context.Context, req *pb.SubmitTaskRequest) (*pb.SubmitTaskResponse, error) {
	session, err := s.getSession(req.ClientId)
	if err != nil {
		return nil, err
	}
	
	// Apply message options
	opts := []client.MessageOption{}
	if req.Options != nil {
		if req.Options.Priority > 0 {
			opts = append(opts, client.WithPriority(int(req.Options.Priority)))
		}
		if req.Options.Ttl != nil {
			opts = append(opts, client.WithTTL(req.Options.Ttl.AsDuration()))
		}
		if req.Options.MaxRetries > 0 {
			opts = append(opts, client.WithMaxRetries(int(req.Options.MaxRetries)))
		}
		for k, v := range req.Options.Metadata {
			opts = append(opts, client.WithMessageMetadata(k, v))
		}
	}
	
	msg, err := session.client.SubmitTask(ctx, req.Topic, req.Payload, opts...)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to submit task: %v", err)
	}
	
	return &pb.SubmitTaskResponse{
		MessageId:   msg.ID,
		SubmittedAt: timestamppb.New(msg.CreatedAt),
	}, nil
}

// SubmitTaskBatch submits multiple tasks in batch
func (s *Server) SubmitTaskBatch(ctx context.Context, req *pb.SubmitTaskBatchRequest) (*pb.SubmitTaskBatchResponse, error) {
	session, err := s.getSession(req.ClientId)
	if err != nil {
		return nil, err
	}
	
	results := make([]*pb.TaskResult, len(req.Items))
	
	// Process in parallel for better performance
	var wg sync.WaitGroup
	for i, item := range req.Items {
		wg.Add(1)
		go func(idx int, task *pb.TaskItem) {
			defer wg.Done()
			
			opts := []client.MessageOption{}
			if task.Options != nil {
				if task.Options.Priority > 0 {
					opts = append(opts, client.WithPriority(int(task.Options.Priority)))
				}
				// Apply other options...
			}
			
			msg, err := session.client.SubmitTask(ctx, task.Topic, task.Payload, opts...)
			if err != nil {
				results[idx] = &pb.TaskResult{
					Success: false,
					Error:   err.Error(),
				}
			} else {
				results[idx] = &pb.TaskResult{
					MessageId: msg.ID,
					Success:   true,
				}
			}
		}(i, item)
	}
	
	wg.Wait()
	
	return &pb.SubmitTaskBatchResponse{
		Results: results,
	}, nil
}

// ReceiveMessages streams messages to the client
func (s *Server) ReceiveMessages(req *pb.ReceiveMessagesRequest, stream pb.AMQ_ReceiveMessagesServer) error {
	session, err := s.getSession(req.ClientId)
	if err != nil {
		return err
	}
	
	// Configure receive options
	maxMessages := 10
	if req.MaxMessages > 0 {
		maxMessages = int(req.MaxMessages)
	}
	
	waitTime := 30 * time.Second
	if req.WaitTime != nil {
		waitTime = req.WaitTime.AsDuration()
	}
	
	// Long polling loop
	ctx := stream.Context()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	
	deadline := time.Now().Add(waitTime)
	messagesSent := 0
	
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			// Check for messages
			messages, err := session.client.Receive(ctx, maxMessages-messagesSent)
			if err != nil {
				return status.Errorf(codes.Internal, "failed to receive messages: %v", err)
			}
			
			// Send messages to client
			for _, msg := range messages {
				pbMsg := convertMessageToProto(msg)
				if err := stream.Send(pbMsg); err != nil {
					return err
				}
				
				// Auto-ack if requested
				if req.AutoAck {
					session.client.Ack(ctx, msg.ID)
				}
				
				messagesSent++
				if messagesSent >= maxMessages {
					return nil
				}
			}
			
			// Check timeout
			if time.Now().After(deadline) {
				return nil
			}
		}
	}
}

// AckMessage acknowledges a message
func (s *Server) AckMessage(ctx context.Context, req *pb.AckMessageRequest) (*emptypb.Empty, error) {
	session, err := s.getSession(req.ClientId)
	if err != nil {
		return nil, err
	}
	
	if err := session.client.Ack(ctx, req.MessageId); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to ack message: %v", err)
	}
	
	return &emptypb.Empty{}, nil
}

// GetQueueStats returns queue statistics
func (s *Server) GetQueueStats(ctx context.Context, req *pb.GetQueueStatsRequest) (*pb.GetQueueStatsResponse, error) {
	stats, err := s.manager.GetQueueStats(ctx, req.QueueName)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get queue stats: %v", err)
	}
	
	return &pb.GetQueueStatsResponse{
		MessageCount:      stats.MessageCount,
		MessagesInFlight:  0, // Not available in current QueueStats
		SubscriberCount:   int32(stats.Subscribers),
		EnqueueRate:       stats.EnqueueRate,
		DequeueRate:       stats.DequeueRate,
		TotalEnqueued:     0, // Not available in current QueueStats
		TotalDequeued:     0, // Not available in current QueueStats
		TotalAcked:        0, // Not available in current QueueStats
		OldestMessage:     timestamppb.New(time.Time{}), // Not available in current QueueStats
	}, nil
}

// Helper functions
func (s *Server) getSession(clientID string) (*clientSession, error) {
	s.sessionsMu.RLock()
	session, exists := s.sessions[clientID]
	s.sessionsMu.RUnlock()
	
	if !exists {
		return nil, status.Error(codes.NotFound, "client not registered")
	}
	
	return session, nil
}

func convertQueueType(pbType pb.QueueType) types.QueueType {
	switch pbType {
	case pb.QueueType_QUEUE_TYPE_TASK:
		return types.QueueTypeTask
	case pb.QueueType_QUEUE_TYPE_DIRECT:
		return types.QueueTypeDirect
	default:
		return types.QueueTypeTask
	}
}

func convertMessageToProto(msg *types.Message) *pb.Message {
	pbMsg := &pb.Message{
		Id:           msg.ID,
		FromClientId: msg.From,
		Topic:        msg.Topic,
		Payload:      msg.Payload,
		Status:       convertMessageStatus(msg.Status),
		Priority:     int32(msg.Priority),
		RetryCount:   int32(msg.RetryCount),
		Metadata:     msg.Metadata,
		CreatedAt:    timestamppb.New(msg.CreatedAt),
	}
	
	if msg.ExpiresAt != nil {
		pbMsg.ExpiresAt = timestamppb.New(*msg.ExpiresAt)
	}
	
	if msg.Metadata != nil {
		pbMsg.CorrelationId = msg.Metadata["correlation_id"]
		pbMsg.ReplyTo = msg.Metadata["reply_to"]
	}
	
	return pbMsg
}

func convertMessageStatus(status types.MessageStatus) pb.MessageStatus {
	switch status {
	case types.MessageStatusPending:
		return pb.MessageStatus_MESSAGE_STATUS_PENDING
	case types.MessageStatusProcessing:
		return pb.MessageStatus_MESSAGE_STATUS_PENDING // Map processing to pending
	case types.MessageStatusCompleted:
		return pb.MessageStatus_MESSAGE_STATUS_ACKNOWLEDGED // Map completed to acknowledged
	case types.MessageStatusFailed:
		return pb.MessageStatus_MESSAGE_STATUS_FAILED
	case types.MessageStatusExpired:
		return pb.MessageStatus_MESSAGE_STATUS_EXPIRED
	case types.MessageStatusDead:
		return pb.MessageStatus_MESSAGE_STATUS_FAILED // Map dead to failed
	default:
		return pb.MessageStatus_MESSAGE_STATUS_UNSPECIFIED
	}
}

func generateSessionID() string {
	return fmt.Sprintf("session-%d", time.Now().UnixNano())
}

// Additional methods to implement:
// - Subscribe
// - Unsubscribe
// - SendDirect
// - NackMessage
// - AckMessageBatch
// - DeleteQueue
// - ListQueues
// - GetClientInfo
// - ListClients
// - GetMetrics