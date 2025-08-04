package pool

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// mockConnection implements the Connection interface for testing
type mockConnection struct {
	id      string
	valid   bool
	closed  bool
	mu      sync.Mutex
}

func (mc *mockConnection) IsValid() bool {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	return mc.valid && !mc.closed
}

func (mc *mockConnection) Close() error {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	if mc.closed {
		return errors.New("already closed")
	}
	mc.closed = true
	return nil
}

func (mc *mockConnection) GetID() string {
	return mc.id
}

func (mc *mockConnection) SetValid(valid bool) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	mc.valid = valid
}

// TestPoolCreation tests pool creation and initialization
func TestPoolCreation(t *testing.T) {
	connID := int32(0)
	factory := func(ctx context.Context) (Connection, error) {
		id := atomic.AddInt32(&connID, 1)
		return &mockConnection{
			id:    string(rune(id)),
			valid: true,
		}, nil
	}
	
	config := Config{
		MinSize:    5,
		MaxSize:    10,
		MaxIdleTime: 5 * time.Minute,
		Factory:    factory,
	}
	
	pool, err := NewPool(config)
	if err != nil {
		t.Fatalf("Failed to create pool: %v", err)
	}
	defer pool.Close()
	
	// Wait for initial connections
	time.Sleep(100 * time.Millisecond)
	
	stats := pool.GetStats()
	if stats.Size < int32(config.MinSize) {
		t.Errorf("Pool size %d is less than MinSize %d", stats.Size, config.MinSize)
	}
	
	if stats.TotalCreated < int64(config.MinSize) {
		t.Errorf("TotalCreated %d is less than MinSize %d", stats.TotalCreated, config.MinSize)
	}
}

// TestPoolAcquireRelease tests acquiring and releasing connections
func TestPoolAcquireRelease(t *testing.T) {
	factory := func(ctx context.Context) (Connection, error) {
		return &mockConnection{
			id:    "test",
			valid: true,
		}, nil
	}
	
	config := Config{
		MinSize:    2,
		MaxSize:    5,
		MaxIdleTime: 5 * time.Minute,
		Factory:    factory,
	}
	
	pool, err := NewPool(config)
	if err != nil {
		t.Fatalf("Failed to create pool: %v", err)
	}
	defer pool.Close()
	
	// Acquire connection
	ctx := context.Background()
	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("Failed to acquire connection: %v", err)
	}
	
	stats := pool.GetStats()
	if stats.Active != 1 {
		t.Errorf("Expected 1 active connection, got %d", stats.Active)
	}
	
	// Release connection
	err = conn.Close()
	if err != nil {
		t.Fatalf("Failed to release connection: %v", err)
	}
	
	// Give time for release
	time.Sleep(10 * time.Millisecond)
	
	stats = pool.GetStats()
	if stats.Active != 0 {
		t.Errorf("Expected 0 active connections, got %d", stats.Active)
	}
}

// TestPoolMaxSize tests that pool respects maximum size
func TestPoolMaxSize(t *testing.T) {
	factory := func(ctx context.Context) (Connection, error) {
		time.Sleep(10 * time.Millisecond) // Simulate connection creation time
		return &mockConnection{
			id:    "test",
			valid: true,
		}, nil
	}
	
	config := Config{
		MinSize:        0,
		MaxSize:        3,
		MaxIdleTime:    5 * time.Minute,
		AcquireTimeout: 100 * time.Millisecond,
		Factory:        factory,
	}
	
	pool, err := NewPool(config)
	if err != nil {
		t.Fatalf("Failed to create pool: %v", err)
	}
	defer pool.Close()
	
	// Acquire max connections
	ctx := context.Background()
	conns := make([]Connection, 0, config.MaxSize)
	
	for i := 0; i < config.MaxSize; i++ {
		conn, err := pool.Acquire(ctx)
		if err != nil {
			t.Fatalf("Failed to acquire connection %d: %v", i, err)
		}
		conns = append(conns, conn)
	}
	
	stats := pool.GetStats()
	if stats.Size != int32(config.MaxSize) {
		t.Errorf("Expected size %d, got %d", config.MaxSize, stats.Size)
	}
	
	// Try to acquire one more - should timeout
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	
	_, err = pool.Acquire(ctx)
	if err != ErrContextDeadline {
		t.Errorf("Expected ErrContextDeadline, got %v", err)
	}
	
	// Release all connections
	for _, conn := range conns {
		conn.Close()
	}
}

// TestPoolHealthCheck tests the health checking mechanism
func TestPoolHealthCheck(t *testing.T) {
	factory := func(ctx context.Context) (Connection, error) {
		return &mockConnection{
			id:    "test",
			valid: true,
		}, nil
	}
	
	config := Config{
		MinSize:             2,
		MaxSize:             5,
		MaxIdleTime:         5 * time.Minute,
		HealthCheckInterval: 50 * time.Millisecond,
		Factory:             factory,
	}
	
	pool, err := NewPool(config)
	if err != nil {
		t.Fatalf("Failed to create pool: %v", err)
	}
	defer pool.Close()
	
	// Wait for initial connections
	time.Sleep(100 * time.Millisecond)
	
	// Get all connections and mark them invalid
	ctx := context.Background()
	conns := make([]Connection, 0)
	
	for i := 0; i < 2; i++ {
		conn, err := pool.Acquire(ctx)
		if err != nil {
			t.Fatalf("Failed to acquire connection: %v", err)
		}
		
		// Mark as invalid
		if mc, ok := conn.(*pooledConnection).entry.conn.(*mockConnection); ok {
			mc.SetValid(false)
		}
		
		conns = append(conns, conn)
	}
	
	// Release connections
	for _, conn := range conns {
		conn.Close()
	}
	
	// Wait for health check to run
	time.Sleep(150 * time.Millisecond)
	
	// Check that invalid connections were removed
	stats := pool.GetStats()
	if stats.TotalHealthCheckFailures == 0 {
		t.Error("Expected health check failures, got none")
	}
}

// TestPoolConcurrency tests concurrent access to the pool
func TestPoolConcurrency(t *testing.T) {
	factory := func(ctx context.Context) (Connection, error) {
		return &mockConnection{
			id:    "test",
			valid: true,
		}, nil
	}
	
	config := Config{
		MinSize:    5,
		MaxSize:    20,
		MaxIdleTime: 5 * time.Minute,
		Factory:    factory,
	}
	
	pool, err := NewPool(config)
	if err != nil {
		t.Fatalf("Failed to create pool: %v", err)
	}
	defer pool.Close()
	
	// Run concurrent acquire/release operations
	var wg sync.WaitGroup
	numGoroutines := 50
	numOperations := 100
	
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			
			ctx := context.Background()
			for j := 0; j < numOperations; j++ {
				conn, err := pool.Acquire(ctx)
				if err != nil {
					t.Errorf("Failed to acquire connection: %v", err)
					return
				}
				
				// Simulate some work
				time.Sleep(time.Microsecond * 100)
				
				if err := conn.Close(); err != nil {
					t.Errorf("Failed to release connection: %v", err)
				}
			}
		}()
	}
	
	wg.Wait()
	
	// Verify pool is still functional
	ctx := context.Background()
	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("Failed to acquire connection after concurrency test: %v", err)
	}
	conn.Close()
}

// BenchmarkPoolAcquireRelease benchmarks acquire/release operations
func BenchmarkPoolAcquireRelease(b *testing.B) {
	factory := func(ctx context.Context) (Connection, error) {
		return &mockConnection{
			id:    "test",
			valid: true,
		}, nil
	}
	
	config := Config{
		MinSize:    10,
		MaxSize:    50,
		MaxIdleTime: 5 * time.Minute,
		Factory:    factory,
	}
	
	pool, err := NewPool(config)
	if err != nil {
		b.Fatalf("Failed to create pool: %v", err)
	}
	defer pool.Close()
	
	// Wait for pool to initialize
	time.Sleep(100 * time.Millisecond)
	
	ctx := context.Background()
	
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			conn, err := pool.Acquire(ctx)
			if err != nil {
				b.Fatalf("Failed to acquire connection: %v", err)
			}
			conn.Close()
		}
	})
}

// BenchmarkPoolConcurrent benchmarks concurrent pool operations
func BenchmarkPoolConcurrent(b *testing.B) {
	factory := func(ctx context.Context) (Connection, error) {
		return &mockConnection{
			id:    "test",
			valid: true,
		}, nil
	}
	
	config := Config{
		MinSize:    50,
		MaxSize:    100,
		MaxIdleTime: 5 * time.Minute,
		Factory:    factory,
	}
	
	pool, err := NewPool(config)
	if err != nil {
		b.Fatalf("Failed to create pool: %v", err)
	}
	defer pool.Close()
	
	// Wait for pool to initialize
	time.Sleep(200 * time.Millisecond)
	
	ctx := context.Background()
	
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			// Acquire multiple connections
			conns := make([]Connection, 5)
			for i := 0; i < 5; i++ {
				conn, err := pool.Acquire(ctx)
				if err != nil {
					b.Fatalf("Failed to acquire connection: %v", err)
				}
				conns[i] = conn
			}
			
			// Release them
			for _, conn := range conns {
				conn.Close()
			}
		}
	})
}