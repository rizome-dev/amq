package pool

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestCircuitBreakerStates tests state transitions
func TestCircuitBreakerStates(t *testing.T) {
	config := CircuitBreakerConfig{
		FailureThreshold:    3,
		SuccessThreshold:    2,
		OpenDuration:        100 * time.Millisecond,
		HalfOpenMaxRequests: 1,
	}
	
	cb := NewCircuitBreaker(config)
	
	// Initial state should be closed
	if cb.GetState() != StateClosed {
		t.Errorf("Expected initial state to be closed, got %s", cb.GetState())
	}
	
	// Simulate failures to open the circuit
	ctx := context.Background()
	for i := 0; i < config.FailureThreshold; i++ {
		err := cb.Execute(ctx, func(ctx context.Context) error {
			return errors.New("test error")
		})
		if err == nil {
			t.Error("Expected error, got nil")
		}
	}
	
	// Circuit should be open now
	if cb.GetState() != StateOpen {
		t.Errorf("Expected state to be open after %d failures, got %s", config.FailureThreshold, cb.GetState())
	}
	
	// Requests should fail immediately when open
	err := cb.Execute(ctx, func(ctx context.Context) error {
		t.Error("Function should not be called when circuit is open")
		return nil
	})
	if !errors.Is(err, ErrCircuitOpen) {
		t.Errorf("Expected ErrCircuitOpen, got %v", err)
	}
	
	// Wait for circuit to transition to half-open
	time.Sleep(config.OpenDuration + 10*time.Millisecond)
	
	// First request should transition to half-open
	called := false
	err = cb.Execute(ctx, func(ctx context.Context) error {
		called = true
		return nil
	})
	if err != nil {
		t.Errorf("Expected success in half-open state, got %v", err)
	}
	if !called {
		t.Error("Function should be called in half-open state")
	}
	
	// Circuit should be half-open
	if cb.GetState() != StateHalfOpen {
		t.Errorf("Expected state to be half-open, got %s", cb.GetState())
	}
	
	// One more success should close the circuit
	err = cb.Execute(ctx, func(ctx context.Context) error {
		return nil
	})
	if err != nil {
		t.Errorf("Expected success, got %v", err)
	}
	
	// Circuit should be closed
	if cb.GetState() != StateClosed {
		t.Errorf("Expected state to be closed after %d successes, got %s", config.SuccessThreshold, cb.GetState())
	}
}

// TestCircuitBreakerHalfOpenLimit tests half-open request limiting
func TestCircuitBreakerHalfOpenLimit(t *testing.T) {
	config := CircuitBreakerConfig{
		FailureThreshold:    2,
		SuccessThreshold:    2,
		OpenDuration:        50 * time.Millisecond,
		HalfOpenMaxRequests: 2,
	}
	
	cb := NewCircuitBreaker(config)
	ctx := context.Background()
	
	// Open the circuit
	for i := 0; i < config.FailureThreshold; i++ {
		cb.Execute(ctx, func(ctx context.Context) error {
			return errors.New("test error")
		})
	}
	
	// Wait for half-open
	time.Sleep(config.OpenDuration + 10*time.Millisecond)
	
	// Launch multiple concurrent requests
	var wg sync.WaitGroup
	var errorCount int32
	var tooManyCount int32
	
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			
			err := cb.Execute(ctx, func(ctx context.Context) error {
				time.Sleep(200 * time.Millisecond) // Hold the slot longer
				return errors.New("test error") // Make requests fail to stay in half-open
			})
			
			if err != nil && !errors.Is(err, ErrTooManyRequests) {
				atomic.AddInt32(&errorCount, 1)
			} else if errors.Is(err, ErrTooManyRequests) {
				atomic.AddInt32(&tooManyCount, 1)
			}
		}()
	}
	
	wg.Wait()
	
	// Should have at most HalfOpenMaxRequests executed
	if errorCount > int32(config.HalfOpenMaxRequests) {
		t.Errorf("Expected at most %d executed requests, got %d", config.HalfOpenMaxRequests, errorCount)
	}
	
	// Should have some requests rejected
	if tooManyCount == 0 {
		t.Error("Expected some requests to be rejected with ErrTooManyRequests")
	}
}

// TestCircuitBreakerFallback tests fallback execution
func TestCircuitBreakerFallback(t *testing.T) {
	config := CircuitBreakerConfig{
		FailureThreshold: 1,
		OpenDuration:     100 * time.Millisecond,
	}
	
	cb := NewCircuitBreaker(config)
	ctx := context.Background()
	
	// Open the circuit
	cb.Execute(ctx, func(ctx context.Context) error {
		return errors.New("test error")
	})
	
	// Execute with fallback
	fallbackCalled := false
	err := cb.ExecuteWithFallback(ctx,
		func(ctx context.Context) error {
			t.Error("Primary function should not be called")
			return errors.New("should not happen")
		},
		func(ctx context.Context) error {
			fallbackCalled = true
			return nil
		},
	)
	
	if err != nil {
		t.Errorf("Expected fallback to succeed, got %v", err)
	}
	
	if !fallbackCalled {
		t.Error("Fallback should have been called")
	}
}

// TestCircuitBreakerTimeout tests timeout handling
func TestCircuitBreakerTimeout(t *testing.T) {
	config := CircuitBreakerConfig{
		FailureThreshold: 3,
		Timeout:          50 * time.Millisecond,
	}
	
	cb := NewCircuitBreaker(config)
	ctx := context.Background()
	
	// Execute function that takes too long
	err := cb.Execute(ctx, func(ctx context.Context) error {
		select {
		case <-time.After(100 * time.Millisecond):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Expected context.DeadlineExceeded, got %v", err)
	}
	
	stats := cb.GetStats()
	if stats.TotalTimeouts != 1 {
		t.Errorf("Expected 1 timeout, got %d", stats.TotalTimeouts)
	}
}

// TestCircuitBreakerReset tests manual reset
func TestCircuitBreakerReset(t *testing.T) {
	config := CircuitBreakerConfig{
		FailureThreshold: 2,
		OpenDuration:     1 * time.Hour, // Long duration
	}
	
	cb := NewCircuitBreaker(config)
	ctx := context.Background()
	
	// Open the circuit
	for i := 0; i < config.FailureThreshold; i++ {
		cb.Execute(ctx, func(ctx context.Context) error {
			return errors.New("test error")
		})
	}
	
	if cb.GetState() != StateOpen {
		t.Error("Circuit should be open")
	}
	
	// Reset the circuit
	cb.Reset()
	
	if cb.GetState() != StateClosed {
		t.Error("Circuit should be closed after reset")
	}
	
	// Should be able to execute again
	called := false
	err := cb.Execute(ctx, func(ctx context.Context) error {
		called = true
		return nil
	})
	
	if err != nil {
		t.Errorf("Expected success after reset, got %v", err)
	}
	
	if !called {
		t.Error("Function should be called after reset")
	}
}

// TestCircuitBreakerStats tests statistics collection
func TestCircuitBreakerStats(t *testing.T) {
	config := CircuitBreakerConfig{
		FailureThreshold: 3,
		SuccessThreshold: 2,
	}
	
	cb := NewCircuitBreaker(config)
	ctx := context.Background()
	
	// Execute some successful requests
	for i := 0; i < 5; i++ {
		cb.Execute(ctx, func(ctx context.Context) error {
			return nil
		})
	}
	
	// Execute some failed requests
	for i := 0; i < 2; i++ {
		cb.Execute(ctx, func(ctx context.Context) error {
			return errors.New("test error")
		})
	}
	
	stats := cb.GetStats()
	
	if stats.TotalRequests != 7 {
		t.Errorf("Expected 7 total requests, got %d", stats.TotalRequests)
	}
	
	if stats.TotalSuccesses != 5 {
		t.Errorf("Expected 5 successes, got %d", stats.TotalSuccesses)
	}
	
	if stats.TotalFailures != 2 {
		t.Errorf("Expected 2 failures, got %d", stats.TotalFailures)
	}
	
	if stats.ConsecutiveFailures != 2 {
		t.Errorf("Expected 2 consecutive failures, got %d", stats.ConsecutiveFailures)
	}
}

// TestPoolWithCircuitBreaker tests the integrated pool with circuit breaker
func TestPoolWithCircuitBreaker(t *testing.T) {
	failureCount := int32(0)
	factory := func(ctx context.Context) (Connection, error) {
		count := atomic.AddInt32(&failureCount, 1)
		t.Logf("Factory called, attempt %d", count)
		// Fail first 2 attempts only
		if count <= 2 {
			return nil, errors.New("connection failed")
		}
		return &mockConnection{
			id:    "test",
			valid: true,
		}, nil
	}
	
	poolConfig := Config{
		MinSize:        0,
		MaxSize:        5,
		MaxIdleTime:    5 * time.Minute,
		AcquireTimeout: 2 * time.Second, // Increase timeout
		Factory:        factory,
	}
	
	cbConfig := CircuitBreakerConfig{
		FailureThreshold:    2,
		SuccessThreshold:    1, // Only need 1 success to close
		OpenDuration:        200 * time.Millisecond, // Increase duration
		Timeout:             2 * time.Second, // Increase timeout
		HalfOpenMaxRequests: 1,
	}
	
	pcb, err := NewPoolWithCircuitBreaker(poolConfig, cbConfig)
	if err != nil {
		t.Fatalf("Failed to create pool with circuit breaker: %v", err)
	}
	defer pcb.Close()
	
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	
	// First attempts should fail and open the circuit
	for i := 0; i < 2; i++ {
		t.Logf("Attempting connection %d", i+1)
		_, err := pcb.Acquire(ctx)
		if err == nil {
			t.Errorf("Expected error on attempt %d, got nil", i+1)
		} else {
			t.Logf("Got expected error: %v", err)
		}
	}
	
	// Circuit should be open
	t.Log("Testing if circuit is open")
	_, err = pcb.Acquire(ctx)
	if !errors.Is(err, ErrCircuitOpen) {
		t.Errorf("Expected ErrCircuitOpen, got %v", err)
	} else {
		t.Log("Circuit is correctly open")
	}
	
	// Wait for circuit to become half-open
	sleepTime := cbConfig.OpenDuration + 50*time.Millisecond
	t.Logf("Waiting %v for circuit to become half-open", sleepTime)
	time.Sleep(sleepTime)
	
	// Now it should succeed (factory will return success)
	t.Log("Attempting connection in half-open state")
	conn, err := pcb.Acquire(ctx)
	if err != nil {
		t.Errorf("Expected success in half-open state, got %v", err)
	} else {
		t.Log("Successfully acquired connection in half-open state")
	}
	
	if conn == nil {
		t.Error("Expected connection, got nil")
	} else {
		conn.Close()
		t.Log("Connection closed successfully")
	}
}

// BenchmarkCircuitBreaker benchmarks circuit breaker overhead
func BenchmarkCircuitBreaker(b *testing.B) {
	config := CircuitBreakerConfig{
		FailureThreshold: 100,
		SuccessThreshold: 10,
	}
	
	cb := NewCircuitBreaker(config)
	ctx := context.Background()
	
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			cb.Execute(ctx, func(ctx context.Context) error {
				return nil
			})
		}
	})
}

// BenchmarkCircuitBreakerConcurrent benchmarks concurrent circuit breaker operations
func BenchmarkCircuitBreakerConcurrent(b *testing.B) {
	config := CircuitBreakerConfig{
		FailureThreshold:    100,
		SuccessThreshold:    10,
		HalfOpenMaxRequests: 50,
	}
	
	cb := NewCircuitBreaker(config)
	ctx := context.Background()
	
	// Mix of success and failure
	var counter int32
	
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			// 10% failure rate
			cb.Execute(ctx, func(ctx context.Context) error {
				if atomic.AddInt32(&counter, 1)%10 == 0 {
					return errors.New("test error")
				}
				return nil
			})
		}
	})
}