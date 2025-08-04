package pool

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// State represents the state of a circuit breaker
type State int32

const (
	// StateClosed allows requests to pass through
	StateClosed State = iota
	
	// StateOpen blocks all requests
	StateOpen
	
	// StateHalfOpen allows limited requests to test recovery
	StateHalfOpen
)

// String returns the string representation of the state
func (s State) String() string {
	switch s {
	case StateClosed:
		return "closed"
	case StateOpen:
		return "open"
	case StateHalfOpen:
		return "half-open"
	default:
		return "unknown"
	}
}

var (
	// ErrCircuitOpen is returned when the circuit breaker is open
	ErrCircuitOpen = errors.New("circuit breaker is open")
	
	// ErrTooManyRequests is returned when half-open circuit has too many requests
	ErrTooManyRequests = errors.New("too many requests in half-open state")
)

// CircuitBreaker implements the circuit breaker pattern
type CircuitBreaker struct {
	config            CircuitBreakerConfig
	state             int32 // atomic State
	failures          int64 // consecutive failures
	successes         int64 // consecutive successes in half-open
	lastFailureTime   int64 // unix nano
	lastStateChange   int64 // unix nano
	halfOpenRequests  int32 // current requests in half-open state
	totalRequests     int64
	totalFailures     int64
	totalSuccesses    int64
	totalTimeouts     int64
	totalCircuitOpens int64
	mu                sync.RWMutex
	onStateChange     func(from, to State)
}

// CircuitBreakerConfig holds circuit breaker configuration
type CircuitBreakerConfig struct {
	// FailureThreshold is the number of failures before opening the circuit
	FailureThreshold int
	
	// SuccessThreshold is the number of successes in half-open before closing
	SuccessThreshold int
	
	// Timeout for operations
	Timeout time.Duration
	
	// OpenDuration is how long the circuit stays open
	OpenDuration time.Duration
	
	// HalfOpenMaxRequests is the max concurrent requests in half-open state
	HalfOpenMaxRequests int
	
	// OnStateChange is called when the circuit breaker changes state
	OnStateChange func(from, to State)
}

// DefaultCircuitBreakerConfig returns default circuit breaker configuration
func DefaultCircuitBreakerConfig() CircuitBreakerConfig {
	return CircuitBreakerConfig{
		FailureThreshold:    5,
		SuccessThreshold:    2,
		Timeout:             30 * time.Second,
		OpenDuration:        60 * time.Second,
		HalfOpenMaxRequests: 3,
	}
}

// NewCircuitBreaker creates a new circuit breaker
func NewCircuitBreaker(config CircuitBreakerConfig) *CircuitBreaker {
	cb := &CircuitBreaker{
		config:        config,
		state:         int32(StateClosed),
		onStateChange: config.OnStateChange,
	}
	
	return cb
}

// Execute runs the given function with circuit breaker protection
func (cb *CircuitBreaker) Execute(ctx context.Context, fn func(context.Context) error) error {
	if err := cb.canExecute(); err != nil {
		return err
	}
	
	// Add timeout if configured
	if cb.config.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, cb.config.Timeout)
		defer cancel()
	}
	
	// Execute the function
	err := fn(ctx)
	
	// Record the result
	cb.recordResult(err)
	
	return err
}

// ExecuteWithFallback runs the function with a fallback on circuit open
func (cb *CircuitBreaker) ExecuteWithFallback(ctx context.Context, fn func(context.Context) error, fallback func(context.Context) error) error {
	err := cb.Execute(ctx, fn)
	if errors.Is(err, ErrCircuitOpen) || errors.Is(err, ErrTooManyRequests) {
		return fallback(ctx)
	}
	return err
}

// canExecute checks if a request can be executed
func (cb *CircuitBreaker) canExecute() error {
	state := cb.GetState()
	
	switch state {
	case StateClosed:
		return nil
		
	case StateOpen:
		// Check if we should transition to half-open
		if cb.shouldTransitionToHalfOpen() {
			cb.transitionTo(StateHalfOpen)
			return cb.canExecuteHalfOpen()
		}
		atomic.AddInt64(&cb.totalCircuitOpens, 1)
		return ErrCircuitOpen
		
	case StateHalfOpen:
		return cb.canExecuteHalfOpen()
		
	default:
		return ErrCircuitOpen
	}
}

// canExecuteHalfOpen checks if request can proceed in half-open state
func (cb *CircuitBreaker) canExecuteHalfOpen() error {
	current := atomic.AddInt32(&cb.halfOpenRequests, 1)
	if current > int32(cb.config.HalfOpenMaxRequests) {
		atomic.AddInt32(&cb.halfOpenRequests, -1)
		return ErrTooManyRequests
	}
	return nil
}

// recordResult records the result of an execution
func (cb *CircuitBreaker) recordResult(err error) {
	atomic.AddInt64(&cb.totalRequests, 1)
	
	state := cb.GetState()
	
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			atomic.AddInt64(&cb.totalTimeouts, 1)
		}
		cb.recordFailure()
	} else {
		cb.recordSuccess()
	}
	
	// Decrement half-open counter if applicable
	if state == StateHalfOpen {
		atomic.AddInt32(&cb.halfOpenRequests, -1)
	}
}

// recordFailure records a failure
func (cb *CircuitBreaker) recordFailure() {
	atomic.AddInt64(&cb.totalFailures, 1)
	atomic.StoreInt64(&cb.lastFailureTime, time.Now().UnixNano())
	
	state := cb.GetState()
	
	switch state {
	case StateClosed:
		failures := atomic.AddInt64(&cb.failures, 1)
		if failures >= int64(cb.config.FailureThreshold) {
			cb.transitionTo(StateOpen)
		}
		
	case StateHalfOpen:
		cb.transitionTo(StateOpen)
	}
}

// recordSuccess records a success
func (cb *CircuitBreaker) recordSuccess() {
	atomic.AddInt64(&cb.totalSuccesses, 1)
	
	state := cb.GetState()
	
	switch state {
	case StateClosed:
		atomic.StoreInt64(&cb.failures, 0)
		
	case StateHalfOpen:
		successes := atomic.AddInt64(&cb.successes, 1)
		if successes >= int64(cb.config.SuccessThreshold) {
			cb.transitionTo(StateClosed)
		}
	}
}

// shouldTransitionToHalfOpen checks if circuit should transition to half-open
func (cb *CircuitBreaker) shouldTransitionToHalfOpen() bool {
	lastChange := atomic.LoadInt64(&cb.lastStateChange)
	return time.Since(time.Unix(0, lastChange)) > cb.config.OpenDuration
}

// transitionTo transitions to a new state
func (cb *CircuitBreaker) transitionTo(newState State) {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	
	oldState := State(atomic.LoadInt32(&cb.state))
	if oldState == newState {
		return
	}
	
	atomic.StoreInt32(&cb.state, int32(newState))
	atomic.StoreInt64(&cb.lastStateChange, time.Now().UnixNano())
	
	// Reset counters based on new state
	switch newState {
	case StateClosed:
		atomic.StoreInt64(&cb.failures, 0)
		atomic.StoreInt64(&cb.successes, 0)
		atomic.StoreInt32(&cb.halfOpenRequests, 0)
		
	case StateOpen:
		atomic.StoreInt64(&cb.successes, 0)
		atomic.StoreInt32(&cb.halfOpenRequests, 0)
		
	case StateHalfOpen:
		atomic.StoreInt64(&cb.failures, 0)
		atomic.StoreInt64(&cb.successes, 0)
		atomic.StoreInt32(&cb.halfOpenRequests, 0)
	}
	
	// Notify state change
	if cb.onStateChange != nil {
		go cb.onStateChange(oldState, newState)
	}
}

// GetState returns the current state
func (cb *CircuitBreaker) GetState() State {
	return State(atomic.LoadInt32(&cb.state))
}

// GetStats returns circuit breaker statistics
func (cb *CircuitBreaker) GetStats() CircuitBreakerStats {
	state := cb.GetState()
	return CircuitBreakerStats{
		State:             state.String(),
		TotalRequests:     atomic.LoadInt64(&cb.totalRequests),
		TotalFailures:     atomic.LoadInt64(&cb.totalFailures),
		TotalSuccesses:    atomic.LoadInt64(&cb.totalSuccesses),
		TotalTimeouts:     atomic.LoadInt64(&cb.totalTimeouts),
		TotalCircuitOpens: atomic.LoadInt64(&cb.totalCircuitOpens),
		ConsecutiveFailures: atomic.LoadInt64(&cb.failures),
		ConsecutiveSuccesses: atomic.LoadInt64(&cb.successes),
		LastFailureTime:   time.Unix(0, atomic.LoadInt64(&cb.lastFailureTime)),
		LastStateChange:   time.Unix(0, atomic.LoadInt64(&cb.lastStateChange)),
	}
}

// Reset resets the circuit breaker to closed state
func (cb *CircuitBreaker) Reset() {
	cb.transitionTo(StateClosed)
}

// CircuitBreakerStats holds circuit breaker statistics
type CircuitBreakerStats struct {
	State                string
	TotalRequests        int64
	TotalFailures        int64
	TotalSuccesses       int64
	TotalTimeouts        int64
	TotalCircuitOpens    int64
	ConsecutiveFailures  int64
	ConsecutiveSuccesses int64
	LastFailureTime      time.Time
	LastStateChange      time.Time
}

// PoolWithCircuitBreaker wraps a pool with circuit breaker protection
type PoolWithCircuitBreaker struct {
	pool    *Pool
	breaker *CircuitBreaker
}

// NewPoolWithCircuitBreaker creates a pool with circuit breaker protection
func NewPoolWithCircuitBreaker(poolConfig Config, cbConfig CircuitBreakerConfig) (*PoolWithCircuitBreaker, error) {
	pool, err := NewPool(poolConfig)
	if err != nil {
		return nil, err
	}
	
	breaker := NewCircuitBreaker(cbConfig)
	
	return &PoolWithCircuitBreaker{
		pool:    pool,
		breaker: breaker,
	}, nil
}

// Acquire gets a connection with circuit breaker protection
func (pcb *PoolWithCircuitBreaker) Acquire(ctx context.Context) (Connection, error) {
	var conn Connection
	err := pcb.breaker.Execute(ctx, func(ctx context.Context) error {
		c, err := pcb.pool.Acquire(ctx)
		if err != nil {
			return err
		}
		conn = c
		return nil
	})
	
	if err != nil {
		return nil, err
	}
	
	return conn, nil
}

// Close closes the pool and circuit breaker
func (pcb *PoolWithCircuitBreaker) Close() error {
	return pcb.pool.Close()
}

// GetPoolStats returns pool statistics
func (pcb *PoolWithCircuitBreaker) GetPoolStats() Stats {
	return pcb.pool.GetStats()
}

// GetCircuitBreakerStats returns circuit breaker statistics
func (pcb *PoolWithCircuitBreaker) GetCircuitBreakerStats() CircuitBreakerStats {
	return pcb.breaker.GetStats()
}