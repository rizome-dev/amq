package pool

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

var (
	// ErrPoolClosed is returned when operations are attempted on a closed pool
	ErrPoolClosed = errors.New("pool is closed")
	
	// ErrPoolExhausted is returned when the pool is at capacity and cannot create new connections
	ErrPoolExhausted = errors.New("pool exhausted")
	
	// ErrContextDeadline is returned when context deadline is exceeded
	ErrContextDeadline = errors.New("context deadline exceeded")
	
	// ErrInvalidConfig is returned when pool configuration is invalid
	ErrInvalidConfig = errors.New("invalid pool configuration")
)

// Connection represents a pooled connection
type Connection interface {
	// IsValid checks if the connection is still valid
	IsValid() bool
	
	// Close closes the underlying connection
	Close() error
	
	// GetID returns the connection ID
	GetID() string
}

// Factory creates new connections
type Factory func(ctx context.Context) (Connection, error)

// Config holds pool configuration
type Config struct {
	// MinSize is the minimum number of connections to maintain
	MinSize int
	
	// MaxSize is the maximum number of connections allowed
	MaxSize int
	
	// MaxIdleTime is the maximum time a connection can be idle before being closed
	MaxIdleTime time.Duration
	
	// MaxLifetime is the maximum lifetime of a connection
	MaxLifetime time.Duration
	
	// HealthCheckInterval is how often to check connection health
	HealthCheckInterval time.Duration
	
	// AcquireTimeout is the maximum time to wait for a connection
	AcquireTimeout time.Duration
	
	// Factory creates new connections
	Factory Factory
}

// DefaultConfig returns default pool configuration
func DefaultConfig() Config {
	return Config{
		MinSize:             5,
		MaxSize:             100,
		MaxIdleTime:         5 * time.Minute,
		MaxLifetime:         30 * time.Minute,
		HealthCheckInterval: 30 * time.Second,
		AcquireTimeout:      5 * time.Second,
	}
}

// entry represents a connection entry in the pool
type entry struct {
	conn       Connection
	createdAt  time.Time
	lastUsedAt time.Time
	useCount   int64
}

// Pool manages a pool of connections
type Pool struct {
	config    Config
	entries   chan *entry
	waiters   chan chan *entry
	closed    int32
	mu        sync.RWMutex
	stats     Stats
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
}

// Stats holds pool statistics
type Stats struct {
	// Current number of connections
	Size int32
	
	// Number of idle connections
	Idle int32
	
	// Number of active connections
	Active int32
	
	// Total connections created
	TotalCreated int64
	
	// Total connections closed
	TotalClosed int64
	
	// Total acquire operations
	TotalAcquires int64
	
	// Total acquire timeouts
	TotalTimeouts int64
	
	// Total health check failures
	TotalHealthCheckFailures int64
}

// NewPool creates a new connection pool
func NewPool(config Config) (*Pool, error) {
	if err := validateConfig(config); err != nil {
		return nil, err
	}
	
	ctx, cancel := context.WithCancel(context.Background())
	
	p := &Pool{
		config:  config,
		entries: make(chan *entry, config.MaxSize),
		waiters: make(chan chan *entry, config.MaxSize),
		ctx:     ctx,
		cancel:  cancel,
	}
	
	// Start background workers
	p.wg.Add(2)
	go p.maintainer()
	go p.healthChecker()
	
	// Pre-populate pool with minimum connections
	for i := 0; i < config.MinSize; i++ {
		conn, err := config.Factory(ctx)
		if err != nil {
			// Log error but continue
			continue
		}
		
		e := &entry{
			conn:       conn,
			createdAt:  time.Now(),
			lastUsedAt: time.Now(),
		}
		
		select {
		case p.entries <- e:
			atomic.AddInt32(&p.stats.Size, 1)
			atomic.AddInt64(&p.stats.TotalCreated, 1)
		default:
			conn.Close()
		}
	}
	
	return p, nil
}

// Acquire gets a connection from the pool
func (p *Pool) Acquire(ctx context.Context) (Connection, error) {
	if atomic.LoadInt32(&p.closed) == 1 {
		return nil, ErrPoolClosed
	}
	
	atomic.AddInt64(&p.stats.TotalAcquires, 1)
	
	// Try to get existing connection
	select {
	case e := <-p.entries:
		if p.isEntryValid(e) {
			atomic.AddInt32(&p.stats.Active, 1)
			atomic.AddInt32(&p.stats.Idle, -1)
			return &pooledConnection{
				entry: e,
				pool:  p,
			}, nil
		}
		// Invalid entry, close it
		p.closeEntry(e)
	default:
		// No idle connections available
	}
	
	// Try to create new connection if under max size
	if atomic.LoadInt32(&p.stats.Size) < int32(p.config.MaxSize) {
		conn, err := p.config.Factory(ctx)
		if err == nil {
			e := &entry{
				conn:       conn,
				createdAt:  time.Now(),
				lastUsedAt: time.Now(),
			}
			atomic.AddInt32(&p.stats.Size, 1)
			atomic.AddInt32(&p.stats.Active, 1)
			atomic.AddInt64(&p.stats.TotalCreated, 1)
			return &pooledConnection{
				entry: e,
				pool:  p,
			}, nil
		}
	}
	
	// Wait for available connection
	waiter := make(chan *entry, 1)
	
	select {
	case p.waiters <- waiter:
		// Successfully added to waiters queue
	case <-ctx.Done():
		atomic.AddInt64(&p.stats.TotalTimeouts, 1)
		return nil, ErrContextDeadline
	case <-p.ctx.Done():
		return nil, ErrPoolClosed
	}
	
	// Wait for connection
	select {
	case e := <-waiter:
		if e != nil {
			atomic.AddInt32(&p.stats.Active, 1)
			atomic.AddInt32(&p.stats.Idle, -1)
			return &pooledConnection{
				entry: e,
				pool:  p,
			}, nil
		}
		return nil, ErrPoolExhausted
	case <-ctx.Done():
		atomic.AddInt64(&p.stats.TotalTimeouts, 1)
		// Try to remove ourselves from waiters
		go func() {
			select {
			case e := <-waiter:
				if e != nil {
					p.releaseEntry(e)
				}
			default:
			}
		}()
		return nil, ErrContextDeadline
	case <-p.ctx.Done():
		return nil, ErrPoolClosed
	}
}

// Release returns a connection to the pool
func (p *Pool) releaseEntry(e *entry) {
	if atomic.LoadInt32(&p.closed) == 1 {
		p.closeEntry(e)
		return
	}
	
	atomic.AddInt32(&p.stats.Active, -1)
	e.lastUsedAt = time.Now()
	e.useCount++
	
	// Check if there are waiters
	select {
	case waiter := <-p.waiters:
		waiter <- e
		atomic.AddInt32(&p.stats.Active, 1)
		return
	default:
		// No waiters
	}
	
	// Return to pool
	select {
	case p.entries <- e:
		atomic.AddInt32(&p.stats.Idle, 1)
	default:
		// Pool is full, close the connection
		p.closeEntry(e)
	}
}

// Close closes the pool and all connections
func (p *Pool) Close() error {
	if !atomic.CompareAndSwapInt32(&p.closed, 0, 1) {
		return nil
	}
	
	// Cancel background workers
	p.cancel()
	
	// Close all waiters
	close(p.waiters)
	for waiter := range p.waiters {
		close(waiter)
	}
	
	// Close all connections
	close(p.entries)
	for e := range p.entries {
		p.closeEntry(e)
	}
	
	// Wait for background workers
	p.wg.Wait()
	
	return nil
}

// GetStats returns pool statistics
func (p *Pool) GetStats() Stats {
	return Stats{
		Size:                     atomic.LoadInt32(&p.stats.Size),
		Idle:                     atomic.LoadInt32(&p.stats.Idle),
		Active:                   atomic.LoadInt32(&p.stats.Active),
		TotalCreated:             atomic.LoadInt64(&p.stats.TotalCreated),
		TotalClosed:              atomic.LoadInt64(&p.stats.TotalClosed),
		TotalAcquires:            atomic.LoadInt64(&p.stats.TotalAcquires),
		TotalTimeouts:            atomic.LoadInt64(&p.stats.TotalTimeouts),
		TotalHealthCheckFailures: atomic.LoadInt64(&p.stats.TotalHealthCheckFailures),
	}
}

// maintainer maintains the pool size and removes stale connections
func (p *Pool) maintainer() {
	defer p.wg.Done()
	
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	
	for {
		select {
		case <-ticker.C:
			p.maintain()
		case <-p.ctx.Done():
			return
		}
	}
}

// maintain performs pool maintenance
func (p *Pool) maintain() {
	// Remove stale connections
	var validEntries []*entry
	
	for {
		select {
		case e := <-p.entries:
			if p.isEntryValid(e) {
				validEntries = append(validEntries, e)
			} else {
				p.closeEntry(e)
			}
		default:
			// No more entries
			goto returnValid
		}
	}
	
returnValid:
	// Return valid entries to pool
	for _, e := range validEntries {
		select {
		case p.entries <- e:
			// Successfully returned
		default:
			// Pool is full
			p.closeEntry(e)
		}
	}
	
	// Ensure minimum pool size
	currentSize := atomic.LoadInt32(&p.stats.Size)
	if currentSize < int32(p.config.MinSize) {
		for i := currentSize; i < int32(p.config.MinSize); i++ {
			ctx, cancel := context.WithTimeout(p.ctx, 5*time.Second)
			conn, err := p.config.Factory(ctx)
			cancel()
			
			if err != nil {
				continue
			}
			
			e := &entry{
				conn:       conn,
				createdAt:  time.Now(),
				lastUsedAt: time.Now(),
			}
			
			select {
			case p.entries <- e:
				atomic.AddInt32(&p.stats.Size, 1)
				atomic.AddInt32(&p.stats.Idle, 1)
				atomic.AddInt64(&p.stats.TotalCreated, 1)
			default:
				conn.Close()
			}
		}
	}
}

// healthChecker periodically checks connection health
func (p *Pool) healthChecker() {
	defer p.wg.Done()
	
	if p.config.HealthCheckInterval <= 0 {
		return
	}
	
	ticker := time.NewTicker(p.config.HealthCheckInterval)
	defer ticker.Stop()
	
	for {
		select {
		case <-ticker.C:
			p.checkHealth()
		case <-p.ctx.Done():
			return
		}
	}
}

// checkHealth checks the health of idle connections
func (p *Pool) checkHealth() {
	var toCheck []*entry
	
	// Get all idle connections
	for {
		select {
		case e := <-p.entries:
			toCheck = append(toCheck, e)
		default:
			goto doCheck
		}
	}
	
doCheck:
	// Check each connection
	for _, e := range toCheck {
		if e.conn.IsValid() {
			// Return to pool
			select {
			case p.entries <- e:
				// Successfully returned
			default:
				// Pool is full
				p.closeEntry(e)
			}
		} else {
			// Invalid connection
			atomic.AddInt64(&p.stats.TotalHealthCheckFailures, 1)
			p.closeEntry(e)
		}
	}
}

// isEntryValid checks if an entry is still valid
func (p *Pool) isEntryValid(e *entry) bool {
	// Check lifetime
	if p.config.MaxLifetime > 0 && time.Since(e.createdAt) > p.config.MaxLifetime {
		return false
	}
	
	// Check idle time
	if p.config.MaxIdleTime > 0 && time.Since(e.lastUsedAt) > p.config.MaxIdleTime {
		return false
	}
	
	// Check connection validity
	return e.conn.IsValid()
}

// closeEntry closes a connection entry
func (p *Pool) closeEntry(e *entry) {
	if err := e.conn.Close(); err != nil {
		// Log error
	}
	atomic.AddInt32(&p.stats.Size, -1)
	atomic.AddInt64(&p.stats.TotalClosed, 1)
}

// validateConfig validates pool configuration
func validateConfig(config Config) error {
	if config.MinSize < 0 {
		return fmt.Errorf("%w: MinSize must be >= 0", ErrInvalidConfig)
	}
	if config.MaxSize <= 0 {
		return fmt.Errorf("%w: MaxSize must be > 0", ErrInvalidConfig)
	}
	if config.MinSize > config.MaxSize {
		return fmt.Errorf("%w: MinSize must be <= MaxSize", ErrInvalidConfig)
	}
	if config.Factory == nil {
		return fmt.Errorf("%w: Factory is required", ErrInvalidConfig)
	}
	return nil
}

// pooledConnection wraps a connection with pool management
type pooledConnection struct {
	entry  *entry
	pool   *Pool
	closed bool
	mu     sync.Mutex
}

func (pc *pooledConnection) IsValid() bool {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	
	if pc.closed {
		return false
	}
	return pc.entry.conn.IsValid()
}

func (pc *pooledConnection) Close() error {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	
	if pc.closed {
		return nil
	}
	
	pc.closed = true
	pc.pool.releaseEntry(pc.entry)
	return nil
}

func (pc *pooledConnection) GetID() string {
	return pc.entry.conn.GetID()
}

// GetConn returns the underlying connection (for advanced usage)
func (pc *pooledConnection) GetConn() Connection {
	return pc.entry.conn
}