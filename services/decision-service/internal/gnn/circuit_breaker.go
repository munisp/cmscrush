package gnn

import (
	"errors"
	"sync"
	"time"
)

var ErrCircuitOpen = errors.New("gnn circuit breaker is open")

type breakerState uint8

const (
	stateClosed breakerState = iota
	stateOpen
	stateHalfOpen
)

type CircuitBreaker struct {
	mu              sync.Mutex
	state           breakerState
	consecutiveFail int
	threshold       int
	cooldown        time.Duration
	openedAt        time.Time
}

func NewCircuitBreaker(threshold int, cooldown time.Duration) *CircuitBreaker {
	if threshold < 1 {
		threshold = 3
	}
	if cooldown <= 0 {
		cooldown = 5 * time.Second
	}
	return &CircuitBreaker{threshold: threshold, cooldown: cooldown}
}

func (b *CircuitBreaker) Allow(now time.Time) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state == stateClosed {
		return nil
	}
	if b.state == stateOpen && now.Sub(b.openedAt) < b.cooldown {
		return ErrCircuitOpen
	}
	if b.state == stateOpen {
		b.state = stateHalfOpen
		return nil
	}
	return ErrCircuitOpen
}

func (b *CircuitBreaker) RecordSuccess() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.state = stateClosed
	b.consecutiveFail = 0
}

func (b *CircuitBreaker) RecordFailure(now time.Time) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state == stateHalfOpen || b.consecutiveFail+1 >= b.threshold {
		b.state = stateOpen
		b.openedAt = now
		b.consecutiveFail = 0
		return
	}
	b.consecutiveFail++
}

func (b *CircuitBreaker) State() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case stateOpen:
		return "OPEN"
	case stateHalfOpen:
		return "HALF_OPEN"
	default:
		return "CLOSED"
	}
}
