package gnn

import (
	"errors"
	"testing"
	"time"
)

func TestCircuitBreakerOpensAfterThresholdAndRecoversWithProbe(t *testing.T) {
	start := time.Unix(100, 0)
	breaker := NewCircuitBreaker(3, 5*time.Second)
	for i := 0; i < 3; i++ {
		if err := breaker.Allow(start); err != nil {
			t.Fatalf("request %d unexpectedly blocked: %v", i, err)
		}
		breaker.RecordFailure(start)
	}
	if breaker.State() != "OPEN" {
		t.Fatalf("expected OPEN, got %s", breaker.State())
	}
	if err := breaker.Allow(start.Add(4 * time.Second)); !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("expected open circuit during cooldown, got %v", err)
	}
	if err := breaker.Allow(start.Add(5 * time.Second)); err != nil {
		t.Fatalf("expected half-open probe, got %v", err)
	}
	if breaker.State() != "HALF_OPEN" {
		t.Fatalf("expected HALF_OPEN, got %s", breaker.State())
	}
	breaker.RecordSuccess()
	if breaker.State() != "CLOSED" {
		t.Fatalf("expected CLOSED after successful probe, got %s", breaker.State())
	}
}

func TestCircuitBreakerHalfOpenFailureReopens(t *testing.T) {
	start := time.Unix(200, 0)
	breaker := NewCircuitBreaker(1, time.Second)
	if err := breaker.Allow(start); err != nil {
		t.Fatal(err)
	}
	breaker.RecordFailure(start)
	if err := breaker.Allow(start.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	breaker.RecordFailure(start.Add(time.Second))
	if breaker.State() != "OPEN" {
		t.Fatalf("expected OPEN after failed probe, got %s", breaker.State())
	}
}
