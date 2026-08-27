package store

import (
	"errors"
	"sync"

	"github.com/munisp/cmscrush/services/decision-service/internal/decision"
)

type Repository interface {
	PreviousHash(tenantID string) string
	FindByIdempotency(tenantID, key string) (decision.EvaluationResult, bool)
	Append(result decision.EvaluationResult) error
	Get(decisionUID string) (decision.DecisionRecord, bool)
}

type MemoryRepository struct {
	mu               sync.RWMutex
	previousByTenant map[string]string
	byIdempotency    map[string]decision.EvaluationResult
	byDecisionUID    map[string]decision.DecisionRecord
}

func NewMemoryRepository() *MemoryRepository {
	return &MemoryRepository{
		previousByTenant: make(map[string]string),
		byIdempotency:    make(map[string]decision.EvaluationResult),
		byDecisionUID:    make(map[string]decision.DecisionRecord),
	}
}

func (r *MemoryRepository) PreviousHash(tenantID string) string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.previousByTenant[tenantID]
}

func (r *MemoryRepository) FindByIdempotency(tenantID, key string) (decision.EvaluationResult, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	result, ok := r.byIdempotency[compoundKey(tenantID, key)]
	return result, ok
}

func (r *MemoryRepository) Append(result decision.EvaluationResult) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := compoundKey(result.Decision.TenantID, result.Decision.IdempotencyKey)
	if existing, exists := r.byIdempotency[key]; exists {
		if existing.Decision.Audit.RecordHash != result.Decision.Audit.RecordHash {
			return errors.New("idempotency key was reused with a different decision")
		}
		return nil
	}
	previous := r.previousByTenant[result.Decision.TenantID]
	if previous == "" {
		previous = "GENESIS"
	}
	if result.Decision.Audit.PreviousRecordHash != previous {
		return errors.New("invalid tenant hash-chain predecessor")
	}
	r.byIdempotency[key] = result
	r.byDecisionUID[result.Decision.DecisionUID] = result.Decision
	r.previousByTenant[result.Decision.TenantID] = result.Decision.Audit.RecordHash
	return nil
}

func (r *MemoryRepository) Get(decisionUID string) (decision.DecisionRecord, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	result, ok := r.byDecisionUID[decisionUID]
	return result, ok
}

func compoundKey(tenantID, idempotencyKey string) string {
	return tenantID + ":" + idempotencyKey
}
