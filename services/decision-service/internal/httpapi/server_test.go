package httpapi

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/munisp/cmscrush/services/decision-service/internal/decision"
	"github.com/munisp/cmscrush/services/decision-service/internal/store"
)

func TestDecisionEndpointRequiresGatewayContext(t *testing.T) {
	server := New(decision.NewEvaluator(), store.NewMemoryRepository()).Routes()
	request := httptest.NewRequest(http.MethodPost, "/v1/claims/decisions", bytes.NewReader(mustJSON(inputFor("demo-tenant"))))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()

	server.ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("expected missing headers to be rejected, got %d", response.Code)
	}
}

func TestDecisionEndpointRejectsTenantSpoofing(t *testing.T) {
	server := New(decision.NewEvaluator(), store.NewMemoryRepository()).Routes()
	request := httptest.NewRequest(http.MethodPost, "/v1/claims/decisions", bytes.NewReader(mustJSON(inputFor("other-tenant"))))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-CRUSH-Tenant-ID", "demo-tenant")
	request.Header.Set("Purpose-Of-Use", "PAYMENT_INTEGRITY")
	request.Header.Set("Idempotency-Key", "idem-1234567890123456")
	response := httptest.NewRecorder()

	server.ServeHTTP(response, request)
	if response.Code != http.StatusForbidden {
		t.Fatalf("expected tenant mismatch to be rejected, got %d", response.Code)
	}
}

func inputFor(tenantID string) decision.DecisionInput {
	now := time.Now().UTC()
	return decision.DecisionInput{
		Claim: decision.ClaimEvent{
			TenantID: tenantID, ClaimControlNumber: "claim-123", ReceivedAt: now,
			BillingProvider: decision.ProviderRef{SubmittedNPI: "1234567890"},
			Beneficiary:     decision.BeneficiaryRef{SubmittedIDHash: "sha256:" + strings.Repeat("b", 64)},
			Lines:           []decision.ClaimLine{{LineNumber: 1, ServiceDate: "2026-08-21", BilledAmount: 100}},
			TotalBilled:     100,
		},
		Features:       decision.Features{VectorHash: "sha256:" + strings.Repeat("c", 64), DefinitionsVersion: "fs-1.0.0"},
		ModelAvailable: true,
	}
}

func mustJSON(value any) []byte {
	encoded, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return encoded
}
