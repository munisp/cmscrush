package e2e

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/munisp/cmscrush/services/decision-service/internal/decision"
	"github.com/munisp/cmscrush/services/decision-service/internal/httpapi"
	"github.com/munisp/cmscrush/services/decision-service/internal/store"
)

func TestHardStopDecisionFlowIsHumanGatedAndIdempotent(t *testing.T) {
	server := httptest.NewServer(httpapi.New(decision.NewEvaluator(), store.NewMemoryRepository()).Routes())
	defer server.Close()

	payload := input(time.Now().UTC())
	payload.Screening.ExcludedAsOfService = true
	body := marshal(payload)
	request := func() *http.Request {
		req, err := http.NewRequest(http.MethodPost, server.URL+"/v1/claims/decisions", bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-CRUSH-Tenant-ID", "demo-tenant")
		req.Header.Set("Purpose-Of-Use", "PAYMENT_INTEGRITY")
		req.Header.Set("Idempotency-Key", "idem-e2e-123456789012345")
		req.Header.Set("X-Request-ID", "trace-e2e-001")
		return req
	}

	response, err := http.DefaultClient.Do(request())
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201, got %d", response.StatusCode)
	}
	var result decision.EvaluationResult
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		t.Fatal(err)
	}
	if result.Decision.Action != "SUSPEND_RECOMMEND" || !result.Decision.HumanReview.Required || result.CaseTask == nil {
		t.Fatalf("hard stop must produce a human-gated suspension recommendation: %+v", result.Decision)
	}
	if result.Ledger.Kind != "EXPOSURE_ESTIMATE" || result.Ledger.Authorization.HumanApproved {
		t.Fatalf("decision must record exposure only, not a human-approved reserve: %+v", result.Ledger)
	}

	read, err := http.NewRequest(http.MethodGet, server.URL+"/v1/decisions/"+result.Decision.DecisionUID, nil)
	if err != nil {
		t.Fatal(err)
	}
	read.Header.Set("X-CRUSH-Tenant-ID", "demo-tenant")
	read.Header.Set("Purpose-Of-Use", "PAYMENT_INTEGRITY")
	fetched, err := http.DefaultClient.Do(read)
	if err != nil {
		t.Fatal(err)
	}
	defer fetched.Body.Close()
	if fetched.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", fetched.StatusCode)
	}

	replay, err := http.DefaultClient.Do(request())
	if err != nil {
		t.Fatal(err)
	}
	defer replay.Body.Close()
	if replay.StatusCode != http.StatusOK {
		t.Fatalf("expected idempotent 200 replay, got %d", replay.StatusCode)
	}
	var replayed decision.EvaluationResult
	if err := json.NewDecoder(replay.Body).Decode(&replayed); err != nil {
		t.Fatal(err)
	}
	if replayed.Decision.Audit.RecordHash != result.Decision.Audit.RecordHash {
		t.Fatal("idempotent replay changed the immutable decision hash")
	}
}

func input(now time.Time) decision.DecisionInput {
	return decision.DecisionInput{
		Claim: decision.ClaimEvent{
			TenantID: "demo-tenant", ClaimControlNumber: "claim-e2e-1", ReceivedAt: now,
			BillingProvider: decision.ProviderRef{SubmittedNPI: "1234567890"},
			Beneficiary:     decision.BeneficiaryRef{SubmittedIDHash: "sha256:" + strings.Repeat("b", 64)},
			Lines:           []decision.ClaimLine{{LineNumber: 1, ServiceDate: "2026-08-27", BilledAmount: 125.75}},
			TotalBilled:     125.75,
		},
		Features:       decision.Features{VectorHash: "sha256:" + strings.Repeat("c", 64), DefinitionsVersion: "fs-1.0.0"},
		ModelAvailable: true,
	}
}

func marshal(value any) []byte {
	data, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return data
}
