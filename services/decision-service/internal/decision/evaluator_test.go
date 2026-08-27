package decision

import (
	"strings"
	"testing"
	"time"
)

func TestExcludedClaimCreatesRecommendationAndCaseTask(t *testing.T) {
	now := time.Date(2026, 8, 27, 10, 0, 0, 0, time.UTC)
	evaluator := NewEvaluator()
	evaluator.Clock = func() time.Time { return now }
	input := validInput(now)
	input.Screening.ExcludedAsOfService = true

	result, err := evaluator.Evaluate(input, "GENESIS")
	if err != nil {
		t.Fatalf("evaluate returned an error: %v", err)
	}
	if result.Decision.Action != "SUSPEND_RECOMMEND" {
		t.Fatalf("expected recommendation, got %s", result.Decision.Action)
	}
	if !result.Decision.HumanReview.Required || result.CaseTask == nil {
		t.Fatal("adverse recommendation must create a human-review case task")
	}
	if !contains(result.Decision.ReasonCodes, "RC-001-EXCLUDED-PROVIDER") {
		t.Fatalf("expected exclusion reason, got %#v", result.Decision.ReasonCodes)
	}
	if result.Decision.Audit.RecordHash == "" || result.Decision.Audit.PreviousRecordHash != "GENESIS" {
		t.Fatal("decision hash chain was not populated")
	}
}

func TestModelOutageEmitsRulesOnlyDegradedPayment(t *testing.T) {
	now := time.Date(2026, 8, 27, 10, 0, 0, 0, time.UTC)
	evaluator := NewEvaluator()
	evaluator.Clock = func() time.Time { return now }
	input := validInput(now)
	input.ModelAvailable = false

	result, err := evaluator.Evaluate(input, "sha256:"+strings.Repeat("a", 64))
	if err != nil {
		t.Fatalf("evaluate returned an error: %v", err)
	}
	if result.Decision.Action != "PAY" || !result.Decision.Degraded {
		t.Fatalf("expected degraded rules-only payment, got action=%s degraded=%t", result.Decision.Action, result.Decision.Degraded)
	}
	if result.CaseTask != nil {
		t.Fatal("rules-only pay must not create a review task")
	}
	if len(result.Decision.Models) != 0 {
		t.Fatal("unavailable models must not be represented as contributors")
	}
}

func TestUseCaseSignalCreatesHumanReviewCase(t *testing.T) {
	now := time.Date(2026, 8, 27, 10, 0, 0, 0, time.UTC)
	evaluator := NewEvaluator()
	evaluator.Clock = func() time.Time { return now }
	input := validInput(now)
	input.Features.UseCaseSignals = []string{"PROVIDER_BILLED_AFTER_REVOCATION_IN_MA"}
	result, err := evaluator.Evaluate(input, "GENESIS")
	if err != nil {
		t.Fatal(err)
	}
	if result.Decision.Action != "PEND_REVIEW" || result.CaseTask == nil {
		t.Fatalf("use-case signal must create human review, got action=%s case=%v", result.Decision.Action, result.CaseTask)
	}
	if !contains(result.Decision.ReasonCodes, "RC-UC-PROVIDER_BILLED_AFTER_REVOCATION_IN_MA") {
		t.Fatalf("use-case reason missing: %#v", result.Decision.ReasonCodes)
	}
}

func TestDeliveryBeforeOrderRequiresHumanReview(t *testing.T) {
	now := time.Date(2026, 8, 27, 10, 0, 0, 0, time.UTC)
	evaluator := NewEvaluator()
	evaluator.Clock = func() time.Time { return now }
	input := validInput(now)
	input.Claim.Lines[0].OrderDate = ptr("2026-08-20")
	input.Claim.Lines[0].DeliveryDate = ptr("2026-08-19")

	result, err := evaluator.Evaluate(input, "GENESIS")
	if err != nil {
		t.Fatalf("evaluate returned an error: %v", err)
	}
	if result.Decision.Action != "PEND_REVIEW" || result.CaseTask == nil {
		t.Fatalf("clinical implausibility must pend for review, got %s", result.Decision.Action)
	}
	if !contains(result.Decision.ReasonCodes, "RC-135-CLINICAL-IMPLAUSIBLE") {
		t.Fatal("missing clinical-implausibility reason")
	}
}

func validInput(now time.Time) DecisionInput {
	return DecisionInput{
		Claim: ClaimEvent{
			TenantID:           "demo-tenant",
			ClaimControlNumber: "claim-123",
			ReceivedAt:         now.Add(-time.Hour),
			BillingProvider:    ProviderRef{SubmittedNPI: "1234567890"},
			Beneficiary:        BeneficiaryRef{SubmittedIDHash: "sha256:" + strings.Repeat("b", 64)},
			Lines:              []ClaimLine{{LineNumber: 1, ServiceDate: "2026-08-21", BilledAmount: 100}},
			TotalBilled:        100,
		},
		Features:       Features{VectorHash: "sha256:" + strings.Repeat("c", 64), DefinitionsVersion: "fs-1.0.0", PeerOutlierScore: 0.2},
		ModelScores:    []ModelScore{{ModelID: "fwa-supervised", Version: "1.0.0", Score: 0.2, LatencyMS: 8}},
		ModelAvailable: true,
		PurposeOfUse:   "PAYMENT_INTEGRITY",
		IdempotencyKey: "idem-1234567890123456",
		TraceID:        "trace-123",
	}
}

func ptr(value string) *string { return &value }
