package decision

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"
)

const genesisHash = "GENESIS"

type Evaluator struct {
	Clock          func() time.Time
	RulesetVersion string
}

func NewEvaluator() Evaluator {
	return Evaluator{
		Clock:          func() time.Time { return time.Now().UTC() },
		RulesetVersion: "edits-2026.08.1",
	}
}

func (e Evaluator) Evaluate(input DecisionInput, previousHash string) (EvaluationResult, error) {
	if err := validateInput(input); err != nil {
		return EvaluationResult{}, err
	}
	if previousHash == "" {
		previousHash = genesisHash
	}

	now := e.Clock().UTC()
	reasons, ruleIDs, hardStop, err := e.evaluateRules(input, now)
	if err != nil {
		return EvaluationResult{}, err
	}

	degraded := !input.ModelAvailable
	risk, usedModels := fuseRisk(input.Features, input.ModelScores, input.ModelAvailable)
	action := route(hardStop, reasons, risk, degraded)
	if len(reasons) == 0 {
		reasons = []string{"RC-114-PEER-OUTLIER"}
		ruleIDs = []string{"EDIT-NO-HARD-STOP"}
	}

	decision := DecisionRecord{
		DecisionUID:    stableUID("dec", input.Claim.TenantID, input.IdempotencyKey),
		TenantID:       input.Claim.TenantID,
		IdempotencyKey: input.IdempotencyKey,
		DecidedAt:      now,
		Action:         action,
		ReasonCodes:    reasons,
		Risk:           risk,
		Models:         usedModels,
		Degraded:       degraded,
		SchemaVersion:  "1.0.0",
	}
	decision.Subject.ClaimControlNumber = input.Claim.ClaimControlNumber
	decision.Subject.ProviderUID = input.Claim.BillingProvider.ResolvedUID
	decision.Subject.BeneficiaryUID = input.Claim.Beneficiary.ResolvedUID
	decision.Inputs.FeatureVectorHash = input.Features.VectorHash
	decision.Inputs.FeatureDefinitionsVersion = input.Features.DefinitionsVersion
	decision.Rules.RulesetVersion = e.RulesetVersion
	decision.Rules.Fired = ruleIDs
	decision.HumanReview.Required = action != "PAY"
	decision.Audit.PreviousRecordHash = previousHash
	decision.Audit.TraceID = input.TraceID
	decision.Audit.PurposeOfUse = input.PurposeOfUse
	decision.Audit.RecordHash = decisionHash(decision)

	ledger := buildLedgerIntent(decision, input.Claim.TotalBilled, now)
	result := EvaluationResult{Decision: decision, Ledger: ledger}
	if decision.HumanReview.Required {
		result.CaseTask = buildCaseTask(decision, now)
	}
	return result, nil
}

func validateInput(input DecisionInput) error {
	if strings.TrimSpace(input.Claim.TenantID) == "" {
		return errors.New("claim.tenant_id is required")
	}
	if strings.TrimSpace(input.Claim.ClaimControlNumber) == "" {
		return errors.New("claim.claim_control_number is required")
	}
	if strings.TrimSpace(input.PurposeOfUse) == "" {
		return errors.New("purpose_of_use is required")
	}
	if len(input.IdempotencyKey) < 16 {
		return errors.New("idempotency_key must be at least 16 characters")
	}
	if strings.TrimSpace(input.Features.VectorHash) == "" || strings.TrimSpace(input.Features.DefinitionsVersion) == "" {
		return errors.New("immutable feature vector hash and definition version are required")
	}
	return nil
}

func (e Evaluator) evaluateRules(input DecisionInput, now time.Time) ([]string, []string, bool, error) {
	reasons := make([]string, 0, 4)
	rules := make([]string, 0, 4)
	hardStop := false

	if input.Screening.ExcludedAsOfService {
		reasons = append(reasons, "RC-001-EXCLUDED-PROVIDER")
		rules = append(rules, "EDIT-LEIE-EXCLUDED-AS-OF-SERVICE")
		hardStop = true
	}
	if input.Screening.PrecludedAsOfService {
		reasons = append(reasons, "RC-002-PRECLUDED-MA")
		rules = append(rules, "EDIT-PRECLUSION-AS-OF-SERVICE")
		hardStop = true
	}

	for _, line := range input.Claim.Lines {
		serviceDate, err := parseDate(line.ServiceDate)
		if err != nil {
			return nil, nil, false, fmt.Errorf("invalid service_date on line %d: %w", line.LineNumber, err)
		}
		if input.Claim.Beneficiary.DateOfDeath != nil {
			deathDate, err := parseDate(*input.Claim.Beneficiary.DateOfDeath)
			if err != nil {
				return nil, nil, false, fmt.Errorf("invalid beneficiary date_of_death: %w", err)
			}
			if serviceDate.After(deathDate) {
				reasons = append(reasons, "RC-041-DECEASED-BENE")
				rules = append(rules, "EDIT-SERVICE-AFTER-DEATH")
				hardStop = true
			}
		}
		if line.OrderDate != nil && line.DeliveryDate != nil {
			orderDate, orderErr := parseDate(*line.OrderDate)
			deliveryDate, deliveryErr := parseDate(*line.DeliveryDate)
			if orderErr != nil || deliveryErr != nil {
				return nil, nil, false, fmt.Errorf("invalid order or delivery date on line %d", line.LineNumber)
			}
			if deliveryDate.Before(orderDate) {
				reasons = append(reasons, "RC-135-CLINICAL-IMPLAUSIBLE")
				rules = append(rules, "EDIT-DELIVERY-PRECEDES-ORDER")
			}
		}
	}

	if now.Sub(input.Claim.ReceivedAt) > 365*24*time.Hour {
		reasons = append(reasons, "RC-011-TIMELY-FILING")
		rules = append(rules, "EDIT-TIMELY-FILING-365D")
		hardStop = true
	}

	sort.Strings(reasons)
	sort.Strings(rules)
	return unique(reasons), unique(rules), hardStop, nil
}

func fuseRisk(features Features, scores []ModelScore, modelAvailable bool) (Risk, []ModelScore) {
	if !modelAvailable {
		return Risk{Score: 0, ConformalLower: 0, ConformalUpper: 1, Tier: "LOW"}, []ModelScore{}
	}
	weightedSum := 0.0
	weight := 0.0
	used := make([]ModelScore, 0, len(scores))
	for _, score := range scores {
		if score.Abstained {
			continue
		}
		value := clamp(score.Score)
		weightedSum += value
		weight++
		used = append(used, score)
	}
	if weight == 0 {
		return Risk{Score: 0, ConformalLower: 0, ConformalUpper: 1, Tier: "LOW"}, used
	}
	base := weightedSum / weight
	featureSignal := 0.20*clamp(features.PeerOutlierScore) + 0.20*clamp(features.GraphRiskScore) + 0.10*clamp(features.GeoImprobabilityScore)
	score := clamp(0.65*base + featureSignal)
	interval := 0.08
	if len(used) == 1 {
		interval = 0.18
	}
	return Risk{Score: score, ConformalLower: clamp(score - interval), ConformalUpper: clamp(score + interval), Tier: tier(score)}, used
}

func route(hardStop bool, reasons []string, risk Risk, degraded bool) string {
	if hardStop {
		return "SUSPEND_RECOMMEND"
	}
	if degraded {
		return "PAY"
	}
	if contains(reasons, "RC-135-CLINICAL-IMPLAUSIBLE") || risk.Tier == "CRITICAL" {
		return "PEND_REVIEW"
	}
	switch risk.Tier {
	case "HIGH":
		return "PEND_REVIEW"
	case "MED":
		return "PREPAY_DOC_REQUEST"
	default:
		return "PAY"
	}
}

func buildCaseTask(decision DecisionRecord, now time.Time) *CaseTask {
	task := &CaseTask{
		CaseUID:         stableUID("case", decision.TenantID, decision.DecisionUID),
		DecisionUID:     decision.DecisionUID,
		TenantID:        decision.TenantID,
		RequestedAction: decision.Action,
		ReasonCodes:     decision.ReasonCodes,
		CreatedAt:       now,
		EvidenceRefs:    []string{decision.Audit.RecordHash, decision.Inputs.FeatureVectorHash},
	}
	task.StatutoryClock.Kind = "REVIEW"
	task.StatutoryClock.DeadlineAt = now.Add(72 * time.Hour)
	return task
}

func buildLedgerIntent(decision DecisionRecord, totalBilled float64, now time.Time) LedgerPostingIntent {
	intent := LedgerPostingIntent{
		IntentUID:      stableUID("lpi", decision.TenantID, decision.DecisionUID),
		TenantID:       decision.TenantID,
		DecisionUID:    decision.DecisionUID,
		Kind:           "EXPOSURE_ESTIMATE",
		Amount:         int64(math.Round(totalBilled * 100)),
		Currency:       "USD",
		DebitAccount:   "crush:exposure:estimated",
		CreditAccount:  "crush:exposure:offset",
		IdempotencyKey: decision.IdempotencyKey,
		CreatedAt:      now,
	}
	intent.Authorization.Source = "DECISION_SERVICE"
	intent.Authorization.HumanApproved = false
	return intent
}

func decisionHash(decision DecisionRecord) string {
	decision.Audit.RecordHash = ""
	canonical, _ := json.Marshal(decision)
	sum := sha256.Sum256(canonical)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func stableUID(prefix, tenantID, material string) string {
	sum := sha256.Sum256([]byte(strings.Join([]string{prefix, tenantID, material}, ":")))
	return prefix + "_" + hex.EncodeToString(sum[:])[:24]
}

func parseDate(value string) (time.Time, error) {
	return time.Parse("2006-01-02", value)
}

func tier(score float64) string {
	switch {
	case score >= 0.90:
		return "CRITICAL"
	case score >= 0.70:
		return "HIGH"
	case score >= 0.40:
		return "MED"
	default:
		return "LOW"
	}
}

func clamp(value float64) float64 {
	return math.Max(0, math.Min(1, value))
}

func contains(values []string, expected string) bool {
	for _, value := range values {
		if value == expected {
			return true
		}
	}
	return false
}

func unique(values []string) []string {
	if len(values) == 0 {
		return values
	}
	result := []string{values[0]}
	for _, value := range values[1:] {
		if value != result[len(result)-1] {
			result = append(result, value)
		}
	}
	return result
}
