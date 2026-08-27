package decision

import "time"

type Point struct {
	Latitude   float64 `json:"latitude"`
	Longitude  float64 `json:"longitude"`
	PrecisionM float64 `json:"precision_m"`
}

type ProviderRef struct {
	SubmittedNPI string  `json:"submitted_npi"`
	ResolvedUID  *string `json:"resolved_uid,omitempty"`
	Location     *Point  `json:"location,omitempty"`
}

type BeneficiaryRef struct {
	SubmittedIDHash string  `json:"submitted_id_hash"`
	ResolvedUID     *string `json:"resolved_uid,omitempty"`
	DateOfDeath     *string `json:"date_of_death,omitempty"`
}

type ClaimLine struct {
	LineNumber      int     `json:"line_number"`
	ServiceDate     string  `json:"service_date"`
	HCPCSCPT        *string `json:"hcpcs_cpt,omitempty"`
	Units           float64 `json:"units"`
	BilledAmount    float64 `json:"billed_amount"`
	OrderDate       *string `json:"order_date,omitempty"`
	DeliveryDate    *string `json:"delivery_date,omitempty"`
	ServiceLocation *Point  `json:"service_location,omitempty"`
}

type Period struct {
	Start string  `json:"start"`
	End   *string `json:"end,omitempty"`
}

type ClaimEvent struct {
	EventID            string            `json:"event_id"`
	TenantID           string            `json:"tenant_id"`
	Program            string            `json:"program"`
	SourceSystem       string            `json:"source_system"`
	SourceMessageID    string            `json:"source_message_id"`
	ClaimControlNumber string            `json:"claim_control_number"`
	ClaimType          string            `json:"claim_type"`
	ReceivedAt         time.Time         `json:"received_at"`
	BillingProvider    ProviderRef       `json:"billing_provider"`
	RenderingProvider  *ProviderRef      `json:"rendering_provider,omitempty"`
	Beneficiary        BeneficiaryRef    `json:"beneficiary"`
	ServicePeriod      Period            `json:"service_period"`
	Lines              []ClaimLine       `json:"lines"`
	TotalBilled        float64           `json:"total_billed"`
	RawRef             string            `json:"raw_ref"`
	IngestQuality      map[string]string `json:"ingest_quality"`
	SchemaVersion      string            `json:"schema_version"`
}

type Features struct {
	VectorHash             string   `json:"vector_hash"`
	DefinitionsVersion     string   `json:"definitions_version"`
	PeerOutlierScore       float64  `json:"peer_outlier_score"`
	GraphRiskScore         float64  `json:"graph_risk_score"`
	SharedBankAccountCount int      `json:"shared_bank_account_count"`
	GeoImprobabilityScore  float64  `json:"geo_improbability_score"`
	FreshnessSeconds       int      `json:"freshness_seconds"`
	UseCaseSignals         []string `json:"use_case_signals,omitempty"`
}

type Screening struct {
	ExcludedAsOfService  bool `json:"excluded_as_of_service"`
	PrecludedAsOfService bool `json:"precluded_as_of_service"`
}

type ModelScore struct {
	ModelID   string  `json:"model_id"`
	Version   string  `json:"version"`
	Score     float64 `json:"score"`
	Abstained bool    `json:"abstained"`
	LatencyMS int     `json:"latency_ms"`
}

type Risk struct {
	Score          float64 `json:"score"`
	ConformalLower float64 `json:"conformal_lower"`
	ConformalUpper float64 `json:"conformal_upper"`
	Tier           string  `json:"tier"`
}

type DecisionInput struct {
	Claim          ClaimEvent   `json:"claim"`
	Features       Features     `json:"features"`
	Screening      Screening    `json:"screening"`
	ModelScores    []ModelScore `json:"model_scores"`
	ModelAvailable bool         `json:"model_available"`
	PurposeOfUse   string       `json:"purpose_of_use"`
	IdempotencyKey string       `json:"idempotency_key"`
	TraceID        string       `json:"trace_id"`
}

type DecisionRecord struct {
	DecisionUID    string    `json:"decision_uid"`
	TenantID       string    `json:"tenant_id"`
	IdempotencyKey string    `json:"idempotency_key"`
	DecidedAt      time.Time `json:"decided_at"`
	Subject        struct {
		ClaimControlNumber string  `json:"claim_control_number"`
		ProviderUID        *string `json:"provider_uid,omitempty"`
		BeneficiaryUID     *string `json:"beneficiary_uid,omitempty"`
	} `json:"subject"`
	Action      string   `json:"action"`
	ReasonCodes []string `json:"reason_codes"`
	Risk        Risk     `json:"risk"`
	Inputs      struct {
		FeatureVectorHash         string `json:"feature_vector_hash"`
		FeatureDefinitionsVersion string `json:"feature_definitions_version"`
	} `json:"inputs"`
	Models []ModelScore `json:"models"`
	Rules  struct {
		RulesetVersion string   `json:"ruleset_version"`
		Fired          []string `json:"fired"`
	} `json:"rules"`
	Degraded    bool `json:"degraded"`
	HumanReview struct {
		Required   bool       `json:"required"`
		ReviewerID *string    `json:"reviewer_id,omitempty"`
		DecidedAt  *time.Time `json:"decided_at,omitempty"`
	} `json:"human_review"`
	Audit struct {
		RecordHash         string `json:"record_hash"`
		PreviousRecordHash string `json:"previous_record_hash"`
		TraceID            string `json:"trace_id"`
		PurposeOfUse       string `json:"purpose_of_use"`
	} `json:"audit"`
	SchemaVersion string `json:"schema_version"`
}

type CaseTask struct {
	CaseUID         string    `json:"case_uid"`
	DecisionUID     string    `json:"decision_uid"`
	TenantID        string    `json:"tenant_id"`
	RequestedAction string    `json:"requested_action"`
	ReasonCodes     []string  `json:"reason_codes"`
	CreatedAt       time.Time `json:"created_at"`
	StatutoryClock  struct {
		DeadlineAt time.Time `json:"deadline_at"`
		Kind       string    `json:"kind"`
	} `json:"statutory_clock"`
	EvidenceRefs []string `json:"evidence_refs"`
}

type LedgerPostingIntent struct {
	IntentUID      string    `json:"intent_uid"`
	TenantID       string    `json:"tenant_id"`
	DecisionUID    string    `json:"decision_uid"`
	Kind           string    `json:"kind"`
	Amount         int64     `json:"amount"`
	Currency       string    `json:"currency"`
	DebitAccount   string    `json:"debit_account"`
	CreditAccount  string    `json:"credit_account"`
	IdempotencyKey string    `json:"idempotency_key"`
	CreatedAt      time.Time `json:"created_at"`
	Authorization  struct {
		Source        string  `json:"source"`
		HumanApproved bool    `json:"human_approved"`
		ApproverID    *string `json:"approver_id,omitempty"`
	} `json:"authorization"`
}

type EvaluationResult struct {
	Decision DecisionRecord      `json:"decision"`
	CaseTask *CaseTask           `json:"case_task,omitempty"`
	Ledger   LedgerPostingIntent `json:"ledger_posting_intent"`
}
