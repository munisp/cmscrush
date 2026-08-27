package gnn

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// Assessment is deliberately advisory. It cannot encode a denial, suspension,
// payment, settlement, revocation, or enrollment action.
type Assessment struct {
	AssessmentID     string  `json:"assessment_id"`
	TenantID         string  `json:"tenant_id"`
	Jurisdiction     string  `json:"jurisdiction"`
	GraphSnapshotID  string  `json:"graph_snapshot_id"`
	Score            float64 `json:"score"`
	LowerBound       float64 `json:"lower_bound"`
	UpperBound       float64 `json:"upper_bound"`
	Abstained        bool    `json:"abstained"`
	AbstainReason    *string `json:"abstain_reason"`
	ModelName        string  `json:"-"`
	ModelVersion     string  `json:"-"`
	Calibration      string  `json:"-"`
	FeatureSet       string  `json:"-"`
	FreshnessSeconds int64   `json:"-"`
	Evidence         []struct {
		Ref            string   `json:"ref"`
		Kind           string   `json:"kind"`
		SourceEventIDs []string `json:"source_event_ids"`
	} `json:"evidence"`
}

type Request struct {
	TenantID        string   `json:"tenant_id"`
	Jurisdiction    string   `json:"jurisdiction"`
	SubjectType     string   `json:"subject_type"`
	SubjectID       string   `json:"subject_id"`
	AsOfTime        string   `json:"as_of_time"`
	FeatureSet      string   `json:"feature_set_version"`
	GraphSnapshotID string   `json:"graph_snapshot_id"`
	FeatureRefs     []string `json:"feature_refs"`
}

type Client struct {
	baseURL string
	http    *http.Client
	breaker *CircuitBreaker
}

func NewClient(baseURL string, timeout time.Duration) (*Client, error) {
	if !strings.HasPrefix(baseURL, "https://") && !strings.HasPrefix(baseURL, "http://") {
		return nil, errors.New("gnn endpoint must use http or https")
	}
	if timeout <= 0 || timeout > 2*time.Second {
		return nil, errors.New("gnn timeout must be greater than zero and no more than two seconds")
	}
	return &Client{
		baseURL: strings.TrimRight(baseURL, "/"),
		http:    &http.Client{Timeout: timeout},
		breaker: NewCircuitBreaker(3, 5*time.Second),
	}, nil
}

func (c *Client) Assess(ctx context.Context, req Request, gatewayTenant, traceID string) (Assessment, error) {
	if req.TenantID == "" || gatewayTenant == "" || req.TenantID != gatewayTenant {
		return Assessment{}, errors.New("tenant context mismatch")
	}
	if req.SubjectType == "" || req.SubjectID == "" || req.GraphSnapshotID == "" || req.FeatureSet == "" {
		return Assessment{}, errors.New("incomplete gnn assessment request")
	}
	if err := c.breaker.Allow(time.Now().UTC()); err != nil {
		return Assessment{}, err
	}
	fail := func(err error) (Assessment, error) {
		c.breaker.RecordFailure(time.Now().UTC())
		return Assessment{}, err
	}
	body, err := json.Marshal(req)
	if err != nil {
		return fail(fmt.Errorf("marshal gnn request: %w", err))
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/v1/assessments", strings.NewReader(string(body)))
	if err != nil {
		return fail(fmt.Errorf("build gnn request: %w", err))
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("X-Tenant-ID", gatewayTenant)
	httpReq.Header.Set("X-Trace-ID", traceID)
	resp, err := c.http.Do(httpReq)
	if err != nil {
		return fail(fmt.Errorf("gnn assessment unavailable: %w", err))
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fail(fmt.Errorf("gnn assessment returned status %d", resp.StatusCode))
	}
	var assessment Assessment
	if err := json.NewDecoder(resp.Body).Decode(&assessment); err != nil {
		return fail(fmt.Errorf("decode gnn assessment: %w", err))
	}
	if assessment.TenantID != gatewayTenant || assessment.GraphSnapshotID != req.GraphSnapshotID {
		return fail(errors.New("gnn response context mismatch"))
	}
	if assessment.Score < 0 || assessment.Score > 1 || assessment.LowerBound < 0 || assessment.UpperBound > 1 {
		return fail(errors.New("gnn response score out of range"))
	}
	c.breaker.RecordSuccess()
	return assessment, nil
}
