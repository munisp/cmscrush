package httpapi

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/munisp/cmscrush/services/decision-service/internal/decision"
	"github.com/munisp/cmscrush/services/decision-service/internal/store"
)

type Server struct {
	evaluator  decision.Evaluator
	repository store.Repository
}

func New(evaluator decision.Evaluator, repository store.Repository) *Server {
	return &Server{evaluator: evaluator, repository: repository}
}

func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.health)
	mux.HandleFunc("POST /v1/claims/decisions", s.createDecision)
	mux.HandleFunc("GET /v1/decisions/{uid}", s.getDecision)
	return withRequestID(mux)
}

func (s *Server) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "service": "decision-service"})
}

func (s *Server) createDecision(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Content-Type") != "application/json" {
		problem(w, http.StatusUnsupportedMediaType, "unsupported-media-type", "Content-Type must be application/json")
		return
	}
	tenantID := strings.TrimSpace(r.Header.Get("X-CRUSH-Tenant-ID"))
	purpose := strings.TrimSpace(r.Header.Get("Purpose-Of-Use"))
	idempotencyKey := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	if tenantID == "" || purpose == "" || idempotencyKey == "" {
		problem(w, http.StatusBadRequest, "missing-required-header", "X-CRUSH-Tenant-ID, Purpose-Of-Use, and Idempotency-Key are required")
		return
	}

	var input decision.DecisionInput
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 2<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil {
		problem(w, http.StatusBadRequest, "invalid-request", "request body does not match the decision contract")
		return
	}
	if input.Claim.TenantID != tenantID {
		problem(w, http.StatusForbidden, "tenant-mismatch", "tenant_id must match the authenticated tenant context")
		return
	}
	input.PurposeOfUse = purpose
	input.IdempotencyKey = idempotencyKey
	input.TraceID = requestID(r)

	if existing, ok := s.repository.FindByIdempotency(tenantID, idempotencyKey); ok {
		writeJSON(w, http.StatusOK, existing)
		return
	}
	result, err := s.evaluator.Evaluate(input, s.repository.PreviousHash(tenantID))
	if err != nil {
		problem(w, http.StatusUnprocessableEntity, "decision-rejected", publicError(err))
		return
	}
	if err := s.repository.Append(result); err != nil {
		problem(w, http.StatusConflict, "idempotency-conflict", "idempotency key conflicts with an existing decision")
		return
	}
	writeJSON(w, http.StatusCreated, result)
}

func (s *Server) getDecision(w http.ResponseWriter, r *http.Request) {
	uid := r.PathValue("uid")
	result, ok := s.repository.Get(uid)
	if !ok {
		problem(w, http.StatusNotFound, "not-found", "decision does not exist")
		return
	}
	if tenantID := strings.TrimSpace(r.Header.Get("X-CRUSH-Tenant-ID")); tenantID == "" || tenantID != result.TenantID {
		problem(w, http.StatusForbidden, "tenant-mismatch", "decision is not available in the authenticated tenant context")
		return
	}
	if strings.TrimSpace(r.Header.Get("Purpose-Of-Use")) == "" {
		problem(w, http.StatusBadRequest, "missing-required-header", "Purpose-Of-Use is required")
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func withRequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		traceID := r.Header.Get("X-Request-ID")
		if traceID == "" {
			traceID = "trace-unset"
		}
		w.Header().Set("X-Request-ID", traceID)
		next.ServeHTTP(w, r)
	})
}

func requestID(r *http.Request) string {
	if traceID := strings.TrimSpace(r.Header.Get("X-Request-ID")); traceID != "" {
		return traceID
	}
	return "trace-unset"
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func problem(w http.ResponseWriter, status int, problemType, detail string) {
	writeJSON(w, status, map[string]any{
		"type":   "https://crush.platform/problems/" + problemType,
		"title":  http.StatusText(status),
		"status": status,
		"detail": detail,
	})
}

func publicError(err error) string {
	if errors.Is(err, errors.New("")) {
		return "decision input is invalid"
	}
	return err.Error()
}
