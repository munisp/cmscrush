-- CRUSH operational state. Immutable DecisionRecord payloads are published to Kafka/WORM
-- storage; Postgres stores projections, configuration, and transactional outbox state only.
CREATE TABLE tenant_config (
  tenant_id text PRIMARY KEY CHECK (tenant_id ~ '^[a-z][a-z0-9-]{2,62}$'),
  policy_overlay_version text NOT NULL,
  allowed_models jsonb NOT NULL DEFAULT '[]'::jsonb,
  retention_policy jsonb NOT NULL DEFAULT '{}'::jsonb,
  mojaloop_settlement_enabled boolean NOT NULL DEFAULT false,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE reason_code_registry (
  code text PRIMARY KEY,
  plain_language text NOT NULL,
  policy_basis text NOT NULL,
  severity text NOT NULL CHECK (severity IN ('HARD_STOP', 'SCORE', 'REVIEW_REQUIRED', 'LEAD_ONLY')),
  effective_from date NOT NULL,
  effective_to date,
  CHECK (effective_to IS NULL OR effective_to >= effective_from)
);

CREATE TABLE case_projection (
  case_uid text PRIMARY KEY,
  tenant_id text NOT NULL REFERENCES tenant_config(tenant_id),
  decision_uid text NOT NULL UNIQUE,
  current_state text NOT NULL CHECK (current_state IN ('OPEN', 'UNDER_REVIEW', 'DOCS_REQUESTED', 'ADVERSE_ACTION_RECOMMENDED', 'DENIED', 'SUSPENDED', 'REFERRED', 'CLOSED', 'OVERTURNED')),
  requested_action text NOT NULL CHECK (requested_action IN ('PEND_REVIEW', 'PREPAY_DOC_REQUEST', 'DENY_RECOMMEND', 'SUSPEND_RECOMMEND', 'REFER')),
  review_deadline_at timestamptz NOT NULL,
  final_actor_id text,
  final_rationale text,
  temporal_workflow_id text NOT NULL UNIQUE,
  created_at timestamptz NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now(),
  CHECK ((current_state NOT IN ('DENIED', 'SUSPENDED', 'ADVERSE_ACTION_RECOMMENDED')) OR (final_actor_id IS NOT NULL AND final_rationale IS NOT NULL))
);
CREATE INDEX case_projection_tenant_state_idx ON case_projection (tenant_id, current_state, review_deadline_at);

CREATE TABLE idempotency_record (
  tenant_id text NOT NULL REFERENCES tenant_config(tenant_id),
  idempotency_key text NOT NULL,
  decision_uid text NOT NULL UNIQUE,
  request_hash text NOT NULL CHECK (request_hash LIKE 'sha256:%'),
  response_hash text NOT NULL CHECK (response_hash LIKE 'sha256:%'),
  expires_at timestamptz NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, idempotency_key)
);

CREATE TABLE transactional_outbox (
  outbox_id uuid PRIMARY KEY,
  tenant_id text NOT NULL REFERENCES tenant_config(tenant_id),
  aggregate_type text NOT NULL CHECK (aggregate_type IN ('DECISION', 'CASE', 'LEDGER_INTENT')),
  aggregate_id text NOT NULL,
  topic text NOT NULL CHECK (topic ~ '^crush\.[a-z][a-z0-9-]{2,62}\.[a-z0-9._-]+\.v1$'),
  payload jsonb NOT NULL,
  payload_hash text NOT NULL CHECK (payload_hash LIKE 'sha256:%'),
  trace_id text NOT NULL,
  purpose_of_use text NOT NULL,
  published_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX transactional_outbox_unpublished_idx ON transactional_outbox (created_at) WHERE published_at IS NULL;

-- Initial governed reason-code registry excerpt. Codes are effective-dated rather than mutated.
INSERT INTO reason_code_registry (code, plain_language, policy_basis, severity, effective_from) VALUES
  ('RC-001-EXCLUDED-PROVIDER', 'Billing entity was excluded on the service date.', 'LEIE', 'HARD_STOP', DATE '2026-01-01'),
  ('RC-002-PRECLUDED-MA', 'Entity was precluded from MA billing on the service date.', 'Preclusion policy', 'HARD_STOP', DATE '2026-01-01'),
  ('RC-011-TIMELY-FILING', 'Claim was received beyond the configured filing deadline.', 'Filing rule', 'HARD_STOP', DATE '2026-01-01'),
  ('RC-041-DECEASED-BENE', 'Service occurred after the recorded beneficiary death date.', 'Eligibility source', 'HARD_STOP', DATE '2026-01-01'),
  ('RC-114-PEER-OUTLIER', 'Billing pattern differs materially from comparable providers.', 'Analytic model', 'SCORE', DATE '2026-01-01'),
  ('RC-135-CLINICAL-IMPLAUSIBLE', 'The claim sequence requires human review because it is clinically implausible.', 'Clinical plausibility rule', 'REVIEW_REQUIRED', DATE '2026-01-01');
