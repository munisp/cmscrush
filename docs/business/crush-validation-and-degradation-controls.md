# CRUSH Validation, Schema Drift, and GNN Degradation Controls

**Author:** Manus AI  
**Scope:** Delta Lake medallion ingestion and Go decision-service integration with Ray/PyTorch Geometric  
**Status:** Implemented baseline with production-hardening actions

## Control objective

A healthcare-integrity platform must distinguish four different conditions: **bad source data**, **changed source schema**, **missing or stale analytical evidence**, and **actual program-integrity suspicion**. Treating these as one “risk” signal creates false positives, makes investigations harder to defend, and can cause technical outages to affect beneficiary or provider outcomes.

The control chain is therefore:

> **Validate → classify → quarantine or promote → calculate point-in-time features → obtain advisory assessment → apply deterministic rules first → create a reviewable decision record.**

## Current implementation review

| Area | Current repository behavior | Assessment |
|---|---|---|
| Tenant isolation | Lakehouse paths include tenant, zone, dataset, and partition; Go GNN requests require matching gateway and request tenant IDs. | Implemented and tested at the unit-contract level. |
| Raw-event provenance | Bronze metadata includes event/source identifiers, raw reference, schema version, event hash, and Delta path. | Implemented; raw payload remains outside model telemetry. |
| Schema contract | Python now computes an order-independent schema fingerprint, checks required/type fields, classifies unexpected fields as `DRIFT_DETECTED`, and malformed fields as `QUARANTINED`. | Implemented in deterministic contract layer. |
| Delta contract alignment | Bronze SQL now stores `schema_fingerprint`, `validation_status`, `validation_errors`, and `unexpected_fields`. | Implemented in schema artifact; cluster migration must be applied before use. |
| Geospatial quality | Haversine oracle validates WGS84 bounds and marks low-precision locations; production Sedona path is documented. | Implemented as reference behavior; production quality thresholds need operating ownership. |
| Point-in-time analytics | Gold tables carry `as_of_time`, feature-set version, snapshot IDs, and model version fields. | Schema supports reproducibility; freshness gates still need runtime metrics and enforcement. |
| GNN timeout | Go HTTP client enforces a maximum two-second client timeout. | Implemented. |
| GNN circuit breaker | Three consecutive downstream failures open the breaker for five seconds; one half-open probe is allowed; success closes it; failed probe reopens it. | Implemented and unit-tested. |
| Retry behavior | The client does not automatically retry, preventing retry storms during Ray overload. | Implemented; upstream queue/backpressure policy must remain bounded. |
| Human-in-the-loop | Non-pay routes create a case task; adverse recommendations are explicitly recommendations and the assessment type is advisory. | Implemented in evaluator/workflow boundary. |
| Degraded route | Model outage sets `Degraded=true`, removes unavailable model contributors, and currently routes to rules-only `PAY`; this behavior is tested. | Deliberate policy choice that must be approved per program and exposed in deployment configuration. |

## End-to-end validation contract

### Bronze: preserve and classify

Bronze is the immutable source-envelope boundary. Every event must retain tenant, program, source, event identity, ingestion time, schema version, and a cryptographic reference to the raw artifact. Contract validation must run before any event is eligible for Silver promotion.

A **valid** record has all required fields and no unknown fields. A **drift-detected** record has valid required fields but one or more unexpected fields. Drift is not automatically fraud and should not be dropped: it is retained with the fingerprint and routed to a compatibility/quarantine workflow. A **quarantined** record has missing or invalid required fields, invalid types, invalid timestamps, or a failed privacy/purpose check.

At peak load, validation must be implemented as a bounded, allocation-conscious operation in the streaming path. Heavy document parsing, graph construction, and model inference must not block the Bronze acknowledgment path. The raw artifact and validation metadata are sufficient for later replay after the contract is approved.

### Silver: normalize without losing lineage

Silver promotion should require a compatible schema fingerprint, successful type/range checks, valid temporal relationships, and a resolvable source-event lineage. Claim lines with invalid dates, impossible amount ranges, missing claim identity, or ambiguous provider/beneficiary resolution should remain queryable as quality exceptions rather than being silently coerced.

For healthcare use cases, the quality reason matters operationally. `MISSING_DELIVERY_PROOF`, `INVALID_SERVICE_DATE`, `AMBIGUOUS_PROVIDER_MATCH`, and `STALE_ENROLLMENT_SOURCE` should feed different queues and should not collapse into a generic “fraud” label.

### Gold: point-in-time and purpose-limited

Gold features and assessments must include `as_of_time`, source snapshot, feature-set version, model version, freshness, missingness flags, uncertainty bounds, and evidence references. A feature vector built after a revocation effective date must not be used to explain a decision made before that date. A model assessment without a valid graph snapshot or feature-set version must abstain.

The Gold layer should expose a composite quality state:

| Quality state | Meaning | Decision implication |
|---|---|---|
| `READY` | Required data, lineage, freshness, and schema checks passed. | Advisory models may contribute. |
| `LIMITED` | Some non-critical features are missing or low precision. | Models may contribute only with widened uncertainty and visible missingness. |
| `ABSTAIN` | Critical evidence, snapshot, or contract is unavailable. | Do not use an AI score; use deterministic policy and explicit degraded route. |
| `QUARANTINED` | Source or transformation failed a blocking quality rule. | Do not promote; replay after remediation. |

## Ray GNN remediation playbook

| Signal | Immediate action | Claim/business behavior | Operator action | Exit criterion |
|---|---|---|---|---|
| Single timeout or 5xx | Record failure and latency; no retry from the decision client. | Continue only under the governed degraded policy; do not label as fraud. | Inspect Ray queue depth, worker saturation, and Flink lag. | Successful downstream response or controlled capacity adjustment. |
| Three consecutive downstream failures | Open breaker for five seconds; fail fast with `ErrCircuitOpen`. | Avoid additional Ray load; deterministic rules remain authoritative. | Page the service owner only if the error/latency budget is exceeded; route affected cases according to policy. | One successful half-open probe. |
| Failed half-open probe | Reopen breaker and reset cooldown window. | Continue bounded degraded behavior; do not repeatedly probe. | Check model server health, network, credentials, and feature payload compatibility. | Successful probe plus stable latency. |
| Schema mismatch or invalid response | Count as downstream failure; reject response context/score. | Never use the response score; use deterministic/degraded route. | Compare model contract and Gold feature-set version; roll back or quarantine incompatible model. | Contract test passes and version is approved. |
| Stale Gold features | Do not send stale features for scoring. | Use explicit stale-data queue or governed rules-only path. | Investigate Flink checkpoint lag, Delta commit lag, and source freshness. | Freshness returns within policy threshold. |
| Drift spike by source/partition | Stop promotion of affected slice, preserve raw data, alert data owner. | Only unaffected partitions continue; affected claims are not silently paid or denied because of drift. | Approve schema compatibility, backfill, or rollback. | Fingerprint is approved and replay succeeds. |

## Use-case alignment

For **revoked-provider migration**, a stale enrollment source or failed provider-identifier resolution is more important than a high model score: the reviewer needs effective dates and a trustworthy identity chain. For **shell-lab referral rings**, schema drift in ordering provider, service code, or beneficiary-token fields can change graph topology, so the affected partition must abstain rather than produce a misleading component score. For **DMEPOS**, delivery and order-date validation remains useful during GNN outage because the deterministic checks directly address the business question. For **AI coding oversight**, model-version mismatch must trigger audit/rollback, not a claim-level adverse action.

## Required production hardening

The next implementation increment should externalize the degraded route as an approved program policy, emit breaker and validation metrics without PHI, add runtime freshness and schema-fingerprint dashboards, and add integration tests that exercise a Ray timeout through the Go client into the final decision record. The current unit tests establish the core primitives; they do not yet prove Kubernetes, Flink, Delta transaction-log, or Ray cluster behavior at 50,000 claims per second.
