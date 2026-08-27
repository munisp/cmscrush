# CRUSH Top-Five Use-Case Implementation, Revoked-Provider Pilot, and Ray GNN Scaling Plan

**Author:** Manus AI  
**Status:** Pilot-ready implementation plan  
**Scope:** Five priority healthcare integrity workflows and high-volume claims-integrity architecture

## 1. Implementation scope

The first implementation wave covers the five highest-priority workflows already selected for CRUSH:

| Use case | Implemented signal family | Primary action | No-autonomous-adverse-action boundary |
|---|---|---|---|
| Revoked-provider migration | `PROVIDER_BILLED_AFTER_REVOCATION_IN_MA` | Provider migration review | A reviewer verifies identity, effective date, program, and authority before any payment suspension, revocation, or other action. |
| Laboratory/genetic-testing referral ring | `HIGH_CONCENTRATION_LAB_REFERRALS` | Network and clinical evidence review | Graph concentration is triage evidence, not proof of fraud or lack of medical necessity. |
| DMEPOS integrity | `DELIVERY_BEFORE_ORDER`, `DELIVERY_NOT_CONFIRMED` | Documentation or medical review | The reviewer distinguishes non-rendered, unnecessary, duplicate, and incomplete-data cases. |
| Claims integrity | `DUPLICATE_CLAIM_CANDIDATE`, `IMPOSSIBLE_TRAVEL` | Claims examiner queue | Deterministic exceptions and source evidence remain visible; model scores do not directly deny claims. |
| AI coding oversight | `MODEL_CHANGE_ACUITY_SHIFT` | Coding audit or education queue | Coding drift triggers audit and version review, not an automatic adverse outcome. |

The implementation now includes a Python signal engine, Go rule-first routing and audit integration, TypeScript human-queue mapping, and Rust read-only Gold dataset allowlisting. The five components are intentionally thin: they create stable, explainable signals that can be replaced by Flink/Spark/Ray production jobs without changing the human-review contract.

## 2. Revoked-provider migration pilot

### Pilot objective

Determine whether CRUSH can identify and assemble review-ready evidence when a provider or supplier revoked from Traditional Medicare subsequently appears in Medicare Advantage or Part D billing activity, while controlling false positives caused by identifier reuse, effective-date errors, legitimate ownership changes, and incomplete source synchronization. The RFI specifically describes providers and suppliers shifting billing operations to MA when they are not included on the preclusion list [1].

The pilot is a **shadow-to-assisted-review** program. It does not change enrollment, payment, preclusion, revocation, or suspension status automatically. Existing authorized processes remain the system of record.

### Pilot population and boundaries

The pilot should use one controlled tenant or program cohort, a bounded set of jurisdictions, and an agreed historical replay window followed by live shadow traffic. The preferred cohort includes providers with authoritative revocation or deactivation events and corresponding MA/Part D claim or enrollment activity. The initial production path should exclude unresolved identity matches, cross-border ownership structures, and records without authoritative effective dates until their review procedures are approved.

### Twelve-week timeline

| Week | Phase | Activities | Exit gate |
|---|---|---|---|
| 1 | Mobilize and govern | Name executive sponsor, CMS/plan program owner, data steward, clinical/program-integrity lead, privacy officer, security lead, and platform SRE. Approve purpose of use, reviewer authority, data-retention rules, and non-adverse-action boundary. | Signed pilot charter and RACI. |
| 2 | Source and identity inventory | Catalog revocation, enrollment, preclusion, MA/Part D affiliation, ownership, provider directory, claims, and audit sources. Record source authority, update cadence, effective-date semantics, identifiers, and data-quality owner. | Source register and data-sharing approvals. |
| 3 | Contract and lineage certification | Configure Bronze schema fingerprints, required-field checks, quarantine rules, source-event hashes, and tenant partitions. Reconcile provider/NPI/UID crosswalks and define unresolved-match states. | 99%+ of pilot source records classified as valid, drift, or quarantine with no silent drops; proposed target. |
| 4 | Historical replay baseline | Replay an agreed historical window through Bronze, Silver, Gold, graph snapshot, and Rust analytics datasets. Produce a labeled candidate set using existing investigator dispositions where available. | Reproducible replay manifest and baseline precision/queue-volume report. |
| 5 | Temporal and graph rules | Enable post-revocation billing detection, identifier migration timelines, ownership/control links, location changes, and plan affiliation transitions. Add Flink CEP patterns for event ordering and bounded windows. | Rule outputs match independently reviewed sample cases. |
| 6 | Evidence-pack workflow | Connect Gold assessments to TypeScript case tasks. Generate timelines containing effective dates, source events, affected claims, graph snapshot IDs, confidence/abstention states, and document references. | Reviewers can open, annotate, request documents, and record rationale. |
| 7 | Shadow mode | Run CRUSH alongside the current process without changing payment or enrollment outcomes. Measure candidate volume, latency, data-quality failures, duplicate cases, and reviewer usefulness. | No critical privacy, tenant-isolation, lineage, or due-process defects. |
| 8 | Calibration and threshold review | Review precision by provider type, jurisdiction, program, identity-match confidence, and source freshness. Adjust thresholds only through versioned policy change with an audit record. | Approved thresholds and rollback version. |
| 9 | Assisted review | Route approved candidates to investigators with explicit “review required” status. Permit documentation requests and referrals; preserve existing authorized enforcement workflow. | At least 90% of eligible pilot candidates receive a complete evidence pack within the service objective; proposed target. |
| 10 | Resilience and degradation test | Inject source lag, schema drift, Delta commit delay, Ray timeout, Ray 5xx, circuit opening, and stale graph snapshot conditions. Confirm deterministic controls and human queues behave as designed. | No outage causes an autonomous adverse action; all injected events are observable and recoverable. |
| 11 | Outcome validation | Compare CRUSH-assisted cases with investigator dispositions, confirmed findings, overturned candidates, time-to-resolution, and avoided duplicate work. Review subgroup fairness and false-positive patterns. | Outcome report accepted by program owner and privacy/compliance reviewers. |
| 12 | Go/no-go decision | Decide whether to expand the cohort, revise the signal, retain shadow mode, or stop. Document residual risks, required controls, operating cost, and next use case. | Signed decision with versioned configuration and rollback plan. |

### Roles and operating responsibilities

| Role | Responsibility |
|---|---|
| Program-integrity owner | Defines the operational question, approves thresholds, owns reviewer outcomes, and authorizes expansion. |
| Data steward | Certifies source authority, schema contracts, effective-date semantics, quality exceptions, and replay. |
| Investigator or enrollment reviewer | Verifies identity, dates, affiliations, and evidence; records rationale and disposition. |
| Clinical reviewer | Evaluates medical or service context when needed; separates clinical uncertainty from fraud suspicion. |
| Privacy/compliance officer | Approves purpose of use, minimization, retention, access, and due-process controls. |
| SRE/platform owner | Operates Flink, Delta, Ray, Go, Rust, and TypeScript components; owns SLOs, breaker policy, and rollback. |
| Security operations | Correlates provider infrastructure incidents without placing PHI in telemetry; coordinates OpenCTI/Wazuh response. |

### Success metrics

Targets below are **pilot acceptance targets**, not measured results. Baselines must be established during Weeks 1–4.

| Metric | Definition | Target for pilot expansion |
|---|---|---|
| Candidate precision | Reviewer-confirmed migration concern divided by reviewed candidates, measured separately by provider type and program. | Improve over existing baseline by an agreed relative margin; do not use a universal target until baseline labels are audited. |
| Evidence-pack completeness | Candidates with provider identity, revocation effective date, MA/Part D relationship, affected claims, source references, graph snapshot, and uncertainty state. | ≥95% of eligible candidates. |
| Time to evidence pack | Event availability to review-ready evidence pack. | p95 ≤15 minutes in shadow/assisted mode; validate against operational need. |
| Review cycle time | Evidence-pack creation to investigator disposition. | Reduce median cycle time by ≥30% versus baseline. |
| False-positive rate | Reviewed candidates not substantiated after applying the pilot’s agreed disposition taxonomy. | No increase versus baseline; analyze by subgroup and source quality. |
| Overturn or correction rate | Cases whose initial CRUSH-supported disposition is corrected after additional evidence or appeal. | Track and set a guardrail before expansion; any material increase pauses rollout. |
| Duplicate-work rate | Candidates already represented by an open case or existing investigation. | <5% after deduplication tuning. |
| Data-quality containment | Drift/quarantine events that are contained without silent promotion. | 100% of blocking drift events contained and replayable. |
| Due-process compliance | Non-routine routes with reviewer identity, rationale, timestamp, and required evidence. | 100%; any violation is a release blocker. |
| Privacy/tenant isolation | Cross-tenant access, PHI telemetry exposure, or unauthorized evidence access. | Zero confirmed violations; immediate stop and incident review for any event. |
| Availability behavior | Ray outage or schema failure that causes an unauthorized adverse action. | Zero; immediate stop if observed. |

### Pilot go/no-go gates

Expansion requires: stable source contracts; no unresolved tenant or privacy defects; evidence-pack completeness at target; reviewer agreement that the signal improves prioritization; no prohibited autonomous action; successful replay; and an approved fallback policy for Ray/model degradation. A high candidate count alone is not a success criterion.

## 3. Ray GNN remediation scaling implications

### Capacity model

At a sustained peak of 50,000 claims per second, the architecture must not send every claim synchronously through a full-graph GNN inference path. The stream should separate three tiers:

1. **Inline deterministic tier.** Go evaluates hard edits, identity/effective-date constraints, basic quality gates, and bounded feature availability within the claim decision latency budget.
2. **Asynchronous feature and graph tier.** Flink emits keyed, windowed events; Delta/Spark materializes reproducible features and graph snapshots; Rust/DataFusion serves bounded investigative queries.
3. **Selective GNN tier.** Only claims or entities that pass governed eligibility gates—fresh features, valid snapshot, sufficient graph context, and non-abstained input—are sent to Ray. GNN results are advisory and can be delayed, abstained, or superseded by a newer version.

If `r` is the fraction of claims eligible for GNN scoring and `t` is the average inference time in seconds per request, the minimum concurrency required is approximately `50,000 × r × t`, before headroom. This is a planning equation, not a benchmark. Batch inference, micro-batching, entity-level scoring, and cache reuse reduce `r` and `t`; synchronous per-claim fan-out increases both operational risk and cost.

### Required scaling changes

| Concern | Implication at high volume | Required architecture |
|---|---|---|
| Ray overload | A breaker at the Go client protects the decision service but does not by itself protect Ray from queue growth. | Add admission control before Ray, bounded queues, per-tenant quotas, concurrency limits, micro-batching, and load-shed rules based on freshness and business priority. |
| Retry storms | Retrying 50,000-cps traffic after a downstream incident can amplify failure. | Keep the decision client retry-free; use one half-open probe and explicit recovery orchestration. Any replay must be asynchronous and rate-limited. |
| Graph snapshot consistency | Mixing graph versions produces irreproducible scores and weak evidence. | Pin every assessment to `graph_snapshot_id`, feature-set version, model version, and `as_of_time`; reject mismatches. |
| Feature freshness | A fresh claim with stale graph features may be less useful than a deterministic signal. | Carry freshness seconds and missingness flags; route stale inputs to abstention or governed rules-only behavior. |
| Hot entities | A single lab, provider, or beneficiary-linked component can create partition hotspots. | Key streams by stable entity and time bucket, use salted hot-key handling, and maintain separate aggregation paths for high-degree entities. |
| Backpressure | Kafka/Flink lag can make a model score appear valid while it is operationally stale. | Monitor consumer lag, checkpoint age, Delta commit age, Ray queue age, and decision age; make queue age a business SLO. |
| Model rollout | A new model can change scores and graph behavior at once. | Shadow models, canary tenants, calibration checks, model-card metadata, and rollback to the last approved version. |
| Evidence volume | Storing full neighborhoods for every claim is expensive and may increase privacy exposure. | Store minimal graph-subgraph references and hashes; materialize expanded evidence only for case candidates under access control. |
| Multi-tenancy | A shared Ray cluster can create noisy-neighbor and data-isolation risk. | Namespace and tenant quotas, gateway-derived tenant context, separate queues for high-sensitivity programs, and negative cross-tenant tests. |

### Remediation state machine

The operational state should be observable as a separate technical state, not confused with fraud risk:

`HEALTHY → DEGRADED → OPEN → HALF_OPEN → RECOVERED`

- **HEALTHY:** Ray responses meet latency, error, contract, and freshness objectives.
- **DEGRADED:** Error or latency budget is being consumed; new work is admitted selectively and telemetry is emitted without PHI.
- **OPEN:** The Go breaker fails fast after its configured consecutive-failure threshold. Claims follow the approved deterministic fallback; the system does not create a fraud signal from the outage.
- **HALF_OPEN:** One probe tests the downstream path after cooldown. Concurrent probes remain blocked.
- **RECOVERED:** A valid, tenant-matched, snapshot-matched response closes the breaker and normal admission resumes gradually.

For high volume, the breaker should be complemented by a **bulkhead hierarchy**: per-request timeout, per-pod concurrency limit, per-tenant queue, global Ray admission budget, and a separate asynchronous replay queue. Recovery should ramp gradually instead of releasing the entire accumulated backlog at once.

### Scaling SLOs and observability

The minimum dashboard should show claims-per-second accepted, deterministic decision latency, GNN-eligible rate, GNN request rate, Ray queue age, Ray p50/p95/p99 latency, timeout and 5xx rate, breaker state transitions, half-open success rate, feature freshness, Delta commit lag, Flink checkpoint age, Kafka lag, quarantine rate, abstention rate, case-task creation rate, and reviewer queue age. Metrics and traces must contain tenant-safe identifiers, hashes, reason codes, and trace IDs—not beneficiary names, addresses, medical record numbers, or raw claim payloads.

The most important scaling metric is not “GNN requests per second.” It is **reviewable, correctly attributed evidence packs per second under bounded freshness and zero unauthorized adverse actions**. That metric ties infrastructure to the actual healthcare business objective.

## 4. Implementation sequence after the pilot

The recommended sequence is to complete the revoked-provider migration pilot first, then reuse the same source-contract, graph-snapshot, evidence-pack, and human-review primitives for laboratory referral rings. DMEPOS follows because deterministic order/delivery controls provide a strong degraded-mode baseline. Claims integrity then broadens the stream and replay load. AI coding oversight should be introduced only after model-version, documentation-span, and auditor-disposition contracts are mature.

## References

[1]: https://www.govinfo.gov/content/pkg/FR-2026-02-27/pdf/2026-03968.pdf "CMS, Request for Information (RFI) Related to Comprehensive Regulations To Uncover Suspicious Healthcare (CRUSH), 91 FR 9803, February 27, 2026"
