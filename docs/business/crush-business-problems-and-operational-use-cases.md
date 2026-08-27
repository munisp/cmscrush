# CRUSH Healthcare Integrity Platform: Business Problems and Operational Use Cases

**Author:** Manus AI  
**Status:** Working business and control baseline  
**Source anchor:** CMS Request for Information, CMS-6098-NC, 91 FR 9803 (February 27, 2026) [1]

## Executive framing

CRUSH should be operated as a **program-integrity decision-support system**, not as an autonomous enforcement engine. Its business value is the earlier assembly of reliable, reviewable evidence for CMS, Medicare Advantage organizations, State Medicaid agencies, CHIP programs, Exchange operators, contractors, investigators, and legitimate providers. The platform should reduce preventable payment leakage and shorten investigation time while preserving notice, human judgment, appeal rights, privacy boundaries, and the ability to explain why a claim, provider, enrollment record, or network relationship was surfaced.

The RFI describes a broad toolbox that includes enrollment screening, payment suspension, claims analytics, pre-payment edits, medical-record review, investigations, audits, revocations, re-enrollment bans, law-enforcement coordination, and provider education [1]. CRUSH therefore needs several operating modes: **preventive screening**, **pre-payment review**, **post-payment investigation**, **provider-network intelligence**, **evidence retrieval**, and **human case management**. A model score is only one input; a hard rule, source lineage, temporal validity, and reviewer decision must remain distinguishable.

## Priority business problems

| Priority | Business problem | Operational consequence | Primary CRUSH response | Human decision point |
|---|---|---|---|---|
| 1 | A provider or supplier revoked from Traditional Medicare migrates billing activity to Medicare Advantage through a preclusion-list gap. The RFI explicitly identifies this migration risk [1]. | A bad actor can continue billing while plan-level teams lack a consolidated, point-in-time view of revocation, ownership, identifiers, and claims. | Build a time-aware provider identity and ownership graph; join enrollment, revocation, preclusion, MA participation, and claims streams; produce a migration evidence pack. | CMS or plan investigator validates identity, effective dates, jurisdiction, and legal authority before any suspension or other action.
| 2 | Shell-laboratory or genetic-testing referral rings create implausible combinations of ordering providers, beneficiaries, laboratories, marketers, and billing locations. The RFI identifies laboratory testing, including genetic and molecular diagnostic testing, as a focus area [1]. | High-volume claims can look individually plausible while the network structure and rapid growth indicate coordinated abuse. | Use Flink CEP for temporal motifs, Delta graph snapshots for lineage, GNN advisory scores, peer residuals, and geospatial impossibility checks. | Clinical/program-integrity reviewer examines medical necessity, ordering relationship, beneficiary contact evidence, and source documents; no model score alone blocks payment.
| 3 | Non-participating DMEPOS suppliers bill MA plans for items not rendered or not needed. The RFI calls out this risk [1]. | Plans may pay for unfulfilled delivery, duplicate equipment, unsupported utilization, or supplier networks that move across jurisdictions. | Reconcile order, delivery, beneficiary, supplier, location, device/service code, replacement interval, and complaint signals; detect delivery-before-order and distance anomalies. | Medical-review or plan reviewer confirms delivery and need, requests records, and determines any payment or recovery action.
| 4 | Fraudulent or abusive Traditional Medicare Part A/B claims are submitted at scale, including identity misuse, impossible travel, duplicate billing, upcoding indicators, and rapid provider outliers. | Static edits miss cross-claim and cross-provider patterns; post-payment recovery becomes the default. | Stream claims through Bronze/Silver/Gold layers; combine deterministic edits, peer residuals, graph components, geospatial features, and evidence retrieval. | Claims examiner chooses pay, pend, documentation request, referral, or recommendation under applicable policy.
| 5 | AI-assisted coding or billing systems introduce systematic coding drift, unsupported acuity, or opaque hospital-billing anomalies. The RFI asks about AI in Medicare Advantage coding oversight and hospital billing [1]. | Errors can scale faster than manual sampling and may be difficult to attribute to a person or vendor. | Track coding/model versions, code-family distributions, note-to-code evidence spans, provider peer baselines, and change-point alerts. | Clinical coder or auditor verifies the record and coding standard; model output is advisory and version-pinned.
| 6 | Beneficiary solicitation, misleading marketing, or unauthorized contact drives inappropriate enrollment or service utilization. | Vulnerable beneficiaries may be harmed even when claims appear payable. | Link contact events, agent/broker identity, consent, complaint, enrollment, language/accessibility, and plan events with strict purpose controls. | Consumer-protection or enrollment specialist determines outreach, correction, referral, or enforcement path.
| 7 | Improper dual enrollment occurs across Medicaid/CHIP and subsidized Exchange coverage, or household/identity records are reused across programs. | Public programs may pay overlapping premiums or services, and consumers may receive confusing or incorrect coverage. | Use privacy-preserving identity resolution, temporal eligibility windows, household/address/device/contact signals, and cross-program reconciliation. | Eligibility and program staff resolve conflicts and contact the consumer when required; CRUSH never changes eligibility autonomously.
| 8 | Provider ownership, control, residency, or identity information is incomplete or inconsistent across enrollment sources. The RFI asks about enhanced identity proofing and ownership requirements [1]. | Screening may miss hidden control relationships or create false positives for legitimate cross-border structures. | Maintain an ownership/control graph, provenance for each assertion, document OCR/NLP references, sanctions/exclusion joins, and an abstain path for unresolved identity. | Enrollment specialist verifies documents and legal identity; ambiguous matches remain “needs review,” not adverse action.
| 9 | Cybersecurity compromise or data exfiltration at a provider, supplier, plan, or contractor changes the fraud risk environment. | Stolen credentials, altered files, or compromised billing infrastructure can produce a burst of suspicious claims. | Correlate Wazuh security alerts, OpenCTI/STIX intelligence, claim bursts, account changes, and data-access telemetry without placing PHI in security logs. | Security and program-integrity teams jointly validate the incident and scope; payment or enrollment actions require the applicable authorized actor.

## Operational use-case cards

### 1. Revoked-provider migration to MA

**Trigger.** A revocation, deactivation, ownership change, identifier change, or preclusion update arrives from an authoritative enrollment source. A claim or network relationship then appears in an MA context.

**Workflow.** The ingestion service records the source event and schema version in Bronze. Silver normalizes provider identifiers, effective dates, organization ownership, MA affiliation, and claim relationships. A point-in-time graph snapshot links the provider to billing entities, locations, owners, plans, and prior enforcement events. CEP detects post-revocation billing or rapid identifier migration. Gold produces an assessment with the exact effective-date logic, source events, graph snapshot, and an evidence pack.

**Business output.** An investigator receives a migration timeline, affected claims, linked entities, confidence bounds, and a recommended review queue. The output must not state that a provider is fraudulent; it states that the record presents a reviewable program-integrity concern.

**Controls.** The medallion pipeline must reject or quarantine records with missing effective dates, conflicting provider identifiers, or stale source versions. If the graph service is unavailable, deterministic enrollment rules and a documentation-request or review route remain available; the system must not manufacture a graph score.

### 2. Shell-lab and referral-ring investigation

**Trigger.** A laboratory or genetic-testing provider shows rapid claim growth, unusual ordering concentration, high beneficiary turnover, or a dense provider-beneficiary-marketer component.

**Workflow.** Flink evaluates bounded temporal patterns such as many beneficiaries linked to a common ordering-provider cluster, repeated service codes over a short interval, or claims following a beneficiary-contact event. Spark/Delta creates reproducible peer cohorts and point-in-time graph snapshots. Ray/PyTorch Geometric produces an advisory component score with uncertainty and model version. Clinical NLP and document retrieval identify supporting or contradictory evidence.

**Business output.** The reviewer sees a ring map, temporal sequence, peer comparison, affected claim lines, order-to-result evidence, and an explicit list of missing evidence. The system can prioritize a medical-record request or investigation; it cannot deny a claim because a graph component is large.

**Controls.** Feature freshness, source coverage, and abstention state are displayed beside the score. A schema drift in ordering-provider, service-code, or beneficiary-token fields routes the affected partition to quarantine rather than silently changing the graph.

### 3. DMEPOS non-rendered or unnecessary service review

**Trigger.** An order, delivery, claim, replacement, complaint, or supplier participation event creates a potential mismatch.

**Workflow.** The system reconciles order date, delivery date, item/service code, beneficiary, supplier, delivery location, replacement interval, and claim status. It applies deterministic checks such as delivery-before-order, duplicate replacement, impossible travel, and missing delivery confirmation. Network and geospatial models provide prioritization only.

**Business output.** A plan reviewer receives a supplier-by-beneficiary timeline and a document request packet with the exact missing proof, rather than a generic fraud label.

**Controls.** The platform distinguishes “not rendered,” “not medically supported,” “duplicate,” and “data incomplete.” These are different operational queues and require different evidence and reviewer expertise.

### 4. Claims integrity at scale

**Trigger.** A claim or claim line enters the stream, or a later event changes its context.

**Workflow.** Bronze preserves the source envelope and hash. Silver validates required fields and normalizes claim lines. Gold materializes point-in-time features and model assessments. The Go decision service evaluates hard rules first, fuses advisory model results, creates a hash-linked decision record, and creates a human-gated case task whenever the route is not ordinary payment.

**Business output.** At 50,000 claims per second, the business objective is not merely throughput. It is **bounded, explainable triage**: deterministic edits remain available during model degradation, every decision is idempotent, and delayed evidence can be replayed from Delta Change Data Feed.

**Controls.** Backpressure, partition quarantine, freshness limits, circuit breakers, and a rules-only degraded mode must be observable. The degraded path must be explicitly governed by policy and must never convert model unavailability into an adverse action.

### 5. AI coding and hospital-billing oversight

**Trigger.** A coding-model version changes, a provider’s code distribution shifts, documentation support falls, or a peer residual exceeds a governed threshold.

**Workflow.** The platform records model, prompt-policy, feature-set, and coding-standard versions. Clinical NLP extracts evidence spans; the claim feature table stores only references and hashes needed for reproducibility. An auditor compares the coded claim to the source record and peer cohort.

**Business output.** The primary queue is “audit or education,” not automatic denial. Repeated, confirmed issues can support authorized program-integrity processes after review.

## Control alignment

| Business control objective | Data/control implementation | Failure behavior | Evidence retained |
|---|---|---|---|
| Know which source and version produced a fact | Bronze source envelope, source event ID, schema version, ingestion run ID, payload hash | Quarantine records that cannot be attributed or validated | Source event IDs, hashes, lineage metadata |
| Prevent temporal leakage | Point-in-time Gold features and graph snapshots; `as_of_time` and validity windows | Abstain or route to review when timestamps conflict | Snapshot ID, feature-set version, effective dates |
| Detect schema drift before it changes decisions | Contract tests, required-field/null-rate checks, type/range checks, unexpected-column policy, drift metrics by source and partition | Stop affected promotion from Bronze to Silver/Gold; preserve raw evidence for replay | Schema fingerprint, validation report, quarantine reason |
| Keep AI advisory and reviewable | Model assessment includes model/version, uncertainty, reason codes, evidence refs, abstention | Missing/stale model returns no score; decision service uses governed degraded route | Assessment record and case task |
| Fail safely under Ray/GNN degradation | Go client timeout plus circuit breaker; no retry storm; rules-first evaluator | Fast failure, bounded fallback, alert, and human queue for policy-defined cases | Breaker state, latency/error counters, trace ID, degraded flag |
| Protect due process | Temporal case workflow, reviewer-required evidence pack, statutory clock, append-only decision hash chain | No autonomous denial, revocation, suspension, or enrollment change | Reviewer identity, rationale, timestamps, evidence hashes |
| Protect privacy | Tokenized beneficiary identifiers, purpose-of-use checks, no PHI in telemetry, separate evidence access grants | Reject cross-tenant or unauthorized access | Tenant, purpose, access decision, audit trace |

## Immediate implementation priorities

1. **Make schema quality a first-class business signal.** Add a data-quality assessment to every Gold assessment, including freshness, completeness, drift status, and quarantine ancestry. A high model score backed by low-quality data must be displayed as low-confidence or abstained.

2. **Separate technical failure from program-integrity suspicion.** The Go decision record should distinguish `MODEL_UNAVAILABLE`, `MODEL_TIMEOUT`, `SCHEMA_QUARANTINED`, and `RULE_HARD_STOP`. A downstream outage is not evidence of fraud and must not be reported as such.

3. **Govern degraded routing explicitly.** The current evaluator marks model unavailability as degraded and deliberately routes it to rules-only `PAY`, with no model contributors and no review task; this is covered by an executable test. That is a valid availability-preserving policy only when the deterministic edits, data-quality gates, and program policy permit it. Production configuration should make the fallback policy explicit and support a stricter non-adverse documentation/review route for programs that do not approve rules-only payment. In all modes, a model outage must never become an adverse action.

4. **Instrument business outcomes, not only infrastructure.** Measure avoided pay-and-chase, time from signal to evidence pack, reviewer overturn rate, false-positive rate by provider type and program, data-quarantine volume, model-abstention rate, and queue aging. Throughput and p99 latency remain necessary service-level measures, but they are not proof of program-integrity value.

5. **Start with two production pilots.** The first pilot should cover revoked-provider migration across Traditional Medicare and MA. The second should cover laboratory/genetic-testing referral-ring triage. Both have clear source events, graph relationships, temporal patterns, investigator workflows, and measurable review outcomes.

## References

[1]: https://www.govinfo.gov/content/pkg/FR-2026-02-27/pdf/2026-03968.pdf "CMS, Request for Information (RFI) Related to Comprehensive Regulations To Uncover Suspicious Healthcare (CRUSH), 91 FR 9803, February 27, 2026"

[2]: https://oig.hhs.gov/reports/all/2026/total-medicare-part-b-spending-on-lab-tests-rose-in-2024-driven-by-increased-spending-on-genetic-tests/ "HHS OIG, Total Medicare Part B Spending on Lab Tests Rose in 2024, Driven by Increased Spending on Genetic Tests"
