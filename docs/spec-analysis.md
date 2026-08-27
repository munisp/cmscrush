# CRUSH Specification Extraction and Implementation Analysis

**Input:** `CRUSH_Platform_Spec_Package_v1`, version 1.0, dated 2026-08-24.  
**Implementation target:** initial repository foundation, selected as a P0/P5 executable vertical slice.

## Extracted Architecture

The supplied documents define a Kubernetes-native, event-driven, multi-tenant healthcare-integrity platform composed of nine planes: ingest; streaming; lakehouse; entity resolution and graph; features; model mesh; decisioning; case/action; and governance. The platform must favor evidence over opaque scores, retain all decision inputs/versions, isolate PHI by tenant, federate rather than pool cross-tenant signals, and permit models to triage but never to make final adverse actions.

The source package recommends a phased implementation. P0 establishes the streaming/lakehouse spine; P1 identity/graph; P2 detection; P3 coding oversight; P4 forensics; P5 decisioning/case management; and P6 multi-tenant federation. The business specification identifies §G coding oversight as the commercial wedge but also requires governance and a minimal case/evidence capability. This foundation therefore constructs the shared contract and control plane needed by P0 and P5, while preserving an unambiguous path to the remaining phases.

## Material Reconciliation

| Source specification default | User-mandated selection | Resolution |
|---|---|---|
| Apache Iceberg table format | Delta Lake + Parquet | Delta Lake is the canonical table format in this profile. The storage plan retains snapshots, schema enforcement, data-quality gates, versioned features, and reproducibility. |
| Kafka or Redpanda; Flink | Kafka + Flink + Fluvio | Kafka/Flink is the canonical regulated decision stream. Fluvio is limited to non-PHI edge telemetry and developer-local WASM transforms, avoiding dual sources of truth. |
| Temporal or Camunda | Temporal workflow engine + Dapr | Temporal owns due-process workflows/timers. Dapr supplies portable service invocation, pub/sub, state/cache, configuration, retries, and telemetry; it does not own case state. |
| Generic payment execution out of scope | Mojaloop + TigerBeetle | TigerBeetle is limited to debit/credit control-ledger intents. Mojaloop is an optional external payment-network adapter disabled by default. Neither converts an analytic output into payment/settlement. |
| Security stack: OpenBao, SPIFFE, OPA, Falco, Sigstore | APISIX, open-appsec, Keycloak, OpenCTI, Wazuh, OpenSearch, Kubecost | The requested stack is integrated at ingress, identity, threat/WAF, runtime security, search, and cost-allocation boundaries. Existing controls remain follow-on production prerequisites. |
| Conventional geospatial feature concepts | Sedona + Spark + Flink | SedonaSpark materializes batch spatial analytics; SedonaFlink produces real-time advisory geo signals. The Python reference implementation supplies an online/batch parity oracle. |
| Query engines include Trino/DuckDB | Rust + DataFusion | DataFusion delivers a bounded, read-only, Arrow-native query service over approved Parquet/Delta-derived extracts. It does not write Delta transaction logs. |
| ML plane with many models | Python + Ray | Ray hosts isolated batch training and model-serving workloads. The decision service only consumes versioned gateway scores; no model weight/vendor is hard-coded. |

## Requirement Traceability

| Requirement IDs | Requirement | Foundation implementation | Verification |
|---|---|---|---|
| TS-DATA-001…004 | Canonical, tenant-scoped, idempotent events with preserved quality flags. | Versioned JSON Schemas, tenant paths, stable hashes, idempotency headers, `ingest_quality` contract field. | Repository verification plus Python tests. |
| FR-141…145 | Versioned stream aggregation, CEP, checkpointing, and backpressure signalling. | Kafka/Dapr component profile; Flink/Sedona job declares RocksDB, object-store checkpoints, and exactly-once semantics. | Manifest review; functional Flink cluster test deferred. |
| FR-181…185 | Feature definition/version, online/offline parity, leakage prevention. | Feature hash/definition references are required in decisions; Python deterministic geo calculation acts as parity oracle. | Go and Python tests. |
| FR-201…205, FR-330…334 | Stable model gateway contract, circuit breaks, model versions, abstention. | Go input captures model contribution/availability; outage produces a rules-only `degraded=true` decision. | Go degraded-mode test. |
| FR-351…360; BR-001…002 | Deterministic gates precede scores; reason codes; recommendations not final actions. | Go decision evaluator implements exclusion, preclusion, death-date, timely-filing, and order/delivery checks before risk fusion. | Go hard-stop and clinical-review tests. |
| FR-271…275; BR-100…104 | Case state transitions and statutory timers with authenticated human approval. | TypeScript Temporal workflow and pure state policy. Final denied/suspended transitions reject absent actor/rationale/timestamp. | TypeScript tests. |
| FR-291…297; BR-101, 107 | Reproducible decision/audit and purpose-of-use controls. | Decision record includes rule/model/feature versions, audit hash chain, trace ID, purpose, tenant, and idempotency key. | Go tests and static contract check. |
| FR-314; BR-081 | Declarative tenant provisioning and isolation. | Helm tenant namespace, labels, tenant storage paths, and default-deny NetworkPolicies. | Static manifest check; controller implementation deferred. |
| NFR-009, 012, 013, 018 | Reproducibility, isolation, immutable auditability, end-to-end traceability. | Hash-linked record chain, versioned input references, tenant path/API checks, trace IDs, security telemetry isolation. | Component and static tests. |

## Safety-Critical Decisions

| Control | Implementation consequence |
|---|---|
| No automatic adverse action | The action vocabulary permits only `DENY_RECOMMEND`/`SUSPEND_RECOMMEND`, never final denial or suspension. Every non-payment action generates a case task. |
| Model failure is fail-safe | Model unavailability cannot silently block a payment cycle. The Go service emits a rules-only decision tagged `degraded=true`, enabling retrospective review. |
| Evidence must be reproducible | Decision records retain exact feature vector hash, definition version, model versions, ruleset version, reason codes, trace ID, purpose, and preceding audit hash. |
| Ledger is distinct from payment | Ledger posting intent carries debit/credit accounts and idempotency but has no payment instruction. Reserve holds need a human approval reference; Mojaloop settlement starts disabled. |
| Geo analytics are advisory | Geo calculations include precision/quality flags and can enter a risk model only as explained evidence; they do not act alone. |
| Security telemetry is data-isolated | Wazuh/OpenCTI/open-appsec telemetry can inform platform security operations, but no raw claim/document/beneficiary/provider data or automatic claim action crosses the boundary. |

## Deferred Work and Exit Conditions

The repository intentionally does not claim a production-ready regulated platform. Production exit requires a schema registry and X12/NCPDP normalizers; durable Kafka/Postgres/WORM audit implementations; actual Delta, Spark, Flink, Sedona, and Ray integration tests with synthetic but representative data; an inference gateway and governed models; Temporal activities and evidence-pack generator; per-tenant KMS, object-lock buckets, graph/feature systems, and identity policies; vulnerability/image/license scanning, SBOM/signing, and deployment admission controls; accessibility/reviewer interface work; and data-use/legal authorization for authoritative verification sources.

The next demonstrable functional milestone is an all-synthetic, one-tenant end-to-end run: submit a canonical claim to the API, write a durable decision/audit event, materialize the matching Delta/Parquet record, create a Temporal task for a recommendation, record a human transition, and query aggregate, tenant-scoped geo/risk information through DataFusion. No real PHI or real payment credentials are required for that milestone.
