# CRUSH Integrity Platform

CRUSH is an open-source, Kubernetes-native platform foundation for defensible healthcare fraud, waste, abuse, and coding-integrity operations. This repository implements a **safe executable vertical slice** from canonical claim intake to rule-first recommendation, hash-linked decision evidence, human-gated case workflow, advisory financial-control posting, and tenant-isolated Delta/Parquet analytics.

> **Not a claims adjudicator.** CRUSH does not autonomously deny, suspend, revoke, settle, or pay a healthcare claim. It emits `PAY` or a recommendation. Any action that withholds payment is gated by a recorded human adjudicator in the durable case workflow.

## Architecture

The initial foundation implements all requested technology roles while avoiding overlapping sources of truth. Kafka is the regulated canonical event backbone; Fluvio is confined to non-PHI edge/developer telemetry. Delta Lake on Parquet is the canonical lakehouse format; Spark/Sedona handles batch spatial data, Flink/Sedona handles real-time spatial enrichments, and Rust/DataFusion provides bounded, read-only analytic access. Temporal owns case workflow state and statutory timers, while Dapr provides portable microservice building blocks without duplicating workflow ownership.

| Service | Language | Current responsibility |
|---|---|---|
| `decision-service` | Go | Rule-first pre-payment recommendation, reproducibility metadata, decision hash chain, idempotency, case/ledger intents. |
| `case-workflow` | TypeScript | Temporal workflow and legal case-state policy; only authenticated human approval can finalize an adverse state. |
| `lakehouse` | Python | Delta/Parquet paths, Spark/Sedona batch jobs, Flink/Sedona stream topology, Ray training boundary, and geo-parity calculations. |
| `analytics-query` | Rust | DataFusion-backed, tenant-scoped, read-only plans over approved Parquet extracts. |

Further design, technology reconciliation, and source citations are in [the implementation profile](docs/architecture/implementation-profile.md). The source specification package remains outside this repository workspace at `../spec/`.

## Repository Structure

```text
contracts/json-schema/       Versioned ClaimEvent, DecisionRecord, CaseTask, and ledger-intent contracts
services/decision-service/   Go pre-payment decision service
services/case-workflow/      TypeScript Temporal workflow and case-state invariant tests
services/lakehouse/          Python Delta/Spark/Flink/Ray/Sedona workload foundation
services/analytics-query/    Rust Apache DataFusion analytics service
deploy/                      Helm, Dapr, APISIX, Keycloak, Temporal, Ray, Flink, and security controls
docs/                        Architecture and operations material
```

## Local Verification

The deterministic unit suites require Go 1.22+, Node 22+, Python 3.11+, Rust 1.98+ (or a current stable toolchain), and standard build tools.

```bash
# Go decisioning safety invariants
cd services/decision-service && go test ./...

# TypeScript human-approval state machine
cd services/case-workflow && npm ci && npm test && npm run build

# Python lakehouse contracts and geospatial oracle
cd services/lakehouse && pip install -e '.[test]' && PYTHONPATH=src pytest -q

# Rust DataFusion tenant/read-only query boundary
cd services/analytics-query && cargo test
```

## Deployment Prerequisites

The manifests are deployable **profiles**, not a production authorization package. A production operator must first provision tenant-specific DNS, KMS/CMEK, object-lock storage, Kafka TLS, Postgres, Redis, Temporal, TigerBeetle, Keycloak, APISIX, OpenSearch, Wazuh, OpenCTI, Dapr, open-appsec, Kubecost, and the appropriate Spark/Flink/Ray operators. Model artifacts, Delta connectors, dependencies, and container images must be pinned by immutable version/digest after security, license, and compatibility review.

| Control | Foundation mechanism | Production completion criterion |
|---|---|---|
| Tenant isolation | Tenant namespace, labels, paths, headers, and default-deny NetworkPolicies. | Automated isolation tests, per-tenant keys/buckets/databases, workload identity, and DUA offboarding record. |
| API authorization | Keycloak claim model and APISIX OIDC/header routes. | Hardware-backed privileged authentication, policy decision point, actual issuer URLs, and audited break-glass process. |
| Due process | TypeScript case policy and Temporal task queue boundary. | Deployed Temporal namespace/history retention, reviewer UI, notices, appeal/rebuttal activities, and workflow runbooks. |
| Auditability | Hash-linked decisions, immutable contract version fields, trace IDs. | WORM/object-lock log sink, periodic anchors, trace export, evidence packs, and independent integrity verification. |
| Data quality and lineage | Deterministic paths, event hash, and Delta/Spark job contracts. | Versioned datasets, Great Expectations gates, schema registry, raw-object checksum manifests, and reproducibility test. |
| Financial controls | Double-entry intent contract and disabled-by-default Mojaloop settlement adapter. | Multi-replica TigerBeetle deployment, reconciliations, authorized payment connector, and separate finance controls. |
| Security operations | open-appsec, Wazuh/OpenCTI/OpenSearch isolation configuration. | Validated WAF policies, signed images/SBOMs, runtime alerting, incident runbooks, and no-PHI telemetry proof. |

## Explicit Safety Constraints

The following constraints are tested and enforced by design.

1. The canonical decision action vocabulary contains no final `DENY` or `SUSPEND` action.
2. Any non-payment recommendation creates a case task, and a final adverse state requires an authenticated human actor, rationale, and timestamp.
3. A model outage produces a rules-only decision with `degraded=true`; it does not silently block a payment cycle.
4. Ledger posting intents never settle payments; a reserve hold requires separately recorded human approval.
5. Mojaloop settlement is disabled by default and requires external authorization evidence from the case workflow.
6. The DataFusion API permits only approved data sets, validates the tenant context, constrains query limits, and emits a fixed read-only query.
7. Direct identifiers are rejected from operational telemetry helpers; security telemetry is network-isolated from tenant PHI namespaces.

## Next Engineering Increments

The supplied specification identifies §G coding oversight as the recommended commercial wedge and P0/P1 foundation elements as prerequisites. The next implementation increments are the X12/NCPDP table-driven normalizers and schema registry, actual Dapr Kafka output binding, Postgres outbox and append-only WORM audit sink, Delta/Parquet Spark/Sedona integration test with synthetic data, Temporal activities/evidence packs, and production-grade identity, policy, signing, data-quality, and observability controls.
