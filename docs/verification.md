# Verification Record — CRUSH Foundation 0.1.0

**Verification date:** 2026-08-27  
**Scope:** Source contracts, deterministic service logic, workflow policy, lakehouse utility functions, Rust analytics boundary, and deployment-profile static checks.

| Component | Command | Result | Evidence validated |
|---|---|---|---|
| Repository contract and asset checks | `python3 scripts/verify_repository.py` | Passed | Required contracts/assets exist; final adverse action vocabulary and API endpoints are absent; tenant/purpose and default-deny policy markers are present. |
| Go decision service | `go test ./...` | Passed | 3 evaluator tests plus 2 API/context tests: hard-stop recommendation, human-review case creation, degraded rules-only payment, clinical review, missing gateway context, tenant-spoof rejection. |
| TypeScript case workflow | `npm test && npm run build` | Passed | 2 policy tests: final suspension requires a human actor/rationale/timestamp; event retains approval evidence; compiler type-check succeeds. |
| Python lakehouse | `PYTHONPATH=src pytest -q` | Passed | 4 tests: tenant-isolated partitioning, deterministic provenance hash, direct-identifier telemetry rejection, distance/quality geospatial feature. |
| Rust analytics query | `cargo fmt --check && cargo test --locked` | Passed | 2 tests: an approved data set produces a tenant-scoped read-only DataFusion plan; tenant traversal and unapproved data sets are rejected. |

## Intentionally Not Executed

This foundation does not start infrastructure containers or contact external services. No Kafka/Flink/Spark/Delta/Sedona/Ray/Temporal/Redis/Postgres/TigerBeetle/Mojaloop/API gateway/Keycloak/OpenSearch/Wazuh/OpenCTI/open-appsec/Kubecost cluster was provisioned, and no real or synthetic healthcare data was sent off-host. The repository instead contains the contracts, deployment profiles, and unit-testable guardrails required before a controlled integration environment can be created.

The next verification tier must use synthetic data only and provision isolated test instances. It must prove Kafka schema compatibility and idempotent outbox publication; Delta Lake transaction and Parquet read/write behavior; Spark/Sedona and Flink/Sedona parity; Ray artifact governance; Redis/feature freshness behavior; Postgres/Temporal case projection durability; TigerBeetle posting idempotency; APISIX/Keycloak authorization; Dapr pub/sub/retry paths; and Wazuh/OpenCTI/OpenSearch/open-appsec/Fluvio/Kubecost operational controls.
