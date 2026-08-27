#!/usr/bin/env python3
"""Static verification for the CRUSH foundation; intentionally no external cluster required."""
from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CONTRACTS = ROOT / "contracts" / "json-schema"


def require(path: Path) -> str:
    if not path.is_file():
        raise AssertionError(f"missing required asset: {path.relative_to(ROOT)}")
    return path.read_text(encoding="utf-8")


def main() -> None:
    contract_files = [
        "claim-event.schema.json",
        "decision-record.schema.json",
        "case-task.schema.json",
        "ledger-posting-intent.schema.json",
    ]
    contracts = {name: json.loads(require(CONTRACTS / name)) for name in contract_files}
    actions = contracts["decision-record.schema.json"]["properties"]["action"]["enum"]
    assert "DENY" not in actions and "SUSPEND" not in actions, "final adverse action leaked into decision contract"
    assert {"DENY_RECOMMEND", "SUSPEND_RECOMMEND"}.issubset(actions), "recommendation actions absent"
    assert "tenant_id" in contracts["claim-event.schema.json"]["required"], "claim tenant boundary missing"
    assert "idempotency_key" in contracts["decision-record.schema.json"]["required"], "decision idempotency missing"

    required_assets = [
        "deploy/helm/crush-platform/Chart.yaml",
        "deploy/helm/crush-platform/values.yaml",
        "deploy/helm/crush-platform/templates/workloads.yaml",
        "deploy/helm/crush-platform/templates/networkpolicy.yaml",
        "deploy/dapr/components/components.yaml",
        "deploy/apisix/routes.yaml",
        "deploy/keycloak/crush-realm.json",
        "deploy/openappsec/policy.yaml",
        "deploy/security/security-telemetry.yaml",
        "deploy/temporal/case-worker.yaml",
        "deploy/flink/sedona-enrichment-job.yaml",
        "deploy/ray/ray-job.yaml",
        "deploy/fluvio/non-phi-telemetry.yaml",
        "deploy/lakehouse/delta-storage-profile.yaml",
        "services/decision-service/migrations/0001_operational_state.sql",
    ]
    for asset in required_assets:
        require(ROOT / asset)

    routes = require(ROOT / "deploy/apisix/routes.yaml")
    assert "/v1/claims/suspend" not in routes and "/v1/claims/deny" not in routes, "final adverse API route is prohibited"
    assert "Purpose-Of-Use" in routes and "X-CRUSH-Tenant-ID" in routes, "gateway headers are required"
    policies = require(ROOT / "deploy/helm/crush-platform/templates/networkpolicy.yaml")
    assert "default-deny" in policies and "NetworkPolicy" in policies, "tenant network isolation missing"
    print("repository verification passed")


if __name__ == "__main__":
    main()
