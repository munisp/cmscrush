#!/usr/bin/env python3
"""CRUSH 50k claims/s benchmark preflight and result validator.

This harness does not fabricate platform performance. It validates that a real
Kafka/Flink/Ray/GNN deployment and an observability collector are supplied
before a load generator is permitted to write a final benchmark report.
"""

from __future__ import annotations

import json
import os
import shutil
import socket
import sys
from datetime import UTC, datetime
from pathlib import Path

TARGET_CPS = 50_000
REQUIRED_ENV = {
    "KAFKA_BOOTSTRAP": "Kafka bootstrap endpoint for healthcare.claim.received.v1",
    "FLINK_REST_URL": "Flink REST URL for the deployed CEP job",
    "RAY_GNN_URL": "Ray Serve or GNN inference endpoint",
    "DECISION_SERVICE_URL": "Go decision-service endpoint",
    "PROMETHEUS_URL": "Prometheus endpoint used to gather measured metrics",
}


def endpoint_reachable(value: str) -> bool:
    value = value.removeprefix("http://").removeprefix("https://")
    host_port = value.split("/", 1)[0]
    host, sep, port = host_port.rpartition(":")
    if not sep:
        return False
    try:
        with socket.create_connection((host, int(port)), timeout=1.0):
            return True
    except OSError:
        return False


def main() -> int:
    findings: list[dict[str, object]] = []
    for binary in ("kafka-console-producer", "flink", "ray", "kubectl"):
        findings.append({"kind": "binary", "name": binary, "available": bool(shutil.which(binary))})

    for name, description in REQUIRED_ENV.items():
        value = os.getenv(name, "")
        findings.append(
            {
                "kind": "endpoint",
                "name": name,
                "description": description,
                "configured": bool(value),
                "reachable": endpoint_reachable(value) if value else False,
            }
        )

    missing = [entry["name"] for entry in findings if not entry.get("available", entry.get("reachable", False))]
    report = {
        "benchmark_name": "crush-flink-ray-gnn-50k",
        "target_claims_per_second": TARGET_CPS,
        "generated_at": datetime.now(UTC).isoformat(),
        "status": "READY" if not missing else "BLOCKED",
        "findings": findings,
        "required_measurements": {
            "flink": [
                "records_in_per_second",
                "records_out_per_second",
                "kafka_consumer_lag",
                "watermark_lag_ms",
                "end_to_end_latency_p50_p95_p99_ms",
                "checkpoint_duration_ms",
                "checkpoint_success_rate",
                "state_size_bytes",
                "busy_time_ms_per_second",
                "backpressured_time_ms_per_second",
            ],
            "ray_gnn": [
                "request_latency_p50_p95_p99_ms",
                "queue_wait_ms",
                "actor_execution_ms",
                "graph_sampling_ms",
                "serialization_ms",
                "batch_size",
                "worker_count",
                "cpu_utilization",
                "gpu_utilization",
                "host_memory_bytes",
                "gpu_memory_bytes",
                "error_rate",
                "abstention_rate",
            ],
            "decision": [
                "decision_latency_p50_p95_p99_ms",
                "rules_only_fallback_rate",
                "gnn_timeout_rate",
                "decision_error_rate",
            ],
        },
        "blocking_reason": (
            "A measured end-to-end result requires deployed Kafka, Flink, Ray/PyG, Go decision service, and Prometheus. "
            "Synthetic/in-process timings are not reported as Flink/Ray pipeline results."
            if missing
            else None
        ),
    }
    target = Path("reports/benchmarks/50k-preflight.json")
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2))
    return 0 if not missing else 2


if __name__ == "__main__":
    raise SystemExit(main())
