# CRUSH Flink CEP and Ray GNN 50,000 Claims/Second Benchmark Report

**Run time:** 2026-08-27T12:07:31Z  
**Target:** 50,000 canonical healthcare claim events per second  
**Status:** **BLOCKED — no performance or latency values reported**

## Executive result

No end-to-end benchmark result is available. The current environment has six CPU cores and approximately 3.8 GiB host memory, but it does not contain a Kafka producer/runtime, Apache Flink runtime, Ray runtime, PyTorch/PyTorch Geometric runtime, a reachable Go decision-service deployment, or a Prometheus endpoint. The local Kubernetes API from the earlier K3s attempt is also unavailable. The preflight harness therefore correctly refuses to generate throughput or latency values.

Reporting synthetic loop timings, unit-test runtimes, or the latency of a mocked scoring function as a “Flink CEP and Ray GNN decision pipeline” benchmark would be materially misleading. The exact preflight result is attached in `50k-preflight.json`.

## Preflight evidence

| Requirement | Observed state | Why it is required |
|---|---|---|
| Kafka producer and `healthcare.claim.received.v1` topic | Not installed/configured | Establishes the real partitioning, producer behavior, and consumer lag. |
| Flink CEP application | Not installed/configured | Measures event-time state, pattern computation, checkpointing, watermarks, and backpressure. |
| Ray + PyTorch Geometric GNN service | Not installed/configured | Measures graph neighborhood sampling, actor queueing, serialization, model execution, and abstention path. |
| Go decision service endpoint | Not reachable/configured | Measures actual request context propagation, rules-first fusion, and fallback behavior. |
| Prometheus/Grafana collector | Not configured | Collects operator, infrastructure, service, and latency time series. |
| Kubernetes cluster | API unavailable | Required for production-representative pod placement, autoscaling, resource limits, and network overhead. |

## Required benchmark topology

A valid 50,000-cps result requires a multi-node Kubernetes deployment, not a single developer sandbox. A minimum evaluation topology uses three Kafka brokers on separate nodes, three Flink TaskManagers with a documented slots/CPU/memory profile, a Ray head plus CPU/GPU worker pools, two or more Go decision-service replicas, a separate online feature store, and Prometheus/Grafana. Produce claims across a representative provider/entity-key distribution rather than a single hot key, while separately running an intentionally skewed-key test to expose state hot spots.

The data generator must emit schema-valid, synthetic canonical events. CMS SynPUFs and Synthea-derived structures can be used for schema realism and functional replay, but their synthetic nature means they cannot validate real-world fraud rates or performance on live PHI workloads. All generated events need a known event time, ingestion time, unique source event ID, jurisdiction, and expected correlation count so that loss, duplication, and late-event correctness are measurable.

## Required measurements

| Plane | Required measurement | Pass/fail interpretation |
|---|---|---|
| Kafka | Producer acks, produce error rate, partition skew, consumer lag, bytes in/out. | Confirms input rate reaches Flink and identifies broker/partition bottlenecks. |
| Flink CEP | Records in/out, watermark lag, end-to-end latency p50/p95/p99, checkpoint duration/success, state size, busy/idle/backpressured time. | Flink documents `backPressuredTimeMsPerSecond`, `busyTimeMsPerSecond`, and `idleTimeMsPerSecond`; these should be captured at each operator. [1] [2] |
| Delta/feature materialization | Commit duration, small-file count, online feature freshness, failed or delayed materializations. | Confirms the serving feature snapshot is both available and timely. |
| Ray/PyG | Gateway latency p50/p95/p99, actor queue wait, graph sampling, serialization, batch size, execution time, worker count, CPU/GPU, RAM/VRAM, error/abstention rates. | Distinguishes scheduling/serialization/graph I/O/model runtime bottlenecks. [3] [4] |
| Go decision | Request latency p50/p95/p99, rule evaluation time, GNN timeout rate, fallback rate, error rate, hash-store time. | Confirms model calls remain bounded and degraded rules-only outcomes are safe. |
| Correctness | Event loss, duplicates, out-of-order handling, CEP precision/recall against injected patterns, stale-feature rate. | A high throughput result without event correctness is not acceptable. |

## Benchmark protocol

Run a 15-minute warm-up at 10,000 cps. Then run 30-minute steady-state plateaus at 25,000, 37,500, and 50,000 cps. Follow with a 15-minute 60,000-cps overload interval and a 20-minute recovery observation. Repeat each run at least three times from a clean, versioned deployment and report median plus run-to-run range. Record exact Git commits, container digests, Flink/Ray/Kafka versions, cluster node types, kernel/CNI, resource requests/limits, Kafka partitions, Flink parallelism, checkpoint interval, graph snapshot size, neighbor-sampling configuration, model precision, batch/concurrency settings, and the full workload-generator seed.

The score request must carry a fixed deadline of no more than two seconds, matching the Go GNN client boundary. A GNN timeout must be counted as a safe `abstain` or rules-only fallback—not a successful GNN inference and not an indication of suspicious healthcare activity.

## Profiling collection plan

Use Flink metrics and backpressure monitoring for operator and task-level behavior. Flink exposes counters, gauges, histograms, and meters, and documents its backpressure, busy, and idle metrics. [1] [2] Use Ray Dashboard plus Prometheus/Grafana for task, actor, Serve, logical/physical resource, and autoscaler metrics. For CPU/memory/GPU root-cause work, use authenticated dashboard profiling, PyTorch Profiler, `py-spy`, Memray, and Nsight System as appropriate; Ray warns that dashboard profiling endpoints are disabled by default for security and should be protected when enabled. [3] [4]

## Rerun command contract

After provisioning the deployment, set the five endpoints and run the saved preflight:

```bash
export KAFKA_BOOTSTRAP='kafka-bootstrap.kafka.svc:9092'
export FLINK_REST_URL='https://flink-rest.platform.example'
export RAY_GNN_URL='https://gnn.platform.example'
export DECISION_SERVICE_URL='https://decision.platform.example'
export PROMETHEUS_URL='https://prometheus.platform.example'
./scripts/benchmark_50k_pipeline.py
```

A `READY` preflight is only the gate to begin the load test; it is not a performance pass. The test runner must collect and attach its raw Prometheus exports, Flink job metrics, Ray traces/profiles, Go histograms, workload-generator events, and configuration manifest before a latency table can be approved.

## References

[1]: https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/metrics/ "Apache Flink Metrics"
[2]: https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/monitoring/back_pressure/ "Apache Flink Back Pressure Monitoring"
[3]: https://docs.ray.io/en/latest/ray-observability/getting-started.html "Ray Dashboard"
[4]: https://docs.ray.io/en/latest/ray-observability/user-guides/profiling.html "Ray Profiling"
