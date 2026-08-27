# CRUSH Flink CEP and Ray GNN Peak-Workload Scaling Review

**Author:** Manus AI  
**Status:** Testable capacity profile; not a certified production-capacity result. The environment has not yet executed a live 50,000-claims-per-second Flink/Ray benchmark.

## 1. Review conclusion

The current baseline manifest is correctly conservative for initial integration—three Flink TaskManagers, each with one slot, and Ray training workers that can scale to 10 CPU or 4 GPU pods. It is **not** a substantiated 50,000-claims-per-second profile. It also correctly models GNN training as a RayJob, but a real-time decision path needs a separate long-lived, request-serving Ray cluster or deployment.

The peak design below uses a two-tier decision budget. Flink evaluates deterministic CEP, aggregates, entity evidence, and low-cost anomaly gates for every claim. It sends only a bounded candidate subset to GNN scoring. This protects latency and prevents an all-claims GNN inference assumption from becoming an untested capacity promise.

| Traffic stage | Input budget | Compute path | Required outcome |
|---|---:|---|---|
| Claims ingress | 50,000 events/s | Kafka → Flink canonicalization and CEP | All events accepted, deduplicated, and checkpointed. |
| Candidate selection | ≤ 1% of ingress, initial cap 500 candidate events/s | Flink threshold/priority filter | Candidate event has deterministic triggers and graph-snapshot reference. |
| GNN scoring | ≤ 500 requests/s initially | Ray Serve / PyG inference | Score, uncertainty, and subgraph evidence, or a bounded abstention. |
| Decision fusion | Candidate-only, with deadline | Go decision service | Rules are authoritative; GNN timeout/error causes `ABSTAIN` and rules-only evaluation. |

The one-percent candidate cap is a **governance and capacity control**, not a fraud-rate estimate. Change it only after representative benchmark and reviewer-capacity testing.

## 2. Baseline-versus-peak allocation

| Component | Current baseline | Peak evaluation starting profile | Scaling trigger | Cap / safety control |
|---|---:|---:|---|---|
| Kafka input topic | External dependency | 12 partitions across ≥3 brokers; replication factor 3 | Producer latency, ISR loss, consumer lag | Validate key distribution; no automatic partition increase during test. |
| Flink JobManager | 2 vCPU / 4 GiB | 2 vCPU / 8 GiB, high-availability deployment | JVM/GC, REST responsiveness, checkpoint coordinator delay | Separate HA metadata/object storage; no PHI in logs. |
| Flink TaskManager | 3 × 2 vCPU / 8 GiB / 1 slot | 16 × 4 vCPU / 16 GiB / 1 slot; parallelism 16 | Sustained busy time > 700 ms/s or backpressure > 10% for 5 min | Maximum 32 TaskManagers pending validation; savepoint before schema or parallelism changes. |
| Flink state/checkpoints | RocksDB, 30-sec checkpoint | RocksDB/local SSD; object-store checkpointing every 30 sec; incremental checkpoints where supported | Checkpoint p95 > 15 sec, state growth, checkpoint failure | Alert at 2 successive failed checkpoints; test recovery at load. |
| Candidate feature gateway | Not separately provisioned | 3 replicas; 2 vCPU / 4 GiB each; HPA 3–12 | p95 > 100 ms or queue depth > 1,000 | Batch/lookup deadline < 150 ms; cache only approved features. |
| Ray head | 2 vCPU / 8 GiB | 2 vCPU / 8 GiB, 0 schedulable workload CPUs | GCS/head memory or scheduling delay | Dedicated node or anti-affinity from workers; dashboard authenticated. |
| Ray GNN CPU workers | Training only, 0–10 | 3 min / 12 max; each 8 vCPU / 32 GiB | CPU actor queue > 2 s, CPU utilization > 70% | Feature serialization and graph I/O only; no inference work that needs GPU. |
| Ray GNN GPU workers | Training only, 0–4 | 4 min / 16 max; each 1 GPU, 8 vCPU / 64 GiB | Queue wait > 250 ms, p95 execution > 750 ms, GPU utilization > 75% | Dedicated tainted GPU pool; max batch size must remain latency-safe. |
| GNN gateway | Helm deployment | 4 min / 20 max; each 2 vCPU / 4 GiB | Gateway p95 > 1 s or 5xx/timeout > 0.1% | Hard Go caller deadline: 2 s; no retry fan-out. |
| Go decision service | Existing Helm workload | 6 min / 30 max; each 1 vCPU / 2 GiB | CPU > 70%, decision p95 > 250 ms, queue/connection saturation | HPA based on requests and latency; strict rules-only fallback. |

The proposed peak profile reserves approximately **66 vCPU and 268 GiB** for the 16 Flink TaskManagers/JobManager, candidate gateway, Ray head, four initial GPU workers, GNN gateways, and six Go decision-service replicas, excluding Kafka, databases, object storage, observability, and Kubernetes system overhead. This is a planning calculation rather than a benchmark result; GPU memory is not included in the host-memory total.

## 3. Flink CEP deployment pattern

Flink must run as a dedicated Application Mode deployment per environment and data-control boundary. The workload uses a stable 16-way input partition plan and matching job parallelism rather than oversubscribing one TaskManager with multiple policy-critical slots. One slot per TaskManager isolates slow CEP/stateful subtasks, makes resource attribution clearer, and simplifies backpressure diagnosis.

Apache Flink exposes counters, gauges, histograms, and meters. Its backpressure monitor exposes `backPressuredTimeMsPerSecond`, `busyTimeMsPerSecond`, and `idleTimeMsPerSecond`; those three values sum to approximately 1,000 milliseconds for a subtask. [1] The peak test must gate progression on end-to-end p95/p99 latency, throughput, lag, checkpoint success/duration, watermark delay, state size, and these per-subtask time metrics. [1] [2]

| Condition | Interpretation | Response |
|---|---|---|
| Backpressure >10% for 5 minutes | Downstream cannot keep up; consumer rate alone is not a pass. | Freeze further load increase; inspect the first downstream saturated operator, then add parallelism only after savepoint validation. |
| Busy time >700 ms/s and low idle time | Operator is near saturation. | Scale targeted operator/parallelism if keys are sufficiently distributed; inspect hot keys otherwise. |
| Checkpoint p95 >15 seconds or failed twice | State/checkpoint I/O threatens recovery and exactly-once behavior. | Pause test escalation; inspect object-store latency/state size; recover from controlled failover. |
| Watermark lag above policy objective | Event-time decision freshness is degraded. | Route model paths to abstain/rules-only if freshness contract fails; preserve replay evidence. |
| Consumer lag grows at constant input | The pipeline is below the offered rate. | Do not label as 50k-capable; identify partition, serialization, CEP, sink, or async-I/O constraint. |

## 4. Ray and PyTorch Geometric online-inference pattern

The existing `RayJob` is retained for **offline training and backtesting only**. It must not serve online request traffic. A separate `RayService` or long-lived `RayCluster` hosts three bounded logical tiers: a CPU feature/sampler tier, GPU GNN inference actors, and an API gateway tier. The gateway receives candidate events from the feature service, not raw claim documents or unbounded Kafka traffic.

The GNN request includes `tenant_id`, `request_id`, `graph_snapshot_id`, `as_of_time`, an approved node type/identifier token, allowed feature names/versions, a deadline, and a candidate reason. The response includes model/version, score, uncertainty, explanation subgraph references, graph freshness, `decision=ASSESS|ABSTAIN`, and no financial/clinical adverse-action field. The Go client rejects stale tenant/version/snapshot contexts, scores outside range, schema mismatch, and responses after its two-second limit.

Ray documents task, actor, logical/physical resource, Serve, and autoscaling visibility through its dashboard, with Prometheus/Grafana recommended for metrics. [3] Ray’s profiling guidance identifies task/actor timelines, CPU, memory, and GPU tooling, and warns that interactive dashboard profiling must be protected when enabled. [4]

| Ray tier | Initial configuration | Peak scaling control | Principal metric |
|---|---|---|---|
| Feature/sampler actors | 3 CPU worker pods, 8 vCPU / 32 GiB each | 3–12 pods through logical CPU demand and backlog | Neighbor-sampling p95; serialization p95; graph-snapshot freshness. |
| GPU inference actors | 4 GPU pods, 1 GPU / 8 vCPU / 64 GiB each, with one primary actor/pod | 4–16 pods from queued requests and p95 execution time | Queue wait, batch size, actor execution p95, GPU utilization and VRAM. |
| Gateway replicas | 4 replicas, 2 vCPU / 4 GiB each | 4–20 through HPA on latency/concurrency | HTTP p95/p99, 5xx, timeouts, response validation failures. |
| Offline training | Ephemeral RayJob, 0–4 GPU workers by default | Separate quota and node pool; never compete with online scoring | Training time, GPU/VRAM, data read, evaluation outcomes. |

## 5. Kubernetes placement, quotas, and autoscaling

Use separate namespaces or at minimum dedicated node pools for `streaming-critical`, `online-ai`, and `offline-training`. Apply taints/tolerations so training cannot take online GPU capacity. Require pod anti-affinity/topology spread for Flink TaskManagers, Ray GPU workers, GNN gateways, and Go decision replicas. Set `PriorityClass` so JobManager, online TaskManagers, GNN gateway, and decision service preempt offline training if cluster capacity is constrained.

Resource requests in the peak profile are scheduling guarantees. Limits should remain equal to requests for policy-critical Flink pods and GPU workloads to minimize CPU throttling and noisy-neighbor behavior. Ray’s logical `num-cpus`/`num-gpus` must align with Kubernetes requests/limits; a mismatch makes autoscaling/scheduling signals unreliable. Admission policy must require explicit image digests, non-root security context, read-only root filesystem where compatible, a dedicated service account, encrypted object-store credentials via workload identity, and network policies that deny clinical/financial service access from AI worker pods.

## 6. Benchmark approval criteria

The peak profile becomes a capacity claim only after the full 30-minute 50,000-cps plateau, three reproducible runs, and controlled overload/recovery are completed with all raw telemetry retained. Initial engineering targets are listed below as **test criteria**, not measured outcomes.

| Criterion | Initial acceptance objective |
|---|---|
| Offered rate | Sustain 50,000 canonical claims/s for 30 minutes after warm-up. |
| Event correctness | No unaccounted loss; duplicate/replay behavior matches contract; injected CEP correlations are recovered. |
| Flink durability | 100% successful checkpoints during steady state; recover from a controlled TaskManager loss without data-loss claim. |
| Flink pressure | No sustained high backpressure and no unbounded consumer lag. |
| Decision latency | Go decision p95 <250 ms for rules-only path; candidate end-to-end p95 <2 s including GNN deadline; report p99 separately. |
| GNN behavior | Explicitly report queue, execution, abstention, timeout, and invalid-response rates. Success is not inferred from a score. |
| Safety | Any stale feature, GNN timeout, schema error, or tenant mismatch produces an evidence-bearing abstention/rules-only decision. |
| Review capacity | Candidate volume stays within agreed human-review capacity; investigate selection bias and disparate operational burden. |

## 7. Required changes to manifests

The accompanying `deploy/ai/healthcare-peak-scale-profile.yaml` is a test overlay. It has scaling min/max and resource values, but it must not be applied as an assertion of production capacity. Provision Kafka, object storage, a healthy Kubernetes CNI, Flink Kubernetes Operator, KubeRay Operator, NVIDIA device plugin (if GPU mode is used), Prometheus, Grafana, and image repositories before application.

## References

[1]: https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/monitoring/back_pressure/ "Apache Flink Back Pressure Monitoring"
[2]: https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/metrics/ "Apache Flink Metrics"
[3]: https://docs.ray.io/en/latest/ray-observability/getting-started.html "Ray Dashboard"
[4]: https://docs.ray.io/en/latest/ray-observability/user-guides/profiling.html "Ray Profiling"
[5]: https://docs.ray.io/en/latest/cluster/kubernetes/user-guides/configuring-autoscaling.html "KubeRay Autoscaling"
