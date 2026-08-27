# CRUSH Flink CEP and Ray GNN 50,000-CPS Chaos Protocol

**Purpose:** validate resilience, correctness, freshness, and safe degradation of the healthcare claims anomaly pipeline under a sustained 50,000 canonical-claim-events-per-second load.

**Status:** Executable test protocol. It is not evidence that the current environment can sustain 50,000 cps. The local sandbox lacks a healthy Kubernetes API and deployed Kafka, Flink, Ray/PyG, Prometheus, and decision services; therefore these experiments must run in a provisioned, isolated evaluation cluster.

## 1. Non-negotiable test boundaries

Run only against synthetic or de-identified, schema-valid healthcare claim events in a dedicated evaluation namespace. Never inject faults into production, real payment/settlement systems, live clinical systems, or real beneficiary communications. Do not use fault injection to bypass authorization or force an adverse claim action.

Every event must carry `run_id`, deterministic `source_event_id`, `event_time`, `ingested_at`, `tenant_id`, `provider_token`, `beneficiary_token`, `trace_id`, and a known expected-correlation label. The load generator, Kafka, Flink, Ray, and Go services must preserve the same `run_id`. Store raw generator input, expected output, checkpoint/savepoint identifiers, and telemetry exports in a locked benchmark evidence location.

The test controller must enforce a global abort if any of these occurs: unbounded consumer lag, evidence loss, tenant-context mismatch, PHI appears in a security/model log, more than one consecutive failed checkpoint, a decision response exceeds the two-second GNN deadline without an explicit abstention, or any model output is interpreted as an autonomous denial/suspension/payment action.

## 2. Baseline and steady-state gate

Deploy the peak overlay with immutable image digests, the same Flink parallelism, Ray worker groups, model artifact, and configuration used for every trial. Verify that the Flink Operator and KubeRay Operator are installed and that all CRDs are established before applying application objects.

Run 15 minutes at 10,000 cps, 15 minutes at 25,000 cps, then 30 minutes at 50,000 cps. The steady-state gate passes only if records in/out match, deduplication is correct, no checkpoint fails, watermark lag is bounded, candidate rate remains at or below the initial 500 GNN assessments/s cap, and all Prometheus/Flink/Ray/Go metrics are being collected. Repeat the 50,000-cps plateau three times before fault experiments.

Record these labels on every metric: `run_id`, `experiment_id`, `tenant_class`, `claim_schema_version`, `cep_job_version`, `graph_snapshot_id`, `gnn_model_version`, `ray_cluster_version`, and `flink_job_id`.

## 3. Experiment matrix

| ID | Injected failure | Injection point and action | Expected behavior | Acceptance criteria |
|---|---|---|---|---|
| F1 | One Flink TaskManager loss | Record pod name; `kubectl delete pod -n crush-platform <taskmanager-pod> --grace-period=0 --force` in the evaluation namespace only. | Operator replaces the pod; Flink restores from the latest checkpoint/savepoint; Kafka consumption resumes. | No unaccounted event loss; duplicate rate matches contract; checkpoint recovery succeeds; no unsafe final action; report recovery time and p50/p95/p99 before/during/after. |
| F2 | JobManager restart | Delete only the JobManager pod after a completed checkpoint. | HA JobManager restarts and job remains in `RUNNING` after recovery. | No permanent job failure; no checkpoint corruption; all outputs trace to the same `run_id`; no decisions from stale or missing features. |
| F3 | Checkpoint-store outage | In an isolated test, deny the Flink service account’s egress to the checkpoint object-store endpoint for 90 seconds, or revoke a test-only credential; restore it after the planned window. | Checkpoint failures are visible; Flink does not claim durability; event processing follows the configured policy; recovery path is tested after restoration. | At least two failed-checkpoint alerts are never allowed; load escalation stops; after restoration a successful checkpoint completes and replay correctness is verified. |
| F4 | Kafka broker loss | Stop or isolate one broker in a three-broker RF=3 test cluster; do not remove more brokers than the replication budget allows. | Producers/consumers continue within replication guarantees; partition leadership changes; lag may rise and recover. | No unaccounted loss; ISR/under-replicated partitions recover; lag returns to baseline; no partition-hotspot-induced unbounded backlog. |
| F5 | Kafka partition/network delay | Apply a controlled 250–500 ms delay to one test partition path using an approved network-chaos tool, or pause one partition consumer for 60 seconds. | Event-time watermarks and late-event handling behave according to the configured lateness policy. | Late events are classified, not silently dropped; CEP match counts remain explainable; watermark lag and late-event counters are captured. |
| F6 | Hot provider/entity key | Generate a deliberate key-skew workload where one provider token receives 10–20% of traffic for 5 minutes. This is a workload fault, not a production attack. | Hot-key state becomes visible; no tenant leakage; keyed operator saturation is isolated. | Skew is detected; the run is marked capacity-limited if one key dominates; no claim is emitted with a wrong provider/entity context. |
| F7 | Ray CPU worker loss | Delete one `feature-sampler` worker pod during candidate traffic. | Ray reschedules actors/tasks; requests queue briefly or abstain if deadline would be exceeded. | No request hangs; queue and recovery are measured; stale graph snapshot produces abstention, not a score. |
| F8 | Ray GPU worker loss | Delete one `gnn-gpu` worker pod during a 500-candidate-assessments/s plateau. | Ray reschedules GPU actor; remaining workers continue; the gateway applies a bounded queue/deadline. | No more than the configured temporary abstention budget; no unbounded retry storm; all failed requests are explicit `ABSTAIN` with evidence. |
| F9 | GNN slow response | Inject 2.5-second response latency in a test-only GNN gateway route, above the Go client’s two-second deadline. | Go client times out once, records a timeout, and executes rules-only/advisory abstention path. | 100% of injected slow calls become bounded timeout/abstain outcomes; zero model score is accepted after deadline; no autonomous adverse action. |
| F10 | GNN error/schema fault | Return HTTP 503, invalid score range, wrong tenant, stale graph snapshot, or mismatched feature version from the test GNN service. | Go client rejects the response and records a typed validation/fallback reason. | 100% of invalid responses rejected; no retry fan-out; all evidence records contain model/version/request IDs. |
| F11 | Go decision replica loss | Delete one decision-service pod during candidate load. | Kubernetes routes traffic to remaining replicas and replaces the pod. | Request error rate remains within the test objective; idempotent replay yields one decision record; hash-chain continuity is preserved. |
| F12 | DNS/service discovery fault | Apply a time-bounded test-only DNS failure or remove one service endpoint while retaining at least one healthy endpoint. | Callers fail fast or use approved fallback; no stale response is accepted. | No request blocks beyond its deadline; rules-only behavior is explicit; recovery after DNS restoration is verified. |
| F13 | Cluster-node drain | Cordon and drain one worker node hosting a TaskManager or Ray worker, respecting PodDisruptionBudgets. | Workloads reschedule within capacity; offline training is preempted before online workloads. | Online critical workloads recover; PDBs and topology spread behave as intended; no co-location surprise is hidden. |
| F14 | Observability loss | Stop one Prometheus collector or block metrics export for 2 minutes in a test namespace. | The pipeline continues but the run is marked unobservable. | The run cannot be approved as a benchmark; evidence contains an observability-gap interval; safety does not depend on telemetry availability. |

Run F1–F4 first because they validate core durable-stream behavior. Run F5–F6 next for event-time and state skew. Run F7–F12 only after the baseline GNN candidate path is stable. Run F13–F14 last. Do not run simultaneous faults until every single-fault experiment passes and the recovery budget is known.

## 4. Kubernetes-native injection commands

The following commands are intentionally scoped to a dedicated namespace and should be executed only by the test operator after confirming the context and experiment ID:

```bash
kubectl config current-context
kubectl get ns crush-platform --show-labels
kubectl -n crush-platform get pods -l crush.io/profile=peak-evaluation -o wide

# F1: TaskManager loss; choose the target by label after recording it.
kubectl -n crush-platform delete pod <taskmanager-pod> --grace-period=0 --force

# F2: JobManager restart after a successful checkpoint.
kubectl -n crush-platform delete pod <jobmanager-pod> --grace-period=30

# F8: Ray GPU worker loss.
kubectl -n crush-platform delete pod <ray-gpu-worker-pod> --grace-period=0 --force

# F11: Go decision replica loss.
kubectl -n crush-platform delete pod -l app.kubernetes.io/name=decision-service \
  --field-selector=status.phase=Running --wait=false

# Recovery evidence.
kubectl -n crush-platform get pods -o wide
kubectl -n crush-platform get events --sort-by=.lastTimestamp
```

Do not use `--force` for JobManager or stateful components unless the experiment explicitly tests abrupt termination. Prefer graceful deletion first so the experiment distinguishes normal rescheduling from crash recovery. Network delay, packet loss, object-store denial, and DNS faults require a reviewed chaos tool or a short-lived test-only sidecar/NetworkPolicy; do not add privileged capabilities to production CRUSH workloads merely to make chaos injection convenient.

## 5. Required telemetry and trace joins

Flink: records in/out, Kafka consumer lag, watermark lag, late events, CEP match counts, checkpoint duration/success/failure, state size, `busyTimeMsPerSecond`, `backPressuredTimeMsPerSecond`, and `idleTimeMsPerSecond`. Ray: request p50/p95/p99, queue wait, actor execution, graph sampling, serialization, batch size, worker count, CPU/GPU/VRAM, errors, timeouts, and abstentions. Go: request latency, GNN timeout/validation failures, rules-only fallback, decision-store latency, idempotency replay, and error counts. Kubernetes: pod restarts, readiness, OOM kills, CPU throttling, node pressure, evictions, and rescheduling time.

Join every claim path through `trace_id` and `source_event_id`; join every fault through `experiment_id` and an immutable fault timestamp. Compare pre-fault, fault-window, and recovery-window distributions. A throughput number without correctness, recovery, and safety evidence is not a pass.

## 6. Approval report

For each experiment, publish a table containing the exact cluster/configuration digest, offered rate, fault start/end, affected pods/partitions, Flink job/checkpoint IDs, Ray worker and model versions, Go decision/fallback counts, event loss/duplicate/late counts, p50/p95/p99 latency, recovery time, and reviewer sign-off. Include a clear `PASS`, `FAIL`, or `INCONCLUSIVE` result.

A final multi-fault campaign may combine only faults whose individual recovery budgets sum below the end-to-end freshness objective. The recommended first combination is one TaskManager loss plus one Ray GPU worker loss, never simultaneous Kafka quorum loss and checkpoint-store outage. Any safety, correctness, or observability violation is an automatic campaign failure even if throughput remains above 50,000 cps.

## References

[1]: https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/monitoring/back_pressure/ "Apache Flink Back Pressure Monitoring"
[2]: https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/metrics/ "Apache Flink Metrics"
[3]: https://docs.ray.io/en/latest/ray-observability/user-guides/profiling.html "Ray Profiling"
[4]: https://kubernetes.io/docs/tasks/configure-pod-container/assign-memory-resource/ "Kubernetes Resource Management"
[5]: https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/ "Kubernetes Pod Lifecycle"
