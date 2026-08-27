# CRUSH Peak Evaluation Overlay: Exact Manifest Inspection

**Manifest:** `deploy/ai/healthcare-peak-scale-profile.yaml`  
**Profile:** `peak-evaluation`  
**Important:** This is a starting profile for a controlled benchmark. It is not a certified 50,000-cps capacity claim.

## Workload objects

| Object | Namespace | Configuration |
|---|---|---|
| `PriorityClass/crush-online-critical` | cluster-scoped | Value 100000; `PreemptLowerPriority`; not default. Used for online CEP, Ray head/workers, and decision-critical services. |
| `PriorityClass/crush-offline-training` | cluster-scoped | Value 1000; `Never`; not default. Intended for offline Ray training/backtesting. |
| `FlinkDeployment/crush-healthcare-claims-peak` | `crush-platform` | Flink v2_3; parallelism 16; one TaskManager slot per pod; RocksDB incremental state; exactly-once checkpoints every 30 seconds; savepoint upgrade mode. |
| `RayService/crush-gnn-online` | `crush-platform` | Four initial gateway replicas, four initial GNN replicas, in-tree autoscaling, conservative scale-up, and separate feature-sampler/GPU worker groups. |
| `PodDisruptionBudget/crush-gnn-gateway-availability` | `crush-platform` | Requires at least three gateway pods; selector expects `serve-deployment=gnn_gateway` labels from the Ray Serve deployment. |

## Flink JobManager and TaskManagers

| Role | Manifest location | Requests | Limits | Replicas/parallelism |
|---|---|---:|---:|---:|
| JobManager | `spec.jobManager.resource` | 2 vCPU / 8 GiB | 2 vCPU / 8 GiB | Operator-managed; one active JobManager plus HA/recovery configuration supplied by the cluster profile. |
| TaskManager | `spec.taskManager.resource` | 4 vCPU / 16 GiB | 4 vCPU / 16 GiB | Job parallelism 16; `taskmanager.numberOfTaskSlots=1`; expected 16 TaskManagers for the starting profile. |

The one-slot configuration deliberately makes one TaskManager the unit of scaling and failure injection. The 16-way parallelism is a starting point; it must be adjusted only after checkpoint/savepoint and hot-key tests. The manifest sends checkpoints and savepoints to object storage paths under `s3://crush-{checkpoints,savepoints}/healthcare/claims-peak` and exposes a Prometheus reporter on port 9249.

## Ray head and worker pools

| Ray role | Initial / min / max | Requests and limits per pod | Ray logical resources | Placement |
|---|---:|---:|---:|---|
| Head | 1 / 1 / 1 | 2 vCPU / 8 GiB | `num-cpus=0` | Online priority; service account token automount disabled; non-root security context. |
| Feature sampler | 3 / 3 / 12 | 8 vCPU / 32 GiB | `num-cpus=8` | Online priority; CPU worker group. |
| GNN GPU inference | 4 / 4 / 16 | 8 vCPU / 64 GiB + 1 GPU | `num-cpus=8`, `num-gpus=1` | Online priority; `workload.crush.io/gpu=true`; NVIDIA toleration; non-root security context. |
| Ray Serve gateway | 4 / 4 / 20 logical replicas | Actor `num_cpus=1` | Target 16 ongoing requests | Ray Serve deployment in `serveConfigV2`; gateway PDB requires three available pods. |
| Ray Serve GNN inference | 4 / 4 / 16 logical replicas | Actor `num_cpus=8`, `num_gpus=1` | Target 8 ongoing requests | Backed by GPU worker group; candidate stream initially capped at ≤500 assessments/s. |

The Kubernetes worker-pod replicas and Ray Serve deployment replicas are separate controls. Ray Serve may scale logical actors within the available Ray worker resources; KubeRay may then scale worker pods to satisfy logical resource demand. Both layers must be observed in the benchmark so logical autoscaling is not confused with physical pod capacity.

## Resource arithmetic

The initial online resource request for the explicit pods is:

| Pool | Count | Per-pod request | Aggregate |
|---|---:|---:|---:|
| Flink JobManager | 1 | 2 vCPU / 8 GiB | 2 vCPU / 8 GiB |
| Flink TaskManagers | 16 | 4 vCPU / 16 GiB | 64 vCPU / 256 GiB |
| Ray head | 1 | 2 vCPU / 8 GiB | 2 vCPU / 8 GiB |
| Ray feature samplers | 3 | 8 vCPU / 32 GiB | 24 vCPU / 96 GiB |
| Ray GPU workers | 4 | 8 vCPU / 64 GiB + 1 GPU | 32 vCPU / 256 GiB + 4 GPUs |

The Flink plus Ray head/worker starting profile therefore requests **124 vCPU and 624 GiB RAM plus 4 GPUs**, before Kafka, Go decision replicas, feature store, object storage, Prometheus, Kubernetes system overhead, and any Ray Serve gateway sidecar allocations. This corrects the earlier smaller planning estimate, which omitted the full 16-TaskManager profile and double-counted/under-counted some logical roles. Maximum configured worker-pool requests rise to 16 feature samplers and 16 GPU workers: **228 vCPU and 1,392 GiB RAM plus 16 GPUs**, excluding JobManager/TaskManager growth, gateway replicas, infrastructure, and headroom.

These numbers are scheduler requests, not observed utilization. A production node-pool plan should add system and failure headroom and must be validated with actual graph size, model precision, sampling fan-out, checkpoint state, Kafka partitioning, and backpressure measurements.

## Review findings and required follow-up

The overlay correctly separates online priority from offline training and disables Ray head scheduling CPUs. It should gain explicit anti-affinity/topology spread for Ray workers if the operator version supports the selected fields, and it should include the same non-root/read-only security context on the CPU feature-sampler container that is already present on the head/GPU containers. The Ray Serve gateway should also expose an explicit Kubernetes Service/health probe configuration through the selected Ray operator version.

The Flink manifest’s `podTemplate` label selector uses `app: crush-healthcare-claims-peak`, while the workload metadata labels currently expose `app.kubernetes.io/part-of` and `crush.io/profile`. Verify the operator-generated pod labels before relying on the topology-spread selector; if the label is absent, change the selector to a label guaranteed on generated pods.

The GPU group’s `nvidia.com/gpu` request and limit are correctly aligned at one GPU per worker. The GPU node selector and toleration require a separately labeled/tainted node pool. The test profile must record GPU model, driver, CUDA/PyTorch/PyG versions, graph snapshot size, neighbor fan-out, batch size, concurrency, and VRAM utilization.

No live capacity claim is made. The companion chaos protocol uses these exact roles and resources for controlled TaskManager, JobManager, CPU-worker, GPU-worker, and gateway failures.
