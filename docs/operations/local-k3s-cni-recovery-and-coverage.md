# Local K3s CNI Recovery and Coverage Improvement Guide

**Scope:** This guide diagnoses the CRUSH local-deployment attempt on 27 August 2026 and prioritizes improvements to the Go decision-service and TypeScript case-policy coverage.

## 1. Diagnosis: Why the Cluster Failed

The chart did **not** fail Helm lint or rendering. K3s did start long enough to register a `Ready` control-plane node. The failure occurred below the application layer, during pod-network setup. The decisive observations were: the host had only a link-local primary address (`169.254.0.21`), K3s was subsequently configured with a synthetic address on loopback (`--node-ip 10.254.0.1 --advertise-address 10.254.0.1 --flannel-iface lo`), and Flannel never created `/run/flannel/subnet.env`. CoreDNS, metrics-server, and local-path-provisioner therefore could not create pod sandboxes.

> `/run/flannel/subnet.env` is an effect, not the root cause. The Flannel CNI plugin reads that file when creating each pod network. It remains absent when the embedded Flannel process cannot establish a viable node/network configuration.

K3s documents Flannel as its default CNI and notes that `--flannel-iface` overrides the interface selected for Flannel traffic. It also documents that `--node-ip` is the address advertised by a node. Both need a real, routable node interface for the normal single-node local-development path. [1] [2]

| Observed condition | Effect | Interpretation |
|---|---|---|
| Primary interface was `169.254.0.21/30` | K3s could not choose an IP from a default route on its first installation. | Link-local addressing is unsuitable as a durable cluster/node identity. |
| K3s was pinned to `lo` | Flannel had no usable non-loopback transport interface. | Loopback can help bootstrap an API listener but is not a viable CNI fabric interface. |
| `/run/flannel/subnet.env` absent | All pod sandbox creations failed through `type="flannel"`. | Embedded Flannel was not successfully initialized. |
| The sandbox rejected `ip link add crush0 type dummy` | No private dummy interface could be created as a workaround. | The runtime lacks the required `CAP_NET_ADMIN` host capability. |
| API later returned unavailable/refused | Server/agent runtime could not remain healthy while pod networking failed. | Chart deployment must stop until CNI health is restored. |

## 2. Recommended Resolution Paths

### Path A — Preferred: Run the Cluster on a Routable Local Host

Use a developer workstation or disposable Linux VM that provides a real private non-loopback interface, a default route, and permission to create CNI bridges/VXLAN interfaces. For a typical single-node K3s machine, identify an interface with an RFC1918 address such as `192.168.1.25` or `10.0.0.25`.

| Check | Expected result | Command |
|---|---|---|
| Interface and default route | A non-loopback interface has a private address and a default route. | `ip -br addr && ip route show default` |
| Required kernel support | `overlay` and `br_netfilter` are available. | `sudo modprobe overlay br_netfilter` |
| Bridge forwarding | IPv4 forwarding is enabled. | `sysctl net.ipv4.ip_forward` |
| No conflicting CNI residue | No stale `cni0`, `flannel.1`, or old CNI configuration remains after teardown. | `ip link show; sudo ls -la /etc/cni/net.d` |

Create `/etc/rancher/k3s/config.yaml` **before first start**. Example values use placeholders and must be replaced with the host’s actual private interface/IP:

```yaml
# /etc/rancher/k3s/config.yaml
node-name: crush-local
node-ip: 192.168.1.25
advertise-address: 192.168.1.25
flannel-iface: enp0s3
flannel-backend: vxlan
write-kubeconfig-mode: "0644"
disable:
  - traefik
  - servicelb
```

Then start or restart K3s and wait for both the node and core pods to be ready:

```bash
sudo systemctl enable --now k3s
kubectl get nodes -w
kubectl -n kube-system get pods -w
# Expected: coredns, local-path-provisioner, and metrics-server reach Running/Ready.
```

Only after core CNI health is established should you install the CRUSH profile:

```bash
cd deploy/helm/crush-platform
helm lint .
helm upgrade --install crush-platform . \
  --namespace crush-demo-tenant-dev \
  --create-namespace \
  --wait --timeout 10m
kubectl -n crush-demo-tenant-dev get pods,svc,networkpolicy
```

This is the shortest path because it preserves K3s’s bundled Flannel configuration. K3s uses VXLAN by default; `host-gw` is a valid alternative only where nodes have direct Layer-2 connectivity. [1]

### Path B — Docker-Based Local Kubernetes (Kind or Minikube)

If the development host has a functioning Docker or Podman engine, create the cluster with Kind or Minikube instead of running a node-level K3s service. The container runtime provides the bridge and network namespaces that were unavailable in the sandbox. This is often the simplest reproducible developer path and avoids choosing a host NIC manually.

| Approach | Best use | Trade-off |
|---|---|---|
| K3s with real host interface | Linux VM or workstation; closest to a compact server topology. | Requires valid host networking, root access, and CNI kernel privileges. |
| Kind | Developer laptop/CI with Docker; fast, declarative ephemeral clusters. | Container runtime must be installed and healthy; images must be loaded/pulled. |
| Minikube | Developer machine needing configurable drivers/add-ons. | More local resource consumption and driver-specific setup. |

For Kind, use a dedicated cluster name and verify the network before deploying:

```bash
kind create cluster --name crush-dev
kubectl cluster-info --context kind-crush-dev
kubectl get nodes
kubectl -n kube-system get pods
```

Load locally built images or configure the Helm values with a reachable immutable registry before installing. Do not rely on the placeholder `ghcr.io/munisp/cmscrush/*:0.1.0` values unless the corresponding images have been published.

### Path C — Replace Flannel Only When Required

K3s supports disabling Flannel with `--flannel-backend=none`; its documentation advises disabling K3s’s embedded network-policy controller too if the replacement CNI has its own policy engine, to avoid conflicts. [1] Use this only if the environment can install and run an alternate CNI such as Cilium or Calico. It does **not** solve the sandbox limitation if the host cannot create network interfaces, namespaces, routes, or eBPF/iptables state.

```yaml
# K3s configuration for a deliberate custom-CNI deployment
flannel-backend: none
disable-network-policy: true
egress-selector-mode: cluster
```

Install the chosen CNI immediately after the API is up and before expecting application workloads to start. Follow the CNI vendor’s version-specific installation guide; the CRUSH NetworkPolicies require a CNI that enforces Kubernetes NetworkPolicy.

## 3. Safe Cleanup and Rebuild Sequence

Use the following sequence only on a disposable local development node. It deletes K3s workloads and state.

```bash
# 1. Preserve diagnostics first.
sudo journalctl -u k3s --since "30 minutes ago" > k3s-before-cleanup.log

# 2. Remove the broken ephemeral K3s install.
sudo /usr/local/bin/k3s-uninstall.sh

# 3. Inspect and remove only stale CNI state created by the removed cluster.
ip link show cni0 2>/dev/null || true
ip link show flannel.1 2>/dev/null || true
sudo rm -rf /etc/cni/net.d/* /var/lib/cni/*

# 4. Ensure required modules and forwarding.
sudo modprobe overlay br_netfilter
cat <<'EOF' | sudo tee /etc/sysctl.d/99-kubernetes-cri.conf
net.bridge.bridge-nf-call-iptables = 1
net.bridge.bridge-nf-call-ip6tables = 1
net.ipv4.ip_forward = 1
EOF
sudo sysctl --system

# 5. Write the real-NIC K3s configuration shown above, reinstall/start K3s,
#    and verify kube-system pods before installing CRUSH.
```

Avoid manually creating `/run/flannel/subnet.env`. It is transient state generated by a working Flannel control loop; fabricating it can conceal the root failure and produce an inconsistent network.

## 4. Post-Recovery Verification Order

| Order | Check | Success criterion |
|---:|---|---|
| 1 | Control plane | `kubectl get --raw=/readyz?verbose` returns `ok`. |
| 2 | Node | `kubectl get nodes` is `Ready` and uses the intended private non-loopback IP. |
| 3 | CNI/core pods | CoreDNS, metrics-server, and local-path-provisioner are `Running`/`Ready`; `/run/flannel/subnet.env` exists. |
| 4 | Pod-to-pod and DNS | A temporary toolbox pod resolves `kubernetes.default.svc` and reaches the API service. |
| 5 | NetworkPolicy | A default-deny test pod cannot reach an unallowed endpoint; explicitly allowed DNS works. |
| 6 | Helm profile | `helm upgrade --install ... --wait` succeeds and CRUSH deployments reach desired availability. |
| 7 | Service smoke tests | Port-forward decision and analytics services; test `/healthz` and the authenticated API contract with synthetic data. |

## 5. Go Coverage Analysis — 69.4% Statements

The report indicates strong coverage of the central decision path, not production-complete coverage of all boundary conditions. The most valuable remaining tests are safety and state-integrity cases rather than merely chasing a percentage.

| Area | Current evidence | Coverage | Priority improvement | Value |
|---|---|---:|---|---|
| `Evaluate` and risk fusion | Hard stop, model degradation, and clinical ordering scenarios run; hash and intent construction are covered. | 93.8% / 95.2% | Test empty/all-abstained scores, one-model interval, MED/HIGH/CRITICAL tiers, score clamping, precise tie/threshold values, and deterministic time. | Prevents routing/tier regressions. |
| Input validation | Only selected valid inputs and header errors run. | 54.5% | Table-driven tests for each missing/blank tenant, claim ID, purpose, short idempotency key, absent feature hash, and absent definition version. | Stops malformed or non-reproducible decisions. |
| Deterministic rules | Exclusion and delivery-before-order run. | 60.5% | Cover preclusion, death-date parsing/after-death, timely filing boundary, invalid service/order/delivery dates, duplicate reason/rule de-duplication, and stable ordering. | Highest integrity-control priority. |
| Router | Hard stop, degraded payment, and one review scenario run. | 60.0% | Assert each action branch: low pay, medium document request, high review, critical review, clinical review, and degraded rule-only behavior with a rule fired. | Prevents accidental adverse-action or wrong workflow routing. |
| HTTP API | Main creation/retrieval happy path plus selected missing-header/tenant cases run. | 75.0% create / 50.0% get | Test `/healthz`, content type variants, body size, malformed/unknown fields, evaluator validation errors, not found, missing read tenant/purpose, and request ID fallback/propagation. | Hardens gateway-facing contract. |
| In-memory store | Happy append/find/get and partial append validation run. | 75.0% append | Test an invalid predecessor and idempotency-key re-use with divergent response to prove conflict protection. | Protects per-tenant hash-chain and replay guarantees. |
| TigerBeetle intent boundary | No direct tests. | 0% | Test invalid amount/currency, equal/missing accounts, rejected unapproved reserve hold, allowed human-approved reserve, and `NoopPoster` propagation. | Financial-control boundary requires explicit evidence. |
| Mojaloop settlement boundary | No direct tests. | 0% | Table-test every missing authorization/case/tenant/payee/amount/currency field; assert disabled adapter never sends settlement. | Confirms payment execution is impossible by default. |
| Process entry point | No direct tests. | 0% | Keep `main` out of a quality gate; test through `httptest`/container smoke tests rather than unit-testing `ListenAndServe`. | Avoids low-value coverage gaming. |

### Go Priority Order

1. Add table-driven tests for all rule and routing boundary conditions.
2. Add financial/settlement adapter guard tests; both currently show 0% and carry high risk despite small size.
3. Add repository conflict/hash predecessor cases.
4. Extend HTTP contract tests for negative paths and tracing.
5. Add a `go test -race ./...` gate and set an initial **80% coverage floor only for `internal/decision`, `internal/ledger`, `internal/payments`, and `internal/store`**. Do not set a global target that pressures the process entry point or generated adapters.

## 6. TypeScript Case Policy Analysis — 94.95% Lines, 52.94% Branches

The impressive line/function score is narrow: it applies only to `casePolicy.ts` loaded by the two tests. The untested branches are the important state-machine alternatives, and `caseWorkflow.ts` plus `worker.ts` are excluded because no Temporal test environment was used.

| Area | Current evidence | Gap | Priority improvement | Value |
|---|---|---|---|---|
| `initialState` | Suspension task initializes to `UNDER_REVIEW`. | `PREPAY_DOC_REQUEST` and `REFER` branches untested. | Test all five `RecommendedAction` variants and expected initial state. | Completes deterministic intake routing. |
| `targetStateFor` | Suspension target is covered. | Four action branches untested. | Parameterize all requested action → target state mappings. | Prevents incorrect case terminal/provisional mapping. |
| `isHumanApprovalRequired` | Exercised indirectly for suspension. | False branch and `ADVERSE_ACTION_RECOMMENDED` branch are untested. | Test each state predicate explicitly. | Protects policy semantics. |
| `validateTransition` | Missing approval rejection is covered. | Terminal state, unchanged-state, and complete-approval branches are untested. | Test every failure mode and the acceptance path. | Critical: these define legal transition validity. |
| `approvalEvent` | Approved suspension preserves identity/rationale. | Non-adverse transition has no approval and uses its timestamp fallback; terminal/invalid errors are untested. | Inject time or pass explicit test approval; test valid `DOCS_REQUESTED`/`REFERRED`, invalid terminal and same-state paths. | Removes non-deterministic and branch gaps. |
| Temporal workflow | Not covered. | Signal, deadline timeout, timer race, history/replay, cancellation, and worker registration are untested. | Use Temporal TypeScript testing with time skipping and a test worker. | Required before claiming durable due-process orchestration. |

### TypeScript Priority Order

1. Add parameterized pure-policy tests that cover all action mappings and transition rules; this should substantially raise branch coverage without infrastructure.
2. Refactor `approvalEvent` to accept an injected clock or require the workflow to supply `at`. This removes the ambient `new Date()` fallback from deterministic policy code.
3. Add Temporal workflow tests for human approval, deadline escalation, approval just before deadline, invalid/malformed signal inputs, workflow retry/replay, and cancellation.
4. Add worker integration checks that confirm a task queue can load the compiled workflow bundle, but keep infrastructure tests separate from pure-policy coverage gates.
5. Set **90% branch coverage** for `casePolicy.ts` after the parameterized tests land. Do not report that target as coverage for `caseWorkflow.ts`/`worker.ts`; report workflow test status independently.

## References

[1]: https://docs.k3s.io/networking/basic-network-options "K3s Basic Network Options"
[2]: https://docs.k3s.io/cli/server "K3s Server CLI"
[3]: https://docs.k3s.io/installation/requirements "K3s Installation Requirements"
