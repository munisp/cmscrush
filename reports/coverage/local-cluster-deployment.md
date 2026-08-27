# Local Kubernetes Deployment Attempt — CRUSH Foundation

**Date:** 2026-08-27  
**Target:** transient, single-node K3s local development cluster  
**Chart:** `deploy/helm/crush-platform`

## Outcome

The Helm chart passed `helm lint` with no failures and rendered successfully into `helm-rendered.yaml`. K3s installed, its node reported `Ready`, and its API briefly listened on TCP 6443. The deployment could not be completed or verified because the sandbox host exposes only a link-local primary interface (`169.254.0.21`) and prevents creation of a private dummy interface. Flannel consequently did not create `/run/flannel/subnet.env`; all core pods remained in `ContainerCreating`, and the K3s API subsequently returned unavailable/refused responses.

| Step | Result | Evidence |
|---|---|---|
| Install K3s | Partially successful | K3s `v1.36.3+k3s1` installed. |
| Configure node address | Partially successful | Private loopback `10.254.0.1` allowed the control-plane process to reach a transient `Ready` node state. |
| Establish CNI | Blocked by sandbox network capability | CoreDNS, metrics-server, and local-path-provisioner could not create sandboxes because Flannel’s `subnet.env` was missing. |
| Create a non-loopback dummy interface | Blocked by host capability | The `crush0` device could not be created. |
| Install Helm | Successful | Helm `v4.2.4` installed. |
| Lint Helm chart | Successful | `1 chart(s) linted, 0 chart(s) failed`. |
| Render Helm chart | Successful | Rendered manifest saved as `reports/coverage/helm-rendered.yaml`. |
| Server-side manifest validation/deployment | Blocked | Kubernetes OpenAPI endpoint was unavailable; chart was not applied. |
| Service readiness verification | Not possible | No application workloads were created because the cluster CNI/API was not operational. |

## Diagnostic Evidence

The material log signatures were:

```text
failed to load flannel 'subnet.env' file: open /run/flannel/subnet.env: no such file or directory
plugin type="flannel" failed (add)
The server is currently unable to handle the request
```

This was an **environmental infrastructure limitation**, not a Helm lint/render failure or an application test failure. No application pod, real external platform dependency, production credential, or healthcare data was deployed.

## Required Environment for the Next Deployment Attempt

A local host with Docker/Kind, Minikube, or K3s operating with a routable private non-loopback network interface is sufficient for the initial chart. To validate actual CRUSH service readiness, the environment must also have loadable local images (or accessible immutable image references) for `decision-service` and `analytics-query`; installed CRDs/operators for Flink and Ray; and separately provisioned, synthetic-data-only test instances of Kafka, Redis, Postgres, Temporal, Keycloak, APISIX, Dapr, TigerBeetle, object storage, OpenSearch, Wazuh, OpenCTI, open-appsec, and Kubecost.

The chart’s `values.yaml` already marks these as external dependencies. The current foundation does not claim that a full regulated-platform deployment can be verified without those intentionally separate systems.
