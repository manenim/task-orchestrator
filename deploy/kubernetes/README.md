# Disposable Kubernetes demonstration

This example uses one orchestrator server, two sample workers, PostgreSQL 16 with a 1 GiB PVC, and an init container that reapplies the additive schema migration. The server runs as a non-root user with a read-only root filesystem. Requests and limits are starting values for this small demo, not measured capacity recommendations.

Requirements: Docker access, kind, kubectl, and a cluster with dynamic volume provisioning. Native gRPC probes require Kubernetes 1.27 or newer ([Kubernetes probe documentation](https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-probes/)). CI installs kind v0.33.0 and executes the same script.

From the repository root:

```bash
kind create cluster --name orchestrator --wait 90s
docker build -t task-orchestrator:dev .
kind load docker-image task-orchestrator:dev --name orchestrator
scripts/verify-kubernetes.sh
```

The script creates only resources in `task-orchestrator-demo`. It uses a deliberately public `local-demo-only` password for the disposable database. It expects a fresh demo environment; for other environments supply an externally managed secret and pinned image digests. No service is exposed outside the cluster.

The script verifies:

1. PostgreSQL, server, and worker rollouts become ready.
2. A smoke task completes through the worker SDK and its result is `success`.
3. The server restarts while workers reconnect.
4. Scaling PostgreSQL to zero removes server readiness; liveness does not restart the server.
5. PostgreSQL resumes on the same PVC, readiness recovers, and the previously completed task/result remain readable.

On failure it prints pods, Kubernetes events, server logs, and smoke-job logs. `cmd/smoke` exits nonzero if health, task completion, or the expected result cannot be observed within 30 seconds. This is a correctness check, not a benchmark.

Inspect or connect locally:

```bash
kubectl -n task-orchestrator-demo get pods,pvc
kubectl -n task-orchestrator-demo logs deployment/server
kubectl -n task-orchestrator-demo logs deployment/worker
kubectl -n task-orchestrator-demo port-forward service/server 50051:50051
grpcurl -plaintext -d '{"service":"readiness"}' localhost:50051 grpc.health.v1.Health/Check
```

For a no-worker failure demonstration, scale `deployment/worker` to zero, delete/reapply `smoke.yaml`, and inspect `ListTasks`. The task should return to `PENDING` while no worker is connected. Restore two workers within 30 seconds to let the smoke check finish. The deterministic no-worker regression test also exercises this without deployment timing.

Keep `deployment/server` at exactly one replica with `Recreate`: startup recovery resets all abandoned assignments and is unsafe alongside another server. This example has no HA database, operator, public ingress, authentication, TLS, backup policy, or exactly-once execution. Scope labels are not tenant isolation. See [the runbook](../../docs/runbook.md#recovery-guarantees-and-limits) for replay and concurrency limits.

Cleanup of this disposable cluster also deletes its local persistent data:

```bash
kind delete cluster --name orchestrator
```

Go/Postgres/distroless tags select maintained image families but are not immutable; for strict supply-chain reproducibility record and pin resolved digests for your environment.
