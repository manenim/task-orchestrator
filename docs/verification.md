# Verification evidence

Verification is recorded for the source change, not for a public production deployment. No throughput, latency, uptime, or high-availability benchmark is claimed.

## Local checks (2026-10-03)

Environment: Go 1.27.0 on Linux; isolated PostgreSQL 16.15 and Redis 7.0.15 listening only on loopback. These services were extracted into a temporary directory without installing system packages.

```bash
go vet ./...
TEST_DATABASE_URL='postgres://USER@127.0.0.1:PORT/orchestrator_test?sslmode=disable' \
TEST_REDIS_ADDR=127.0.0.1:PORT go test -race ./... -count=1
kubectl kustomize deploy/kubernetes
bash -n scripts/verify-kubernetes.sh
```

Backend tests use dedicated disposable services. They are skipped if environment variables are absent; CI sets both variables so a broken connection fails rather than skips.

Regression tests were observed failing before their fixes: retry-budget handling/validation, no-worker requeue, queue-full shutdown, memory snapshot isolation, PostgreSQL error/version persistence, stale-worker completion, payload/result opt-in flags, control-plane cancellation/log retrieval, and Redis terminal-state preservation. A real streaming test verifies worker disconnect followed by reassignment and completion. A separate local smoke run started the server and worker as real processes, observed a completed result, restarted the server, and verified the same stored result without resubmitting.

The local Kubernetes cluster could not be reached and the Docker daemon was unavailable to this user. Rendering manifests and checking shell syntax do not prove a rollout. The `Kubernetes Restart + Readiness` CI job builds the image and runs an actual disposable kind cluster; its result is the deployment evidence to inspect before merging.

## CI checks

`.github/workflows/ci.yml` defines two independent jobs:

- `Go Vet + Race + Storage Recovery`: formatting, vet, and the complete race-enabled suite with PostgreSQL 16 and Redis 7 services.
- `Kubernetes Restart + Readiness`: image build, migrations, native gRPC probes, task execution, server restart, storage outage, readiness recovery, and persisted-result verification in kind.

A passing unit suite alone does not establish Kubernetes rollout, backend durability under host loss, multi-server safety, or production hardening. See [recovery limits](runbook.md#recovery-guarantees-and-limits).
