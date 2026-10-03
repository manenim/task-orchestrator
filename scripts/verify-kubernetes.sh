#!/usr/bin/env bash
# Run against a disposable local kind cluster with task-orchestrator:dev loaded.
set -euo pipefail
namespace=task-orchestrator-demo
on_exit() {
  status=$?
  if [ "$status" -ne 0 ]; then
    kubectl -n "$namespace" get pods
    kubectl -n "$namespace" get events --sort-by=.lastTimestamp
    kubectl -n "$namespace" logs deployment/server --all-containers --tail=80 || true
    kubectl -n "$namespace" logs job/orchestrator-smoke --tail=80 || true
  fi
  exit "$status"
}
trap on_exit EXIT
kubectl apply -f deploy/kubernetes/namespace.yaml
kubectl -n "$namespace" create secret generic orchestrator-db \
  --from-literal=password=local-demo-only \
  --from-literal=database-url='postgres://orchestrator:local-demo-only@postgres:5432/orchestrator?sslmode=disable' \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -k deploy/kubernetes
kubectl -n "$namespace" rollout status deployment/postgres --timeout=180s
kubectl -n "$namespace" rollout status deployment/server --timeout=180s
kubectl -n "$namespace" rollout status deployment/worker --timeout=180s
kubectl -n "$namespace" delete job orchestrator-smoke --ignore-not-found
kubectl apply -f deploy/kubernetes/smoke.yaml
kubectl -n "$namespace" wait --for=condition=complete job/orchestrator-smoke --timeout=90s
kubectl -n "$namespace" logs job/orchestrator-smoke
task_id=$(kubectl -n "$namespace" logs job/orchestrator-smoke | sed -n 's/.*task \([^ ]*\) completed.*/\1/p')
[ -n "$task_id" ]
# Restart the server; the completed task and result must still be readable.
kubectl -n "$namespace" rollout restart deployment/server
kubectl -n "$namespace" rollout status deployment/server --timeout=180s
# A database outage removes readiness, without liveness restarts.
server_pod=$(kubectl -n "$namespace" get pod -l app=server -o jsonpath='{.items[0].metadata.name}')
restart_count=$(kubectl -n "$namespace" get pod "$server_pod" -o jsonpath='{.status.containerStatuses[0].restartCount}')
kubectl -n "$namespace" scale deployment/postgres --replicas=0
kubectl -n "$namespace" wait --for=condition=Ready=false pod/"$server_pod" --timeout=60s
kubectl -n "$namespace" scale deployment/postgres --replicas=1
kubectl -n "$namespace" rollout status deployment/postgres --timeout=180s
kubectl -n "$namespace" wait --for=condition=Ready pod/"$server_pod" --timeout=60s
[ "$(kubectl -n "$namespace" get pod "$server_pod" -o jsonpath='{.status.containerStatuses[0].restartCount}')" = "$restart_count" ]
kubectl -n "$namespace" delete pod verify-restart --ignore-not-found
kubectl -n "$namespace" run verify-restart --restart=Never --image=task-orchestrator:dev \
  --env=SERVER_ADDR=server:50051 --env=SMOKE_VERIFY_ONLY=true --env=SMOKE_TASK_ID="$task_id" \
  --command -- /app/smoke
kubectl -n "$namespace" wait --for=jsonpath='{.status.phase}'=Succeeded pod/verify-restart --timeout=60s
kubectl -n "$namespace" logs verify-restart
printf 'PASS: Kubernetes task flow, server restart, persisted result, and readiness recovery\n'
