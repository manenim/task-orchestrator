-- name: CreateTask :exec
INSERT INTO tasks (
    id, client_id, task_type, payload, state, run_at, 
    worker_id, result, retry_count, max_retries, 
    timeout_seconds, last_failed_at, created_at, updated_at
) VALUES (
    $1, $2, $3, $4, $5, $6, 
    $7, $8, $9, $10, 
    $11, $12, $13, $14
);
-- name: GetTask :one
SELECT * FROM tasks
WHERE id = $1 LIMIT 1;
-- name: UpdateTask :exec
UPDATE tasks
SET 
    state = $2,
    worker_id = $3,
    result = $4,
    retry_count = $5,
    last_failed_at = $6,
    run_at = $7,
    updated_at = NOW()
WHERE id = $1;
-- name: ListEligibleTasks :many
SELECT * FROM tasks
WHERE state IN ('PENDING', 'SCHEDULED')
  AND run_at <= $1
ORDER BY run_at ASC
LIMIT $2;
-- name: ReleaseTasks :exec
UPDATE tasks
SET 
    state = 'PENDING',
    worker_id = NULL,
    updated_at = NOW()
WHERE worker_id = $1 
  AND state IN ('RUNNING', 'SCHEDULED');