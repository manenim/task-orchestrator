CREATE TABLE tasks (
    id UUID PRIMARY KEY,
    client_id TEXT NOT NULL,
    task_type TEXT NOT NULL,
    payload BYTEA NOT NULL,
    state TEXT NOT NULL,
    run_at TIMESTAMPTZ NOT NULL,
    worker_id TEXT,
    result BYTEA,
    retry_count INT NOT NULL DEFAULT 0,
    max_retries INT NOT NULL,
    timeout_seconds INT NOT NULL DEFAULT 0,
    last_failed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_tasks_state_run_at ON tasks(state, run_at);