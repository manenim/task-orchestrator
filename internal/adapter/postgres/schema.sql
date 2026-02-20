CREATE TABLE IF NOT EXISTS tasks (
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

CREATE INDEX IF NOT EXISTS idx_tasks_state_run_at ON tasks(state, run_at);

CREATE TABLE IF NOT EXISTS task_events (
    event_id BIGSERIAL PRIMARY KEY,
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    event_type TEXT NOT NULL,
    task_id TEXT NOT NULL,
    previous_state TEXT NOT NULL,
    current_state TEXT NOT NULL,
    task_version BIGINT NOT NULL DEFAULT 0,
    worker_id TEXT NOT NULL DEFAULT '',
    reason TEXT NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_task_events_task_id ON task_events(task_id);
CREATE INDEX IF NOT EXISTS idx_task_events_occurred_at ON task_events(occurred_at DESC);

CREATE TABLE IF NOT EXISTS task_logs (
    sequence BIGSERIAL PRIMARY KEY,
    task_id TEXT NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    level TEXT NOT NULL,
    component TEXT NOT NULL,
    message TEXT NOT NULL,
    fields JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_task_logs_task_sequence ON task_logs(task_id, sequence DESC);
