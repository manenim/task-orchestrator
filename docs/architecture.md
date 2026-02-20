# System Architecture

## High-Level Overview

```mermaid
graph TB
    subgraph Clients
        SDK["Go SDK<br/>pkg/client"]
        Browser["Browser<br/>gRPC-Web"]
    end

    subgraph Proxy
        Envoy["Envoy<br/>:8080"]
    end

    subgraph Server[":50051 gRPC Server"]
        Orch["OrchestratorService<br/>api.v1"]
        CP["ControlPlaneService<br/>orchestrator.v1"]
        SM["StateManager"]
        Disp["Dispatcher"]
        WM["WorkerManager"]
    end

    subgraph Workers
        W1["Worker 1<br/>pkg/worker"]
        W2["Worker N<br/>pkg/worker"]
    end

    subgraph Storage
        PG["PostgreSQL"]
        Redis["Redis"]
        Mem["In-Memory"]
    end

    SDK --> Orch
    Browser --> Envoy --> CP
    Orch --> WM
    CP --> WM
    SM -->|poll eligible| Orch
    SM -->|enqueue| Disp
    Disp -->|stream| W1
    Disp -->|stream| W2
    W1 -->|CompleteTask| Orch
    W2 -->|CompleteTask| Orch
    Orch --> PG
    Orch --> Redis
    Orch --> Mem
    CP --> PG
```

## Task Lifecycle (State Machine)

```mermaid
stateDiagram-v2
    [*] --> PENDING: SubmitTask
    PENDING --> SCHEDULED: StateManager poll
    SCHEDULED --> RUNNING: Dispatcher assigns
    SCHEDULED --> PENDING: Reschedule
    RUNNING --> COMPLETED: Worker success
    RUNNING --> FAILED: Worker fatal error
    RUNNING --> PENDING: Worker retryable error
    PENDING --> CANCELLED: CancelTask
    SCHEDULED --> CANCELLED: CancelTask
    RUNNING --> CANCELLED: CancelTask
    COMPLETED --> [*]
    FAILED --> [*]
    CANCELLED --> [*]
```

## Data Flow (CQRS)

```mermaid
flowchart LR
    subgraph Write["Write Path"]
        Submit["SubmitTask"] --> Repo["TaskRepository"]
        Complete["CompleteTask"] --> Repo
        Cancel["CancelTask"] --> Repo
    end
    subgraph Read["Read Path"]
        List["ListTasks"] --> Repo
        Get["GetTask"] --> Repo
        Stats["GetClusterStats"] --> WM["WorkerManager"]
    end
    subgraph Stream["Event Stream"]
        Pub["PublishTaskEvent"] --> Sub["StreamTaskEvents"]
    end
    Write --> Pub
```

## Directory Structure

```
task-orchestrator/
├── api/proto/                  # Protobuf definitions
│   ├── v1/orchestrator.proto   # Worker-plane API
│   └── orchestrator/v1/        # Control-plane API
├── cmd/
│   ├── server/                 # Server entrypoint
│   ├── worker/                 # Worker entrypoint (uses pkg/worker)
│   ├── client/                 # Test utilities (uses pkg/client)
│   ├── migrate/                # DB migration tool
│   └── seed/                   # Sample data seeder
├── deploy/envoy/               # Envoy proxy config
├── internal/
│   ├── domain/                 # Task, TaskState, errors
│   ├── port/                   # TaskRepository, Logger interfaces
│   ├── adapter/                # Implementations (postgres, redis, memory, zap)
│   └── service/                # Orchestrator, ControlPlane, Dispatcher, StateManager
├── pkg/
│   ├── api/                    # Generated protobuf Go code
│   ├── worker/                 # Embeddable worker library
│   └── client/                 # Go SDK
└── explainer/                  # Phase-by-phase design docs
```
