package service

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/manenim/task-orchestrator/internal/domain"
	"github.com/manenim/task-orchestrator/internal/port"
	cpb "github.com/manenim/task-orchestrator/pkg/api/orchestrator/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	defaultPageSize    = 50
	maxPageSize        = 500
	defaultStreamBatch = 100
)

type streamSubscriber struct {
	ch chan *cpb.TaskEvent
}

type ControlPlane struct {
	cpb.UnimplementedControlPlaneServiceServer

	repo          port.TaskRepository
	logger        port.Logger
	workerManager *WorkerManager
	pool          *pgxpool.Pool

	defaultScope *cpb.Scope

	subscribersMu sync.RWMutex
	subscribers   map[int64]streamSubscriber
	subscriberSeq int64
}

func NewControlPlane(
	repo port.TaskRepository,
	logger port.Logger,
	workerManager *WorkerManager,
	pool *pgxpool.Pool,
	defaultTenant string,
	defaultNamespace string,
) *ControlPlane {
	if defaultTenant == "" {
		defaultTenant = "default"
	}
	if defaultNamespace == "" {
		defaultNamespace = "default"
	}

	return &ControlPlane{
		repo:          repo,
		logger:        logger,
		workerManager: workerManager,
		pool:          pool,
		defaultScope: &cpb.Scope{
			TenantId:    defaultTenant,
			NamespaceId: defaultNamespace,
		},
		subscribers: make(map[int64]streamSubscriber),
	}
}

func (s *ControlPlane) Init(ctx context.Context) error {
	if s.pool == nil {
		return nil
	}

	const ddl = `
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
`

	_, err := s.pool.Exec(ctx, ddl)
	if err != nil {
		return fmt.Errorf("init control-plane schema: %w", err)
	}

	return nil
}

func (s *ControlPlane) normalizeScope(scope *cpb.Scope) *cpb.Scope {
	if scope == nil {
		return &cpb.Scope{TenantId: s.defaultScope.TenantId, NamespaceId: s.defaultScope.NamespaceId}
	}

	tenant := scope.GetTenantId()
	namespace := scope.GetNamespaceId()
	if tenant == "" {
		tenant = s.defaultScope.TenantId
	}
	if namespace == "" {
		namespace = s.defaultScope.NamespaceId
	}

	return &cpb.Scope{TenantId: tenant, NamespaceId: namespace}
}

func mapDomainStateToProto(state domain.TaskState) cpb.TaskState {
	switch state {
	case domain.Pending:
		return cpb.TaskState_TASK_STATE_PENDING
	case domain.Scheduled:
		return cpb.TaskState_TASK_STATE_SCHEDULED
	case domain.Running:
		return cpb.TaskState_TASK_STATE_RUNNING
	case domain.Completed:
		return cpb.TaskState_TASK_STATE_COMPLETED
	case domain.Failed:
		return cpb.TaskState_TASK_STATE_FAILED
	case domain.Cancelled:
		return cpb.TaskState_TASK_STATE_CANCELLED
	default:
		return cpb.TaskState_TASK_STATE_UNSPECIFIED
	}
}

func mapProtoStateToDomain(state cpb.TaskState) domain.TaskState {
	switch state {
	case cpb.TaskState_TASK_STATE_PENDING:
		return domain.Pending
	case cpb.TaskState_TASK_STATE_SCHEDULED:
		return domain.Scheduled
	case cpb.TaskState_TASK_STATE_RUNNING:
		return domain.Running
	case cpb.TaskState_TASK_STATE_COMPLETED:
		return domain.Completed
	case cpb.TaskState_TASK_STATE_FAILED:
		return domain.Failed
	case cpb.TaskState_TASK_STATE_CANCELLED:
		return domain.Cancelled
	default:
		return domain.Pending
	}
}

func mapDBStateToProto(state string) cpb.TaskState {
	switch strings.ToUpper(state) {
	case "PENDING":
		return cpb.TaskState_TASK_STATE_PENDING
	case "SCHEDULED":
		return cpb.TaskState_TASK_STATE_SCHEDULED
	case "RUNNING":
		return cpb.TaskState_TASK_STATE_RUNNING
	case "COMPLETED":
		return cpb.TaskState_TASK_STATE_COMPLETED
	case "FAILED":
		return cpb.TaskState_TASK_STATE_FAILED
	case "CANCELLED":
		return cpb.TaskState_TASK_STATE_CANCELLED
	default:
		return cpb.TaskState_TASK_STATE_UNSPECIFIED
	}
}

func mapProtoStateToDB(state cpb.TaskState) string {
	switch state {
	case cpb.TaskState_TASK_STATE_PENDING:
		return "PENDING"
	case cpb.TaskState_TASK_STATE_SCHEDULED:
		return "SCHEDULED"
	case cpb.TaskState_TASK_STATE_RUNNING:
		return "RUNNING"
	case cpb.TaskState_TASK_STATE_COMPLETED:
		return "COMPLETED"
	case cpb.TaskState_TASK_STATE_FAILED:
		return "FAILED"
	case cpb.TaskState_TASK_STATE_CANCELLED:
		return "CANCELLED"
	default:
		return "UNSPECIFIED"
	}
}

func mapEventTypeToString(eventType cpb.TaskEventType) string {
	switch eventType {
	case cpb.TaskEventType_TASK_EVENT_TYPE_CREATED:
		return "CREATED"
	case cpb.TaskEventType_TASK_EVENT_TYPE_STATE_CHANGED:
		return "STATE_CHANGED"
	case cpb.TaskEventType_TASK_EVENT_TYPE_RETRIED:
		return "RETRIED"
	case cpb.TaskEventType_TASK_EVENT_TYPE_CANCEL_REQUESTED:
		return "CANCEL_REQUESTED"
	case cpb.TaskEventType_TASK_EVENT_TYPE_ASSIGNMENT_CHANGED:
		return "ASSIGNMENT_CHANGED"
	case cpb.TaskEventType_TASK_EVENT_TYPE_HEARTBEAT:
		return "HEARTBEAT"
	default:
		return "UNSPECIFIED"
	}
}

func derefString(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

type listCursor struct {
	Offset int `json:"offset"`
}

func encodeCursor(offset int) string {
	payload, _ := json.Marshal(listCursor{Offset: offset})
	return base64.RawURLEncoding.EncodeToString(payload)
}

func decodeCursor(token string) (int, error) {
	if token == "" {
		return 0, nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return 0, fmt.Errorf("invalid cursor: %w", err)
	}
	var cursor listCursor
	if err := json.Unmarshal(raw, &cursor); err != nil {
		return 0, fmt.Errorf("invalid cursor payload: %w", err)
	}
	if cursor.Offset < 0 {
		return 0, fmt.Errorf("invalid cursor offset")
	}
	return cursor.Offset, nil
}

func sanitizePageSize(pageSize int32) int {
	switch {
	case pageSize <= 0:
		return defaultPageSize
	case pageSize > maxPageSize:
		return maxPageSize
	default:
		return int(pageSize)
	}
}

func (s *ControlPlane) recordTaskLog(ctx context.Context, taskID string, level cpb.LogLevel, component string, message string, fields map[string]string) {
	if s.pool == nil || taskID == "" {
		return
	}
	if component == "" {
		component = "control-plane"
	}
	if fields == nil {
		fields = map[string]string{}
	}

	data, err := json.Marshal(fields)
	if err != nil {
		s.logger.Error("failed to marshal task log fields", err)
		return
	}

	_, err = s.pool.Exec(ctx, `
		INSERT INTO task_logs(task_id, level, component, message, fields)
		VALUES ($1, $2, $3, $4, $5::jsonb)
	`, taskID, level.String(), component, message, string(data))
	if err != nil {
		s.logger.Error("failed to insert task log", err, port.String("task_id", taskID))
	}
}

func (s *ControlPlane) insertEvent(ctx context.Context, event *cpb.TaskEvent) error {
	if s.pool == nil {
		if event.EventId == 0 {
			event.EventId = time.Now().UnixNano()
		}
		if event.OccurredAt == nil {
			event.OccurredAt = timestamppb.Now()
		}
		return nil
	}

	if event.OccurredAt == nil {
		event.OccurredAt = timestamppb.Now()
	}

	var eventID int64
	var occurredAt time.Time

	err := s.pool.QueryRow(ctx, `
		INSERT INTO task_events (
			event_type, task_id, previous_state, current_state, task_version, worker_id, reason, occurred_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		RETURNING event_id, occurred_at
	`,
		mapEventTypeToString(event.GetEventType()),
		event.GetTaskId(),
		mapProtoStateToDB(event.GetPreviousState()),
		mapProtoStateToDB(event.GetCurrentState()),
		event.GetTaskVersion(),
		event.GetWorkerId(),
		event.GetReason(),
		event.GetOccurredAt().AsTime(),
	).Scan(&eventID, &occurredAt)
	if err != nil {
		return err
	}

	event.EventId = eventID
	event.OccurredAt = timestamppb.New(occurredAt)
	return nil
}

func (s *ControlPlane) publish(event *cpb.TaskEvent) {
	s.subscribersMu.RLock()
	defer s.subscribersMu.RUnlock()

	for _, subscriber := range s.subscribers {
		select {
		case subscriber.ch <- event:
		default:
		}
	}
}

func (s *ControlPlane) PublishTaskEvent(
	ctx context.Context,
	eventType cpb.TaskEventType,
	previousState domain.TaskState,
	task *domain.Task,
	reason string,
) {
	if task == nil {
		return
	}

	scope := &cpb.Scope{TenantId: s.defaultScope.TenantId, NamespaceId: s.defaultScope.NamespaceId}

	taskSnapshot := mapDomainTaskToProto(scope, task, true, true)

	event := &cpb.TaskEvent{
		OccurredAt:    timestamppb.Now(),
		EventType:     eventType,
		Scope:         scope,
		TaskId:        task.ID,
		PreviousState: mapDomainStateToProto(previousState),
		CurrentState:  mapDomainStateToProto(task.State),
		TaskVersion:   int64(task.Version),
		WorkerId:      task.WorkerID,
		Reason:        reason,
		TaskSnapshot:  taskSnapshot,
	}

	if err := s.insertEvent(ctx, event); err != nil {
		s.logger.Error("failed to persist task event", err, port.String("task_id", task.ID))
		return
	}

	if reason != "" {
		level := cpb.LogLevel_LOG_LEVEL_INFO
		if task.State == domain.Failed {
			level = cpb.LogLevel_LOG_LEVEL_ERROR
		}
		s.recordTaskLog(ctx, task.ID, level, "orchestrator", reason, map[string]string{
			"event_type": eventType.String(),
		})
	}

	s.publish(event)
}

func (s *ControlPlane) PublishWorkerEvent(ctx context.Context, workerID string, connected bool) {
	stateReason := "worker joined"
	if !connected {
		stateReason = "worker left"
	}

	event := &cpb.TaskEvent{
		OccurredAt:    timestamppb.Now(),
		EventType:     cpb.TaskEventType_TASK_EVENT_TYPE_ASSIGNMENT_CHANGED,
		Scope:         &cpb.Scope{TenantId: s.defaultScope.TenantId, NamespaceId: s.defaultScope.NamespaceId},
		TaskId:        fmt.Sprintf("worker:%s", workerID),
		PreviousState: cpb.TaskState_TASK_STATE_UNSPECIFIED,
		CurrentState:  cpb.TaskState_TASK_STATE_UNSPECIFIED,
		TaskVersion:   0,
		WorkerId:      workerID,
		Reason:        stateReason,
	}
	if err := s.insertEvent(ctx, event); err != nil {
		s.logger.Error("failed to persist worker event", err, port.String("worker_id", workerID))
		return
	}
	s.publish(event)
}

func (s *ControlPlane) ListTasks(ctx context.Context, req *cpb.ListTasksRequest) (*cpb.ListTasksResponse, error) {
	scope := s.normalizeScope(req.GetScope())
	pageSize := sanitizePageSize(req.GetPageSize())
	offset, err := decodeCursor(req.GetCursor())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	filter := &domain.TaskFilter{
		Limit:  pageSize + 1,
		Offset: offset,
	}

	if reqFilter := req.GetFilter(); reqFilter != nil {
		if len(reqFilter.GetStates()) > 0 {
			filter.States = make([]domain.TaskState, len(reqFilter.GetStates()))
			for i, state := range reqFilter.GetStates() {
				filter.States[i] = mapProtoStateToDomain(state)
			}
		}

		filter.TaskTypes = reqFilter.GetTaskTypes()
		filter.WorkerID = reqFilter.GetWorkerId()
		filter.TaskIDPrefix = reqFilter.GetTaskIdPrefix()
		filter.TextQuery = reqFilter.GetTextQuery()

		if tr := reqFilter.GetCreatedAt(); tr != nil {
			filter.CreatedAt = &domain.TimeRange{}
			if tr.Start != nil {
				filter.CreatedAt.Start = tr.Start.AsTime()
			}
			if tr.End != nil {
				filter.CreatedAt.End = tr.End.AsTime()
			}
		}

		if tr := reqFilter.GetUpdatedAt(); tr != nil {
			filter.UpdatedAt = &domain.TimeRange{}
			if tr.Start != nil {
				filter.UpdatedAt.Start = tr.Start.AsTime()
			}
			if tr.End != nil {
				filter.UpdatedAt.End = tr.End.AsTime()
			}
		}

		if tr := reqFilter.GetRunAt(); tr != nil {
			filter.RunAt = &domain.TimeRange{}
			if tr.Start != nil {
				filter.RunAt.Start = tr.Start.AsTime()
			}
			if tr.End != nil {
				filter.RunAt.End = tr.End.AsTime()
			}
		}
	}

	switch req.GetSort().GetField() {
	case cpb.TaskSortField_TASK_SORT_FIELD_CREATED_AT:
		filter.SortBy = domain.SortByCreatedAt
	case cpb.TaskSortField_TASK_SORT_FIELD_RUN_AT:
		filter.SortBy = domain.SortByRunAt
	case cpb.TaskSortField_TASK_SORT_FIELD_UPDATED_AT:
		filter.SortBy = domain.SortByUpdatedAt
	default:
		filter.SortBy = domain.SortByUpdatedAt
	}

	if req.GetSort().GetDirection() == cpb.SortDirection_SORT_DIRECTION_ASC {
		filter.SortDir = domain.SortAsc
	} else {
		filter.SortDir = domain.SortDesc
	}

	tasks, err := s.repo.ListTasks(ctx, filter)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to list tasks: %v", err)
	}

	hasMore := false
	if len(tasks) > pageSize {
		hasMore = true
		tasks = tasks[:pageSize]
	}

	nextCursor := ""
	if hasMore {
		nextCursor = encodeCursor(offset + pageSize)
	}

	pbTasks := make([]*cpb.Task, len(tasks))
	for i, t := range tasks {
		pbTasks[i] = mapDomainTaskToProto(scope, t, req.IncludePayload, req.IncludeResult)
	}

	var snapshotEventID int64
	if s.pool != nil {
		_ = s.pool.QueryRow(ctx, "SELECT MAX(event_id) FROM task_events").Scan(&snapshotEventID)
	}

	return &cpb.ListTasksResponse{
		Tasks:           pbTasks,
		NextCursor:      nextCursor,
		HasMore:         hasMore,
		SnapshotTime:    timestamppb.Now(),
		SnapshotEventId: snapshotEventID,
	}, nil
}

func (s *ControlPlane) GetTask(ctx context.Context, req *cpb.GetTaskRequest) (*cpb.GetTaskResponse, error) {
	if req.GetTaskId() == "" {
		return nil, status.Error(codes.InvalidArgument, "task_id is required")
	}

	task, err := s.repo.Get(ctx, req.GetTaskId())
	if err != nil {
		if err == domain.ErrTaskNotFound {
			return nil, status.Error(codes.NotFound, "task not found")
		}
		return nil, status.Errorf(codes.Internal, "failed to get task: %v", err)
	}

	scope := s.normalizeScope(req.GetScope())
	pbTask := mapDomainTaskToProto(scope, task, true, true)

	return &cpb.GetTaskResponse{
		Task: pbTask,
	}, nil
}

func mapDomainTaskToProto(scope *cpb.Scope, task *domain.Task, includePayload, includeResult bool) *cpb.Task {
	attributes := map[string]string{
		"client_id":       task.ClientID,
		"timeout_seconds": strconv.FormatInt(int64(task.TimeoutSeconds), 10),
	}

	pbTask := &cpb.Task{
		Scope:        scope,
		TaskId:       task.ID,
		TaskType:     task.Type,
		State:        mapDomainStateToProto(task.State),
		WorkerId:     task.WorkerID,
		RetryCount:   int32(task.RetryCount),
		MaxRetries:   int32(task.MaxRetries),
		RunAt:        timestamppb.New(task.RunAt),
		CreatedAt:    timestamppb.New(task.CreatedAt),
		UpdatedAt:    timestamppb.New(task.UpdatedAt),
		Version:      int64(task.Version),
		ErrorMessage: task.ErrorMessage,
		Attributes:   attributes,
	}

	if !task.LastFailedAt.IsZero() {
		pbTask.LastFailedAt = timestamppb.New(task.LastFailedAt)
	}

	if includePayload {
		pbTask.Payload = task.Payload
	}
	if includeResult {
		pbTask.Result = task.Result
	}

	return pbTask
}

func (s *ControlPlane) StreamTaskEvents(req *cpb.StreamTaskEventsRequest, stream cpb.ControlPlaneService_StreamTaskEventsServer) error {
	ch := make(chan *cpb.TaskEvent, defaultStreamBatch)
	s.subscribersMu.Lock()
	s.subscriberSeq++
	id := s.subscriberSeq
	s.subscribers[id] = streamSubscriber{ch: ch}
	s.subscribersMu.Unlock()

	defer func() {
		s.subscribersMu.Lock()
		delete(s.subscribers, id)
		close(ch)
		s.subscribersMu.Unlock()
	}()

	for {
		select {
		case <-stream.Context().Done():
			return nil
		case event := <-ch:
			resp := &cpb.StreamTaskEventsResponse{
				Events: []*cpb.TaskEvent{event},
			}
			if err := stream.Send(resp); err != nil {
				return err
			}
		}
	}
}

func (s *ControlPlane) GetClusterStats(ctx context.Context, req *cpb.GetClusterStatsRequest) (*cpb.GetClusterStatsResponse, error) {
	if s.pool != nil {
		var pending, running, completed, failed int32

		_ = s.pool.QueryRow(ctx, "SELECT COUNT(*) FROM tasks WHERE state = 'PENDING'").Scan(&pending)
		_ = s.pool.QueryRow(ctx, "SELECT COUNT(*) FROM tasks WHERE state = 'RUNNING'").Scan(&running)
		_ = s.pool.QueryRow(ctx, "SELECT COUNT(*) FROM tasks WHERE state = 'COMPLETED' AND updated_at > NOW() - INTERVAL '24 hours'").Scan(&completed)
		_ = s.pool.QueryRow(ctx, "SELECT COUNT(*) FROM tasks WHERE state = 'FAILED' AND updated_at > NOW() - INTERVAL '24 hours'").Scan(&failed)

		activeWorkers := int64(s.workerManager.Count())

		return &cpb.GetClusterStatsResponse{
			ActiveWorkers: activeWorkers,
			CurrentStateCounts: []*cpb.StateCount{
				{State: cpb.TaskState_TASK_STATE_PENDING, Count: int64(pending)},
				{State: cpb.TaskState_TASK_STATE_RUNNING, Count: int64(running)},
			},
			CompletedInWindow: int64(completed),
			FailedInWindow:    int64(failed),
		}, nil
	}

	return &cpb.GetClusterStatsResponse{
		ActiveWorkers: int64(s.workerManager.Count()),
	}, nil
}
