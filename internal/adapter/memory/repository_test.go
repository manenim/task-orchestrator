package memory

import (
	"context"
	"testing"
	"time"

	"github.com/manenim/task-orchestrator/internal/domain"
	"github.com/manenim/task-orchestrator/internal/port"
)

type noopLogger struct{}

func (noopLogger) Info(string, ...port.Field)         {}
func (noopLogger) Error(string, error, ...port.Field) {}
func (noopLogger) Sync() error                        { return nil }

func newTestRepo() *InMemoryTaskRepository {
	return New(&noopLogger{})
}

func seedTasks(t *testing.T, repo *InMemoryTaskRepository, tasks ...*domain.Task) {
	t.Helper()
	for _, task := range tasks {
		if err := repo.Create(context.Background(), task); err != nil {
			t.Fatalf("seed: failed to create task %s: %v", task.ID, err)
		}
	}
}

func TestCreate_Success(t *testing.T) {
	repo := newTestRepo()
	task := domain.NewTask("t1", "c1", "email", []byte("hi"), time.Time{}, 0)
	if err := repo.Create(context.Background(), task); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	got, err := repo.Get(context.Background(), "t1")
	if err != nil {
		t.Fatalf("Get after Create failed: %v", err)
	}
	if got.Type != "email" {
		t.Errorf("expected type 'email', got %q", got.Type)
	}
}

func TestCreate_DuplicateID(t *testing.T) {
	repo := newTestRepo()
	task := domain.NewTask("dup", "c1", "email", nil, time.Time{}, 0)
	repo.Create(context.Background(), task)

	err := repo.Create(context.Background(), domain.NewTask("dup", "c2", "sms", nil, time.Time{}, 0))
	if err == nil {
		t.Fatal("expected error on duplicate ID, got nil")
	}
}

func TestGet_NotFound(t *testing.T) {
	repo := newTestRepo()
	_, err := repo.Get(context.Background(), "nonexistent")
	if err != domain.ErrTaskNotFound {
		t.Errorf("expected ErrTaskNotFound, got %v", err)
	}
}

func TestUpdate_Success(t *testing.T) {
	repo := newTestRepo()
	task := domain.NewTask("u1", "c1", "email", nil, time.Time{}, 0)
	repo.Create(context.Background(), task)

	task.State = domain.Scheduled
	task.Version = 2
	if err := repo.Update(context.Background(), task); err != nil {
		t.Fatalf("Update failed: %v", err)
	}

	got, _ := repo.Get(context.Background(), "u1")
	if got.State != domain.Scheduled {
		t.Errorf("expected Scheduled, got %v", got.State)
	}
	if got.Version != 2 {
		t.Errorf("expected version 2, got %d", got.Version)
	}
}

func TestUpdate_NotFound(t *testing.T) {
	repo := newTestRepo()
	task := domain.NewTask("ghost", "c1", "email", nil, time.Time{}, 0)
	if err := repo.Update(context.Background(), task); err == nil {
		t.Fatal("expected error updating non-existent task")
	}
}

func TestListEligible(t *testing.T) {
	repo := newTestRepo()
	now := time.Now().UTC()

	seedTasks(t, repo,
		domain.NewTask("1", "c", "t", nil, now.Add(-1*time.Hour), 0),
		domain.NewTask("2", "c", "t", nil, now.Add(1*time.Hour), 0),
	)
	completed := domain.NewTask("3", "c", "t", nil, now.Add(-1*time.Hour), 0)
	completed.State = domain.Completed
	seedTasks(t, repo, completed)

	eligible, err := repo.ListEligible(context.Background(), now, 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(eligible) != 1 {
		t.Errorf("expected 1 eligible task, got %d", len(eligible))
	}
	if len(eligible) > 0 && eligible[0].ID != "1" {
		t.Errorf("expected task 1, got %s", eligible[0].ID)
	}
}

func TestReleaseTasks(t *testing.T) {
	repo := newTestRepo()

	t1 := domain.NewTask("r1", "c", "t", nil, time.Time{}, 0)
	t1.State = domain.Running
	t1.WorkerID = "worker-A"

	t2 := domain.NewTask("r2", "c", "t", nil, time.Time{}, 0)
	t2.State = domain.Running
	t2.WorkerID = "worker-B"

	seedTasks(t, repo, t1, t2)

	if err := repo.ReleaseTasks(context.Background(), "worker-A"); err != nil {
		t.Fatalf("ReleaseTasks failed: %v", err)
	}

	got, _ := repo.Get(context.Background(), "r1")
	if got.State != domain.Pending {
		t.Errorf("expected released task to be Pending, got %v", got.State)
	}
	if got.WorkerID != "" {
		t.Errorf("expected WorkerID to be cleared, got %q", got.WorkerID)
	}

	got2, _ := repo.Get(context.Background(), "r2")
	if got2.State != domain.Running {
		t.Errorf("worker-B task should still be Running, got %v", got2.State)
	}
}

func TestListTasks_NoFilter(t *testing.T) {
	repo := newTestRepo()
	seedTasks(t, repo,
		domain.NewTask("a", "c", "email", nil, time.Time{}, 0),
		domain.NewTask("b", "c", "sms", nil, time.Time{}, 0),
	)

	tasks, err := repo.ListTasks(context.Background(), nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(tasks) != 2 {
		t.Errorf("expected 2 tasks, got %d", len(tasks))
	}
}

func TestListTasks_FilterByState(t *testing.T) {
	repo := newTestRepo()

	pending := domain.NewTask("p1", "c", "t", nil, time.Time{}, 0)
	running := domain.NewTask("r1", "c", "t", nil, time.Time{}, 0)
	running.State = domain.Running
	completed := domain.NewTask("c1", "c", "t", nil, time.Time{}, 0)
	completed.State = domain.Completed

	seedTasks(t, repo, pending, running, completed)

	tasks, _ := repo.ListTasks(context.Background(), &domain.TaskFilter{
		States: []domain.TaskState{domain.Running},
	})
	if len(tasks) != 1 || tasks[0].ID != "r1" {
		t.Errorf("expected [r1], got %v", taskIDs(tasks))
	}

	tasks, _ = repo.ListTasks(context.Background(), &domain.TaskFilter{
		States: []domain.TaskState{domain.Pending, domain.Completed},
	})
	if len(tasks) != 2 {
		t.Errorf("expected 2 tasks, got %d", len(tasks))
	}
}

func TestListTasks_FilterByType(t *testing.T) {
	repo := newTestRepo()
	seedTasks(t, repo,
		domain.NewTask("e1", "c", "email", nil, time.Time{}, 0),
		domain.NewTask("s1", "c", "sms", nil, time.Time{}, 0),
		domain.NewTask("e2", "c", "email", nil, time.Time{}, 0),
	)

	tasks, _ := repo.ListTasks(context.Background(), &domain.TaskFilter{
		TaskTypes: []string{"email"},
	})
	if len(tasks) != 2 {
		t.Errorf("expected 2 email tasks, got %d", len(tasks))
	}
}

func TestListTasks_Sorting(t *testing.T) {
	repo := newTestRepo()

	now := time.Now().UTC()
	t1 := domain.NewTask("old", "c", "t", nil, time.Time{}, 0)
	t1.CreatedAt = now.Add(-2 * time.Hour)
	t1.UpdatedAt = t1.CreatedAt

	t2 := domain.NewTask("new", "c", "t", nil, time.Time{}, 0)
	t2.CreatedAt = now
	t2.UpdatedAt = t2.CreatedAt

	seedTasks(t, repo, t1, t2)

	tasks, _ := repo.ListTasks(context.Background(), &domain.TaskFilter{
		SortBy:  domain.SortByCreatedAt,
		SortDir: domain.SortAsc,
	})
	if len(tasks) == 2 && tasks[0].ID != "old" {
		t.Errorf("ASC: expected 'old' first, got %q", tasks[0].ID)
	}

	tasks, _ = repo.ListTasks(context.Background(), &domain.TaskFilter{
		SortBy:  domain.SortByCreatedAt,
		SortDir: domain.SortDesc,
	})
	if len(tasks) == 2 && tasks[0].ID != "new" {
		t.Errorf("DESC: expected 'new' first, got %q", tasks[0].ID)
	}
}

func TestListTasks_Pagination(t *testing.T) {
	repo := newTestRepo()
	now := time.Now().UTC()

	for i := 0; i < 5; i++ {
		task := domain.NewTask(string(rune('a'+i)), "c", "t", nil, time.Time{}, 0)
		task.CreatedAt = now.Add(time.Duration(i) * time.Minute)
		task.UpdatedAt = task.CreatedAt
		seedTasks(t, repo, task)
	}

	page1, _ := repo.ListTasks(context.Background(), &domain.TaskFilter{
		Limit:   2,
		Offset:  0,
		SortBy:  domain.SortByCreatedAt,
		SortDir: domain.SortAsc,
	})
	if len(page1) != 2 {
		t.Errorf("page1: expected 2, got %d", len(page1))
	}

	page2, _ := repo.ListTasks(context.Background(), &domain.TaskFilter{
		Limit:   2,
		Offset:  2,
		SortBy:  domain.SortByCreatedAt,
		SortDir: domain.SortAsc,
	})
	if len(page2) != 2 {
		t.Errorf("page2: expected 2, got %d", len(page2))
	}

	if len(page1) > 0 && len(page2) > 0 && page1[0].ID == page2[0].ID {
		t.Error("page1 and page2 should not overlap")
	}

	page3, _ := repo.ListTasks(context.Background(), &domain.TaskFilter{
		Limit:   2,
		Offset:  4,
		SortBy:  domain.SortByCreatedAt,
		SortDir: domain.SortAsc,
	})
	if len(page3) != 1 {
		t.Errorf("page3: expected 1, got %d", len(page3))
	}
}

func TestListTasks_FilterByWorkerID(t *testing.T) {
	repo := newTestRepo()

	t1 := domain.NewTask("w1", "c", "t", nil, time.Time{}, 0)
	t1.WorkerID = "worker-A"
	t2 := domain.NewTask("w2", "c", "t", nil, time.Time{}, 0)
	t2.WorkerID = "worker-B"

	seedTasks(t, repo, t1, t2)

	tasks, _ := repo.ListTasks(context.Background(), &domain.TaskFilter{
		WorkerID: "worker-A",
	})
	if len(tasks) != 1 || tasks[0].ID != "w1" {
		t.Errorf("expected [w1], got %v", taskIDs(tasks))
	}
}

func TestListTasks_FilterByIDPrefix(t *testing.T) {
	repo := newTestRepo()
	seedTasks(t, repo,
		domain.NewTask("task-abc-1", "c", "t", nil, time.Time{}, 0),
		domain.NewTask("task-abc-2", "c", "t", nil, time.Time{}, 0),
		domain.NewTask("task-xyz-1", "c", "t", nil, time.Time{}, 0),
	)

	tasks, _ := repo.ListTasks(context.Background(), &domain.TaskFilter{
		TaskIDPrefix: "task-abc",
	})
	if len(tasks) != 2 {
		t.Errorf("expected 2 tasks with prefix 'task-abc', got %d", len(tasks))
	}
}

func taskIDs(tasks []*domain.Task) []string {
	ids := make([]string, len(tasks))
	for i, t := range tasks {
		ids[i] = t.ID
	}
	return ids
}

func TestRepository_ReturnsIndependentSnapshots(t *testing.T) {
	repo := newTestRepo()
	task := domain.NewTask("snapshot", "", "job", []byte("original"), time.Time{}, 0)
	seedTasks(t, repo, task)
	task.Payload[0] = 'X'
	for _, read := range []func() *domain.Task{
		func() *domain.Task { got, _ := repo.Get(context.Background(), task.ID); return got },
		func() *domain.Task {
			got, _ := repo.ListEligible(context.Background(), time.Now().Add(time.Second), 10)
			return got[0]
		},
		func() *domain.Task { got, _ := repo.ListTasks(context.Background(), nil); return got[0] },
	} {
		got := read()
		if string(got.Payload) != "original" {
			t.Fatalf("stored payload mutated: %s", got.Payload)
		}
		got.Payload[0] = 'Y'
		got.State = domain.Cancelled
	}
	got, _ := repo.Get(context.Background(), task.ID)
	if got.State != domain.Pending || string(got.Payload) != "original" {
		t.Fatalf("read mutated store: %+v", got)
	}
}
