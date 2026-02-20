package domain

import (
	"testing"
	"time"
)

func TestNewTask_Defaults(t *testing.T) {
	task := NewTask("abc", "client-1", "email", []byte("hello"), time.Time{}, 30)

	if task.State != Pending {
		t.Errorf("expected state Pending, got %v", task.State)
	}
	if task.Version != 1 {
		t.Errorf("expected version 1, got %d", task.Version)
	}
	if task.MaxRetries != 3 {
		t.Errorf("expected MaxRetries 3, got %d", task.MaxRetries)
	}
	if task.TimeoutSeconds != 30 {
		t.Errorf("expected TimeoutSeconds 30, got %d", task.TimeoutSeconds)
	}
	if task.CreatedAt.IsZero() {
		t.Error("expected CreatedAt to be set")
	}
	if task.RunAt.IsZero() {
		t.Error("expected RunAt to be auto-set when zero time is passed")
	}
}

func TestNewTask_ExplicitRunAt(t *testing.T) {
	future := time.Now().Add(1 * time.Hour)
	task := NewTask("abc", "client-1", "email", nil, future, 0)

	if !task.RunAt.Equal(future) {
		t.Errorf("expected RunAt to be %v, got %v", future, task.RunAt)
	}
}

func TestTaskLifecycle(t *testing.T) {
	task := NewTask("123", "test-client", "email", nil, time.Time{}, 0)
	if task.State != Pending {
		t.Errorf("expected Pending, got %v", task.State)
	}

	if err := task.UpdateState(Scheduled); err != nil {
		t.Fatalf("unexpected error transition to Scheduled: %v", err)
	}

	if err := task.UpdateState(Running); err != nil {
		t.Fatalf("unexpected error transition to Running: %v", err)
	}

	if err := task.UpdateState(Completed); err != nil {
		t.Fatalf("unexpected error transition to Completed: %v", err)
	}

	if err := task.UpdateState(Failed); err != ErrTaskFinalized {
		t.Errorf("expected ErrTaskFinalized when trying to fail a completed task, got: %v", err)
	}
}

func TestTask_AllValidTransitions(t *testing.T) {
	tests := []struct {
		name string
		from TaskState
		to   TaskState
		err  error
	}{
		{"Pending → Scheduled", Pending, Scheduled, nil},
		{"Pending → Completed (invalid)", Pending, Completed, ErrInvalidTransition},
		{"Pending → Failed (invalid)", Pending, Failed, ErrInvalidTransition},
		{"Pending → Cancelled", Pending, Cancelled, nil},

		{"Scheduled → Running", Scheduled, Running, nil},
		{"Scheduled → Pending (reschedule)", Scheduled, Pending, nil},
		{"Scheduled → Completed (invalid)", Scheduled, Completed, ErrInvalidTransition},
		{"Scheduled → Cancelled", Scheduled, Cancelled, nil},

		{"Running → Completed", Running, Completed, nil},
		{"Running → Failed", Running, Failed, nil},
		{"Running → Pending (retry)", Running, Pending, nil},
		{"Running → Cancelled", Running, Cancelled, nil},

		{"Completed → Running (invalid)", Completed, Running, ErrTaskFinalized},
		{"Completed → Pending (invalid)", Completed, Pending, ErrTaskFinalized},
		{"Completed → Cancelled (invalid)", Completed, Cancelled, ErrTaskFinalized},
		{"Failed → Running (invalid)", Failed, Running, ErrTaskFinalized},
		{"Failed → Pending (invalid)", Failed, Pending, ErrTaskFinalized},
		{"Failed → Cancelled (invalid)", Failed, Cancelled, ErrTaskFinalized},
		{"Cancelled → Running (invalid)", Cancelled, Running, ErrTaskFinalized},
		{"Cancelled → Pending (invalid)", Cancelled, Pending, ErrTaskFinalized},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			task := NewTask("1", "c", "t", nil, time.Time{}, 0)
			task.State = tt.from
			got := task.ValidateTransition(tt.to)
			if got != tt.err {
				t.Errorf("expected %v, got %v", tt.err, got)
			}
		})
	}
}

func TestTask_VersionIncrement(t *testing.T) {
	task := NewTask("v1", "c", "t", nil, time.Time{}, 0)
	if task.Version != 1 {
		t.Fatalf("initial version should be 1, got %d", task.Version)
	}

	task.UpdateState(Scheduled)
	if task.Version != 2 {
		t.Errorf("expected version 2 after first transition, got %d", task.Version)
	}

	task.UpdateState(Running)
	if task.Version != 3 {
		t.Errorf("expected version 3 after second transition, got %d", task.Version)
	}

	task.UpdateState(Completed)
	if task.Version != 4 {
		t.Errorf("expected version 4 after third transition, got %d", task.Version)
	}
}

func TestTask_CancellationPaths(t *testing.T) {
	t.Run("cancel from Pending", func(t *testing.T) {
		task := NewTask("c1", "c", "t", nil, time.Time{}, 0)
		if err := task.UpdateState(Cancelled); err != nil {
			t.Errorf("should be able to cancel from Pending: %v", err)
		}
		if task.State != Cancelled {
			t.Errorf("expected Cancelled, got %v", task.State)
		}
	})

	t.Run("cancel from Scheduled", func(t *testing.T) {
		task := NewTask("c2", "c", "t", nil, time.Time{}, 0)
		task.State = Scheduled
		if err := task.UpdateState(Cancelled); err != nil {
			t.Errorf("should be able to cancel from Scheduled: %v", err)
		}
	})

	t.Run("cancel from Running", func(t *testing.T) {
		task := NewTask("c3", "c", "t", nil, time.Time{}, 0)
		task.State = Running
		if err := task.UpdateState(Cancelled); err != nil {
			t.Errorf("should be able to cancel from Running: %v", err)
		}
	})

	for _, state := range []TaskState{Completed, Failed, Cancelled} {
		t.Run("cannot cancel from "+string(state), func(t *testing.T) {
			task := NewTask("cx", "c", "t", nil, time.Time{}, 0)
			task.State = state
			if err := task.UpdateState(Cancelled); err != ErrTaskFinalized {
				t.Errorf("expected ErrTaskFinalized, got %v", err)
			}
		})
	}
}

func TestTask_UpdateState_SetsUpdatedAt(t *testing.T) {
	task := NewTask("u1", "c", "t", nil, time.Time{}, 0)
	original := task.UpdatedAt

	time.Sleep(1 * time.Millisecond)
	task.UpdateState(Scheduled)

	if !task.UpdatedAt.After(original) {
		t.Error("UpdatedAt should advance after state change")
	}
}
