package domain

import (
	"testing"
	"time"
)

func TestTaskLifecycle(t *testing.T) {
	task := NewTask("123", "email", nil, time.Time{})
	if task.State != Pending {
		t.Errorf("expected Pending, got %v", task.State)
	}

	err := task.UpdateState(Scheduled)
	if err != nil {
		t.Fatalf("unexpected error transition to Scheduled: %v", err)
	}

	errRun := task.UpdateState(Running)
	if errRun != nil {
		t.Fatalf("unexpected error transition to Running: %v", errRun)
	}

	if err := task.UpdateState(Completed); err != nil {
		t.Fatalf("unexpected error transition to Completed: %v", err)
	}

	if err := task.UpdateState(Failed); err != ErrTaskFinalized {
		t.Errorf("expected ErrTaskFinalized when trying to fail a completed task, got: %v", err)
	}
}

func TestTask_IllegalTransitions(t *testing.T) {
	tests := []struct {
		name  string
		start TaskState
		end   TaskState
		want  error
	}{
		{"Pending -> Completed", Pending, Completed, ErrInvalidTransition},
		{"Pending -> Failed", Pending, Failed, ErrInvalidTransition},
		{"Completed -> Running", Completed, Running, ErrTaskFinalized},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			task := NewTask("1", "type", nil, time.Time{})
			task.State = tt.start
			got := task.ValidateTransition(tt.end)
			if got != tt.want {
				t.Errorf("expected %v, got %v", tt.want, got)
			}
		})
	}
}

func TestTask_RetryLogic(t *testing.T) {
	task := Task{
		ID:        "123",
		Type:      "email",
		Payload:   nil,
		State:     Running,
		Version:   3,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	err := task.ValidateTransition(Pending)

	if err != nil {
		t.Errorf("expected retry (Running->Pending) to be possible, got: %v", err)
	}
}
