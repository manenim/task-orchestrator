package domain

import (
	"time"
)

type TaskState string

const (
	Pending   TaskState = "PENDING"
	Scheduled TaskState = "SCHEDULED"
	Running   TaskState = "RUNNING"
	Completed TaskState = "COMPLETED"
	Failed    TaskState = "FAILED"
)

type Task struct {
	ID        string
	Type      string
	Payload   []byte
	State     TaskState
	Version   int
	CreatedAt time.Time
	UpdatedAt time.Time
}

func NewTask(id string, taskType string, payload []byte) *Task {
	Task := &Task{
		ID:        id,
		Type:      taskType,
		Payload:   payload,
		State:     Pending,
		Version:   1,
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	}
	return Task
}

func (t *Task) ValidateTransition(target TaskState) error {
	if t.State == Completed || t.State == Failed {
		return ErrTaskFinalized
	}

	switch t.State {
	case Pending:
		if target != Scheduled {
			return ErrInvalidTransition
		}
	case Scheduled:
		if target != Running && target != Pending {
			return ErrInvalidTransition
		}
	case Running:
		if target != Pending && target != Failed && target != Completed {
			return ErrInvalidTransition
		}
	}
	return nil
}

func (t *Task) UpdateState(newState TaskState) error {
	if err := t.ValidateTransition(newState); err != nil {
		return err
	}

	t.State = newState
	t.Version++
	t.UpdatedAt = time.Now().UTC()
	return nil
}
