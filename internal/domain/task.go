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
	Cancelled TaskState = "CANCELLED"
)

type Task struct {
	ID        string
	Type      string
	Payload   []byte
	Result    []byte
	State     TaskState
	RunAt     time.Time
	Version   int
	WorkerID  string
	ClientID  string
	RetryCount int
	MaxRetries int
	LastFailedAt time.Time
	CreatedAt time.Time
	UpdatedAt time.Time
}

func NewTask(id, clientID, taskType string, payload []byte, runAt time.Time) *Task {
	if runAt.IsZero() {
		runAt = time.Now().UTC()
	}
	return &Task{
		ID:        id,
		ClientID:  clientID,
		Type:      taskType,
		Payload:   payload,
		State:     Pending,
		RunAt:     runAt,
		Version:   1,
		MaxRetries: 3,
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	}
}

func (t *Task) ValidateTransition(target TaskState) error {

	if target == Cancelled {
		if t.State == Completed || t.State == Failed || t.State == Cancelled {
			return ErrTaskFinalized
		}
		return nil
	}

	if t.State == Completed || t.State == Failed || t.State == Cancelled {
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
		if target != Pending && target != Failed && target != Completed && target != Cancelled {
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
