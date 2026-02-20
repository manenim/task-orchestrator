package domain

import "errors"

var (
	ErrTaskNotFound = errors.New("task not found")

	ErrInvalidTransition = errors.New("invalid task state transition")

	ErrTaskFinalized = errors.New("task is already in a terminal state")
)
