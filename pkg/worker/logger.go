package worker

// Logger is the minimal logging interface for the worker SDK.
// Users can plug in any logger that satisfies this interface (e.g. zap, logrus, slog).
type Logger interface {
	Info(msg string, keysAndValues ...any)
	Error(msg string, err error, keysAndValues ...any)
}

// noopLogger discards all log output. Used as the default when no logger is provided.
type noopLogger struct{}

func (noopLogger) Info(string, ...any)         {}
func (noopLogger) Error(string, error, ...any) {}
