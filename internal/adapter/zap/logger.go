package zap

import (
	"github.com/manenim/task-orchestrator/internal/port"
	"go.uber.org/zap"
)

type ZapAdapter struct {
	logger *zap.Logger
}

func New() (*ZapAdapter, error) {
	l, err := zap.NewProduction()
	if err != nil {
		return nil, err
	}
	return &ZapAdapter{logger: l}, nil
}

func (z *ZapAdapter) Info(msg string, fields ...port.Field) {
	z.logger.Info(msg, z.mapFields(fields)...)
}

func (z *ZapAdapter) Error(msg string, err error, fields ...port.Field) {
	zapFields := z.mapFields(fields)
	zapFields = append(zapFields, zap.Error(err))
	z.logger.Error(msg, zapFields...)
}

func (z *ZapAdapter) Sync() error {
	return z.logger.Sync()
}

func (z *ZapAdapter) mapFields(fields []port.Field) []zap.Field {
	zapFields := make([]zap.Field, len(fields))
	for i, f := range fields {
		zapFields[i] = zap.Any(f.Key, f.Value)
	}
	return zapFields
}
