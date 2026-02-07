package port

type Logger interface {
	Info(msg string, fields ...Field)
	Error(msg string, err error, fields ...Field)
	Sync() error
}

type Field struct {
	Key   string
	Value interface{}
}

func String(k, v string) Field {
	return Field{Key: k, Value: v}
}

func Int(k string, v int) Field {
	return Field{Key: k, Value: v}
}

func Any(k string, v interface{}) Field {
	return Field{Key: k, Value: v}
}
