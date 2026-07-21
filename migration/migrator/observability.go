package migrator

import (
	"context"
	"time"
)

// ObservationAttribute is a structured attribute attached to migration
// observability events.
type ObservationAttribute struct {
	Key   string
	Value any
}

// ObservationSpan is an in-progress observed operation.
type ObservationSpan interface {
	SetAttributes(attrs ...ObservationAttribute)
	End(err error)
}

// Observer receives migration tracing and metrics events.
type Observer interface {
	StartSpan(ctx context.Context, name string, attrs ...ObservationAttribute) (context.Context, ObservationSpan)
	AddCounter(ctx context.Context, name string, value int64, attrs ...ObservationAttribute)
	RecordDuration(ctx context.Context, name string, duration time.Duration, attrs ...ObservationAttribute)
}

// NoopObserver discards all migration observability events.
type NoopObserver struct{}

// StartSpan starts a no-op span.
func (NoopObserver) StartSpan(ctx context.Context, _ string, _ ...ObservationAttribute) (context.Context, ObservationSpan) {
	return ctx, noopSpan{}
}

// AddCounter discards a counter observation.
func (NoopObserver) AddCounter(context.Context, string, int64, ...ObservationAttribute) {}

// RecordDuration discards a duration observation.
func (NoopObserver) RecordDuration(context.Context, string, time.Duration, ...ObservationAttribute) {}

type noopSpan struct{}

func (noopSpan) SetAttributes(...ObservationAttribute) {}
func (noopSpan) End(error)                             {}

func (m *Migrator) migrationObserver() Observer {
	if m == nil || m.observer == nil {
		return NoopObserver{}
	}
	return m.observer
}

type rootSpanKey struct{}

func contextWithRootSpan(ctx context.Context, span ObservationSpan) context.Context {
	return context.WithValue(ctx, rootSpanKey{}, span)
}

func rootSpanFromContext(ctx context.Context) ObservationSpan {
	span, _ := ctx.Value(rootSpanKey{}).(ObservationSpan)
	return span
}

func attr(key string, value any) ObservationAttribute {
	return ObservationAttribute{Key: key, Value: value}
}
