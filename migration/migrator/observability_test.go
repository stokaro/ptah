package migrator

import (
	"context"
	"testing"
	"testing/fstest"
	"time"

	"github.com/stokaro/ptah/dbschema"
)

func TestMigratorObserverRecordsMigrateUp(t *testing.T) {
	ctx := context.Background()
	conn := openSQLiteMigratorTestDB(t)
	defer func() {
		if err := conn.Close(); err != nil {
			t.Fatalf("close SQLite connection: %v", err)
		}
	}()

	observer := &recordingObserver{}
	m := newObservedTestMigrator(t, conn, observer)

	if err := m.MigrateUp(ctx); err != nil {
		t.Fatalf("migrate up: %v", err)
	}

	root := observer.requireSpan(t, "ptah.migrate.up")
	requireAttr(t, root.attrs, "migration.direction", "up")
	requireAttr(t, root.attrs, "migration.target_version", int64(1))
	requireAttr(t, root.attrs, "migration.pending_count", 1)
	requireAttrExists(t, root.attrs, "lock.wait_ms")

	apply := observer.requireSpan(t, "ptah.migrate.apply")
	requireAttr(t, apply.attrs, "migration.direction", "up")
	requireAttr(t, apply.attrs, "migration.version", int64(1))
	requireAttr(t, apply.attrs, "migration.description", "Create Observed")
	observer.requireCounter(t, "ptah_migrations_applied_total", "up")
	observer.requireCounterWithoutAttr(t, "ptah_migrations_applied_total", "migration.description")
	observer.requireDuration(t, "ptah_migration_lock_wait_seconds")
	observer.requireDuration(t, "ptah_migration_duration_seconds")
	observer.requireDurationWithoutAttr(t, "ptah_migration_duration_seconds", "migration.description")
}

func TestMigratorObserverRecordsMigrateDown(t *testing.T) {
	ctx := context.Background()
	conn := openSQLiteMigratorTestDB(t)
	defer func() {
		if err := conn.Close(); err != nil {
			t.Fatalf("close SQLite connection: %v", err)
		}
	}()

	setupObserver := &recordingObserver{}
	m := newObservedTestMigrator(t, conn, setupObserver)
	if err := m.MigrateUp(ctx); err != nil {
		t.Fatalf("migrate up: %v", err)
	}

	observer := &recordingObserver{}
	m = m.WithObserver(observer)
	if err := m.MigrateDownTo(ctx, 0); err != nil {
		t.Fatalf("migrate down: %v", err)
	}

	root := observer.requireSpan(t, "ptah.migrate.down")
	requireAttr(t, root.attrs, "migration.direction", "down")
	requireAttr(t, root.attrs, "migration.target_version", int64(0))
	requireAttr(t, root.attrs, "migration.pending_count", 1)

	rollback := observer.requireSpan(t, "ptah.migrate.rollback")
	requireAttr(t, rollback.attrs, "migration.direction", "down")
	requireAttr(t, rollback.attrs, "migration.version", int64(1))
	requireAttr(t, rollback.attrs, "migration.description", "Create Observed")
	observer.requireCounter(t, "ptah_migrations_rolled_back_total", "down")
	observer.requireCounterWithoutAttr(t, "ptah_migrations_rolled_back_total", "migration.description")
}

func TestMigratorObserverRecordsMigrationStatus(t *testing.T) {
	ctx := context.Background()
	conn := openSQLiteMigratorTestDB(t)
	defer func() {
		if err := conn.Close(); err != nil {
			t.Fatalf("close SQLite connection: %v", err)
		}
	}()

	observer := &recordingObserver{}
	m := newObservedTestMigrator(t, conn, observer)
	if _, err := m.GetMigrationStatus(ctx); err != nil {
		t.Fatalf("get migration status: %v", err)
	}

	status := observer.requireSpan(t, "ptah.migrate.status")
	requireAttr(t, status.attrs, "db.system", "sqlite")
	requireAttr(t, status.attrs, "migration.current_version", int64(0))
	requireAttr(t, status.attrs, "migration.pending_count", 1)
	requireAttr(t, status.attrs, "migration.total_count", 1)
}

func newObservedTestMigrator(t *testing.T, conn *dbschema.DatabaseConnection, observer Observer) *Migrator {
	t.Helper()
	m, err := NewFSMigrator(conn, fstest.MapFS{
		"000001_create_observed.up.sql": {
			Data: []byte("CREATE TABLE observed_migrations (id INTEGER PRIMARY KEY);"),
		},
		"000001_create_observed.down.sql": {
			Data: []byte("DROP TABLE observed_migrations;"),
		},
	})
	if err != nil {
		t.Fatalf("create filesystem migrator: %v", err)
	}
	return m.WithObserver(observer)
}

type recordedSpan struct {
	name  string
	err   error
	attrs []ObservationAttribute
}

type recordedCounter struct {
	name  string
	value int64
	attrs []ObservationAttribute
}

type recordedDuration struct {
	name     string
	duration time.Duration
	attrs    []ObservationAttribute
}

type recordingObserver struct {
	spans     []*recordedSpan
	counters  []recordedCounter
	durations []recordedDuration
}

func (o *recordingObserver) StartSpan(ctx context.Context, name string, attrs ...ObservationAttribute) (context.Context, ObservationSpan) {
	span := &recordedSpan{name: name, attrs: append([]ObservationAttribute(nil), attrs...)}
	o.spans = append(o.spans, span)
	return ctx, recordingSpan{span: span}
}

func (o *recordingObserver) AddCounter(_ context.Context, name string, value int64, attrs ...ObservationAttribute) {
	o.counters = append(o.counters, recordedCounter{
		name:  name,
		value: value,
		attrs: append([]ObservationAttribute(nil), attrs...),
	})
}

func (o *recordingObserver) RecordDuration(_ context.Context, name string, duration time.Duration, attrs ...ObservationAttribute) {
	o.durations = append(o.durations, recordedDuration{
		name:     name,
		duration: duration,
		attrs:    append([]ObservationAttribute(nil), attrs...),
	})
}

func (o *recordingObserver) requireSpan(t *testing.T, name string) *recordedSpan {
	t.Helper()
	for _, span := range o.spans {
		if span.name == name {
			return span
		}
	}
	t.Fatalf("span %q not recorded; got %v", name, spanNames(o.spans))
	return nil
}

func (o *recordingObserver) requireCounter(t *testing.T, name, direction string) {
	t.Helper()
	for _, counter := range o.counters {
		if counter.name == name && counter.value == 1 && attrValue(counter.attrs, "migration.direction") == direction {
			return
		}
	}
	t.Fatalf("counter %q direction %q not recorded: %+v", name, direction, o.counters)
}

func (o *recordingObserver) requireCounterWithoutAttr(t *testing.T, name, attrName string) {
	t.Helper()
	for _, counter := range o.counters {
		if counter.name == name && attrValue(counter.attrs, attrName) != nil {
			t.Fatalf("counter %q includes forbidden attr %q: %+v", name, attrName, counter.attrs)
		}
	}
}

func (o *recordingObserver) requireDuration(t *testing.T, name string) {
	t.Helper()
	for _, duration := range o.durations {
		if duration.name == name {
			return
		}
	}
	t.Fatalf("duration %q not recorded: %+v", name, o.durations)
}

func (o *recordingObserver) requireDurationWithoutAttr(t *testing.T, name, attrName string) {
	t.Helper()
	for _, duration := range o.durations {
		if duration.name == name && attrValue(duration.attrs, attrName) != nil {
			t.Fatalf("duration %q includes forbidden attr %q: %+v", name, attrName, duration.attrs)
		}
	}
}

type recordingSpan struct {
	span *recordedSpan
}

func (s recordingSpan) SetAttributes(attrs ...ObservationAttribute) {
	s.span.attrs = append(s.span.attrs, attrs...)
}

func (s recordingSpan) End(err error) {
	s.span.err = err
}

func requireAttr(t *testing.T, attrs []ObservationAttribute, key string, want any) {
	t.Helper()
	if got := attrValue(attrs, key); got != want {
		t.Fatalf("attr %s = %#v, want %#v; attrs=%+v", key, got, want, attrs)
	}
}

func requireAttrExists(t *testing.T, attrs []ObservationAttribute, key string) {
	t.Helper()
	if got := attrValue(attrs, key); got == nil {
		t.Fatalf("attr %s missing; attrs=%+v", key, attrs)
	}
}

func attrValue(attrs []ObservationAttribute, key string) any {
	for _, attr := range attrs {
		if attr.Key == key {
			return attr.Value
		}
	}
	return nil
}

func spanNames(spans []*recordedSpan) []string {
	names := make([]string, 0, len(spans))
	for _, span := range spans {
		names = append(names, span.name)
	}
	return names
}
