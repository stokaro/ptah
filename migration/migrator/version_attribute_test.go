package migrator_test

import (
	"context"
	"sync"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/migration/migrator"
)

func TestMigratorVersionAttribute_HappyPath(t *testing.T) {
	c := qt.New(t)
	positions := numberlessMigrator(c)
	numbered := atlasMigrator(c, "1_a.sql", "2_b.sql")

	c.Assert(positions.VersionAttribute("migration.version", 3), qt.Equals,
		migrator.ObservationAttribute{Key: "migration.version_key", Value: "v1"})
	c.Assert(positions.VersionAttribute("migration.current_version", 0), qt.Equals,
		migrator.ObservationAttribute{Key: "migration.current_version_key", Value: ""})
	c.Assert(numbered.VersionAttribute("migration.version", 2), qt.Equals,
		migrator.ObservationAttribute{Key: "migration.version", Value: int64(2)})
}

// attributeRecorder records the attributes of every span and metric, by
// span or metric name, in the order they arrive.
type attributeRecorder struct {
	mu    sync.Mutex
	attrs map[string][][]migrator.ObservationAttribute
}

func (r *attributeRecorder) add(name string, attrs []migrator.ObservationAttribute) *[]migrator.ObservationAttribute {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.attrs == nil {
		r.attrs = make(map[string][][]migrator.ObservationAttribute)
	}
	r.attrs[name] = append(r.attrs[name], append([]migrator.ObservationAttribute(nil), attrs...))
	return &r.attrs[name][len(r.attrs[name])-1]
}

func (r *attributeRecorder) StartSpan(ctx context.Context, name string, attrs ...migrator.ObservationAttribute) (context.Context, migrator.ObservationSpan) {
	return ctx, recordedSpan{recorder: r, attrs: r.add(name, attrs)}
}

func (r *attributeRecorder) AddCounter(_ context.Context, name string, _ int64, attrs ...migrator.ObservationAttribute) {
	r.add(name, attrs)
}

func (r *attributeRecorder) RecordDuration(_ context.Context, name string, _ time.Duration, attrs ...migrator.ObservationAttribute) {
	r.add(name, attrs)
}

// values returns the value of every attribute named key, over every span or
// metric named name.
func (r *attributeRecorder) values(name, key string) []any {
	r.mu.Lock()
	defer r.mu.Unlock()
	values := make([]any, 0)
	for _, attrs := range r.attrs[name] {
		for _, attr := range attrs {
			if attr.Key == key {
				values = append(values, attr.Value)
			}
		}
	}
	return values
}

type recordedSpan struct {
	recorder *attributeRecorder
	attrs    *[]migrator.ObservationAttribute
}

func (s recordedSpan) SetAttributes(attrs ...migrator.ObservationAttribute) {
	s.recorder.mu.Lock()
	defer s.recorder.mu.Unlock()
	*s.attrs = append(*s.attrs, attrs...)
}

func (recordedSpan) End(error) {}

// TestMigratorObserverNamesPositionsByKey runs 1_a.sql, 2_b.sql and v1_x.sql
// under the positions 1 to 3, and the spans and metrics name each migration
// by its key: 3 is v1_x.sql in this run and another file once a file sorts
// before it, so no attribute carries a position.
func TestMigratorObserverNamesPositionsByKey(t *testing.T) {
	c := qt.New(t)
	recorder := &attributeRecorder{}
	mig := numberlessMigrator(c).WithObserver(recorder)

	c.Assert(mig.MigrateUp(c.Context()), qt.IsNil)
	_, err := mig.GetMigrationStatus(c.Context())
	c.Assert(err, qt.IsNil)

	c.Assert(recorder.values("ptah.migrate.up", "migration.current_version_key"), qt.DeepEquals, []any{""})
	c.Assert(recorder.values("ptah.migrate.up", "migration.target_version_key"), qt.DeepEquals, []any{"v1"})
	c.Assert(recorder.values("ptah.migrate.apply", "migration.version_key"), qt.DeepEquals, []any{"1", "2", "v1"})
	c.Assert(recorder.values("ptah_migrations_applied_total", "migration.version_key"), qt.DeepEquals, []any{"1", "2", "v1"})
	c.Assert(recorder.values("ptah.migrate.status", "migration.current_version_key"), qt.DeepEquals, []any{"v1"})
	for _, name := range []string{"ptah.migrate.up", "ptah.migrate.apply", "ptah_migrations_applied_total", "ptah.migrate.status"} {
		for _, key := range []string{"migration.current_version", "migration.target_version", "migration.version"} {
			c.Assert(recorder.values(name, key), qt.HasLen, 0, qt.Commentf("%s %s", name, key))
		}
	}
}
