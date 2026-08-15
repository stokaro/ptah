package migrator

// White-box testing required: the advisory lock a migration run requests is
// observable only through the migrator's own observation span, and the span is
// emitted from an unexported code path. Asserting it through a live database
// would need PostgreSQL, MySQL, MariaDB or SQL Server, since those are the only
// dialects with advisory-lock semantics; the span records the request itself,
// which is what the `--lock-name` and `--skip-lock` compat flags steer.

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
)

func TestMigrateUpLockRequest(t *testing.T) {
	tests := []struct {
		name string
		// configure applies the lock decision under test.
		configure func(*Migrator) *Migrator
		// wantAcquire is the lock.name attribute the run must record, or the
		// empty string when the run must record no acquisition at all.
		wantAcquire string
	}{
		{
			name:        "default name",
			configure:   func(m *Migrator) *Migrator { return m },
			wantAcquire: migrationAdvisoryLockName,
		},
		{
			name:        "named lock",
			configure:   func(m *Migrator) *Migrator { return m.WithMigrationLockName("atlas_migrate_execute") },
			wantAcquire: "atlas_migrate_execute",
		},
		{
			name:        "blank name keeps the default",
			configure:   func(m *Migrator) *Migrator { return m.WithMigrationLockName("   ") },
			wantAcquire: migrationAdvisoryLockName,
		},
		{
			name:        "skipped lock requests nothing",
			configure:   func(m *Migrator) *Migrator { return m.WithoutMigrationLock() },
			wantAcquire: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openSQLiteMigratorTestDB(t)
			t.Cleanup(func() {
				c.Check(conn.Close(), qt.IsNil)
			})
			observer := &recordingObserver{}
			m := tt.configure(newObservedTestMigrator(t, conn, observer))

			c.Assert(m.MigrateUp(context.Background()), qt.IsNil)

			// The migration ran either way: skipping the lock must not skip
			// the work, and naming it must not change which work is selected.
			status, err := m.GetMigrationStatus(context.Background())
			c.Assert(err, qt.IsNil)
			c.Assert(status.CurrentVersion, qt.Equals, int64(1))

			assertLockAcquisition(c, observer, tt.wantAcquire)
		})
	}
}

func TestMigrationLockAccessors(t *testing.T) {
	c := qt.New(t)

	base := NewMigrator(nil, nil)
	c.Assert(base.MigrationLockName(), qt.Equals, migrationAdvisoryLockName)
	c.Assert(base.MigrationLockSkipped(), qt.IsFalse)

	named := base.WithMigrationLockName("atlas_migrate_execute")
	c.Assert(named.MigrationLockName(), qt.Equals, "atlas_migrate_execute")
	c.Assert(named.MigrationLockSkipped(), qt.IsFalse)

	// The skipped migrator still names the lock it declined to take, so a
	// caller can report the decision precisely.
	skipped := named.WithoutMigrationLock()
	c.Assert(skipped.MigrationLockName(), qt.Equals, "atlas_migrate_execute")
	c.Assert(skipped.MigrationLockSkipped(), qt.IsTrue)

	// Every option returns a copy; the originals are untouched.
	c.Assert(base.MigrationLockSkipped(), qt.IsFalse)
	c.Assert(named.MigrationLockSkipped(), qt.IsFalse)
	c.Assert(base.MigrationLockName(), qt.Equals, migrationAdvisoryLockName)
}

// assertLockAcquisition asserts the observed acquisition against want, where
// the empty string means "no lock was requested at all".
func assertLockAcquisition(c *qt.C, observer *recordingObserver, want string) {
	c.Helper()
	got, acquired := observedLockName(observer)
	c.Assert(acquired, qt.Equals, want != "",
		qt.Commentf("acquired=%v name=%q spans=%v", acquired, got, spanNames(observer.spans)))
	c.Assert(got, qt.Equals, want)
	c.Assert(observedLockWaitRecorded(observer), qt.Equals, want != "")
}

// observedLockName reports the lock.name attribute of the acquisition span and
// whether such a span was recorded at all.
func observedLockName(observer *recordingObserver) (string, bool) {
	for _, span := range observer.spans {
		if span.name != "ptah.lock.acquire" {
			continue
		}
		name, _ := attrValue(span.attrs, "lock.name").(string)
		return name, true
	}
	return "", false
}

func observedLockWaitRecorded(observer *recordingObserver) bool {
	for _, duration := range observer.durations {
		if duration.name == "ptah_migration_lock_wait_seconds" {
			return true
		}
	}
	return false
}
