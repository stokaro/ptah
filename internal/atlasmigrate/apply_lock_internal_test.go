package atlasmigrate

// White-box testing required: the whole point of this test is that the lock
// request in ApplyOptions reaches the migrator that PrepareApply builds, and
// the migrator is an unexported field of ApplyPlan. Observing it from outside
// the package would need a live PostgreSQL, MySQL, MariaDB or SQL Server
// instance, since those are the only dialects that take a real advisory lock.

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/atlasurl"
	"go.5x5.cz/ptah/migration/migrator"
)

func TestPrepareApply_LockRequestReachesMigrator(t *testing.T) {
	tests := []struct {
		name string
		// lock applies the named-lock decision to the apply options.
		lock func(*ApplyOptions)
		// wantAcquire is the lock.name the executed run must record, or the
		// empty string when no lock may be requested at all.
		wantAcquire string
		wantSkipped bool
	}{
		{
			name:        "default",
			lock:        func(*ApplyOptions) {},
			wantAcquire: "ptah_migrate",
		},
		{
			name:        "lock name",
			lock:        func(o *ApplyOptions) { o.MigrationLockName = "atlas_migrate_execute" },
			wantAcquire: "atlas_migrate_execute",
		},
		{
			name:        "skip lock",
			lock:        func(o *ApplyOptions) { o.SkipMigrationLock = true },
			wantAcquire: "",
			wantSkipped: true,
		},
		{
			name: "skip lock keeps the name it declined",
			lock: func(o *ApplyOptions) {
				o.MigrationLockName = "atlas_migrate_execute"
				o.SkipMigrationLock = true
			},
			wantAcquire: "",
			wantSkipped: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			ctx := context.Background()
			dir := t.TempDir()
			migrationsDir := filepath.Join(dir, "migrations")
			writeLockTestMigration(c, migrationsDir)
			conn := connectLockTestSQLite(c, filepath.Join(dir, "apply.db"))
			defer dbschema.CloseAndWarn(conn)

			opts := ApplyOptions{
				Dir:       migrationsDir,
				FS:        os.DirFS(migrationsDir),
				ExecOrder: migrator.ExecOrderLinear,
				TxMode:    migrator.MigrationTxModeFile,
			}
			tt.lock(&opts)

			plan, err := PrepareApply(ctx, conn, opts)
			c.Assert(err, qt.IsNil)
			c.Assert(plan.MigrationLockSkipped(), qt.Equals, tt.wantSkipped)

			observer := &lockRecordingObserver{}
			plan.mig = plan.mig.WithObserver(observer)

			result, err := plan.Execute(ctx)

			c.Assert(err, qt.IsNil)
			c.Assert(result.Applied, qt.IsTrue)
			name, acquired := observer.acquiredLockName()
			c.Assert(acquired, qt.Equals, tt.wantAcquire != "",
				qt.Commentf("acquired=%v name=%q", acquired, name))
			c.Assert(name, qt.Equals, tt.wantAcquire)
		})
	}
}

func writeLockTestMigration(c *qt.C, dir string) {
	c.Helper()
	c.Assert(os.MkdirAll(dir, 0o755), qt.IsNil)
	c.Assert(os.WriteFile(
		filepath.Join(dir, "1_lock.sql"),
		[]byte("CREATE TABLE apply_lock_probe (id INTEGER PRIMARY KEY);"),
		0o600,
	), qt.IsNil)
}

func connectLockTestSQLite(c *qt.C, path string) *dbschema.DatabaseConnection {
	c.Helper()
	dbURL := atlasurl.SQLiteURLFromPath(path)
	conn, err := dbschema.ConnectToDatabase(context.Background(), dbURL)
	c.Assert(err, qt.IsNil)
	return conn
}

// lockRecordingObserver records only what this test asks about: whether an
// advisory lock acquisition was started, and under which name.
type lockRecordingObserver struct {
	lockNames []string
}

func (o *lockRecordingObserver) StartSpan(
	ctx context.Context,
	name string,
	attrs ...migrator.ObservationAttribute,
) (context.Context, migrator.ObservationSpan) {
	o.recordAcquisition(name, attrs)
	return ctx, lockRecordingSpan{}
}

func (o *lockRecordingObserver) recordAcquisition(name string, attrs []migrator.ObservationAttribute) {
	if name != "ptah.lock.acquire" {
		return
	}
	for _, attr := range attrs {
		if attr.Key == "lock.name" {
			value, _ := attr.Value.(string)
			o.lockNames = append(o.lockNames, value)
		}
	}
}

func (o *lockRecordingObserver) AddCounter(
	context.Context, string, int64, ...migrator.ObservationAttribute,
) {
}

func (o *lockRecordingObserver) RecordDuration(
	context.Context, string, time.Duration, ...migrator.ObservationAttribute,
) {
}

func (o *lockRecordingObserver) acquiredLockName() (string, bool) {
	for _, name := range o.lockNames {
		return name, true
	}
	return "", false
}

type lockRecordingSpan struct{}

func (lockRecordingSpan) SetAttributes(...migrator.ObservationAttribute) {}
func (lockRecordingSpan) End(error)                                      {}
