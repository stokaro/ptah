//go:build integration

package ydb_test

import (
	"context"
	"net/url"
	"path"
	"sync"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	ydbsdk "github.com/ydb-platform/ydb-go-sdk/v3"

	"ptah.run/dbschema"
	"ptah.run/internal/atlasschema"
	"ptah.run/internal/dblock"
	"ptah.run/internal/dbtarget"
	"ptah.run/migration/migrator"
)

// dropLockNode drops Ptah's coordination node through a driver of its own.
// The server ends every session on the node, so every lock held there is
// lost; the next run that locks creates the node again.
func dropLockNode(c *qt.C) {
	c.Helper()
	ctx := context.Background()
	driver, err := ydbsdk.Open(ctx, dbtarget.DriverDSN(c, dbtarget.YDB))
	c.Assert(err, qt.IsNil)
	defer func() { _ = driver.Close(ctx) }()
	c.Assert(driver.Coordination().DropNode(ctx, path.Join(driver.Name(), dblock.YDBLockNode)), qt.IsNil)
}

// closedWithin reports whether done closes within timeout.
func closedWithin(done <-chan struct{}, timeout time.Duration) bool {
	select {
	case <-done:
		return true
	case <-time.After(timeout):
		return false
	}
}

// A lock the server takes away is reported as lost: here its coordination
// node is dropped, which ends the session that held it. The holder reads the
// loss within the half session timeout an answer is trusted, the context
// Guard returned ends with it, and releasing it afterwards only ends the
// session.
func TestYDBLock_ReportsALoss(t *testing.T) {
	c := qt.New(t)
	held, err := dblock.Acquire(c.Context(), openYDB(c), "ptah_ydb_lock_lost", 0)
	c.Assert(err, qt.IsNil)
	guarded, stop := held.Guard(context.Background())
	c.Cleanup(stop)
	c.Assert(held.Err(), qt.IsNil)

	dropLockNode(c)

	c.Assert(closedWithin(guarded.Done(), 15*time.Second), qt.IsTrue)
	c.Assert(dblock.IsLost(context.Cause(guarded)), qt.IsTrue, qt.Commentf("cause: %v", context.Cause(guarded)))
	c.Assert(held.Err(), qt.ErrorMatches, `advisory lock "ptah_ydb_lock_lost" on ydb was lost while it was held: .*`)
	c.Assert(held.Release(context.Background()), qt.IsNil)
}

// A run that loses the migration lock stops before its next statement and
// writes nothing more: the body's context ends with the loss, the CREATE
// TABLE after it does not run, and the revision row still records the
// statement committed before it, not a failure another runner would have to
// read past.
func TestYDBMigrator_StopsWhenTheLockIsLost(t *testing.T) {
	c := qt.New(t)
	conn := openYDB(c)
	const dir = "ptah_ydb_mig_lock_lost"
	dropDirectory(c, conn, dir, "x", "y")
	c.Cleanup(func() { dropDirectory(c, conn, dir, "x", "y") })
	files := migrationFiles(map[string]string{
		"0000000001_x.up.sql": "CREATE TABLE `" + dir + "/x` (id Int64 NOT NULL, PRIMARY KEY (id));\n" +
			"CREATE TABLE `" + dir + "/y` (id Int64 NOT NULL, PRIMARY KEY (id));\n",
		"0000000001_x.down.sql": "DROP TABLE `" + dir + "/y`;\nDROP TABLE `" + dir + "/x`;\n",
	})
	var dropOnce sync.Once
	var cause error
	loseTheLock := migrator.StatementObserverFunc(func(ctx context.Context, _ migrator.StatementEvent) error {
		dropOnce.Do(func() {
			dropLockNode(c)
			closedWithin(ctx.Done(), 15*time.Second)
			cause = context.Cause(ctx)
		})
		return nil
	})
	m, err := migrator.NewFSMigrator(conn, files, migrator.WithStatementObserver(loseTheLock))
	c.Assert(err, qt.IsNil)
	m = m.WithMigrationsTable(dir, "")

	err = m.MigrateUp(c.Context())

	c.Assert(err, qt.ErrorMatches,
		`(?s).*advisory lock "ptah_migrate" on ydb was lost while it was held: .*the run stopped there.*`)
	c.Assert(dblock.IsLost(cause), qt.IsTrue, qt.Commentf("the body's context ended with %v", cause))
	c.Assert(tableNames(readScoped(c, conn, []string{dir})), qt.DeepEquals, []string{dir + "|x"})
	c.Assert(revisionProgress(c, newMigrator(c, openYDB(c), map[string]string{}, migrator.RevisionTableFormatPtah, dir)),
		qt.DeepEquals, []progress{{Version: 1, State: "pending", Applied: 1, Total: 2}})
}

// connectAs opens the test database as user, who logs in with password.
func connectAs(c *qt.C, user, password string) *dbschema.DatabaseConnection {
	c.Helper()
	parsed, err := url.Parse(dbtarget.URL(c, dbtarget.YDB))
	c.Assert(err, qt.IsNil)
	parsed.User = url.UserPassword(user, password)
	conn, err := dbschema.ConnectToDatabase(c.Context(), parsed.String())
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = conn.Close() })
	return conn
}

// A user who may connect but may not create a coordination node at the root
// is told what is missing at each step, instead of an error that repeats the
// connection's credentials or a wait without end: the node does not exist and
// cannot be created, then it exists and may not be used, then the user may
// describe it but the server opens no session on it, and once granted the
// node the user takes the lock.
func TestYDBLock_TellsAUserWhatRightIsMissing(t *testing.T) {
	c := qt.New(t)
	admin := openYDB(c)
	const user, password = "ptahlocktest", "lockrights1"
	c.Assert(admin.Writer().ExecuteSQL(c.Context(), "DROP USER IF EXISTS "+user), qt.IsNil)
	c.Assert(admin.Writer().ExecuteSQL(c.Context(), "CREATE USER "+user+" PASSWORD '"+password+"'"), qt.IsNil)
	c.Cleanup(func() { _ = admin.Writer().ExecuteSQL(context.Background(), "DROP USER IF EXISTS "+user) })
	c.Assert(admin.Writer().ExecuteSQL(c.Context(), "GRANT CONNECT ON `/local` TO "+user), qt.IsNil)
	removeLockNode(c)
	locker := connectAs(c, user, password)
	grant := func(right string) {
		c.Assert(admin.Writer().ExecuteSQL(c.Context(), "GRANT "+right+" ON `/local/"+dblock.YDBLockNode+"` TO "+user),
			qt.IsNil)
	}

	_, noNode := dblock.Acquire(c.Context(), locker, "ptah_ydb_lock_rights", dblock.NoWait)
	created, err := dblock.Acquire(c.Context(), admin, "ptah_ydb_lock_rights", dblock.NoWait)
	c.Assert(err, qt.IsNil)
	c.Assert(created.Release(context.Background()), qt.IsNil)
	_, noAccess := dblock.Acquire(c.Context(), locker, "ptah_ydb_lock_rights", dblock.NoWait)
	grant("DESCRIBE SCHEMA")
	_, noSession := dblock.Acquire(c.Context(), locker, "ptah_ydb_lock_rights", dblock.NoWait)
	grant("ALL")
	taken, takenErr := dblock.Acquire(c.Context(), locker, "ptah_ydb_lock_rights", dblock.NoWait)

	c.Assert(noNode, qt.ErrorMatches, `Ptah's locks live on the YDB coordination node /local/ptah_locks, which does `+
		`not exist, and this user may not create it \(UNAUTHORIZED\)\. Run once as a user who may create a `+
		`coordination node at the database root, which creates it, and grant this user the right to use it`)
	c.Assert(noAccess, qt.ErrorMatches, `this user may not use the YDB coordination node /local/ptah_locks, which `+
		`holds Ptah's locks \(UNAUTHORIZED\); grant the user the right to use it`)
	c.Assert(noSession, qt.ErrorMatches, `open a session on the YDB coordination node /local/ptah_locks, which holds `+
		`Ptah's locks: the server opened none within 10s\. .*`)
	c.Assert(takenErr, qt.IsNil)
	c.Assert(taken.Release(context.Background()), qt.IsNil)
}

// A schema apply whose lock is lost fails with the loss even when everything
// it ran succeeded, and the work it ran under the lock saw its context end.
func TestYDBSchemaApplyLock_ReportsALoss(t *testing.T) {
	c := qt.New(t)
	var cause error

	runErr, releaseErr := atlasschema.WithApplyLockSession(c.Context(), openYDB(c), "ptah_ydb_apply_lost", 0,
		func(ctx context.Context, _ *dbschema.DatabaseConnection, _ *atlasschema.ApplyLock) error {
			dropLockNode(c)
			closedWithin(ctx.Done(), 15*time.Second)
			cause = context.Cause(ctx)
			return nil
		},
	)

	c.Assert(runErr, qt.ErrorMatches, `schema apply lock: advisory lock "ptah_ydb_apply_lost" on ydb was lost `+
		`while it was held: .*; the apply stopped there`)
	c.Assert(releaseErr, qt.IsNil)
	c.Assert(dblock.IsLost(cause), qt.IsTrue, qt.Commentf("the apply's context ended with %v", cause))
}
