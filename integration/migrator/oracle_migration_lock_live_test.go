//go:build integration

package migrator_test

import (
	"context"
	"database/sql"
	"strings"
	"sync"
	"testing"
	"testing/fstest"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/dblock"
	"ptah.run/internal/dbtarget"
	"ptah.run/migration/migrator"
)

// oracleLockTarget is an Oracle account of its own for one test, a connection
// to it, and one to the server's administrative account, which reads
// v$session.
type oracleLockTarget struct {
	url     string
	account string
	conn    *dbschema.DatabaseConnection
	admin   *dbschema.DatabaseConnection
}

func newOracleLockTarget(c *qt.C) *oracleLockTarget {
	c.Helper()
	target := &oracleLockTarget{url: oracleScratchAccountURL(c, dbtarget.OracleAdmin, "ptah_oralock")}
	target.account = strings.SplitN(strings.TrimPrefix(target.url, "oracle://"), ":", 2)[0]
	target.conn = connectOracleLock(c, target.url)
	target.admin = connectOracleLock(c, dbtarget.URL(c, dbtarget.OracleAdmin))
	return target
}

func connectOracleLock(c *qt.C, url string) *dbschema.DatabaseConnection {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(context.Background(), url)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	return conn
}

// migrator opens a migrator over fsys on a connection of its own, so each run
// is another session.
func (o *oracleLockTarget) migrator(c *qt.C, fsys fstest.MapFS) *migrator.Migrator {
	c.Helper()
	mig, err := migrator.NewFSMigrator(connectOracleLock(c, o.url), fsys)
	c.Assert(err, qt.IsNil)
	return mig
}

// holdMigrationLock takes the default migration lock from a session of its own
// until the returned function gives it back.
func (o *oracleLockTarget) holdMigrationLock(c *qt.C) *dblock.Lock {
	c.Helper()
	lock, err := dblock.Acquire(context.Background(), connectOracleLock(c, o.url), "ptah_migrate", time.Second)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = lock.Release(context.Background()) })
	return lock
}

// tables counts the tables of the account named table: one or none.
func (o *oracleLockTarget) tables(c *qt.C, table string) int {
	c.Helper()
	var count int
	c.Assert(o.conn.QueryRowContext(context.Background(),
		"SELECT COUNT(*) FROM user_tables WHERE table_name = UPPER(:1)", table).Scan(&count), qt.IsNil)
	return count
}

// rows counts the rows of a table the account has.
func (o *oracleLockTarget) rows(c *qt.C, table string) int {
	c.Helper()
	var count int
	c.Assert(o.conn.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM "+table).Scan(&count), qt.IsNil)
	return count
}

// waitForSessions reports whether, within half a minute, the server shows want
// sessions of the account that query reads from v$session counts.
func (o *oracleLockTarget) waitForSessions(c *qt.C, query string, want int) bool {
	c.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		var count int
		c.Assert(o.admin.QueryRowContext(context.Background(), query, strings.ToUpper(o.account)).Scan(&count), qt.IsNil)
		if count >= want {
			return true
		}
		time.Sleep(50 * time.Millisecond)
	}
	return false
}

// lockWaiters counts the sessions whose last statement asked for a lock row:
// the holder, and every run still asking.
const lockWaiters = `SELECT COUNT(DISTINCT s.sid) FROM v$session s JOIN v$sql q ON q.sql_id IN (s.sql_id, s.prev_sql_id)
WHERE s.username = :1 AND q.sql_text LIKE 'SELECT name FROM ptah_locks%'`

// blockedSessions counts the sessions of the account that wait for a row
// another transaction holds.
const blockedSessions = `SELECT COUNT(*) FROM v$session WHERE username = :1 AND blocking_session IS NOT NULL`

// oracleRevisionTable is the migrator's revision table as a statement names
// it: the migrator quotes the name, which keeps it lower case.
const oracleRevisionTable = `"schema_migrations"`

// oracleWidgets is one migration whose body cannot run twice: the second
// CREATE TABLE fails.
func oracleWidgets() fstest.MapFS {
	return fstest.MapFS{
		"0000000001_widgets.up.sql":   &fstest.MapFile{Data: []byte("CREATE TABLE widgets (id INTEGER PRIMARY KEY);\nINSERT INTO widgets (id) VALUES (1);\n")},
		"0000000001_widgets.down.sql": &fstest.MapFile{Data: []byte("DROP TABLE widgets;\n")},
	}
}

// Two runs that start while a third session holds the migration lock both
// wait for it, and then take it one after the other: the first applies the
// migration, and the second finds nothing pending. Without the lock both read
// an empty revision table, and one of them fails on the key of the row the
// other inserted, or runs the body a second time.
func TestOracleMigrationLockLive_HappyPath(t *testing.T) {
	c := qt.New(t)
	o := newOracleLockTarget(c)
	holder := o.holdMigrationLock(c)
	c.Assert(holder.Supported(), qt.IsTrue)
	runs := []*migrator.Migrator{
		o.migrator(c, oracleWidgets()).WithMigrationLockTimeout(time.Minute),
		o.migrator(c, oracleWidgets()).WithMigrationLockTimeout(time.Minute),
	}
	errs := make([]error, len(runs))
	var wg sync.WaitGroup
	for i, run := range runs {
		wg.Go(func() { errs[i] = run.MigrateUp(context.Background()) })
	}

	bothWaited := o.waitForSessions(c, lockWaiters, 3)
	c.Assert(holder.Release(context.Background()), qt.IsNil)
	wg.Wait()

	c.Assert(errs, qt.DeepEquals, []error{nil, nil})
	c.Assert(o.rows(c, "widgets"), qt.Equals, 1)
	c.Assert(o.rows(c, oracleRevisionTable), qt.Equals, 1)
	c.Assert(bothWaited, qt.IsTrue)
}

// A run that cannot take the lock within its timeout refuses with the
// migrator's timeout error, as on every engine with a lock, and applies
// nothing. A negative timeout asks once.
func TestOracleMigrationLockLive_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		timeout time.Duration
		wantErr string
		minWait time.Duration
	}{
		{
			name:    "a bounded wait",
			timeout: 500 * time.Millisecond,
			wantErr: `failed to acquire migration lock for migrate up: timed out acquiring migration lock "ptah_migrate" for oracle after 500ms`,
			minWait: 500 * time.Millisecond,
		},
		{
			name:    "no wait",
			timeout: dblock.NoWait,
			wantErr: `failed to acquire migration lock for migrate up: migration lock "ptah_migrate" for oracle is held by another runner`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			o := newOracleLockTarget(c)
			o.holdMigrationLock(c)
			run := o.migrator(c, oracleWidgets()).WithMigrationLockTimeout(test.timeout)
			started := time.Now()

			err := run.MigrateUp(context.Background())

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(err, qt.ErrorAs, new(*migrator.MigrationLockTimeoutError))
			c.Assert(time.Since(started) >= test.minWait, qt.IsTrue)
			c.Assert(o.tables(c, "widgets"), qt.Equals, 0)
		})
	}
}

// A retry of a failed migration rewrites the row it read only while the row
// is still the one it read. Here the stand-in for a first retry rewrites the
// row in a transaction it has not committed, to pending as a retry starts, or
// to a further applied statement as a retry that failed again leaves it. The
// second retry, which runs without the lock and read the row as it was, waits
// on that row, and once the first commits it matches nothing, refuses, and
// runs no statement of the body. Without the guard it rewrites the row again
// and runs the body a second time.
func TestOracleMigrationRetryLive_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		rewrite string
	}{
		{name: "another state", rewrite: "UPDATE " + oracleRevisionTable + " SET state = 'pending' WHERE version = 1"},
		{name: "a further statement applied", rewrite: "UPDATE " + oracleRevisionTable + " SET applied = 1 WHERE version = 1"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			o := newOracleLockTarget(c)
			fsys := fstest.MapFS{
				"0000000001_repairs.up.sql":   &fstest.MapFile{Data: []byte("INSERT INTO repairs (id) VALUES (1);\n")},
				"0000000001_repairs.down.sql": &fstest.MapFile{Data: []byte("DELETE FROM repairs;\n")},
			}
			c.Assert(o.migrator(c, fsys).MigrateUp(context.Background()), qt.ErrorMatches, `(?s).*ORA-00942.*`)
			_, err := o.conn.ExecContext(context.Background(), "CREATE TABLE repairs (id INTEGER)")
			c.Assert(err, qt.IsNil)
			first := o.beginRowWrite(c, test.rewrite)
			retry := o.migrator(c, fsys).WithoutMigrationLock()
			var retryErr error
			var wg sync.WaitGroup
			wg.Go(func() {
				retryErr = retry.MigrateUpWithOptions(context.Background(), migrator.MigrateUpOptions{AllowDirty: true})
			})

			waited := o.waitForSessions(c, blockedSessions, 1)
			c.Assert(first.Commit(), qt.IsNil)
			wg.Wait()

			c.Assert(retryErr, qt.ErrorIs, migrator.ErrRevisionClaimLost)
			c.Assert(retryErr, qt.ErrorMatches, `(?s).*the row of migration 1 no longer has state "failed".* 0 applied statements.*`)
			c.Assert(o.rows(c, "repairs"), qt.Equals, 0)
			c.Assert(waited, qt.IsTrue)
		})
	}
}

// A run that reaches the INSERT of a revision row another run inserted after
// it read the table refuses with the named error and runs no statement of the
// body. The stand-in for the other run inserts the row in a transaction it has
// not committed; the run without the lock waits on the key, and once the
// other commits, the server refuses its INSERT with ORA-00001.
func TestOracleMigrationRecordLive_FailurePath(t *testing.T) {
	c := qt.New(t)
	o := newOracleLockTarget(c)
	c.Assert(o.migrator(c, oracleWidgets()).MigrateUp(context.Background()), qt.IsNil)
	fsys := oracleWidgets()
	fsys["0000000002_gadgets.up.sql"] = &fstest.MapFile{Data: []byte("CREATE TABLE gadgets (id INTEGER);\n")}
	fsys["0000000002_gadgets.down.sql"] = &fstest.MapFile{Data: []byte("DROP TABLE gadgets;\n")}
	other := o.beginRowWrite(c, `INSERT INTO `+oracleRevisionTable+`
(version, description, applied_at, state, applied, total, error, error_stmt, execution_time_ms, checksum)
VALUES (2, 'gadgets', SYSTIMESTAMP, 'pending', 0, 1, NULL, NULL, 0, 'other')`)
	run := o.migrator(c, fsys).WithoutMigrationLock()
	var runErr error
	var wg sync.WaitGroup
	wg.Go(func() { runErr = run.MigrateUp(context.Background()) })

	waited := o.waitForSessions(c, blockedSessions, 1)
	c.Assert(other.Commit(), qt.IsNil)
	wg.Wait()

	c.Assert(runErr, qt.ErrorIs, migrator.ErrRevisionClaimLost)
	c.Assert(runErr, qt.ErrorMatches, `(?s)failed to record pending migration 2: another run recorded this migration after `+
		`this run read the revision table: the row of migration 2 has state "pending": .*ORA-00001.*`)
	c.Assert(o.tables(c, "gadgets"), qt.Equals, 0)
	c.Assert(waited, qt.IsTrue)
}

// beginRowWrite runs statement in a transaction it leaves open, so the row it
// writes stays locked until the test commits it.
func (o *oracleLockTarget) beginRowWrite(c *qt.C, statement string) *sql.Tx {
	c.Helper()
	tx, err := o.conn.BeginTx(context.Background(), nil)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = tx.Rollback() })
	_, err = tx.ExecContext(context.Background(), statement)
	c.Assert(err, qt.IsNil)
	return tx
}
