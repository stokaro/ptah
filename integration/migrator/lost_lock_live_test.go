//go:build integration

package migrator_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"testing/fstest"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/dblock"
	"ptah.run/internal/dbtarget"
	"ptah.run/migration/migrator"
)

// lostLockName is the migration lock these tests take, so no other test's
// run waits on it or ends its session.
const lostLockName = "ptah_lost_lock_live"

// A migration run holds its lock on a session of its own and runs its
// statements over the pool. The server releases a session lock when the
// session ends -- pg_terminate_backend on PostgreSQL, KILL on MySQL -- and
// another runner may take it at once, so the run stops there: the statement in
// flight is canceled, nothing after it runs, and the run reports the lost
// lock. A second session takes the lock while the first run is still in its
// sleep, which is the overlap the stop keeps short.
func TestMigrator_StopsWhenItsLockSessionEnds(t *testing.T) {
	tests := []struct {
		name   string
		engine dbtarget.Engine
		// sleep runs long enough for the lock's session to be ended under it.
		sleep string
		// holder answers the id of the session holding the lock, given lockArg.
		holder  string
		lockArg any
		// running answers how many sessions run the sleep, given its text.
		running string
		// kill ends the session whose id it is given.
		kill string
		// tryLock takes the lock from another session without waiting, given
		// lockArg, and answers whether it did.
		tryLock string
		// exists answers whether the table named by its argument exists.
		exists string
	}{
		{
			name:    "postgres",
			engine:  dbtarget.PostgreSQL,
			sleep:   "SELECT pg_sleep(20)",
			holder:  "SELECT pid FROM pg_locks WHERE locktype = 'advisory' AND granted AND objid::bigint = $1",
			lockArg: dblock.PostgresKey(lostLockName),
			running: "SELECT COUNT(*) FROM pg_stat_activity WHERE state = 'active' AND starts_with(query, $1)",
			kill:    "SELECT pg_terminate_backend(%d)",
			tryLock: "SELECT pg_try_advisory_lock($1)::int",
			exists:  "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = current_schema() AND table_name = $1",
		},
		{
			name:    "mysql",
			engine:  dbtarget.MySQL,
			sleep:   "SELECT SLEEP(20)",
			holder:  "SELECT IS_USED_LOCK(?)",
			lockArg: lostLockName,
			running: "SELECT COUNT(*) FROM information_schema.processlist WHERE info LIKE CONCAT(?, '%')",
			kill:    "KILL %d",
			tryLock: "SELECT GET_LOCK(?, 0)",
			exists:  "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = ?",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			url := dbtarget.URL(c, test.engine)
			conn := connectLostLock(c, url)
			witness := connectLostLock(c, url)
			const after, revisions = "ptah_lost_lock_after", "ptah_lost_lock_revisions"
			dropLostLockTables(c, conn, after, revisions, revisions+"_log")
			c.Cleanup(func() { dropLostLockTables(c, conn, after, revisions, revisions+"_log") })
			m, err := migrator.NewFSMigrator(conn, fstest.MapFS{
				"0000000001_sleep.up.sql":   {Data: []byte(test.sleep + ";\nCREATE TABLE " + after + " (id INT);\n")},
				"0000000001_sleep.down.sql": {Data: []byte("DROP TABLE " + after + ";\n")},
			})
			c.Assert(err, qt.IsNil)
			m = m.WithMigrationsTable("", revisions).WithMigrationLockName(lostLockName)

			done := make(chan error, 1)
			started := time.Now()
			go func() { done <- m.MigrateUp(context.Background()) }()
			holder := lockHolder(c, witness, test.holder, test.lockArg)
			// The session is ended once the statement runs, after the lock
			// was taken and checked.
			waitForStatement(c, witness, test.running, test.sleep)
			_, err = witness.ExecContext(c.Context(), fmt.Sprintf(test.kill, holder))
			c.Assert(err, qt.IsNil)
			taken := takeLock(c, witness, test.tryLock, test.lockArg)
			runErr := <-done

			c.Assert(taken, qt.Equals, int64(1))
			c.Assert(dblock.IsLost(runErr), qt.IsTrue, qt.Commentf("error: %v", runErr))
			c.Assert(runErr, qt.ErrorMatches, `(?s)migration lock for migrate up: advisory lock "`+lostLockName+`" on `+
				test.name+` was lost while it was held.*`)
			c.Assert(time.Since(started) < 15*time.Second, qt.IsTrue, qt.Commentf("the run went on for %s", time.Since(started)))
			c.Assert(scalarInt(c, witness, test.exists, after), qt.Equals, int64(0))
		})
	}
}

// connectLostLock opens a connection the test closes.
func connectLostLock(c *qt.C, url string) *dbschema.DatabaseConnection {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), url)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	return conn
}

// dropLostLockTables drops what a run of the test may have left behind.
func dropLostLockTables(c *qt.C, conn *dbschema.DatabaseConnection, tables ...string) {
	c.Helper()
	for _, table := range tables {
		_, err := conn.ExecContext(context.Background(), "DROP TABLE IF EXISTS "+table)
		c.Assert(err, qt.IsNil)
	}
}

// lockHolder waits until a session holds the lock, and returns its id.
func lockHolder(c *qt.C, conn *dbschema.DatabaseConnection, query string, arg any) int64 {
	c.Helper()
	deadline := time.Now().Add(10 * time.Second)
	var holder sql.NullInt64
	for time.Now().Before(deadline) {
		err := conn.QueryRowContext(c.Context(), query, arg).Scan(&holder)
		if err == nil && holder.Valid {
			return holder.Int64
		}
		time.Sleep(50 * time.Millisecond)
	}
	c.Fatalf("no session took the lock within ten seconds")
	return 0
}

// takeLock asks for the lock until it is granted or ten seconds pass, and
// returns the last answer.
//
// KILL and pg_terminate_backend return once the server has been told to end
// the session, and the session gives its locks back when it has ended, a moment
// later. In CI on MySQL 26.7, GET_LOCK(name, 0) straight after KILL once
// answered 0. A lock nobody gives back is still not granted, so the answer is
// still 0 when the ten seconds are up.
func takeLock(c *qt.C, conn *dbschema.DatabaseConnection, query string, arg any) int64 {
	c.Helper()
	deadline := time.Now().Add(10 * time.Second)
	var taken int64
	for time.Now().Before(deadline) {
		taken = scalarInt(c, conn, query, arg)
		if taken == 1 {
			return taken
		}
		time.Sleep(50 * time.Millisecond)
	}
	return taken
}

// scalarInt reads one integer.
func scalarInt(c *qt.C, conn *dbschema.DatabaseConnection, query string, args ...any) int64 {
	c.Helper()
	var value int64
	c.Assert(conn.QueryRowContext(c.Context(), query, args...).Scan(&value), qt.IsNil, qt.Commentf("query: %s", query))
	return value
}

// waitForStatement waits until running, given statement, answers that a
// session runs it.
func waitForStatement(c *qt.C, conn *dbschema.DatabaseConnection, running, statement string) {
	c.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		var sessions int64
		err := conn.QueryRowContext(c.Context(), running, statement).Scan(&sessions)
		if err == nil && sessions > 0 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	c.Fatalf("no session ran %q within ten seconds", statement)
}
