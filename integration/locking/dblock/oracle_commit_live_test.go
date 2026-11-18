//go:build integration

package dblock_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/dblock"
	"ptah.run/internal/dbtarget"
)

// Oracle's lock is a row lock, which a commit gives back, and Oracle commits
// every DDL statement. A caller that pins a session for its work, as `schema
// apply` does, runs DDL on it, so the lock is taken on another session: after
// the work's CREATE TABLE committed on the pinned session, a second session is
// still refused the lock. Taken on the pinned session, the CREATE TABLE would
// have given it back, and the second session would get it.
func TestWithLockSessionOracleLive_HappyPath(t *testing.T) {
	c := qt.New(t)
	databaseURL := dbtarget.URL(c, dbtarget.Oracle)
	conn, err := dbschema.ConnectToDatabase(c.Context(), databaseURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	other, err := dbschema.ConnectToDatabase(c.Context(), databaseURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(other) })
	const name = "ptah_dblock_oracle_ddl_live"
	table := fmt.Sprintf("ptah_dblock_ddl_%d", os.Getpid())
	c.Cleanup(func() { _, _ = conn.ExecContext(context.Background(), "DROP TABLE "+table+" PURGE") })
	var ddlErr, duringWork error
	var supported bool

	runErr, releaseErr := dblock.WithLockSession(c.Context(), conn, name, 5*time.Second,
		func(session *dbschema.DatabaseConnection, lock *dblock.Lock) error {
			supported = lock.Supported()
			_, ddlErr = session.ExecContext(c.Context(), "CREATE TABLE "+table+" (id INTEGER)")
			_, duringWork = dblock.Acquire(c.Context(), other, name, dblock.NoWait)
			return nil
		})

	c.Assert(runErr, qt.IsNil)
	c.Assert(releaseErr, qt.IsNil)
	c.Assert(supported, qt.IsTrue)
	c.Assert(ddlErr, qt.IsNil)
	c.Assert(duringWork, qt.ErrorMatches, `advisory lock "ptah_dblock_oracle_ddl_live" on oracle is held by another session`)
	taken, err := dblock.Acquire(c.Context(), other, name, dblock.NoWait)
	c.Assert(err, qt.IsNil)
	c.Assert(taken.Release(c.Context()), qt.IsNil)
}
