//go:build integration

package dblock_test

import (
	"context"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/dblock"
	"ptah.run/internal/dbtarget"
)

// TestAdvisoryLock_NoWaitRefusesAHeldLockAtOnceLive: under [dblock.NoWait] a
// lock another session holds is refused on the first try, on every engine
// with a session advisory lock. Each engine is asked in its own spelling --
// pg_try_advisory_lock once, GET_LOCK with a zero wait, sp_getapplock with a
// zero @LockTimeout -- so each has to answer. The context would outlast any
// wait, so a refusal that came from it would name the deadline instead; and
// the lock is taken again under NoWait once it is free, so the refusal is
// about the holder and not about NoWait itself.
func TestAdvisoryLock_NoWaitRefusesAHeldLockAtOnceLive(t *testing.T) {
	tests := []struct {
		name    string
		engine  dbtarget.Engine
		dialect string
	}{
		{name: "postgres", engine: dbtarget.PostgreSQL, dialect: "postgres"},
		{name: "yugabytedb", engine: dbtarget.YugabyteDB, dialect: "yugabytedb"},
		{name: "mysql", engine: dbtarget.MySQL, dialect: "mysql"},
		{name: "mariadb", engine: dbtarget.MariaDB, dialect: "mariadb"},
		{name: "sqlserver", engine: dbtarget.SQLServer, dialect: "sqlserver"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			databaseURL := dbtarget.URL(c, test.engine)
			first, err := dbschema.ConnectToDatabase(c.Context(), databaseURL)
			c.Assert(err, qt.IsNil)
			c.Cleanup(func() { dbschema.CloseAndWarn(first) })
			second, err := dbschema.ConnectToDatabase(c.Context(), databaseURL)
			c.Assert(err, qt.IsNil)
			c.Cleanup(func() { dbschema.CloseAndWarn(second) })
			const name = "ptah_dblock_nowait_live"
			held, err := dblock.Acquire(c.Context(), first, name, 5*time.Second)
			c.Assert(err, qt.IsNil)
			c.Assert(held.Supported(), qt.IsTrue)
			waitCtx, cancel := context.WithTimeout(c.Context(), time.Minute)
			defer cancel()

			refused, err := dblock.Acquire(waitCtx, second, name, dblock.NoWait)

			c.Assert(err, qt.ErrorMatches,
				`advisory lock "ptah_dblock_nowait_live" on `+test.dialect+` is held by another session`)
			c.Assert(dblock.IsTimeout(err), qt.IsTrue)
			c.Assert(refused, qt.IsNil)
			c.Assert(waitCtx.Err(), qt.IsNil)
			c.Assert(held.Release(c.Context()), qt.IsNil)
			taken, err := dblock.Acquire(waitCtx, second, name, dblock.NoWait)
			c.Assert(err, qt.IsNil)
			c.Assert(taken.Release(c.Context()), qt.IsNil)
		})
	}
}
