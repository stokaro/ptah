package atlasschema_test

// Live PostgreSQL coverage for the NAMED schema apply lock (stokaro/ptah#951).
//
// The two questions `--lock-name` raises can only be answered against a real
// advisory lock: does a run wait on a lock another process holds under the SAME
// name, and does it run straight through when the names DIFFER. Gated on
// POSTGRES_TEST_DSN / TEST_DATABASE_URL like the sibling lock live tests.

import (
	"context"
	"database/sql"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/atlasschema"
	"go.5x5.cz/ptah/internal/dblock"
)

const namedLockLive = "atlas_migrate_execute"

func TestAcquireApplyLock_NamedLockContendsLive(t *testing.T) {
	dbURL := livePostgresURLForLock(t)
	c := qt.New(t)
	ctx := context.Background()

	holder := holdNamedLockSession(c, dbURL, namedLockLive)
	blocked := connectPostgresForLock(c, dbURL)

	start := time.Now()
	lock, err := atlasschema.AcquireApplyLock(ctx, blocked, namedLockLive, 200*time.Millisecond)
	elapsed := time.Since(start)

	// A lock another process holds under the same name makes the apply wait
	// and then fail before the target is inspected.
	c.Assert(atlasschema.IsLockTimeout(err), qt.IsTrue, qt.Commentf("error: %v", err))
	c.Assert(err, qt.ErrorMatches,
		`acquire schema apply lock: timed out acquiring advisory lock "`+namedLockLive+`" on postgres after 200ms`)
	c.Assert(lock, qt.IsNil)
	c.Assert(elapsed < 5*time.Second, qt.IsTrue, qt.Commentf("acquire waited %s", elapsed))

	releaseNamedLockSession(c, holder, namedLockLive)
	lock, err = atlasschema.AcquireApplyLock(ctx, blocked, namedLockLive, 5*time.Second)
	c.Assert(err, qt.IsNil)
	c.Assert(lock.Supported(), qt.IsTrue)
	c.Assert(lock.Name(), qt.Equals, namedLockLive)
	c.Assert(lock.Release(), qt.IsNil)
}

func TestAcquireApplyLock_DistinctNamesDoNotContendLive(t *testing.T) {
	dbURL := livePostgresURLForLock(t)
	c := qt.New(t)
	ctx := context.Background()

	holder := holdNamedLockSession(c, dbURL, namedLockLive)
	defer releaseNamedLockSession(c, holder, namedLockLive)
	other := connectPostgresForLock(c, dbURL)

	// The default lock is a different name, so the held lock is irrelevant to
	// it. This is what makes --lock-name a real choice rather than a label: a
	// run that names a different lock does not serialize against this holder.
	lock, err := atlasschema.AcquireApplyLock(ctx, other, "", 200*time.Millisecond)

	c.Assert(err, qt.IsNil)
	c.Assert(lock.Supported(), qt.IsTrue)
	c.Assert(lock.Name(), qt.Equals, atlasschema.ApplyLockName)
	c.Assert(lock.Release(), qt.IsNil)
}

func holdNamedLockSession(c *qt.C, dbURL, name string) *sql.Conn {
	c.Helper()
	ctx := context.Background()
	holder := connectPostgresForLock(c, dbURL)
	session, err := holder.Conn(ctx)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = session.Close() })
	_, err = session.ExecContext(ctx, "SELECT pg_advisory_lock($1)", dblock.PostgresKey(name))
	c.Assert(err, qt.IsNil)
	return session
}

func releaseNamedLockSession(c *qt.C, session *sql.Conn, name string) {
	c.Helper()
	_, _ = session.ExecContext(context.Background(), "SELECT pg_advisory_unlock($1)", dblock.PostgresKey(name))
}
