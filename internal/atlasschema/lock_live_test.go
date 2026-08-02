package atlasschema_test

// Live PostgreSQL coverage for the schema apply advisory lock: timeout under
// contention, context cancellation while waiting, and release unblocking the
// next acquirer. Gated on POSTGRES_TEST_DSN / TEST_DATABASE_URL like the
// migrator's lock integration tests.

import (
	"context"
	"database/sql"
	"os"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/atlasschema"
	"go.5x5.cz/ptah/internal/dblock"
)

func TestAcquireApplyLock_PostgresTimeoutLive(t *testing.T) {
	dbURL := livePostgresURLForLock(t)
	c := qt.New(t)
	ctx := context.Background()

	holderSession := holdApplyLockSession(c, dbURL)
	blocked := connectPostgresForLock(c, dbURL)

	start := time.Now()
	lock, err := atlasschema.AcquireApplyLock(ctx, blocked, 200*time.Millisecond)
	elapsed := time.Since(start)

	c.Assert(atlasschema.IsLockTimeout(err), qt.IsTrue, qt.Commentf("error: %v", err))
	c.Assert(err, qt.ErrorMatches, `acquire schema apply lock: timed out acquiring advisory lock "ptah_schema_apply" on postgres after 200ms`)
	c.Assert(lock, qt.IsNil)
	c.Assert(elapsed < 5*time.Second, qt.IsTrue, qt.Commentf("acquire waited %s", elapsed))

	// Releasing the competing session unblocks a bounded retry, proving the
	// lock is real, released, and re-acquirable.
	releaseApplyLockSession(c, holderSession)
	lock, err = atlasschema.AcquireApplyLock(ctx, blocked, 5*time.Second)
	c.Assert(err, qt.IsNil)
	c.Assert(lock.Supported(), qt.IsTrue)
	c.Assert(lock.Release(), qt.IsNil)
}

func TestAcquireApplyLock_PostgresCancellationLive(t *testing.T) {
	dbURL := livePostgresURLForLock(t)
	c := qt.New(t)

	holderSession := holdApplyLockSession(c, dbURL)
	defer releaseApplyLockSession(c, holderSession)
	blocked := connectPostgresForLock(c, dbURL)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	timer := time.AfterFunc(150*time.Millisecond, cancel)
	defer timer.Stop()

	start := time.Now()
	// A zero timeout waits indefinitely, so only the canceled context can
	// interrupt the wait; the result must not be misreported as a timeout.
	lock, err := atlasschema.AcquireApplyLock(ctx, blocked, 0)
	elapsed := time.Since(start)

	c.Assert(err, qt.IsNotNil)
	c.Assert(atlasschema.IsLockTimeout(err), qt.IsFalse, qt.Commentf("error: %v", err))
	c.Assert(lock, qt.IsNil)
	c.Assert(elapsed < 5*time.Second, qt.IsTrue, qt.Commentf("acquire waited %s", elapsed))
	c.Assert(ctx.Err(), qt.ErrorIs, context.Canceled)
}

func TestAcquireApplyLock_PostgresSerializesRunsLive(t *testing.T) {
	dbURL := livePostgresURLForLock(t)
	c := qt.New(t)
	ctx := context.Background()

	first := connectPostgresForLock(c, dbURL)
	second := connectPostgresForLock(c, dbURL)

	firstLock, err := atlasschema.AcquireApplyLock(ctx, first, 5*time.Second)
	c.Assert(err, qt.IsNil)
	c.Assert(firstLock.Supported(), qt.IsTrue)

	_, err = atlasschema.AcquireApplyLock(ctx, second, 200*time.Millisecond)
	c.Assert(atlasschema.IsLockTimeout(err), qt.IsTrue, qt.Commentf("error: %v", err))

	c.Assert(firstLock.Release(), qt.IsNil)

	secondLock, err := atlasschema.AcquireApplyLock(ctx, second, 5*time.Second)
	c.Assert(err, qt.IsNil)
	c.Assert(secondLock.Supported(), qt.IsTrue)
	c.Assert(secondLock.Release(), qt.IsNil)
}

// livePostgresURLForLock gates the live lock tests on the same environment
// variables as the migrator's PostgreSQL lock integration tests.
func livePostgresURLForLock(t *testing.T) string {
	t.Helper()
	dbURL := os.Getenv("POSTGRES_TEST_DSN")
	if dbURL == "" {
		dbURL = os.Getenv("TEST_DATABASE_URL")
	}
	if dbURL == "" {
		t.Skip("POSTGRES_TEST_DSN or TEST_DATABASE_URL not set")
	}
	if !strings.HasPrefix(dbURL, "postgres://") && !strings.HasPrefix(dbURL, "postgresql://") {
		t.Skip("PostgreSQL URL required for schema apply lock live tests")
	}
	return dbURL
}

func connectPostgresForLock(c *qt.C, dbURL string) *dbschema.DatabaseConnection {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(context.Background(), dbURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	return conn
}

// holdApplyLockSession takes the schema apply advisory lock from a competing
// session, so AcquireApplyLock calls in the test must wait.
func holdApplyLockSession(c *qt.C, dbURL string) *sql.Conn {
	c.Helper()
	ctx := context.Background()
	holder := connectPostgresForLock(c, dbURL)
	session, err := holder.Conn(ctx)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = session.Close() })
	_, err = session.ExecContext(ctx, "SELECT pg_advisory_lock($1)", dblock.PostgresKey(atlasschema.ApplyLockName))
	c.Assert(err, qt.IsNil)
	return session
}

func releaseApplyLockSession(c *qt.C, session *sql.Conn) {
	c.Helper()
	_, _ = session.ExecContext(context.Background(), "SELECT pg_advisory_unlock($1)", dblock.PostgresKey(atlasschema.ApplyLockName))
}
