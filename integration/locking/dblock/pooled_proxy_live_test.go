//go:build integration

package dblock_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/jackc/pgx/v5/stdlib"

	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/dblock"
	"go.5x5.cz/ptah/internal/dbtarget"
)

// TestAdvisoryLock_RefusedThroughATransactionPooler is the live half of the
// refusal stokaro/ptah#1029 added.
//
// A PostgreSQL advisory lock is SESSION-scoped, and a transaction pooler hands
// a client whichever backend is free between transactions. So two clients can
// be handed the SAME backend, where the lock is reentrant and answers true
// again: it fails OPEN, and two migration runs would both believe they held
// it. That is the one shape no offline test can reproduce, because the fake
// server on both sides is written by the same hand.
func TestAdvisoryLock_RefusedThroughATransactionPooler(t *testing.T) {
	pooledURL := dbtarget.URL(t, dbtarget.PostgreSQLPooled)
	c := qt.New(t)
	ctx := t.Context()

	conn, err := dbschema.ConnectToDatabase(ctx, pooledURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	name := fmt.Sprintf("ptah_pooled_%d", time.Now().UnixNano())
	lock, err := dblock.Acquire(ctx, conn, name, 5*time.Second)

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "excludes nothing on this connection")
	c.Assert(err.Error(), qt.Contains, "PTAH_ALLOW_UNVERIFIED_MIGRATION_LOCK")
	c.Assert(lock, qt.IsNil)
}

// TestAdvisoryLock_AcquiredDirectlyAgainstTheSameDatabase is the control the
// test above needs.
//
// Both reach one PostgreSQL server; only the route differs. Without this, a
// refusal that fired for any other reason -- a wrong DSN, a server that
// refuses advisory locks at all -- would read as the proxy being detected.
func TestAdvisoryLock_AcquiredDirectlyAgainstTheSameDatabase(t *testing.T) {
	directURL := dbtarget.URL(t, dbtarget.PostgreSQL)
	c := qt.New(t)
	ctx := t.Context()

	conn, err := dbschema.ConnectToDatabase(ctx, directURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	name := fmt.Sprintf("ptah_direct_%d", time.Now().UnixNano())
	lock, err := dblock.Acquire(ctx, conn, name, 5*time.Second)

	c.Assert(err, qt.IsNil)
	c.Assert(lock, qt.IsNotNil)
	c.Assert(lock.Release(ctx), qt.IsNil)
}

// TestAdvisoryLock_TheOptOutIsHonoredThroughTheProxy pins the documented
// escape, so the refusal cannot become one nobody can get past.
//
// What the variable turns off is the PROOF, not the lock: the lock is still
// taken, and what is skipped is the check that it excludes anybody. An
// operator who knows the topology and accepts the risk gets through; nobody
// gets through by accident.
func TestAdvisoryLock_TheOptOutIsHonoredThroughTheProxy(t *testing.T) {
	pooledURL := dbtarget.URL(t, dbtarget.PostgreSQLPooled)
	c := qt.New(t)
	ctx := t.Context()
	t.Setenv("PTAH_ALLOW_UNVERIFIED_MIGRATION_LOCK", "1")

	conn, err := dbschema.ConnectToDatabase(ctx, pooledURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	name := fmt.Sprintf("ptah_optout_%d", time.Now().UnixNano())
	lock, err := dblock.Acquire(ctx, conn, name, 5*time.Second)

	c.Assert(err, qt.IsNil)
	c.Assert(lock, qt.IsNotNil)
	c.Assert(lock.Release(ctx), qt.IsNil)
}

// TestTransactionScopedAdvisoryLock_ExcludesThroughTheProxy records the
// mechanism the session lock cannot use, measured rather than asserted from
// the documentation.
//
// pg_advisory_xact_lock is held to the end of the TRANSACTION, and a
// transaction pooler keeps one backend for the whole of one -- so two
// concurrent transactions get two backends and the second is refused.
// Measured through PgBouncer 1.25.2 in transaction mode:
//
//	a = true  (backend 79)
//	b = false (backend 82)
//
// It is not what dblock.Acquire uses, because the lock there is held across
// planning and applying rather than inside one transaction. This test exists
// so the property is a pinned fact for whoever makes that change, and so a
// PgBouncer release that stopped separating the backends is noticed here
// rather than in a migration (stokaro/ptah#1029).
func TestTransactionScopedAdvisoryLock_ExcludesThroughTheProxy(t *testing.T) {
	pooledURL := dbtarget.DriverDSN(t, dbtarget.PostgreSQLPooled)
	c := qt.New(t)
	ctx := t.Context()

	first := openPooledClient(c, pooledURL)
	defer func() { c.Check(first.Close(), qt.IsNil) }()
	second := openPooledClient(c, pooledURL)
	defer func() { c.Check(second.Close(), qt.IsNil) }()

	key := time.Now().UnixNano() % 1_000_000_000

	firstTx, err := first.BeginTx(ctx, nil)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(firstTx.Rollback(), qt.IsNil) }()
	c.Assert(tryTransactionLock(c, ctx, firstTx, key), qt.IsTrue)

	secondTx, err := second.BeginTx(ctx, nil)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(secondTx.Rollback(), qt.IsNil) }()
	c.Assert(tryTransactionLock(c, ctx, secondTx, key), qt.IsFalse)

	// The two transactions really did land on different backends, which is
	// what makes the refusal above the lock's doing rather than the pooler's.
	c.Assert(backendPID(c, ctx, firstTx), qt.Not(qt.Equals), backendPID(c, ctx, secondTx))
}

// openPooledClient opens a client whose pool holds exactly one connection, so
// two clients are two clients rather than two handles onto one.
func openPooledClient(c *qt.C, dsn string) *sql.DB {
	c.Helper()
	db, err := sql.Open("pgx", dsn)
	c.Assert(err, qt.IsNil)
	db.SetMaxOpenConns(1)
	return db
}

func tryTransactionLock(c *qt.C, ctx context.Context, tx *sql.Tx, key int64) bool {
	c.Helper()
	var acquired bool
	c.Assert(tx.QueryRowContext(ctx, "SELECT pg_try_advisory_xact_lock($1)", key).Scan(&acquired), qt.IsNil)
	return acquired
}

func backendPID(c *qt.C, ctx context.Context, tx *sql.Tx) int {
	c.Helper()
	var pid int
	c.Assert(tx.QueryRowContext(ctx, "SELECT pg_backend_pid()").Scan(&pid), qt.IsNil)
	return pid
}
