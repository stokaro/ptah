package dbschema

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/stdlib"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
)

// WithRolledBackTransaction runs body inside one transaction, and rolls that
// transaction back whatever body does.
//
// It is the shape every caller that asks the server to normalize a declaration
// needs -- the expression probes in internal/dbexprprobe are those callers: a
// declaration is put through the same rewrite the catalog form went through,
// which means creating something, and nothing may survive the call. The
// caller's own loop lives in body; what this method owns is the session, the
// transaction and the guarantee.
//
// On a pool-backed connection the transaction runs on a session of its own,
// which is discarded rather than returned to the pool, because body may leave
// session-level state -- a temporary table, a search_path -- that the next
// borrower must not inherit.
//
// On a connection already pinned to a session (see
// [DatabaseConnection.WithSession]) the transaction runs on that session, and
// only when the driver reports the session outside any transaction: a
// PostgreSQL-wire session whose server status is idle, or a SQL Server session
// whose @@TRANCOUNT is zero. The rollback then takes back only what body did.
// A session inside a transaction, or one whose driver cannot say, reports ran
// false with a nil error and never runs body, because the rollback would
// discard the owner's work along with the probe's. The false is the whole
// answer -- a caller that must not proceed without the transaction has to check
// ran, not just err. label names the caller in every error.
//
// Do not call this on a pool-backed in-memory SQLite connection. Discarding the
// session takes such a database with it, because it has no existence apart from
// its only connection: the next statement runs against a fresh, empty one.
// Measured: a table created before the call is gone after it.
// [DatabaseConnection.WithSession] and
// [DatabaseConnection.WithIsolatedQuerySession] keep an in-memory database
// alive and are the methods to reach for there.
func (dc *DatabaseConnection) WithRolledBackTransaction(
	ctx context.Context,
	label string,
	body func(ctx context.Context, tx *sql.Tx) error,
) (ran bool, resultErr error) {
	if dc == nil || dc.db == nil {
		return false, fmt.Errorf("%s: database connection is nil", label)
	}
	if dc.pinned {
		if !dc.pinnedSessionOutsideTransaction(ctx) {
			return false, nil
		}
		return dc.rollBackOn(ctx, dc.session, label, body)
	}

	session, err := dc.db.Conn(ctx)
	if err != nil {
		return false, fmt.Errorf("%s: pin session: %w", label, err)
	}
	defer func() {
		resultErr = errors.Join(resultErr, discardSQLConnection(session, label+" session"))
	}()
	return dc.rollBackOn(ctx, session, label, body)
}

// rollBackOn runs body inside one transaction on session and rolls it back,
// whatever body does and whether or not the setup around it succeeded.
func (dc *DatabaseConnection) rollBackOn(
	ctx context.Context,
	session *sql.Conn,
	label string,
	body func(ctx context.Context, tx *sql.Tx) error,
) (ran bool, resultErr error) {
	tx, err := session.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return false, fmt.Errorf("%s: begin transaction: %w", label, err)
	}
	defer func() {
		// The rollback is the point of the transaction, not its error path:
		// everything the body created exists only until this line runs.
		if rollbackErr := tx.Rollback(); rollbackErr != nil && !errors.Is(rollbackErr, sql.ErrTxDone) {
			resultErr = errors.Join(resultErr, fmt.Errorf("%s: roll back: %w", label, rollbackErr))
		}
	}()
	if err := keepDDLInsideTheTransaction(ctx, tx, dc.info.Version); err != nil {
		return false, fmt.Errorf("%s: %w", label, err)
	}

	return true, body(ctx, tx)
}

// pinnedSessionOutsideTransaction reports whether the pinned session is
// provably outside any transaction.
//
// Comparisons run on pinned sessions: `schema apply` compares on the session
// that holds its apply lock, and `migrate diff` on the session its migration
// replay ran on. Answering false for every pinned session left each of them
// without a server-normalized expression, so a CHECK, a policy, an index
// predicate or a column default the server rewrites compared unequal to its own
// declaration and was dropped and created again on every run
// (stokaro/ptah#3643).
//
// The answer comes from the driver or the server, never from Ptah's own
// bookkeeping, because an owner can open a transaction with a plain BEGIN that
// no wrapper sees. pgx reports the status byte of the server's last
// ReadyForQuery message; SQL Server reports @@TRANCOUNT. Any other driver, and
// any failure to ask, answers false.
func (dc *DatabaseConnection) pinnedSessionOutsideTransaction(ctx context.Context) bool {
	if dc.session == nil {
		return false
	}
	if platform.NormalizeDialect(dc.info.Dialect) == platform.SQLServer {
		var open int
		if err := dc.session.QueryRowContext(ctx, "SELECT @@TRANCOUNT").Scan(&open); err != nil {
			return false
		}
		return open == 0
	}
	idle := false
	err := dc.session.Raw(func(driverConn any) error {
		conn, ok := driverConn.(*stdlib.Conn)
		if !ok {
			return nil
		}
		idle = conn.Conn().PgConn().TxStatus() == pgxTxStatusIdle
		return nil
	})
	return err == nil && idle
}

// pgxTxStatusIdle is the ReadyForQuery status byte of a session outside any
// transaction block; 'T' is inside one and 'E' inside a failed one.
const pgxTxStatusIdle = 'I'

// keepDDLInsideTheTransaction asks a server that would not to keep the caller's
// DDL where the caller put it.
//
// CockroachDB defaults autocommit_before_ddl to on: a DDL statement issued
// inside an explicit transaction makes the server COMMIT that transaction
// first and run the DDL in one of its own. Everything this file guarantees
// rests on the transaction still being there, and it is not.
//
// The symptom is not the commit, which is silent. It is what happens next --
// measured on cockroachdb/cockroach:v25.4.0, `ptah schema apply --dry-run`
// against any schema holding a CHECK constraint:
//
//	error: compare database schema: compare schemas: resolve check expressions:
//	roll back to savepoint after "customers.customers_amount_ck":
//	ERROR: savepoint "ptah_check_probe" does not exist (SQLSTATE 3B001)
//
// The probe's CREATE TEMPORARY TABLE is refused there anyway -- temp tables are
// experimental and off -- and the probe treats a refusal as "unresolved",
// which is the honest answer. It never gets to: the auto-commit ahead of the
// refused DDL has already taken the savepoint with it, so the recovery fails
// and the whole comparison fails with it. The reported error names the
// rollback, which is the one thing that was not wrong.
//
// With the setting off the same sequence recovers and the comparison continues.
// SET LOCAL rather than SET, so it lasts exactly as long as the transaction and
// cannot reach the next borrower of a pooled connection (stokaro/ptah#2140).
//
// Asked of CockroachDB alone. PostgreSQL has no such variable and answers
// `unrecognized configuration parameter`, and a failed statement inside a
// PostgreSQL transaction poisons every later one -- so a version this does not
// recognize is left alone rather than probed.
func keepDDLInsideTheTransaction(ctx context.Context, tx *sql.Tx, version string) error {
	if capability.BannerPlatform(version) != platform.CockroachDB {
		return nil
	}
	if _, err := tx.ExecContext(ctx, "SET LOCAL autocommit_before_ddl = off"); err != nil {
		return fmt.Errorf("keep DDL inside the transaction: %w", err)
	}
	return nil
}
