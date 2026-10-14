package dbschema

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
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
	// CockroachDB commits an explicit transaction ahead of a DDL statement
	// unless the transaction asks it not to; its connector does.
	if err := dc.engine().KeepDDLInTransaction(ctx, tx, dc.info); err != nil {
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
	return dc.engine().OutsideTransaction(ctx, dc.session)
}
