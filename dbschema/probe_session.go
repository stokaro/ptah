package dbschema

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/capability"
)

// WithRolledBackTransaction runs body inside one throwaway session and one
// transaction, and rolls that transaction back whatever body does.
//
// It is the shape every caller that asks the server to normalize a declaration
// needs -- the expression probes in internal/dbexprprobe are those callers: a
// declaration is put through the same rewrite the catalog form went through,
// which means creating something, and nothing may survive the call. The
// caller's own loop lives in body; what this method owns is the session, the
// transaction and the guarantee.
//
// The session is pinned and discarded rather than returned to the pool,
// because body may leave session-level state -- a temporary table, a
// search_path -- that the next borrower must not inherit.
//
// A connection already pinned to a session (see
// [DatabaseConnection.WithSession]) reports ran false with a nil error and
// never runs body: it is already inside somebody else's session, and the
// rollback would discard their work rather than the caller's. The false is the
// whole answer -- a caller that must not proceed without the transaction has
// to check ran, not just err. label names the caller in every error.
//
// Do not call this on an in-memory SQLite connection. Discarding the session
// takes such a database with it, because it has no existence apart from its
// only connection: the next statement runs against a fresh, empty one.
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
		return false, nil
	}

	session, err := dc.db.Conn(ctx)
	if err != nil {
		return false, fmt.Errorf("%s: pin session: %w", label, err)
	}
	defer func() {
		resultErr = errors.Join(resultErr, discardSQLConnection(session, label+" session"))
	}()

	tx, err := session.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return false, fmt.Errorf("%s: begin transaction: %w", label, err)
	}
	if err := keepDDLInsideTheTransaction(ctx, tx, dc.info.Version); err != nil {
		return false, fmt.Errorf("%s: %w", label, err)
	}
	defer func() {
		// The rollback is the point of the transaction, not its error path:
		// everything the body created exists only until this line runs.
		if rollbackErr := tx.Rollback(); rollbackErr != nil && !errors.Is(rollbackErr, sql.ErrTxDone) {
			resultErr = errors.Join(resultErr, fmt.Errorf("%s: roll back: %w", label, rollbackErr))
		}
	}()

	return true, body(ctx, tx)
}

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
