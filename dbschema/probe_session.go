package dbschema

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"ptah.run/core/platform"
	"ptah.run/internal/probetx"
	"ptah.run/internal/sessiontx"
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
// On PostgreSQL, CockroachDB, YugabyteDB and SQL Server the server names the
// transaction before body runs and again when body returns. A body that ended
// the transaction and began another, with a COMMIT among the statements it ran,
// fails with an error whatever it returned: the statements before the COMMIT
// may be committed, and the rollback undoes only the transaction the session is
// in by then. On the PostgreSQL family the transaction also runs with
// standard_conforming_strings on, the reading declared text is checked under,
// and a body that turned it off fails the same way.
//
// A connection a [NativeConnector] opened reports ran false with a nil error
// too, and never runs body. It carries the provider's session and no *sql.DB,
// so there is no database/sql transaction the host could roll back, and the
// engine name the provider serves does not change that: a provider serving
// postgres answers the same way. The spellings a probe asks for are the
// provider's to give through its own comparison, so nothing is probed and
// each declaration compares as the provider reads it.
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
	if dc != nil && dc.native != nil {
		return false, nil
	}
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
	tx, err := sessiontx.Begin(ctx, session, &sql.TxOptions{})
	if err != nil {
		return false, fmt.Errorf("%s: begin transaction: %w", label, err)
	}
	defer func() {
		// The rollback is the point of the transaction, not its error path:
		// everything the body created exists only until this line runs.
		if rollbackErr := tx.Rollback(); rollbackErr != nil && !errors.Is(rollbackErr, sql.ErrTxDone) && !rollbackAfterCancel(ctx) {
			resultErr = errors.Join(resultErr, fmt.Errorf("%s: roll back: %w", label, rollbackErr))
		}
	}()
	// CockroachDB commits an explicit transaction ahead of a DDL statement
	// unless the transaction asks it not to; its connector does.
	if err := dc.engine().KeepDDLInTransaction(ctx, tx, dc.info); err != nil {
		return false, fmt.Errorf("%s: %w", label, err)
	}
	// Before the transaction is named, so the name carries the reading and a
	// probe that changes it fails the check as one that ends the transaction.
	if err := dc.engine().PinTextReading(ctx, tx); err != nil {
		return false, fmt.Errorf("%s: %w", label, err)
	}
	watched, err := probetx.Watch(ctx, func(ctx context.Context) (string, error) {
		return dc.engine().TransactionIdentity(ctx, tx)
	})
	if err != nil {
		return false, fmt.Errorf("%s: %w", label, err)
	}

	restore, err := markProbeStart(watched, tx, dc.info.Dialect)
	if err != nil {
		return false, fmt.Errorf("%s: %w", label, err)
	}

	err = body(watched, tx)
	// The rollback below undoes the transaction the session is in when it
	// runs. Where body ended the one begun above and began another, that is
	// all it undoes, so the check runs whatever body answered. A failure to
	// ask after body failed is the same failure seen again, and its own error
	// is not added; a changed transaction always is.
	if checkErr := probetx.Settle(watched, restore); checkErr != nil && (err == nil || errors.Is(checkErr, probetx.ErrTransactionChanged)) {
		err = errors.Join(err, fmt.Errorf("%s: %w", label, checkErr))
	}
	return true, err
}

// probeStartSavepoint is the savepoint [markProbeStart] sets. No probe names
// it, so the transaction it was set in is the only one that holds it.
const probeStartSavepoint = "ptah_probe_transaction"

// markProbeStart sets a savepoint in a watched transaction and returns the
// rollback to it, for [probetx.Settle] to name a transaction the body left
// failed: measured on PostgreSQL 18, a single-probe body that answers the
// server's refusal of its statement returns with the transaction aborted
// (SQLSTATE 25P02), and nothing names it until the rollback to a savepoint.
// An unwatched transaction, or an engine with no savepoint, gets none.
func markProbeStart(ctx context.Context, tx *sql.Tx, dialect string) (func(context.Context) error, error) {
	described, _ := platform.Lookup(dialect)
	syntax := described.Savepoint
	if !probetx.Watched(ctx) || syntax.Create == "" || syntax.Rollback == "" {
		return nil, nil
	}
	if _, err := tx.ExecContext(ctx, syntax.Create+" "+probeStartSavepoint); err != nil {
		return nil, fmt.Errorf("set savepoint %s: %w", probeStartSavepoint, err)
	}
	return func(ctx context.Context) error {
		_, err := tx.ExecContext(ctx, syntax.Rollback+" "+probeStartSavepoint)
		return err
	}, nil
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

// SessionOutsideTransaction reports whether the server says the session this
// connection is pinned to holds no transaction. pinned is false on a connection
// that is pinned to no session, and outside means nothing then.
//
// An answer of outside after a statement sent inside a transaction means the
// server ended that transaction. CockroachDB does so before a schema statement
// unless autocommit_before_ddl is off. Asking sends nothing on a PostgreSQL-wire
// session, so a failed statement cannot disturb the answer. Any driver or engine
// that cannot answer says it is not outside.
func (dc *DatabaseConnection) SessionOutsideTransaction(ctx context.Context) (outside, pinned bool) {
	if dc == nil || dc.session == nil {
		return false, false
	}
	return dc.engine().OutsideTransaction(ctx, dc.session), true
}
