package dbschema

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// probeSession runs a body inside one pinned session and one transaction, and
// rolls that transaction back whatever the body does.
//
// It is the shape every resolver in this package that asks the server to
// normalize a declaration needs: the declaration is put through the same
// rewrite the catalog form went through, which means creating something, and
// nothing may survive the call. Each caller's own loop lives in body; what is
// shared is the session, the transaction and the guarantee.
//
// The session is pinned and discarded rather than returned to the pool, because
// a probe may leave session-level state -- a temporary table, a search_path --
// that the next borrower must not inherit.
//
// A pinned connection returns false without running anything: it is already
// inside somebody else's session, and the rollback would discard their work
// rather than the probe's.
func (dc *DatabaseConnection) probeSession(
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
	defer func() {
		// The rollback is the point of the transaction, not its error path:
		// everything the body created exists only until this line runs.
		if rollbackErr := tx.Rollback(); rollbackErr != nil && !errors.Is(rollbackErr, sql.ErrTxDone) {
			resultErr = errors.Join(resultErr, fmt.Errorf("%s: roll back: %w", label, rollbackErr))
		}
	}()

	return true, body(ctx, tx)
}

// resolveProbes runs every probe through one rolled-back transaction and
// collects the answers, keyed the way the caller keys them.
//
// It is a package function rather than a method because a method cannot carry
// type parameters, and the two things that vary between resolvers are exactly
// the probe and the answer. What does not vary -- the session, the transaction,
// the rollback, and returning nil for a pinned connection -- lives in
// [DatabaseConnection.probeSession] beneath it.
func resolveProbes[Probe any, Answer any](
	ctx context.Context,
	dc *DatabaseConnection,
	label string,
	probes []Probe,
	key func(Probe) string,
	one func(ctx context.Context, tx *sql.Tx, index int, probe Probe) (Answer, error),
) (map[string]Answer, error) {
	resolved := make(map[string]Answer, len(probes))
	ran, err := dc.probeSession(ctx, label, func(ctx context.Context, tx *sql.Tx) error {
		for i, probe := range probes {
			answer, err := one(ctx, tx, i, probe)
			if err != nil {
				return err
			}
			resolved[key(probe)] = answer
		}
		return nil
	})
	if err != nil || !ran {
		return nil, err
	}
	return resolved, nil
}

// runProbe creates one probe's objects inside a savepoint, reads the answer
// back, and releases the savepoint.
//
// The savepoint is what keeps one refused declaration from taking the rest with
// it: a statement the server rejects aborts the transaction, and without it the
// first unparseable expression would report a whole schema as uncomparable.
//
// It answers false, with no error, for a declaration the server refused. That
// is the honest result: refusing here would fail a comparison over an object
// the server will refuse later anyway, with a worse message.
func runProbe(
	ctx context.Context,
	tx *sql.Tx,
	label, key, savepoint string,
	statements []string,
	read func(ctx context.Context, tx *sql.Tx) error,
) (bool, error) {
	if _, err := tx.ExecContext(ctx, "SAVEPOINT "+savepoint); err != nil {
		return false, fmt.Errorf("%s: savepoint: %w", label, err)
	}
	for _, statement := range statements {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			if _, rollbackErr := tx.ExecContext(ctx, "ROLLBACK TO SAVEPOINT "+savepoint); rollbackErr != nil {
				return false, fmt.Errorf("%s: roll back to savepoint after %q: %w", label, key, rollbackErr)
			}
			return false, nil
		}
	}
	if err := read(ctx, tx); err != nil {
		return false, fmt.Errorf("%s: read back %q: %w", label, key, err)
	}
	if _, err := tx.ExecContext(ctx, "ROLLBACK TO SAVEPOINT "+savepoint); err != nil {
		return false, fmt.Errorf("%s: release probe: %w", label, err)
	}
	return true, nil
}
