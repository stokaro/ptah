package embedpg

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"slices"
	"time"

	"ptah.run/internal/embedgen"
	"ptah.run/internal/embedrun"
	"ptah.run/internal/embedstore"
)

// EnsureRunIndex builds a generation's index and records the run phase behind
// the same generation lifecycle lock.
//
// CREATE INDEX CONCURRENTLY cannot share a transaction with the run update.
// The advisory lock bridges that boundary: abandonment and retirement happen
// either before the initial validation or after the phase is durable. The
// final row lock and fencing-token increment preserve a checkpoint that lands
// during the index build and fence a worker still carrying its old snapshot.
func (s *Store) EnsureRunIndex(
	ctx context.Context, runID string, spec embedgen.Spec,
) (IndexOutcome, error) {
	initial, err := s.Run(ctx, runID)
	if err != nil {
		return "", err
	}
	identity := spec.Identity().Digest
	if err := validateIndexRun(initial, identity); err != nil {
		return "", err
	}

	var outcome IndexOutcome
	err = s.withLifecycleSession(ctx, "generation", identity, func(conn *sql.Conn) error {
		stored, err := scanRun(conn.QueryRowContext(ctx, selectRunSQL, runID), runID)
		if err != nil {
			return err
		}
		if err := validateIndexRun(stored, identity); err != nil {
			return err
		}
		// Validate the phase before changing the catalog. A run that has not
		// caught up cannot truthfully record indexing, and building first would
		// leave a successful artifact beside a refused lifecycle transition.
		if _, err := indexedRun(stored, identity); err != nil {
			return err
		}

		outcome, err = EnsureIndex(ctx, conn, spec)
		if err != nil {
			return err
		}
		if err := recordIndexedRun(ctx, conn, runID, identity); err != nil {
			return fmt.Errorf(
				"the index operation completed as %q, but run %s did not record phase %s: %w",
				outcome, runID, embedrun.PhaseIndexed, err)
		}
		return nil
	})
	return outcome, err
}

// recordIndexedRun rereads and updates the exact row in one transaction. A
// checkpoint that started before this transaction either commits first and is
// included, or loses the token comparison after this update commits.
func recordIndexedRun(
	ctx context.Context, conn *sql.Conn, runID, identity string,
) error {
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin index phase transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	stored, err := scanRun(tx.QueryRowContext(ctx, selectRunForUpdateSQL, runID), runID)
	if err != nil {
		return err
	}
	indexed, err := indexedRun(stored, identity)
	if err != nil {
		return err
	}
	cursor, err := encodeCursor(indexed.Cursor)
	if err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, updateRunSQL, runArguments(indexed, cursor)...)
	if err != nil {
		return fmt.Errorf("record index phase on run %s: %w", runID, err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("record index phase on run %s: %w", runID, err)
	}
	if changed != 1 {
		return fmt.Errorf("record index phase on run %s: %w", runID, embedstore.ErrConflict)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("record index phase on run %s: %w", runID, err)
	}
	return nil
}

// lifecycleSessionScope names one session-level advisory lock. Callers pass
// source scopes before generation scopes, matching every multi-lock lifecycle
// transaction in this package.
type lifecycleSessionScope struct {
	kind     string
	identity string
}

// withLifecycleSession pins DDL and one lifecycle lock to the same PostgreSQL
// session.
func (s *Store) withLifecycleSession(
	ctx context.Context, kind, identity string, action func(*sql.Conn) error,
) error {
	return s.withLifecycleSessionLocks(ctx,
		[]lifecycleSessionScope{{kind: kind, identity: identity}}, action)
}

// withLifecycleSessionLocks pins DDL and every lifecycle lock to one
// PostgreSQL session. A transaction-scoped lock held by another connection is
// not sufficient: it deadlocks a one-connection pool, and CREATE INDEX
// CONCURRENTLY may wait for the old transaction snapshot that is itself
// waiting for the index build to return.
func (s *Store) withLifecycleSessionLocks(
	ctx context.Context,
	scopes []lifecycleSessionScope,
	action func(*sql.Conn) error,
) error {
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("open inference lifecycle session: %w", err)
	}
	names := make([]string, 0, len(scopes))
	const acquire = `SELECT pg_advisory_lock(hashtextextended($1, 0))`
	for _, scope := range scopes {
		name := lifecycleLockName(scope.kind, scope.identity)
		if len(names) > 0 && names[len(names)-1] == name {
			continue
		}
		if _, err := conn.ExecContext(ctx, acquire, name); err != nil {
			cleanupErr := discardLifecycleConnection(conn)
			return errors.Join(
				fmt.Errorf("lock inference %s %s: %w", scope.kind, scope.identity, err),
				cleanupErr)
		}
		names = append(names, name)
	}

	runErr := action(conn)
	releaseCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var releaseErr error
	for _, name := range slices.Backward(names) {
		var released bool
		err := conn.QueryRowContext(releaseCtx,
			`SELECT pg_advisory_unlock(hashtextextended($1, 0))`, name).Scan(&released)
		if err == nil && !released {
			err = fmt.Errorf("inference lifecycle lock %s was not held", name)
		}
		if err != nil {
			releaseErr = errors.Join(releaseErr,
				fmt.Errorf("release inference lifecycle lock %s: %w", name, err))
		}
	}
	if releaseErr != nil {
		// A session lock survives transactions, so a connection whose explicit
		// unlock cannot be confirmed must never return to the pool. If the work
		// itself committed, discarding the session completes the cleanup and a
		// retry would misleadingly report the successful operation as failed.
		discardErr := discardLifecycleConnection(conn)
		if runErr != nil {
			return errors.Join(runErr, releaseErr, discardErr)
		}
		if discardErr != nil {
			return errors.Join(releaseErr, discardErr)
		}
		return nil
	}
	closeErr := conn.Close()
	if runErr != nil {
		return errors.Join(runErr, closeErr)
	}
	return closeErr
}

func lifecycleLockName(kind, identity string) string {
	return "ptah:inference:" + kind + ":" + identity
}

// discardLifecycleConnection makes a failed session unlock release its lock
// by closing the underlying PostgreSQL session rather than returning it to the
// database/sql pool.
func discardLifecycleConnection(conn *sql.Conn) error {
	rawErr := conn.Raw(func(any) error { return driver.ErrBadConn })
	if errors.Is(rawErr, driver.ErrBadConn) {
		rawErr = nil
	}
	closeErr := conn.Close()
	if errors.Is(closeErr, sql.ErrConnDone) {
		closeErr = nil
	}
	return errors.Join(rawErr, closeErr)
}

// indexedRun returns the exact row to persist after an index build.
func indexedRun(run embedrun.Run, identity string) (embedrun.Run, error) {
	if err := validateIndexRun(run, identity); err != nil {
		return embedrun.Run{}, err
	}
	run.FencingToken++
	run.LeaseOwner = ""
	run.LeaseExpires = time.Time{}
	if err := run.Reach(run.FencingToken, embedrun.PhaseIndexed); err != nil {
		return embedrun.Run{}, err
	}
	// Reach is deliberately a no-op when the run is already beyond indexed.
	// Raising the token still changed the row and needs its own timestamp.
	run.UpdatedAt = time.Now().UTC()
	return run, nil
}

func validateIndexRun(run embedrun.Run, identity string) error {
	if run.Terminal() {
		return fmt.Errorf("%w: run %s is %s", embedrun.ErrTerminal, run.ID, run.Status)
	}
	return run.DescribesGeneration(identity)
}
