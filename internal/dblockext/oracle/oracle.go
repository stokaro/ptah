// Package oracle takes Ptah's lock on Oracle for internal/dblock: a row lock
// on the row a lock name has in the table ptah_locks, taken with
// SELECT ... FOR UPDATE NOWAIT, asked again until the wait ends, and given
// back with ROLLBACK.
//
// Oracle's own named lock, DBMS_LOCK.REQUEST with release_on_commit set to
// FALSE, would outlive the commit each DDL statement makes, and needs no
// table. It is not used because an ordinary account cannot call it: EXECUTE
// on SYS.DBMS_LOCK goes to no role an account is given by default, and only
// SYS can grant it. Measured on Oracle AI Database 26ai Free 23.26.4
// (gvenzl/oracle-free:slim): the image's application account gets
// `PLS-00201: identifier 'DBMS_LOCK' must be declared`, and SYSTEM's `GRANT
// EXECUTE ON SYS.DBMS_LOCK` answers ORA-01031. A lock that only SYS can
// enable would refuse every run of an ordinary account. A row lock needs what
// a migration needs already: CREATE TABLE in its own schema.
//
// A row lock ends with the transaction that took it, and Oracle commits every
// DDL statement, so the session holding it must run nothing but the lock's
// own statements. The services say so with ReleasedByCommit, and dblock takes
// the lock on a session of its own. Measured on the same server through
// go-ora: a lock taken with FOR UPDATE NOWAIT outside an explicit transaction
// held through a ping and through a CREATE TABLE on another session, which
// was refused the row with ORA-00054, and a CREATE TABLE on the session
// holding it released it.
package oracle

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"ptah.run/core/platform"
	"ptah.run/internal/dblockext"
	"ptah.run/internal/servercode"
)

// table keeps one row per lock name, keyed by the name exactly as the caller
// gives it, which is what the lock locks. Its rows are never deleted: a row
// outlives every run, and only its lock ends with the session that holds it.
const table = "ptah_locks"

// pollInterval is how often a wait asks for the lock again.
const pollInterval = 25 * time.Millisecond

// The server's numbers for the answers a lock statement reads.
const (
	// resourceBusy is ORA-00054: another session holds the row, and NOWAIT
	// does not wait for it.
	resourceBusy = 54
	// tableMissing is ORA-00942: the lock table does not exist yet.
	tableMissing = 942
)

// ensureRow creates the lock table when it is missing, inserts the row of the
// lock name when it is missing, and commits. It runs on the lock's session
// before the row is locked, so the commit gives back nothing. A second
// session inserting the same name concurrently waits for the first one's
// commit and then finds the name taken, which is what DUP_VAL_ON_INDEX
// ignores; ORA-00955 is a table another session created first.
const ensureRow = `DECLARE
  lock_name VARCHAR2(1000) := :1;
  missing_table EXCEPTION;
  PRAGMA EXCEPTION_INIT(missing_table, -942);
  name_taken EXCEPTION;
  PRAGMA EXCEPTION_INIT(name_taken, -955);
BEGIN
  BEGIN
    EXECUTE IMMEDIATE 'INSERT INTO ` + table + ` (name) VALUES (:name)' USING lock_name;
  EXCEPTION
    WHEN DUP_VAL_ON_INDEX THEN NULL;
    WHEN missing_table THEN
      BEGIN
        EXECUTE IMMEDIATE 'CREATE TABLE ` + table + ` (name VARCHAR2(1000) PRIMARY KEY)';
      EXCEPTION
        WHEN name_taken THEN NULL;
      END;
      BEGIN
        EXECUTE IMMEDIATE 'INSERT INTO ` + table + ` (name) VALUES (:name)' USING lock_name;
      EXCEPTION
        WHEN DUP_VAL_ON_INDEX THEN NULL;
      END;
  END;
  COMMIT;
END;`

// lockRow locks the row of the lock name, or fails at once with ORA-00054
// when another session holds it.
const lockRow = "SELECT name FROM " + table + " WHERE name = :1 FOR UPDATE NOWAIT"

// Services returns Oracle's lock.
func Services() dblockext.Services {
	return dblockext.Services{
		Dialects:         []string{platform.Oracle},
		Session:          rowLock{},
		ReleasedByCommit: true,
		Table:            table,
	}
}

type rowLock struct{}

// Acquire makes sure the row exists, then asks for its lock every
// pollInterval until it is granted, the timeout passes or ctx ends. A
// negative timeout asks once.
//
// A preview writes nothing, so it creates neither the table nor the row. A
// lock name without its row has never been taken in this schema, so there is
// nobody to wait for, and the preview goes on holding nothing.
func (rowLock) Acquire(ctx context.Context, session dblockext.Session, request dblockext.Request) error {
	if !request.Preview {
		if _, err := session.ExecContext(ctx, ensureRow, request.Name); err != nil {
			return &dblockext.AmbiguousError{Err: fmt.Errorf("create the row of lock %q in %s: %w",
				request.Name, table, err)}
		}
	}

	lockCtx := ctx
	var cancel context.CancelFunc
	if request.Timeout > 0 {
		lockCtx, cancel = context.WithTimeout(ctx, request.Timeout)
		defer cancel()
	}
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	for {
		var locked string
		err := session.QueryRowContext(lockCtx, lockRow, request.Name).Scan(&locked)
		_, number := servercode.Read(err)
		switch {
		case err == nil:
			return nil
		case request.Preview && (number == tableMissing || errors.Is(err, sql.ErrNoRows)):
			return nil
		case errors.Is(err, sql.ErrNoRows):
			return fmt.Errorf("the row of lock %q is missing from %s", request.Name, table)
		case number != resourceBusy:
			return &dblockext.AmbiguousError{Err: waitError(ctx, lockCtx, request, err)}
		case request.Timeout < 0:
			return request.Held
		}

		select {
		case <-lockCtx.Done():
			return waitError(ctx, lockCtx, request, lockCtx.Err())
		case <-ticker.C:
		}
	}
}

// waitError is request.Held when the wait ended because its timeout passed
// while ctx was still live, and fallback otherwise.
func waitError(ctx, lockCtx context.Context, request dblockext.Request, fallback error) error {
	if request.Timeout > 0 &&
		errors.Is(lockCtx.Err(), context.DeadlineExceeded) &&
		ctx.Err() == nil {
		return request.Held
	}
	return fallback
}

// Release ends the session's transaction, which gives the row back. The
// session runs nothing else, so the transaction holds nothing but the lock.
func (rowLock) Release(ctx context.Context, session dblockext.Session, _ dblockext.Request) error {
	_, err := session.ExecContext(ctx, "ROLLBACK")
	return err
}
