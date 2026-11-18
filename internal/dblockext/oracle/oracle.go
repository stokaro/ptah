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
// The table lives in the schema the request names, and in the lock session's
// current schema when it names none. The migrator names the schema its
// revision table is configured into, so two accounts that keep one revision
// table lock one row. With none configured, the revision table is in the
// session's current schema too, which the connector reads as
// SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA') on a session of the same pool. An
// Oracle URL cannot name another one: go-ora refuses ?schema= with "unknown
// URL option: schema" before the connection opens. The schema is quoted as the
// revision table's is, and the table name is not, so it folds to PTAH_LOCKS as
// an unquoted name does.
//
// In a schema the account does not own, creating the table needs CREATE ANY
// TABLE. Without it, a DBA creates the table and grants the account SELECT,
// which FOR UPDATE needs (READ does not allow it), and INSERT, for the row of
// each new lock name:
//
//	CREATE TABLE "APP".ptah_locks (name VARCHAR2(1000) PRIMARY KEY);
//	GRANT SELECT, INSERT ON "APP".ptah_locks TO deployer;
//
// Measured on the same server: with those grants the account locks the table's
// rows, and with neither the table nor CREATE ANY TABLE the lock refuses and
// names both remedies. It does not fall back to the account's own schema,
// whose table would exclude no other account recording in the same revision
// table.
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
	"ptah.run/internal/sqlident"
)

// table keeps one row per lock name, keyed by the name exactly as the caller
// gives it, which is what the lock locks. Its rows are never deleted: a row
// outlives every run, and only its lock ends with the session that holds it.
const table = "ptah_locks"

// tableIn is the lock table in schema, or in the session's current schema when
// schema is empty.
func tableIn(schema string) string {
	if schema == "" {
		return table
	}
	return sqlident.Quote(platform.Oracle, schema) + "." + table
}

// pollInterval is how often a wait asks for the lock again.
const pollInterval = 25 * time.Millisecond

// The server's numbers for the answers a lock statement reads.
const (
	// resourceBusy is ORA-00054: another session holds the row, and NOWAIT
	// does not wait for it.
	resourceBusy = 54
	// tableMissing is ORA-00942: the lock table does not exist yet.
	tableMissing = 942
	// cannotCreate is the error [ensureRow] raises when the lock table is
	// missing, or is there with no privilege granted on it, and the account
	// may not create it.
	cannotCreate = 20001
)

// ensureRow creates the lock table when it is missing, inserts the row of the
// lock name when it is missing, and commits. It runs on the lock's session
// before the row is locked, so the commit gives back nothing. A second
// session inserting the same name concurrently waits for the first one's
// commit and then finds the name taken, which is what DUP_VAL_ON_INDEX
// ignores; ORA-00955 is a table another session created first. The table, as
// [tableIn] spells it, is the second bind, so no name is written into a string
// literal.
//
// The CREATE runs only after the INSERT answered ORA-00942, which Oracle gives
// for a missing table and for an existing one the account holds no privilege
// on. A table that exists and grants INSERT takes the row without any CREATE
// privilege. Where the CREATE is refused with ORA-01031, the block raises
// [cannotCreate] and keeps the server's errors beneath it, so a missing table
// is reported, never skipped.
const ensureRow = `DECLARE
  lock_name VARCHAR2(1000) := :1;
  lock_table VARCHAR2(300) := :2;
  missing_table EXCEPTION;
  PRAGMA EXCEPTION_INIT(missing_table, -942);
  name_taken EXCEPTION;
  PRAGMA EXCEPTION_INIT(name_taken, -955);
  no_privilege EXCEPTION;
  PRAGMA EXCEPTION_INIT(no_privilege, -1031);
BEGIN
  BEGIN
    EXECUTE IMMEDIATE 'INSERT INTO ' || lock_table || ' (name) VALUES (:name)' USING lock_name;
  EXCEPTION
    WHEN DUP_VAL_ON_INDEX THEN NULL;
    WHEN missing_table THEN
      BEGIN
        EXECUTE IMMEDIATE 'CREATE TABLE ' || lock_table || ' (name VARCHAR2(1000) PRIMARY KEY)';
      EXCEPTION
        WHEN name_taken THEN NULL;
        WHEN no_privilege THEN
          RAISE_APPLICATION_ERROR(-20001, 'lock table ' || lock_table || ' cannot be created', TRUE);
      END;
      BEGIN
        EXECUTE IMMEDIATE 'INSERT INTO ' || lock_table || ' (name) VALUES (:name)' USING lock_name;
      EXCEPTION
        WHEN DUP_VAL_ON_INDEX THEN NULL;
      END;
  END;
  COMMIT;
END;`

// lockRow locks the row of the lock name in lockTable, or fails at once with
// ORA-00054 when another session holds it.
func lockRow(lockTable string) string {
	return "SELECT name FROM " + lockTable + " WHERE name = :1 FOR UPDATE NOWAIT"
}

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
//
// The row's creation runs under the timeout's context too, but nothing on the
// server bounds it. Oracle has no wait bound for an INSERT, which meets
// another session's uncommitted INSERT of the same name by waiting for that
// session's transaction to end, and an ended context does not end the call:
// measured on 23.26 through go-ora, a request under NoWait whose caller
// context ends after 30 seconds is still waiting after three minutes, with
// DISABLE_OOB=ON and without it. Ptah's own sessions commit the row in the
// call that inserts it, so only a session outside Ptah holding an uncommitted
// INSERT into the lock table makes a first acquisition wait.
func (rowLock) Acquire(ctx context.Context, session dblockext.Session, request dblockext.Request) error {
	lockCtx := ctx
	var cancel context.CancelFunc
	if request.Timeout > 0 {
		lockCtx, cancel = context.WithTimeout(ctx, request.Timeout)
		defer cancel()
	}
	lockTable := tableIn(request.Schema)
	if !request.Preview {
		if _, err := session.ExecContext(lockCtx, ensureRow, request.Name, lockTable); err != nil {
			if _, number := servercode.Read(err); number == cannotCreate {
				return cannotCreateError(request.Schema, lockTable, err)
			}
			return &dblockext.AmbiguousError{Err: waitError(ctx, lockCtx, request,
				fmt.Errorf("create the row of lock %q in %s: %w", request.Name, lockTable, err))}
		}
	}
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	for {
		var locked string
		err := session.QueryRowContext(lockCtx, lockRow(lockTable), request.Name).Scan(&locked)
		_, number := servercode.Read(err)
		switch {
		case err == nil:
			return nil
		case request.Preview && (number == tableMissing || errors.Is(err, sql.ErrNoRows)):
			return nil
		case errors.Is(err, sql.ErrNoRows):
			return fmt.Errorf("the row of lock %q is missing from %s", request.Name, lockTable)
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

// cannotCreateError says what lets an account that may not create the lock
// table lock anyway. Nothing was written: the block stopped before its COMMIT,
// so the error is not ambiguous.
//
// In a schema the account does not own, the remedy is a table a DBA creates
// and grants SELECT and INSERT on: SELECT for FOR UPDATE, which READ does not
// allow, and INSERT for the row of each new lock name. Falling back to the
// account's own schema is not one, since that table excludes no other
// account recording in the same revision table.
func cannotCreateError(schema, lockTable string, err error) error {
	if schema == "" {
		return fmt.Errorf("lock table %s does not exist and the account may not create it: grant it CREATE TABLE: %w",
			lockTable, err)
	}
	return fmt.Errorf("lock table %s does not exist, or this account holds no privilege on it, "+
		"and the account may not create it: create it with CREATE TABLE %s (name VARCHAR2(1000) PRIMARY KEY) "+
		"and GRANT SELECT, INSERT ON %s TO the account, or grant the account CREATE ANY TABLE: %w",
		lockTable, lockTable, lockTable, err)
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
