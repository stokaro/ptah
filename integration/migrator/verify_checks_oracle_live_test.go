//go:build integration

package migrator_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
	"ptah.run/migration/migrator"
)

// A verification run opens the strictest session the engine offers, and on
// Oracle the driver cannot open it: go-ora answers "readonly transaction is not
// supported" to a BeginTx that asks for one, so the request is carried to the
// server as SET TRANSACTION READ ONLY instead.
//
// SELECT ... FOR UPDATE is what measures that it arrives. The static rules
// accept it -- it is one SELECT and writes no row -- and in an ordinary
// transaction Oracle runs it and holds a row lock every other writer waits on.
// Under the read-only transaction the server answers ORA-01456, so the
// assertion is reported errored and nothing was locked.
func TestVerifyChecksOpensAReadOnlyOracleSessionLive(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	conn, err := dbschema.ConnectToDatabase(ctx, dbtarget.URL(c, dbtarget.Oracle))
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	table := fmt.Sprintf("ptah_verify_lock_%d", time.Now().UnixNano()%100000)
	dropOracleVerifyObjects(conn, table)
	_, err = conn.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (id NUMBER(10))", table))
	c.Assert(err, qt.IsNil)
	defer dropOracleVerifyObjects(conn, table)
	_, err = conn.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s (id) VALUES (1)", table))
	c.Assert(err, qt.IsNil)

	report, err := migrator.VerifyChecks(ctx, conn, []migrator.Check{
		{Name: "locks a row behind a SELECT", Assert: fmt.Sprintf("SELECT id FROM %s FOR UPDATE", table)},
	})

	c.Assert(err, qt.IsNil)
	c.Assert(report.Results[0].Status, qt.Equals, migrator.VerifyStatusErrored)
	c.Assert(report.Results[0].Err, qt.ErrorMatches, `(?s)ORA-01456.*READ ONLY transaction.*`)
	c.Assert(report.Verdict(), qt.Equals, migrator.VerifyVerdictErrored)
}

// Oracle refuses a writing routine inside a query on its own: a function that
// inserts answers ORA-14551 whether or not the transaction is read-only. So the
// call a reader worries about -- a SELECT whose write hides behind a function
// -- is closed on Oracle by the engine rather than by the session.
//
// What that leaves is a routine declared PRAGMA AUTONOMOUS_TRANSACTION, which
// runs in a transaction of its own and commits there. No session setting
// reaches it, on any engine: a transaction that is by definition not this one
// is not rolled back with this one. This pins that limit, which
// docs/site/src/content/docs/operate/verify-a-release.md states, so a reader who
// meets a committed row after a verification run finds it measured rather than
// denied.
func TestVerifyChecksAndTheOracleRoutineLimitLive(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	conn, err := dbschema.ConnectToDatabase(ctx, dbtarget.URL(c, dbtarget.Oracle))
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	suffix := time.Now().UnixNano() % 100000
	table := fmt.Sprintf("ptah_verify_rows_%d", suffix)
	plain := fmt.Sprintf("ptah_verify_write_%d", suffix)
	autonomous := fmt.Sprintf("ptah_verify_auto_%d", suffix)
	dropOracleVerifyObjects(conn, table, plain, autonomous)
	defer dropOracleVerifyObjects(conn, table, plain, autonomous)

	_, err = conn.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (id NUMBER(10))", table))
	c.Assert(err, qt.IsNil)
	_, err = conn.ExecContext(ctx, fmt.Sprintf(`CREATE FUNCTION %s RETURN NUMBER IS
BEGIN
  INSERT INTO %s (id) VALUES (1);
  RETURN 1;
END;`, plain, table))
	c.Assert(err, qt.IsNil)
	_, err = conn.ExecContext(ctx, fmt.Sprintf(`CREATE FUNCTION %s RETURN NUMBER IS
  PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN
  INSERT INTO %s (id) VALUES (2);
  COMMIT;
  RETURN 1;
END;`, autonomous, table))
	c.Assert(err, qt.IsNil)

	report, err := migrator.VerifyChecks(ctx, conn, []migrator.Check{
		{Name: "an ordinary writing routine", Assert: fmt.Sprintf("SELECT %s() = 1 FROM dual", plain)},
		{Name: "a routine with a transaction of its own", Assert: fmt.Sprintf("SELECT %s() = 1 FROM dual", autonomous)},
	})

	c.Assert(err, qt.IsNil)
	c.Assert(report.Results[0].Status, qt.Equals, migrator.VerifyStatusErrored)
	c.Assert(report.Results[0].Err, qt.ErrorMatches, `(?s)ORA-14551.*`)
	c.Assert(oracleVerifyRowCount(c, conn, table, 1), qt.Equals, 0)
	// The measured limit, not an outcome anyone wants: the autonomous routine
	// committed its row, and the verification run reported the assertion as an
	// ordinary evaluation because that is all it can see.
	c.Assert(report.Results[1].Status, qt.Equals, migrator.VerifyStatusVerified)
	c.Assert(oracleVerifyRowCount(c, conn, table, 2), qt.Equals, 1)
}

func oracleVerifyRowCount(c *qt.C, conn *dbschema.DatabaseConnection, table string, id int) int {
	c.Helper()
	var count int
	err := conn.QueryRowContext(context.Background(),
		fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE id = :1", table), id).Scan(&count)
	c.Assert(err, qt.IsNil)
	return count
}

// dropOracleVerifyObjects removes what a test created from the shared server.
// An object that is not there answers ORA-00942 or ORA-04043, which is the
// state this wants, so the error is not read.
func dropOracleVerifyObjects(conn *dbschema.DatabaseConnection, table string, functions ...string) {
	for _, function := range functions {
		_, _ = conn.ExecContext(context.Background(), "DROP FUNCTION "+function)
	}
	_, _ = conn.ExecContext(context.Background(), "DROP TABLE "+table+" PURGE")
}
