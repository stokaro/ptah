//go:build integration

package migrator_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/migration/migrator"
)

// A verification run promises to change nothing, and two rules hold it to
// that. The first is Ptah's: an assertion must be a single SELECT, proved from
// its text. That one a unit test can measure.
//
// The second belongs to the server. A SELECT can still write by calling a
// function that writes, and no reading of the statement catches it -- the text
// is a SELECT and the write is behind the call. What stops it is the read-only
// transaction the session opens, and only an engine that has one can say
// whether it does. PostgreSQL refuses the write with 25006, so the assertion
// is reported as errored and the table it targeted is untouched.
func TestVerifyChecksRefusesAWritingFunctionLive(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	conn, err := dbschema.ConnectToDatabase(ctx, postgresTestURL(t))
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	suffix := time.Now().UnixNano()
	table := fmt.Sprintf("ptah_verify_rows_%d", suffix)
	function := fmt.Sprintf("ptah_verify_write_%d", suffix)

	_, err = conn.ExecContext(ctx, fmt.Sprintf(`CREATE TABLE %s (id integer PRIMARY KEY)`, table))
	c.Assert(err, qt.IsNil)
	defer func() {
		_, _ = conn.ExecContext(ctx, fmt.Sprintf(`DROP FUNCTION IF EXISTS %s()`, function))
		_, _ = conn.ExecContext(ctx, fmt.Sprintf(`DROP TABLE IF EXISTS %s`, table))
	}()

	_, err = conn.ExecContext(ctx, fmt.Sprintf(`
		CREATE FUNCTION %s() RETURNS integer LANGUAGE plpgsql AS $$
		BEGIN
			INSERT INTO %s (id) VALUES (1);
			RETURN 1;
		END;
		$$`, function, table))
	c.Assert(err, qt.IsNil)

	report, err := migrator.VerifyChecks(ctx, conn, []migrator.Check{
		{Name: "writes behind a call", Assert: fmt.Sprintf("SELECT %s() = 1", function)},
		{Name: "the table is still empty", Assert: fmt.Sprintf("SELECT COUNT(*) = 0 FROM %s", table)},
	})

	c.Assert(err, qt.IsNil)
	c.Assert(report.Results[0].Status, qt.Equals, migrator.VerifyStatusErrored)
	c.Assert(report.Results[0].Err, qt.ErrorMatches, `(?s).*read-only transaction.*`)
	c.Assert(report.Results[1].Status, qt.Equals, migrator.VerifyStatusVerified)
	c.Assert(report.Verdict(), qt.Equals, migrator.VerifyVerdictErrored)

	// Read the table back outside the verification session: the refusal has to
	// mean no row, not a row this test could not see from inside it.
	var rows int
	c.Assert(conn.QueryRowContext(ctx, fmt.Sprintf(`SELECT COUNT(*) FROM %s`, table)).Scan(&rows), qt.IsNil)
	c.Assert(rows, qt.Equals, 0)
}

// The outcomes a release report separates are decided by the server, not by
// Ptah: whether a predicate over real rows holds is the database's answer. Both
// are asked in one run so the report is measured as the list it is, rather than
// as two runs of one assertion each.
func TestVerifyChecksSeparatesOutcomesOnLiveRowsLive(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	conn, err := dbschema.ConnectToDatabase(ctx, postgresTestURL(t))
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	table := fmt.Sprintf("ptah_verify_users_%d", time.Now().UnixNano())
	_, err = conn.ExecContext(ctx, fmt.Sprintf(`CREATE TABLE %s (id integer PRIMARY KEY, tier text)`, table))
	c.Assert(err, qt.IsNil)
	defer func() {
		_, _ = conn.ExecContext(ctx, fmt.Sprintf(`DROP TABLE IF EXISTS %s`, table))
	}()
	_, err = conn.ExecContext(ctx,
		fmt.Sprintf(`INSERT INTO %s (id, tier) VALUES (1, 'pro'), (2, NULL)`, table))
	c.Assert(err, qt.IsNil)

	report, err := migrator.VerifyChecks(ctx, conn, []migrator.Check{
		{Name: "rows arrived", Assert: fmt.Sprintf("SELECT COUNT(*) = 2 FROM %s", table)},
		{Name: "backfill covered every row", Assert: fmt.Sprintf("SELECT COUNT(*) = 0 FROM %s WHERE tier IS NULL", table)},
	})

	c.Assert(err, qt.IsNil)
	c.Assert(report.Results[0].Status, qt.Equals, migrator.VerifyStatusVerified)
	c.Assert(report.Results[1].Status, qt.Equals, migrator.VerifyStatusFailed)
	c.Assert(report.Results[1].Err, qt.IsNil)
	c.Assert(report.Verdict(), qt.Equals, migrator.VerifyVerdictFailed)
}

// A run whose context ends under a statement did not establish anything, and
// saying "errored" about the requirement would tell a caller the database was
// asked and answered. The deadline is what makes this measurable: the
// assertion sleeps past it, so the statement is in flight when the context
// ends (stokaro/ptah#3404).
func TestVerifyChecksReportsACancelledRunAsCancelledLive(t *testing.T) {
	c := qt.New(t)
	conn, err := dbschema.ConnectToDatabase(context.Background(), postgresTestURL(t))
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	report, err := migrator.VerifyChecks(ctx, conn, []migrator.Check{
		{Name: "outlives the deadline", Assert: "SELECT pg_sleep(30) IS NULL"},
	})

	c.Assert(err, qt.ErrorIs, context.DeadlineExceeded)
	c.Assert(report.Results, qt.HasLen, 0)
}
