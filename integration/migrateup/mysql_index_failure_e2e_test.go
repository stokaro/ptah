//go:build integration

package migrateup_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
	"ptah.run/migration/migrator"
)

// A duplicate value makes the server reject an atomic index build. The
// revision records a known failure, so status cannot offer an unknown-outcome
// repair for an index the server never created.
func TestMySQLUniqueIndexRefusalRecordsKnownFailureLive(t *testing.T) {
	c := qt.New(t)
	target := newRollbackTarget(c, dbtarget.MySQL)
	_, err := target.conn.ExecContext(c.Context(), "CREATE TABLE duplicate_values (id INT PRIMARY KEY, item_value INT)")
	c.Assert(err, qt.IsNil)
	_, err = target.conn.ExecContext(c.Context(), "INSERT INTO duplicate_values VALUES (1, 7), (2, 7)")
	c.Assert(err, qt.IsNil)
	dir := migrationsDir(c, "DROP INDEX by_value ON duplicate_values;\n",
		"CREATE UNIQUE INDEX by_value ON duplicate_values (item_value);\n")

	result, report := target.runUp(c, dir)

	c.Assert(result.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", result.Stdout, result.Stderr))
	c.Assert(report.Outcome, qt.Equals, migrator.RunOutcomeFailed)
	c.Assert(report.Failure, qt.IsNotNil)
	c.Assert(report.Failure.Statement, qt.Equals, 1)
	c.Assert(report.Failure.Statements, qt.Equals, 1)
	c.Assert(report.Failure.ErrorNumber, qt.Equals, 1062)
	revision := target.dirtyRow(c, dir)
	c.Assert(revision, qt.IsNotNil)
	c.Assert(revision.State, qt.Equals, "failed")
	c.Assert(revision.Applied, qt.Equals, 0)
	c.Assert(revision.Total, qt.Equals, 1)
	c.Assert(revision.StatementOutcomeUnknown(), qt.IsFalse)
	var indexCount int
	c.Assert(target.conn.QueryRowContext(c.Context(), `SELECT COUNT(*) FROM information_schema.statistics
		WHERE table_schema = DATABASE() AND table_name = 'duplicate_values' AND index_name = 'by_value'`).Scan(&indexCount), qt.IsNil)
	c.Assert(indexCount, qt.Equals, 0)
}
