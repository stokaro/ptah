package sqlutil_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/sqlutil"
)

// TestSplitSQLStatementsForDialect_KeepsTheTextAfterAZeroByte pins that a
// zero byte ends no file: the statement holding it is kept as written, for
// the server to refuse, and the statements after it are split as any others.
func TestSplitSQLStatementsForDialect_KeepsTheTextAfterAZeroByte(t *testing.T) {
	c := qt.New(t)
	got := sqlutil.SplitSQLStatementsForDialect("CREATE TABLE a (id int);\nSELECT 1 \x00;\nCREATE TABLE b (id int);\n", "postgres")
	c.Assert(got, qt.DeepEquals, []string{"CREATE TABLE a (id int)", "SELECT 1 \x00", "CREATE TABLE b (id int)"})
}
