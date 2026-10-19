package sqlreach_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/sqlreach"
)

// SQLite quotes a name with brackets, so the text between them is a name and
// starts no statement: SQLite 3.51 creates the table
// [x; ATTACH DATABASE 'f.db' AS y] and attaches nothing.
func TestScan_SQLiteReadsABracketedNameAsOne(t *testing.T) {
	c := qt.New(t)

	finding, found, err := sqlreach.Scan("CREATE TABLE [x; ATTACH DATABASE 'f.db' AS y] (a INTEGER)", "sqlite")

	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsFalse)
	c.Assert(finding, qt.DeepEquals, sqlreach.Finding{})
}

// A bracketed name the statement closes hides nothing after it.
func TestScan_SQLiteRefusesAConstructAfterABracketedName(t *testing.T) {
	c := qt.New(t)

	finding, found, err := sqlreach.Scan("CREATE TABLE [x] (a INTEGER); ATTACH DATABASE 'f.db' AS y", "sqlite")

	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	c.Assert(finding.Construct, qt.Equals, "ATTACH")
}
