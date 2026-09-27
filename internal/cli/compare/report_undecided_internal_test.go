package compare

// White-box testing required: the report is written by the unexported
// writeComparison. The exported command reaches an undecided object only when a
// live server refuses a catalog read, which the e2e suite measures once; the
// layout of the report beside a difference, and its order, are asserted here
// where the input can be stated directly.

import (
	"bytes"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/coverage"
	"ptah.run/migration/schemadiff/difftypes"
)

// An empty diff beside a withheld object is not "No schema differences
// detected.": the comparison planned nothing for the object because it could
// not look, and saying the schemas agree would claim a check that never ran
// (stokaro/ptah#3834).
func TestWriteComparisonReportsUndecidedWithoutDifferences(t *testing.T) {
	c := qt.New(t)
	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}

	writeComparison(stdout, stderr, &difftypes.SchemaDiff{}, []coverage.Object{undecidedRole("reporter")}, "", "mysql")

	c.Assert(stdout.String(), qt.Equals, `No differences planned, but 1 declared object could not be decided:
  role "reporter"
`)
	c.Assert(stderr.String(), qt.Equals, `Warning: role "reporter" is declared by the desired schema`+
		` but no change was planned for it: the database does not describe role objects because`+
		` the read was refused the catalog that would have listed them, so this comparison`+
		` cannot tell it apart from one that already exists, and the creation Ptah renders`+
		` for it cannot safely converge from an unknown current state.
`)
}

// The withheld objects follow the differences, and they are listed in one
// order whatever order the comparator found them in: two objects given in
// reverse name order come out sorted.
func TestWriteComparisonReportsUndecidedBesideDifferences(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{TablesAdded: difftypes.TableChanges{{Name: "users"}}}
	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}

	writeComparison(stdout, stderr, diff,
		[]coverage.Object{undecidedRole("reporter"), undecidedRole("auditor")},
		"CREATE TABLE users (id INT);\n", "mysql")

	c.Assert(stdout.String(), qt.Equals, `Differences detected (1 category):
  tables_added (1): users

Reconciling SQL:
CREATE TABLE users (id INT);

Undecided (2):
  role "auditor"
  role "reporter"
`)
	c.Assert(stderr.String(), qt.Contains, `Warning: role "reporter" is declared by the desired schema`)
	c.Assert(stderr.String(), qt.Contains, `Warning: role "auditor" is declared by the desired schema`)
}
