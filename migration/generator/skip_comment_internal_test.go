package generator

// White-box testing required: withSkipComments is reached by the exported
// generator only through a live database and a project configuration, and the
// property under test is how it writes an object name it read from that
// database.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/planext"
	"ptah.run/migration/diffpolicy"
)

// A skipped change names the live object, and the live database decides what
// that name holds. A name with a line break must not put a second line in the
// file.
func TestWithSkipComments_KeepsAnObjectNameOnOneLine(t *testing.T) {
	c := qt.New(t)
	specs := []generatedMigrationSpec{{UpSQL: "CREATE TABLE t (id INT);"}}

	got := withSkipComments(specs, []diffpolicy.SkippedChange{
		{Kind: planext.ChangeKind("drop_table"), Object: "gone\nDROP TABLE victim_dp;\n--z"},
	})

	c.Assert(got[0].UpSQL, qt.Equals,
		`-- SKIP: DROP TABLE of gone\nDROP TABLE victim_dp;\n--z omitted by diff policy (skip: drop_table)`+"\nCREATE TABLE t (id INT);")
}
