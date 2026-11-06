package compare

// White-box testing required: writeComparison decides what the report says over
// each combination of differences, undecided objects and objects nothing compares,
// and through the command only a live server can produce the last of them, which
// the engine-level test in integration/mysqlaccount measures once.

import (
	"bytes"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/coverage"
	"ptah.run/core/difftypes"
	"ptah.run/migration/schemadiff"
)

// TestWriteComparisonNamesWhatItDidNotCompare holds the sentence a report of no
// differences opens with, and the section each other report carries.
func TestWriteComparisonNamesWhatItDidNotCompare(t *testing.T) {
	event := []catalog.UnmodeledObject{{Kind: "event", Name: "e_tick", Reason: "nothing compares it"}}
	withheld := schemadiff.Diagnostics{Common: []coverage.Object{undecidedRole("reporter")}}
	tests := []struct {
		name      string
		diff      *difftypes.SchemaDiff
		undecided schemadiff.Diagnostics
		unmodeled []catalog.UnmodeledObject
		sql       string
		want      string
	}{
		{
			name:      "nothing differs",
			diff:      &difftypes.SchemaDiff{},
			unmodeled: event,
			want: `No schema differences detected, but 1 event was not compared:
  event "e_tick": nothing compares it
`,
		},
		{
			name: "nothing differs and nothing was left out",
			diff: &difftypes.SchemaDiff{},
			want: "No schema differences detected.\n",
		},
		{
			name:      "a difference",
			diff:      &difftypes.SchemaDiff{TablesAdded: difftypes.TableChanges{{Name: "users"}}},
			unmodeled: event,
			sql:       "CREATE TABLE users (id INT);\n",
			want: `Differences detected (1 category):
  tables_added (1): users

Reconciling SQL:
CREATE TABLE users (id INT);

Not compared (1):
  event "e_tick": nothing compares it
`,
		},
		{
			name:      "an undecided object",
			diff:      &difftypes.SchemaDiff{},
			undecided: withheld,
			unmodeled: event,
			want: `No differences planned, but 1 declared object could not be decided:
  role "reporter"

Not compared (1):
  event "e_tick": nothing compares it
`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var stdout, stderr bytes.Buffer

			writeComparison(c.Context(), &stdout, &stderr, test.diff, test.undecided, test.unmodeled, test.sql, "mysql")

			c.Assert(stdout.String(), qt.Equals, test.want)
		})
	}
}
