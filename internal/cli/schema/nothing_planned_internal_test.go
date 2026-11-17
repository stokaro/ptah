package schema

// White-box testing required: printNothingPlanned is the sentence `schema
// plan` and `schema apply` print when no statement was planned. Reaching its
// undecided branch through the command needs a server that refuses a catalog
// read, which the e2e suite measures once; the choice between the two
// sentences is pinned here.

import (
	"bytes"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/coverage"
	"ptah.run/migration/schemadiff"
)

// TestPrintNothingPlanned says "synced" only when the comparison withheld
// nothing (stokaro/ptah#3844) and the plan holds no note about a difference it
// plans no statement for (stokaro/ptah#6160).
func TestPrintNothingPlanned(t *testing.T) {
	withheld := coverage.Refused(coverage.Role)
	withheld.Name = "reporter"
	tests := []struct {
		name      string
		undecided schemadiff.Diagnostics
		notes     int
		want      string
	}{
		{
			name: "nothing withheld",
			want: "Schema is synced, no changes to be made.\n",
		},
		{
			name:      "a declared role withheld",
			undecided: schemadiff.Diagnostics{Common: []coverage.Object{withheld}},
			want:      "No changes planned, but 1 declared object could not be decided.\n",
		},
		{
			name:  "a note the plan holds",
			notes: 1,
			want:  "No statements planned, but the plan holds the notes above, so the schema is not synced.\n",
		},
		{
			name:      "a note and a declared role withheld",
			undecided: schemadiff.Diagnostics{Common: []coverage.Object{withheld}},
			notes:     2,
			want: "No statements planned, but the plan holds the notes above, so the schema is not synced.\n" +
				"No changes planned, but 1 declared object could not be decided.\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var out bytes.Buffer

			printNothingPlanned(&out, test.undecided, test.notes)

			c.Assert(out.String(), qt.Equals, test.want)
		})
	}
}
