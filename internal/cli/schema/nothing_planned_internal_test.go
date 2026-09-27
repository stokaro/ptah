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
)

// TestPrintNothingPlanned says "synced" only when the comparison withheld
// nothing (stokaro/ptah#3844).
func TestPrintNothingPlanned(t *testing.T) {
	withheld := coverage.Refused(coverage.Role)
	withheld.Name = "reporter"
	tests := []struct {
		name      string
		undecided []coverage.Object
		want      string
	}{
		{
			name: "nothing withheld",
			want: "Schema is synced, no changes to be made.\n",
		},
		{
			name:      "a declared role withheld",
			undecided: []coverage.Object{withheld},
			want:      "No changes planned, but 1 declared object could not be decided.\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var out bytes.Buffer

			printNothingPlanned(&out, test.undecided)

			c.Assert(out.String(), qt.Equals, test.want)
		})
	}
}
