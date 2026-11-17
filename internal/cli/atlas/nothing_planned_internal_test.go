package atlas

// White-box testing required: nothingPlannedMessage is the sentence `schema
// apply` and `schema plan` print when no statement was planned. A plan that
// holds notes and no statement needs a target that cannot apply a difference,
// such as CockroachDB with a generated column expression change, which the
// e2e suite measures; no offline engine plans one, so the choice between the
// two sentences is pinned here.

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

// TestNothingPlannedMessage says "synced" only when the plan holds no note
// about a difference it plans no statement for (stokaro/ptah#6160). The synced
// sentence keeps the bytes the pinned community binary writes.
func TestNothingPlannedMessage(t *testing.T) {
	tests := []struct {
		name  string
		notes int
		want  string
	}{
		{
			name: "no note",
			want: "Schema is synced, no changes to be made",
		},
		{
			name:  "one note",
			notes: 1,
			want:  "No statements planned, but the plan holds the notes above, so the schema is not synced",
		},
		{
			name:  "several notes",
			notes: 3,
			want:  "No statements planned, but the plan holds the notes above, so the schema is not synced",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(nothingPlannedMessage(test.notes), qt.Equals, test.want)
		})
	}
}
