package parser

// White-box testing required: alterIndexKind spells the ALTER TABLE ADD refusal
// for an index, and that branch is not reachable from the exported parser
// today. isAlterAddConstraintStart gates on the constraint keywords, so an
// added index takes its own statement and never arrives there. The branch is
// kept as a guard for the day that gate widens, and this is the only way to
// assert what it would say -- the alternative is a formatting fix nothing
// measures (stokaro/ptah#2713).

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

func TestAlterIndexKind(t *testing.T) {
	tests := []struct {
		name   string
		method string
		want   string
	}{
		// The empty method is the plain KEY/INDEX case. Joining it onto
		// " INDEX" spelled the refusal "ADD  INDEX", with two spaces.
		{name: "plain", method: "", want: "INDEX"},
		{name: "spatial", method: "SPATIAL", want: "SPATIAL INDEX"},
		{name: "lowercase is normalized", method: "spatial", want: "SPATIAL INDEX"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(alterIndexKind(test.method), qt.Equals, test.want)
		})
	}
}
