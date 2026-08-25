package rlspolicy_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/rlspolicy"
)

// TestCommand_FoldsAnUnspecifiedClauseOntoALL pins the fold.
//
// The spellings that must agree are listed against each other rather than
// against a hand-written expectation, because the property is that two sides of
// a comparison land on one value -- not what that value is called.
func TestCommand_FoldsAnUnspecifiedClauseOntoALL(t *testing.T) {
	tests := []struct {
		name string
		a    string
		b    string
		same bool
	}{
		{name: "unspecified equals ALL", a: "", b: "ALL", same: true},
		{name: "ALL equals unspecified", a: "ALL", b: "", same: true},
		{name: "whitespace is unspecified", a: "   ", b: "ALL", same: true},
		{name: "case does not separate them", a: "all", b: "ALL", same: true},
		{name: "surrounding space does not either", a: " ALL ", b: "ALL", same: true},
		{name: "SELECT is not ALL", a: "SELECT", b: "ALL", same: false},
		{name: "SELECT is not unspecified", a: "SELECT", b: "", same: false},
		{name: "INSERT is not UPDATE", a: "INSERT", b: "UPDATE", same: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			folded := rlspolicy.Command(test.a) == rlspolicy.Command(test.b)

			c.Assert(folded, qt.Equals, test.same,
				qt.Commentf("%q vs %q folded to %q and %q",
					test.a, test.b, rlspolicy.Command(test.a), rlspolicy.Command(test.b)))
		})
	}
}

// The fold answers with a spelling a renderer would accept, so a caller that
// does write it somewhere writes a real clause rather than a marker.
func TestCommand_AnswersWithARealClause(t *testing.T) {
	c := qt.New(t)

	c.Assert(rlspolicy.Command(""), qt.Equals, "ALL")
	c.Assert(rlspolicy.Command("select"), qt.Equals, "SELECT")
}
