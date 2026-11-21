package referentialaction_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/referentialaction"
)

func TestSpelling(t *testing.T) {
	tests := []struct {
		name   string
		action string
		want   string
	}{
		{name: "already spelled", action: "NO ACTION", want: "NO ACTION"},
		{name: "lower case", action: "cascade", want: "CASCADE"},
		{name: "an HCL underscore", action: "SET_NULL", want: "SET NULL"},
		{name: "surrounding space", action: "  restrict ", want: "RESTRICT"},
		{name: "a run of spaces between words", action: "set   default", want: "SET DEFAULT"},
		{name: "a tab between words", action: "no\taction", want: "NO ACTION"},
		{name: "empty", action: "", want: ""},
		{name: "blank", action: "  ", want: ""},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(referentialaction.Spelling(test.action), qt.Equals, test.want)
		})
	}
}
