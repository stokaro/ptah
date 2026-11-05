package sqlutil_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/sqlutil"
)

func TestQuoteYQLIdentifier_HappyPath(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{name: "a plain name", input: "items", want: "`items`"},
		{name: "a path and a literal dot keep their spelling", input: "jobs.daily/tick", want: "`jobs.daily/tick`"},
		{name: "a backtick is escaped, not doubled", input: "a`b", want: "`a\\`b`"},
		{name: "a backslash is escaped", input: `a\b`, want: "`a\\\\b`"},
		{name: "a trailing backslash cannot escape the closing backtick", input: `a\`, want: "`a\\\\`"},
		{name: "an empty name", input: "", want: "``"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(sqlutil.QuoteYQLIdentifier(test.input), qt.Equals, test.want)
		})
	}
}
