package atlashcl_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlashcl"
)

func TestParseViewSQLSecurity(t *testing.T) {
	tests := []struct {
		name      string
		attribute string
		want      string
	}{
		{name: "server default"},
		{name: "invoker", attribute: "security = INVOKER", want: "INVOKER"},
		{name: "definer", attribute: `security = "DEFINER"`, want: "DEFINER"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			parsed, err := atlashcl.Parse(c.Context(), checkOptionDocument(test.attribute), "schema.hcl")
			c.Assert(err, qt.IsNil)
			c.Assert(parsed.Views, qt.HasLen, 1)
			c.Assert(parsed.Views[0].Security, qt.Equals, test.want)
		})
	}
}

func TestParseViewSQLSecurityRejectsInvalidMode(t *testing.T) {
	for _, attribute := range []string{"security = OTHER", `security = ""`} {
		t.Run(attribute, func(t *testing.T) {
			c := qt.New(t)
			parsed, err := atlashcl.Parse(c.Context(), checkOptionDocument(attribute), "schema.hcl")
			c.Assert(err, qt.ErrorMatches, `(?s).*view "own_rows" security must be DEFINER or INVOKER.*`)
			c.Assert(parsed, qt.IsNil)
		})
	}
}
