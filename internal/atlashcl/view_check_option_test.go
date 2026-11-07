package atlashcl_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlashcl"
)

// checkOptionDocument is a view whose block holds attribute.
func checkOptionDocument(attribute string) []byte {
	return []byte(`
schema "public" {}

view "own_rows" {
  schema = schema.public
  as     = "SELECT 1 AS v"
  ` + attribute + `
}
`)
}

// TestParseViewCheckOption_HappyPath pins that check_option names the level
// of a view's check option, as Atlas documents it: LOCAL or CASCADED, as a
// bare word or a string. Without the level a document read every check option
// as one, so a LOCAL view and a CASCADED one compared equal (stokaro/ptah#5647).
func TestParseViewCheckOption_HappyPath(t *testing.T) {
	tests := []struct {
		name          string
		attribute     string
		wantWithCheck bool
		wantLocal     bool
	}{
		{name: "no attribute", attribute: ""},
		{name: "CASCADED", attribute: "check_option = CASCADED", wantWithCheck: true},
		{name: "LOCAL", attribute: "check_option = LOCAL", wantWithCheck: true, wantLocal: true},
		{name: "LOCAL as a string", attribute: `check_option = "LOCAL"`, wantWithCheck: true, wantLocal: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			db, err := atlashcl.Parse(t.Context(), checkOptionDocument(test.attribute), "schema.hcl")

			c.Assert(err, qt.IsNil)
			c.Assert(db.Views, qt.HasLen, 1)
			c.Assert(db.Views[0].WithCheck, qt.Equals, test.wantWithCheck)
			c.Assert(db.Views[0].LocalCheck, qt.Equals, test.wantLocal)
		})
	}
}

// TestParseViewCheckOption_FailurePath pins that a check_option naming no
// level is refused by name rather than read as either one.
func TestParseViewCheckOption_FailurePath(t *testing.T) {
	tests := []struct {
		name      string
		attribute string
		wantErr   string
	}{
		{name: "a level in lower case", attribute: `check_option = "cascaded"`,
			wantErr: `(?s).*view "own_rows" check_option must be LOCAL or CASCADED, got "cascaded"`},
		{name: "NONE", attribute: "check_option = NONE",
			wantErr: `(?s).*view "own_rows" check_option must be LOCAL or CASCADED, got "NONE"`},
		{name: "a bool", attribute: "check_option = true",
			wantErr: `(?s).*view "own_rows" check_option must be LOCAL or CASCADED, got "true"`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			db, err := atlashcl.Parse(t.Context(), checkOptionDocument(test.attribute), "schema.hcl")

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(db, qt.IsNil)
		})
	}
}
