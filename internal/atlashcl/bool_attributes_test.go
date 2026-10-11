package atlashcl_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlashcl"
)

// TestParse_RefusesABoolAttributeThatIsNoBool refuses a value that is not a
// bool in every bool attribute the reader reads as optional, positioned at
// the attribute. The pinned community binary v1.3.0 refuses each of these
// with `bool value is required`, a string spelling true included; this
// reader read them as absent, so `null = "true"` declared a NOT NULL column.
func TestParse_RefusesABoolAttributeThatIsNoBool(t *testing.T) {
	tests := []struct {
		name     string
		document string
		wantErr  string
	}{
		{name: "a column's null", document: "table \"t\" {\n  column \"a\" {\n    type = int\n    null = \"true\"\n  }\n}\n",
			wantErr: `parse HCL schema at schema\.hcl:4,5-9: column attribute "null" must be a bool`},
		{name: "a column's auto_increment", document: "table \"t\" {\n  column \"a\" {\n    type           = int\n    auto_increment = \"true\"\n  }\n}\n",
			wantErr: `parse HCL schema at schema\.hcl:4,5-19: column attribute "auto_increment" must be a bool`},
		{name: "a column's unique", document: "table \"t\" {\n  column \"a\" {\n    type   = int\n    unique = 1\n  }\n}\n",
			wantErr: `parse HCL schema at schema\.hcl:4,5-11: column attribute "unique" must be a bool`},
		{name: "a column's unsigned", document: "table \"t\" {\n  column \"a\" {\n    type     = int\n    unsigned = \"yes\"\n  }\n}\n",
			wantErr: `parse HCL schema at schema\.hcl:4,5-13: column attribute "unsigned" must be a bool`},
		{name: "an index's unique", document: "table \"t\" {\n  column \"a\" {\n    type = int\n  }\n  index \"i\" {\n    columns = [column.a]\n    unique  = \"false\"\n  }\n}\n",
			wantErr: `parse HCL schema at schema\.hcl:7,5-11: index attribute "unique" must be a bool`},
		{name: "a primary key part's desc", document: "table \"t\" {\n  column \"a\" {\n    type = int\n  }\n  primary_key {\n    on {\n      column = column.a\n      desc   = \"yes\"\n    }\n  }\n}\n",
			wantErr: `parse HCL schema at schema\.hcl:8,7-11: primary_key on attribute "desc" must be a bool`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			db, err := atlashcl.Parse([]byte(test.document), "schema.hcl")

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(db, qt.IsNil)
		})
	}
}

// TestParse_ReadsABoolAttribute reads a bool, a variable holding one, and
// null, which the pinned community binary reads as an attribute left out.
func TestParse_ReadsABoolAttribute(t *testing.T) {
	tests := []struct {
		name         string
		null         string
		wantNullable bool
	}{
		{name: "a bool", null: "true", wantNullable: true},
		{name: "a variable", null: "var.nullable", wantNullable: true},
		{name: "null", null: "null", wantNullable: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			document := "variable \"nullable\" {\n  type    = bool\n  default = true\n}\n\ntable \"t\" {\n  column \"a\" {\n    type = int\n    null = " +
				test.null + "\n  }\n}\n"

			db, err := atlashcl.Parse([]byte(document), "schema.hcl")

			c.Assert(err, qt.IsNil)
			c.Assert(db.Fields, qt.HasLen, 1)
			c.Assert(db.Fields[0].Nullable, qt.Equals, test.wantNullable)
		})
	}
}
