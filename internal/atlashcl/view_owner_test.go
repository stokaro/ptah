package atlashcl_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlashcl"
)

// TestParseView_OwnerIsRefused pins that an Atlas HCL document cannot declare
// the owner of a view or a materialized view: Atlas HCL has no owner
// attribute, and one written there is refused rather than read as a
// declaration the other formats would not see.
func TestParseView_OwnerIsRefused(t *testing.T) {
	tests := []struct {
		name, document, wantErr string
	}{
		{
			name:     "a view",
			document: "schema \"public\" {}\n\nview \"report\" {\n  schema = schema.public\n  as     = \"SELECT 1 AS v\"\n  owner  = \"reporting\"\n}\n",
			wantErr:  `(?s).*unsupported view attribute "owner".*`,
		},
		{
			name:     "a materialized view",
			document: "schema \"public\" {}\n\nmaterialized \"totals\" {\n  schema = schema.public\n  as     = \"SELECT 1 AS v\"\n  owner  = \"reporting\"\n}\n",
			wantErr:  `(?s).*unsupported materialized attribute "owner".*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			db, err := atlashcl.Parse(t.Context(), []byte(test.document), "schema.hcl")

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(db, qt.IsNil)
		})
	}
}
