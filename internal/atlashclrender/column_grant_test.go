package atlashclrender_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/atlashcl"
	"ptah.run/internal/atlashclrender"
)

// TestRenderedColumnGrantRoundTrips carries a column list through `schema
// inspect > out.hcl` and back, on a `permission` and on a `revoke` block.
// Dropped on the way, the grant would come back covering the whole table.
func TestRenderedColumnGrantRoundTrips(t *testing.T) {
	c := qt.New(t)
	db := inspectedTable("public")
	db.Grants = []schemamodel.Grant{{Role: "app", Privileges: []string{"UPDATE"}, OnTable: "public.t", Columns: []string{"state", "note"}}}
	db.RevokedGrants = []schemamodel.Grant{{Role: "app", Privileges: []string{"SELECT"}, OnTable: "public.t", Columns: []string{"secret"}}}

	result, err := atlashclrender.RenderInspected(db, platform.Postgres, "public")
	c.Assert(err, qt.IsNil)
	c.Assert(result.Diagnostics, qt.HasLen, 0)
	parsed, err := atlashcl.Parse(result.Data, "rendered.hcl")

	c.Assert(err, qt.IsNil)
	c.Assert(parsed.Grants, qt.HasLen, 1)
	c.Assert(parsed.Grants[0].Columns, qt.DeepEquals, []string{"state", "note"})
	c.Assert(parsed.RevokedGrants, qt.HasLen, 1)
	c.Assert(parsed.RevokedGrants[0].Columns, qt.DeepEquals, []string{"secret"})
}

// TestParseColumnGrant_FailurePath pins the refusal of a column list on a
// target that has no columns.
func TestParseColumnGrant_FailurePath(t *testing.T) {
	c := qt.New(t)
	document := "schema \"app\" {}\n\npermission {\n  to = \"r\"\n  for = schema.app\n  privileges = [\"USAGE\"]\n  columns = [\"a\"]\n}\n"

	parsed, err := atlashcl.Parse([]byte(document), "schema.hcl")

	c.Assert(err, qt.ErrorMatches, `(?s).*permission columns need a table target: column privileges apply to a table.*`)
	c.Assert(parsed, qt.IsNil)
}
