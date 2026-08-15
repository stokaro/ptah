package atlashcl_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/atlashcl"
)

func TestParsePermissionSequenceTarget(t *testing.T) {
	c := qt.New(t)

	db, err := atlashcl.Parse([]byte(`
permission {
  to         = role.app_user
  for        = sequence.order_seq
  privileges = [USAGE, SELECT]
}
`), "schema.hcl")
	c.Assert(err, qt.IsNil)
	c.Assert(db.Grants, qt.HasLen, 1)
	c.Assert(db.Grants[0].OnSequence, qt.Equals, "order_seq")
	c.Assert(db.Grants[0].OnTable, qt.Equals, "")
	c.Assert(db.Grants[0].OnSchema, qt.Equals, "")

	sql := legacyRenderedSQL(strings.Join(renderStatements(c, db, "postgres"), "\n"))
	c.Assert(sql, qt.Contains, `GRANT USAGE, SELECT ON SEQUENCE order_seq TO app_user;`)
}

func TestParsePermissionSchemaQualifiedSequenceTarget(t *testing.T) {
	c := qt.New(t)

	db, err := atlashcl.Parse([]byte(`
permission {
  to         = role.app_user
  for        = sequence.app.order_seq
  privileges = [USAGE]
}
`), "schema.hcl")
	c.Assert(err, qt.IsNil)
	c.Assert(db.Grants, qt.HasLen, 1)
	c.Assert(db.Grants[0].OnSequence, qt.Equals, "app.order_seq")
}

// TestParsePermissionRelationTargets pins that a permission naming a view or a
// materialized view is read back to the same grant as one naming a table
// (stokaro/ptah#1234).
//
// A reference in HCL names a BLOCK, so the block type is part of the spelling
// and a grant on a view has to be written `for = view.<name>` -- the pinned
// Atlas community binary v1.3.0 refuses `for = table.<view>` with
// `This object does not have an attribute named "<view>"`, measured on the
// document `ptah-compat schema inspect` writes for an ordinary PostgreSQL
// database carrying a view. This side is what keeps that from costing Ptah the
// ability to read its own output.
//
// Grant.OnTable stays the home for all three: which relation kind it is, is in
// the document, in the block that declares it.
func TestParsePermissionRelationTargets(t *testing.T) {
	tests := []struct {
		name   string
		target string
		want   string
	}{
		{name: "a table", target: "table.reporting", want: "reporting"},
		{name: "a view", target: "view.reporting", want: "reporting"},
		{name: "a materialized view", target: "materialized.reporting", want: "reporting"},
		{name: "a schema-qualified view", target: "view.app.reporting", want: "app.reporting"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)

			db, err := atlashcl.Parse([]byte(`
permission {
  to         = role.app_user
  for        = `+test.target+`
  privileges = [SELECT]
}
`), "schema.hcl")

			c.Assert(err, qt.IsNil)
			c.Assert(db.Grants, qt.HasLen, 1)
			c.Assert(db.Grants[0].OnTable, qt.Equals, test.want)
			c.Assert(db.Grants[0].OnSchema, qt.Equals, "")
			c.Assert(db.Grants[0].OnSequence, qt.Equals, "")
		})
	}
}

// TestParsePermissionRejectsUnknownTargetKind is the control for the row above:
// widening the accepted block types is only safe if a block type Ptah does not
// model is still refused, rather than every traversal being taken as a name.
func TestParsePermissionRejectsUnknownTargetKind(t *testing.T) {
	c := qt.New(t)

	_, err := atlashcl.Parse([]byte(`
permission {
  to         = role.app_user
  for        = wibble.reporting
  privileges = [SELECT]
}
`), "schema.hcl")
	c.Assert(err, qt.ErrorMatches, `.*permission requires table, view, schema, or sequence target.*`)
}
