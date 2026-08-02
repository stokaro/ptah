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

func TestParsePermissionRejectsUnknownTargetKind(t *testing.T) {
	c := qt.New(t)

	_, err := atlashcl.Parse([]byte(`
permission {
  to         = role.app_user
  for        = view.reporting
  privileges = [SELECT]
}
`), "schema.hcl")
	c.Assert(err, qt.ErrorMatches, `.*permission requires table, schema, or sequence target.*`)
}
