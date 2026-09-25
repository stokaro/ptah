package goschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/goschema"
	"ptah.run/core/ptaherr"
)

// TestParseSource_GrantColumns pins the columns attribute of the grant and
// revoke directives, and its refusal on a target that has no columns.
func TestParseSource_GrantColumns(t *testing.T) {
	c := qt.New(t)
	source := `package models

//ptah:schema:revoke role="app" privilege="UPDATE" on_table="proposals"
//ptah:schema:grant role="app" privilege="UPDATE" on_table="proposals" columns="state, decided_at"
type AccessControl struct{}
`
	refused := "package models\n\n" +
		`//ptah:schema:grant role="app" privilege="USAGE" on_schema="app" columns="a"` + "\ntype AccessControl struct{}\n"

	db := mustParseSource(c, "access.go", source)
	_, err := goschema.ParseSource("refused.go", refused)

	c.Assert(db.Grants, qt.HasLen, 1)
	c.Assert(db.Grants[0].Columns, qt.DeepEquals, []string{"state", "decided_at"})
	c.Assert(db.RevokedGrants, qt.HasLen, 1)
	c.Assert(db.RevokedGrants[0].Columns, qt.IsNil)
	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidAttributeValue)
	c.Assert(err, qt.ErrorMatches, `columns on //ptah:schema:grant at .* needs on_table: column privileges apply to a table`)
}
