package sqlite_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/renderer"
)

func TestRenderIndex_UsesAttachedSchemaOnIndexName(t *testing.T) {
	c := qt.New(t)
	create := ast.NewIndex("idx_users_email", "tenant.users", "email").SetIfNotExists()
	drop := ast.NewDropIndex("idx_users_email").SetTable("tenant.users").SetIfExists()

	sql, err := renderer.RenderSQL("sqlite", create, drop)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Equals, "CREATE INDEX IF NOT EXISTS \"tenant\".\"idx_users_email\" ON \"users\" (\"email\");\n"+
		"DROP INDEX IF EXISTS \"tenant\".\"idx_users_email\";\n")
}
