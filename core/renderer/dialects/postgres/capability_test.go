package postgres_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/ast"
	"github.com/stokaro/ptah/core/platform"
	"github.com/stokaro/ptah/core/renderer/dialects/postgres"
)

func TestPostgreSQLRenderer_NilCapabilitiesAreConservative(t *testing.T) {
	c := qt.New(t)

	renderer := postgres.NewWithCapabilities(nil, platform.CockroachDB)

	idx := ast.NewIndex("idx_users_email", "users", "email")
	idx.Concurrently = true
	sql, err := renderer.Render(idx)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Not(qt.Contains), "CONCURRENTLY")

	xmlTable := ast.NewCreateTable("events").
		AddColumn(ast.NewColumn("payload", "XML"))
	_, err = renderer.Render(xmlTable)
	c.Assert(err, qt.ErrorMatches, `error rendering column payload: cockroachdb does not support XML columns; use a platform-specific type override`)
}
