package atlasmigrate_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/renderer"
	"ptah.run/internal/atlasmigrate"
)

type selectedRendering func(context.Context, renderer.Request) (renderer.Result, error)

func (f selectedRendering) Render(ctx context.Context, request renderer.Request) (renderer.Result, error) {
	return f(ctx, request)
}

func TestBuildMigrationFileContentsUsesOneSelectedBatch(t *testing.T) {
	c := qt.New(t)
	calls := 0
	nodes := []ast.Node{ast.NewRawSQL("SELECT 1;"), ast.NewRawSQL("SELECT 2;")}
	caps := capability.ForDialect("postgres")
	service := selectedRendering(func(ctx context.Context, request renderer.Request) (renderer.Result, error) {
		calls++
		c.Assert(ctx, qt.Equals, t.Context())
		c.Assert(request.Target, qt.Equals, "postgres")
		c.Assert(request.Nodes, qt.DeepEquals, nodes)
		c.Assert(request.Capabilities, qt.DeepEquals, caps)
		return renderer.Result{Complete: true, Fragments: []string{"SELECT 3;\n", "SELECT 4;\n"}}, nil
	})
	contents, err := atlasmigrate.BuildMigrationFileContents(t.Context(), service, "pgx", caps, defaultMigrateDiffFormat(), nodes)
	c.Assert(err, qt.IsNil)
	c.Assert(calls, qt.Equals, 1)
	c.Assert(contents, qt.HasLen, 1)
	c.Assert(contents[0].SQL, qt.Contains, "SELECT 3;")
	c.Assert(contents[0].SQL, qt.Contains, "SELECT 4;")
}

func TestBuildMigrationFileContentsRefusesIncompleteRendering(t *testing.T) {
	c := qt.New(t)
	service := selectedRendering(func(context.Context, renderer.Request) (renderer.Result, error) {
		return renderer.Result{Complete: true, Fragments: []string{"SELECT 1;"}}, nil
	})
	nodes := []ast.Node{ast.NewRawSQL("SELECT 1;"), ast.NewRawSQL("SELECT 2;")}
	contents, err := atlasmigrate.BuildMigrationFileContents(t.Context(), service, "postgres", nil, defaultMigrateDiffFormat(), nodes)
	c.Assert(err, qt.ErrorIs, renderer.ErrInvalidResult)
	c.Assert(contents, qt.IsNil)
}
