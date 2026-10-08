package atlasschema_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/renderer"
	"ptah.run/dbschema"
	"ptah.run/engine"
	"ptah.run/engine/builtin"
	"ptah.run/internal/atlasschema"
	"ptah.run/internal/atlasurl"
)

type materializationRenderer struct {
	*engine.Runtime
	render func(context.Context, renderer.SchemaRequest) (renderer.SchemaResult, error)
}

func (s materializationRenderer) RenderSchema(ctx context.Context, request renderer.SchemaRequest) (renderer.SchemaResult, error) {
	return s.render(ctx, request)
}

type materializationContextKey struct{}

func TestDiffMaterializationUsesSelectedSchemaRenderer(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	source := filepath.Join(root, "from.sql")
	c.Assert(os.WriteFile(source, []byte("CREATE TABLE declared (id INTEGER PRIMARY KEY);"), 0o600), qt.IsNil)
	target := seedSourceSQLite(t, "")
	calls := 0
	ctx := context.WithValue(t.Context(), materializationContextKey{}, "caller")
	selected := materializationRenderer{Runtime: must.Must(builtin.New()), render: func(ctx context.Context, request renderer.SchemaRequest) (renderer.SchemaResult, error) {
		calls++
		c.Assert(ctx.Value(materializationContextKey{}), qt.Equals, "caller")
		c.Assert(request.Target, qt.Equals, "sqlite")
		c.Assert(request.Schema.Tables, qt.HasLen, 1)
		c.Assert(request.Schema.Tables[0].Name, qt.Equals, "declared")
		return renderer.SchemaResult{Complete: true, Statements: []string{"CREATE TABLE selected (id INTEGER PRIMARY KEY);"}}, nil
	}}
	report, err := atlasschema.Diff(ctx, atlasschema.DiffOptions{
		Runtime: selected, FromURLs: []string{"file://" + filepath.ToSlash(source)}, ToURLs: []string{atlasurl.SQLiteURLFromPath(target)}, DevURL: atlasurl.SQLiteURLFromPath(filepath.Join(root, "dev.db")),
	})
	c.Assert(err, qt.IsNil)
	sql, err := report.MarshalSQL()
	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, `DROP TABLE IF EXISTS "selected"`)
	c.Assert(calls, qt.Equals, 1)
}

func TestDiffMaterializationPropagatesSelectedRenderingFailure(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	source := filepath.Join(root, "from.sql")
	c.Assert(os.WriteFile(source, []byte("CREATE TABLE declared (id INTEGER PRIMARY KEY);"), 0o600), qt.IsNil)
	target := seedSourceSQLite(t, "")
	failure := errors.New("materialization provider failed")
	calls := 0
	selected := materializationRenderer{Runtime: must.Must(builtin.New()), render: func(context.Context, renderer.SchemaRequest) (renderer.SchemaResult, error) {
		calls++
		return renderer.SchemaResult{Complete: true, Statements: []string{"CREATE TABLE unusable (id INTEGER PRIMARY KEY);"}}, failure
	}}
	_, err := atlasschema.Diff(t.Context(), atlasschema.DiffOptions{
		Runtime: selected, FromURLs: []string{"file://" + filepath.ToSlash(source)}, ToURLs: []string{atlasurl.SQLiteURLFromPath(target)}, DevURL: atlasurl.SQLiteURLFromPath(filepath.Join(root, "dev.db")),
	})
	c.Assert(err, qt.ErrorIs, failure)
	c.Assert(calls, qt.Equals, 1)
	dev := connectSQLite(c, filepath.Join(root, "dev.db"))
	defer dbschema.CloseAndWarn(dev)
	observed, err := dev.Reader().ReadSchemaContext(t.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(observed.Tables, qt.HasLen, 0)
}
