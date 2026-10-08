package embedpg_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"
	_ "modernc.org/sqlite" // executes selected test SQL without a PostgreSQL server

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemavalidation"
	"ptah.run/internal/embedpg"
	"ptah.run/internal/embedstore"
)

type storeRenderFunc func(context.Context, renderer.Request) (renderer.Result, error)

func (f storeRenderFunc) Render(ctx context.Context, request renderer.Request) (renderer.Result, error) {
	return f(ctx, request)
}

func storeTestDatabase(t *testing.T) *sql.DB {
	t.Helper()
	c := qt.New(t)
	db := must.Must(sql.Open("sqlite", filepath.Join(t.TempDir(), "store.db")))
	t.Cleanup(func() { c.Assert(db.Close(), qt.IsNil) })
	return db
}

func storeFragments(request renderer.Request) []string {
	fragments := []string{"CREATE TABLE selected_store (seq INTEGER);"}
	for index := 1; index < len(request.Nodes); index++ {
		fragments = append(fragments, fmt.Sprintf("INSERT INTO selected_store VALUES (%d);", index))
	}
	return fragments
}

func TestEnsureSchemaExecutesTheCompleteSelectedBatch(t *testing.T) {
	c := qt.New(t)
	db := storeTestDatabase(t)
	caps := capability.ForDialect(embedpg.Dialect)
	calls := 0
	service := storeRenderFunc(func(ctx context.Context, request renderer.Request) (renderer.Result, error) {
		calls++
		c.Assert(ctx, qt.Equals, t.Context())
		c.Assert(request.Target, qt.Equals, embedpg.Dialect)
		c.Assert(request.Capabilities, qt.DeepEquals, caps)
		c.Assert(request.Nodes, qt.HasLen, 6)
		run, ok := request.Nodes[1].(*ast.CreateTableNode)
		c.Assert(ok, qt.IsTrue)
		c.Assert(run.Name, qt.Equals, embedstore.RunTable)
		c.Assert(run.IfNotExists, qt.IsTrue)
		index, ok := request.Nodes[4].(*ast.IndexNode)
		c.Assert(ok, qt.IsTrue)
		c.Assert(index.Name, qt.Equals, embedstore.RunTable+"_generation_idx")
		c.Assert(index.IfNotExists, qt.IsTrue)
		return renderer.Result{Complete: true, Fragments: storeFragments(request)}, nil
	})
	c.Assert(embedpg.NewStore(db).EnsureSchema(t.Context(), service, caps), qt.IsNil)
	c.Assert(calls, qt.Equals, 1)
	var rows string
	c.Assert(db.QueryRowContext(t.Context(), "SELECT group_concat(seq, '|') FROM (SELECT seq FROM selected_store ORDER BY rowid)").Scan(&rows), qt.IsNil)
	c.Assert(rows, qt.Equals, "1|2|3|4|5")
}

type storeRenderCase struct {
	name   string
	want   error
	err    error
	cancel bool
}

func (tc storeRenderCase) service(cancel context.CancelFunc) renderer.Service {
	return storeRenderFunc(func(_ context.Context, request renderer.Request) (renderer.Result, error) {
		result := renderer.Result{Complete: true, Fragments: storeFragments(request)}
		switch tc.name {
		case "missing completion":
			result.Complete = false
		case "missing fragment":
			result.Fragments = result.Fragments[:len(result.Fragments)-1]
		case "empty fragment":
			result.Fragments[len(result.Fragments)-1] = " \n"
		case "omission":
			result.Omissions = []renderer.Omission{{Dialect: embedpg.Dialect, Kind: "table", Reason: "unsupported", Property: "primary key"}}
		case "refusal":
			result = renderer.Result{Complete: true, Diagnostics: []renderer.Diagnostic{{Problem: schemavalidation.Diagnostic{
				Code: schemavalidation.InvalidSchema, Kind: "table", Message: "invalid store schema",
			}, Input: new(5)}}}
		}
		if tc.cancel {
			cancel()
		}
		return result, tc.err
	})
}

func TestEnsureSchemaRejectsTheWholeBatchBeforeExecutingAPrefix(t *testing.T) {
	failure := errors.New("renderer disconnected")
	for _, test := range []storeRenderCase{
		{name: "service failure", err: failure, want: failure},
		{name: "missing completion", want: renderer.ErrInvalidResult},
		{name: "missing fragment", want: renderer.ErrInvalidResult},
		{name: "empty fragment", want: renderer.ErrInvalidResult},
		{name: "omission", want: ptaherr.ErrUnsupportedFeature},
		{name: "refusal", want: ptaherr.ErrInvalidSchemaDiff},
		{name: "cancellation", cancel: true, want: context.Canceled},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db := storeTestDatabase(t)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			err := embedpg.NewStore(db).EnsureSchema(ctx, test.service(cancel), capability.ForDialect(embedpg.Dialect))
			c.Assert(err, qt.ErrorIs, test.want)
			var created int
			c.Assert(db.QueryRowContext(t.Context(), "SELECT count(*) FROM sqlite_master WHERE name = 'selected_store'").Scan(&created), qt.IsNil)
			c.Assert(created, qt.Equals, 0)
		})
	}
}

func TestEnsureSchemaRequiresASelectedRenderer(t *testing.T) {
	c := qt.New(t)
	db := storeTestDatabase(t)
	err := embedpg.NewStore(db).EnsureSchema(t.Context(), nil, capability.ForDialect(embedpg.Dialect))
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
}
