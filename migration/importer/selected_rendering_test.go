package importer_test

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/renderer"
	"ptah.run/core/schemaext"
	"ptah.run/migration/importer"
)

type importRenderFunc func(context.Context, renderer.Request) (renderer.Result, error)

func (f importRenderFunc) Render(ctx context.Context, request renderer.Request) (renderer.Result, error) {
	return f(ctx, request)
}

func TestImportUsesSelectedRendererAndCallerContext(t *testing.T) {
	c := qt.New(t)
	parser := must.Must(importer.ParserByName("liquibase"))
	caps := capability.Capabilities{capability.TransactionalDDL: true}
	var captured []ast.Node
	service := importRenderFunc(func(ctx context.Context, request renderer.Request) (renderer.Result, error) {
		c.Assert(ctx, qt.Equals, t.Context())
		c.Assert(request.Target, qt.Equals, "custom")
		c.Assert(request.Capabilities[capability.TransactionalDDL], qt.IsTrue)
		request.Capabilities[capability.TransactionalDDL] = false
		captured = append(captured, request.Nodes...)
		return renderer.Result{Complete: true, Fragments: []string{fmt.Sprintf("SELECT %d;", len(captured))}}, nil
	})
	selected, err := importer.WithRendering(parser, "custom", caps, service)
	c.Assert(err, qt.IsNil)
	caps[capability.TransactionalDDL] = false
	parsed, err := selected.Parse(t.Context(), liquibaseXMLOneChangeSet(`<createTable tableName="t"><column name="id" type="int"/></createTable>`))
	c.Assert(err, qt.IsNil)
	c.Assert(parsed.Migrations, qt.HasLen, 1)
	c.Assert(parsed.Migrations[0].UpSQL, qt.Equals, "SELECT 1;")
	c.Assert(parsed.Migrations[0].DownSQL, qt.Equals, "SELECT 2;")
	c.Assert(captured, qt.HasLen, 2)
	created, ok := captured[0].(*ast.CreateTableNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(created.Name, qt.Equals, "t")
	dropped, ok := captured[1].(*ast.DropTableNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(dropped.Name, qt.Equals, "t")
}

type importRenderCase struct {
	name      string
	result    renderer.Result
	failure   error
	want      error
	message   string
	cancel    bool
	failAfter int
}

func (tc importRenderCase) renderer(cancel context.CancelFunc, calls *int) importRenderFunc {
	return func(context.Context, renderer.Request) (renderer.Result, error) {
		*calls++
		if *calls <= tc.failAfter {
			return renderer.Result{Complete: true, Fragments: []string{"SELECT 1;"}}, nil
		}
		if tc.cancel {
			cancel()
		}
		return tc.result, tc.failure
	}
}

func TestImportRefusesFailedOrLossyRenderingBeforeWriting(t *testing.T) {
	failure := errors.New("provider unavailable")
	for _, test := range []importRenderCase{
		{name: "failure", result: renderer.Result{Complete: true, Fragments: []string{"prefix"}}, failure: failure, want: failure, message: "provider unavailable"},
		{name: "rollback failure", result: renderer.Result{Complete: true, Fragments: []string{"prefix"}}, failure: failure, want: failure, failAfter: 1, message: "provider unavailable"},
		{name: "incomplete", result: renderer.Result{Complete: true}, want: renderer.ErrInvalidResult, message: "expected 1 node fragments, received 0"},
		{name: "canceled", result: renderer.Result{Complete: true, Fragments: []string{"prefix"}}, cancel: true, want: context.Canceled, message: "context canceled"},
		{name: "malformed omission", result: renderer.Result{Complete: true, Fragments: []string{"prefix"}, Omissions: []renderer.Omission{{Kind: "table"}}}, want: renderer.ErrInvalidResult, message: "omission requires target, kind, and reason"},
		{name: "lossy", result: renderer.Result{Complete: true, Fragments: []string{"prefix"}, Omissions: []renderer.Omission{{Dialect: "custom", Kind: "table", Name: "t", Reason: "unsupported", Property: "storage", Remedy: "choose supported storage"}}}, message: "custom cannot carry the whole change: table t: storage would be skipped (choose supported storage)"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			calls := 0
			service := test.renderer(cancel, &calls)
			parser := must.Must(importer.WithRendering(must.Must(importer.ParserByName("liquibase")), "custom", nil, service))
			out := t.TempDir()
			result, err := importer.Import(ctx, liquibaseXMLOneChangeSet(`<createTable tableName="t"><column name="id" type="int"/></createTable>`), parser, out, importer.Options{})
			c.Assert(err, qt.IsNotNil)
			c.Assert(errors.Is(err, test.want), qt.Equals, test.want != nil)
			c.Assert(err.Error(), qt.Contains, test.message)
			c.Assert(result, qt.IsNil)
			c.Assert(calls, qt.Equals, test.failAfter+1)
			c.Assert(must.Must(os.ReadDir(out)), qt.HasLen, 0)
		})
	}
}

func TestWithRenderingRequiresSelectedService(t *testing.T) {
	for _, service := range []renderer.Service{nil, importRenderFunc(nil)} {
		c := qt.New(t)
		parser, err := importer.WithRendering(must.Must(importer.ParserByName("liquibase")), "custom", nil, service)
		c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
		c.Assert(parser, qt.IsNil)
	}
}

func TestImportParsersRequireLiveContext(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	for _, parser := range importer.Parsers() {
		t.Run(parser.Name(), func(t *testing.T) {
			c := qt.New(t)
			var missingContext context.Context
			missing, err := parser.Parse(missingContext, nil)
			c.Assert(err, qt.ErrorMatches, "migration import requires a context")
			c.Assert(missing, qt.IsNil)
			canceled, err := parser.Parse(ctx, nil)
			c.Assert(err, qt.ErrorIs, context.Canceled)
			c.Assert(canceled, qt.IsNil)
		})
	}
}

type cancelAfterParse struct {
	importer.Parser
	cancel context.CancelFunc
}

func (p cancelAfterParse) Parse(ctx context.Context, source fs.FS) (*importer.ParseResult, error) {
	result, err := p.Parser.Parse(ctx, source)
	p.cancel()
	return result, err
}

func TestImportChecksCancellationBeforeWriting(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	parser := cancelAfterParse{Parser: must.Must(importer.ParserByName("golang-migrate")), cancel: cancel}
	out := t.TempDir()
	result, err := importer.Import(ctx, golangMigrateFS(), parser, out, importer.Options{})
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(result, qt.IsNil)
	c.Assert(must.Must(os.ReadDir(out)), qt.HasLen, 0)
}

type cancelingImportSource struct {
	fstest.MapFS
	cancel context.CancelFunc
}

func (s cancelingImportSource) ReadFile(name string) ([]byte, error) {
	data, err := s.MapFS.ReadFile(name)
	s.cancel()
	return data, err
}

func TestImportParserDiscardsResultIfSourceReadCancels(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	source := cancelingImportSource{MapFS: fstest.MapFS{"1_init.up.sql": {Data: []byte("SELECT 1;")}}, cancel: cancel}
	parser := must.Must(importer.ParserByName("golang-migrate"))
	parsed, err := parser.Parse(ctx, source)
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(parsed, qt.IsNil)
}
