package generator_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/renderer"
	"ptah.run/engine"
	"ptah.run/engine/builtin"
	"ptah.run/internal/atlasurl"
	"ptah.run/migration/generator"
)

type recordedRenderer struct {
	*engine.Runtime
	requests []renderer.Request
	contexts []context.Context
	failAt   int
	failure  error
	cancel   context.CancelFunc
}

func (s *recordedRenderer) Render(ctx context.Context, request renderer.Request) (renderer.Result, error) {
	s.requests = append(s.requests, request)
	s.contexts = append(s.contexts, ctx)
	result, err := s.Runtime.Render(ctx, request)
	if err != nil {
		return result, err
	}
	for i := range result.Fragments {
		result.Fragments[i] = fmt.Sprintf("-- selected batch %d\n", len(s.requests)) + result.Fragments[i]
	}
	if len(s.requests) == s.failAt {
		if s.cancel != nil {
			s.cancel()
		}
		return result, s.failure
	}
	return result, nil
}

func selectedRenderingOptions(c *qt.C, selected generator.Runtime) generator.GenerateMigrationOptions {
	c.Helper()
	root := c.TempDir()
	entities := writeEntities(c, root, `//ptah:schema:table name="widgets"
type Widget struct {
	//ptah:schema:field name="id" type="INTEGER" primary="true"
	ID int64
}`)
	return generator.GenerateMigrationOptions{
		Runtime: selected, GoEntitiesDir: entities,
		DatabaseURL: atlasurl.SQLiteURLFromPath(filepath.Join(root, "target.db")),
		OutputDir:   makeDir(c, root, "migrations"), MigrationName: "widgets",
	}
}

func TestGenerateMigrationUsesSelectedRenderingForSafetyAndBothFiles(t *testing.T) {
	c := qt.New(t)
	runtime, err := builtin.New()
	c.Assert(err, qt.IsNil)
	selected := &recordedRenderer{Runtime: runtime}
	files, err := generator.GenerateMigration(t.Context(), selectedRenderingOptions(c, selected))
	c.Assert(err, qt.IsNil)
	c.Assert(files.Files, qt.HasLen, 1)
	c.Assert(readFile(c, files.Files[0].UpFile), qt.Contains, "-- selected batch 2")
	c.Assert(readFile(c, files.Files[0].DownFile), qt.Contains, "-- selected batch 3")
	c.Assert(selected.requests, qt.HasLen, 3)
	for i, request := range selected.requests {
		c.Assert(selected.contexts[i], qt.Equals, t.Context())
		c.Assert(request.Target, qt.Equals, "sqlite")
		c.Assert(request.Capabilities, qt.Not(qt.HasLen), 0)
		c.Assert(request.Nodes, qt.HasLen, 1)
	}
	c.Assert(selected.requests[0].Nodes[0], qt.Satisfies, func(node ast.Node) bool { _, ok := node.(*ast.CreateTableNode); return ok })
	c.Assert(selected.requests[1].Nodes[0], qt.Satisfies, func(node ast.Node) bool { _, ok := node.(*ast.CreateTableNode); return ok })
	c.Assert(selected.requests[2].Nodes[0], qt.Satisfies, func(node ast.Node) bool { _, ok := node.(*ast.DropTableNode); return ok })
}

func TestGenerateMigrationPublishesNothingAfterSelectedRenderingFails(t *testing.T) {
	failure := errors.New("selected render failed")
	for _, failAt := range []int{1, 2, 3} {
		t.Run(fmt.Sprint(failAt), func(t *testing.T) {
			c := qt.New(t)
			runtime, err := builtin.New()
			c.Assert(err, qt.IsNil)
			selected := &recordedRenderer{Runtime: runtime, failAt: failAt, failure: failure}
			opts := selectedRenderingOptions(c, selected)
			files, err := generator.GenerateMigration(t.Context(), opts)
			c.Assert(err, qt.ErrorIs, failure)
			c.Assert(files, qt.IsNil)
			c.Assert(selected.requests, qt.HasLen, failAt)
			entries, err := os.ReadDir(opts.OutputDir)
			c.Assert(err, qt.IsNil)
			c.Assert(entries, qt.HasLen, 0)
		})
	}
}

func TestGenerateMigrationPublishesNothingAfterSelectedRenderingCancels(t *testing.T) {
	c := qt.New(t)
	runtime, err := builtin.New()
	c.Assert(err, qt.IsNil)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	selected := &recordedRenderer{Runtime: runtime, failAt: 3, cancel: cancel}
	opts := selectedRenderingOptions(c, selected)
	files, err := generator.GenerateMigration(ctx, opts)
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(files, qt.IsNil)
	c.Assert(selected.requests, qt.HasLen, 3)
	entries, err := os.ReadDir(opts.OutputDir)
	c.Assert(err, qt.IsNil)
	c.Assert(entries, qt.HasLen, 0)
}
