package planner_test

import (
	"context"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/renderer"
	"ptah.run/core/schemaext"
	"ptah.run/engine"
	"ptah.run/engine/builtin"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff/difftypes"
)

type selectedRenderer struct {
	*engine.Runtime
	render func(context.Context, renderer.Request) (renderer.Result, error)
}

func (s selectedRenderer) Render(ctx context.Context, request renderer.Request) (renderer.Result, error) {
	return s.render(ctx, request)
}

func TestPlanningUsesSelectedRendererWithCallerContext(t *testing.T) {
	c := qt.New(t)
	runtime, err := builtin.New()
	c.Assert(err, qt.IsNil)
	calls := 0
	selected := selectedRenderer{Runtime: runtime, render: func(ctx context.Context, request renderer.Request) (renderer.Result, error) {
		calls++
		c.Assert(ctx, qt.Equals, t.Context())
		c.Assert(request.Target, qt.Equals, "postgres")
		c.Assert(request.Nodes, qt.HasLen, 1)
		return renderer.Result{Complete: true, Fragments: []string{"selected output"}}, nil
	}}
	diff := &difftypes.SchemaDiff{TablesRemoved: []string{"items"}}
	sql, err := planner.GenerateSchemaDiffSQL(t.Context(), selected, diff, "pgx")
	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Equals, "selected output")
	c.Assert(calls, qt.Equals, 1)
}

func TestPlanningDiscardsFailedRendering(t *testing.T) {
	c := qt.New(t)
	runtime, err := builtin.New()
	c.Assert(err, qt.IsNil)
	failure := errors.New("selected renderer stopped")
	selected := selectedRenderer{Runtime: runtime, render: func(context.Context, renderer.Request) (renderer.Result, error) {
		return renderer.Result{Complete: true, Fragments: []string{"unusable prefix"}}, failure
	}}
	sql, err := planner.GenerateSchemaDiffSQL(t.Context(), selected, &difftypes.SchemaDiff{}, "postgres")
	c.Assert(err, qt.ErrorIs, failure)
	c.Assert(sql, qt.Equals, "")
}

func TestPlanningDiscardsCanceledRendering(t *testing.T) {
	c := qt.New(t)
	runtime, err := builtin.New()
	c.Assert(err, qt.IsNil)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	selected := selectedRenderer{Runtime: runtime, render: func(context.Context, renderer.Request) (renderer.Result, error) {
		cancel()
		return renderer.Result{Complete: true, Fragments: []string{"unusable prefix"}}, nil
	}}
	sql, err := planner.GenerateSchemaDiffSQL(ctx, selected, &difftypes.SchemaDiff{}, "postgres")
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(sql, qt.Equals, "")
}

func TestPlanningRequiresRuntimeBeforeReturningANoop(t *testing.T) {
	c := qt.New(t)
	nodes, err := planner.GenerateSchemaDiffAST(t.Context(), nil, &difftypes.SchemaDiff{}, "postgres")
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(nodes, qt.IsNil)
}
