package renderer_test

import (
	"context"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/renderer"
	"ptah.run/core/schemaext"
)

type renderFunc func(context.Context, renderer.Request) (renderer.Result, error)

func (f renderFunc) Render(ctx context.Context, request renderer.Request) (renderer.Result, error) {
	return f(ctx, request)
}

func TestRenderPreservesBatchContextAndIsolatesContainers(t *testing.T) {
	c := qt.New(t)
	node := ast.NewRawSQL("SELECT 1;")
	nodes := []ast.Node{node, &ast.StatementList{}}
	caps := capability.Capabilities{capability.TransactionalDDL: true}
	fragments := []string{"SELECT 1;\n", ""}
	omissions := []renderer.Omission{{Dialect: "custom", Kind: "table", Name: "items", Reason: "unsupported", Property: "storage", Detail: "tier", Remedy: "choose another tier"}}
	calls := 0
	service := renderFunc(func(ctx context.Context, request renderer.Request) (renderer.Result, error) {
		calls++
		c.Assert(ctx, qt.Equals, t.Context())
		c.Assert(request.Target, qt.Equals, "custom")
		c.Assert(request.Nodes, qt.DeepEquals, nodes)
		c.Assert(request.Capabilities, qt.DeepEquals, caps)
		request.Nodes[0] = nil
		request.Capabilities[capability.TransactionalDDL] = false
		return renderer.Result{Complete: true, Fragments: fragments, Omissions: omissions}, nil
	})
	result, err := renderer.Render(t.Context(), service, renderer.Request{Target: "custom", Capabilities: caps, Nodes: nodes})
	c.Assert(err, qt.IsNil)
	fragments[0] = "changed"
	omissions[0].Name = "changed"
	c.Assert(result.SQL(), qt.Equals, "SELECT 1;\n")
	c.Assert(result.Fragments, qt.DeepEquals, []string{"SELECT 1;\n", ""})
	c.Assert(result.Omissions, qt.DeepEquals, []renderer.Omission{{Dialect: "custom", Kind: "table", Name: "items", Reason: "unsupported", Property: "storage", Detail: "tier", Remedy: "choose another tier"}})
	c.Assert(nodes[0], qt.Equals, node)
	c.Assert(caps[capability.TransactionalDDL], qt.IsTrue)
	c.Assert(calls, qt.Equals, 1)
}

func TestRenderRejectsIncompleteReplies(t *testing.T) {
	for _, fragments := range [][]string{nil, {"prefix"}, {"one", "two", "extra"}} {
		c := qt.New(t)
		service := renderFunc(func(context.Context, renderer.Request) (renderer.Result, error) {
			return renderer.Result{Complete: true, Fragments: fragments}, nil
		})
		result, err := renderer.Render(t.Context(), service, renderer.Request{Nodes: []ast.Node{ast.NewRawSQL("first"), ast.NewRawSQL("second")}})
		c.Assert(err, qt.ErrorIs, renderer.ErrInvalidResult)
		c.Assert(result, qt.DeepEquals, renderer.Result{})
	}
}

func TestRenderDiscardsFailedAndCanceledReplies(t *testing.T) {
	failure := errors.New("provider unavailable")
	for _, test := range []struct {
		name string
		err  error
		want error
	}{
		{name: "failure", err: failure, want: failure},
		{name: "cancellation", want: context.Canceled},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			service := renderFunc(func(context.Context, renderer.Request) (renderer.Result, error) {
				cancel()
				return renderer.Result{Complete: true, Fragments: []string{"unusable prefix"}, Omissions: []renderer.Omission{{Dialect: "custom", Kind: "table", Reason: "unsupported"}}}, test.err
			})
			result, err := renderer.Render(ctx, service, renderer.Request{})
			c.Assert(err, qt.ErrorIs, test.want)
			c.Assert(result, qt.DeepEquals, renderer.Result{})
		})
	}
}

func TestRenderRejectsMalformedOmissions(t *testing.T) {
	for _, omission := range []renderer.Omission{
		{Dialect: " ", Kind: "table", Reason: "unsupported"},
		{Dialect: "custom", Kind: " ", Reason: "unsupported"},
		{Dialect: "custom", Kind: "table", Reason: " "},
	} {
		c := qt.New(t)
		service := renderFunc(func(context.Context, renderer.Request) (renderer.Result, error) {
			return renderer.Result{Complete: true, Fragments: []string{"prefix"}, Omissions: []renderer.Omission{omission}}, nil
		})
		result, err := renderer.Render(t.Context(), service, renderer.Request{Nodes: []ast.Node{ast.NewRawSQL("SELECT 1;")}})
		c.Assert(err, qt.ErrorIs, renderer.ErrInvalidResult)
		c.Assert(result, qt.DeepEquals, renderer.Result{})
	}
}

func TestRenderRequiresContextAndServiceBeforeAnEmptyBatch(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	calls := 0
	service := renderFunc(func(context.Context, renderer.Request) (renderer.Result, error) {
		calls++
		return renderer.Result{}, nil
	})
	for _, test := range []struct {
		name    string
		ctx     context.Context
		service renderer.Service
		want    error
	}{
		{name: "missing context", service: service, want: schemaext.ErrInvalidValue},
		{name: "missing service", ctx: t.Context(), want: schemaext.ErrInvalidValue},
		{name: "typed nil", ctx: t.Context(), service: renderFunc(nil), want: schemaext.ErrInvalidValue},
		{name: "canceled", ctx: ctx, service: service, want: context.Canceled},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			result, err := renderer.Render(test.ctx, test.service, renderer.Request{})
			c.Assert(err, qt.ErrorIs, test.want)
			c.Assert(result, qt.DeepEquals, renderer.Result{})
		})
	}
	qt.New(t).Assert(calls, qt.Equals, 0)
}
