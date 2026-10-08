package engine_test

import (
	"context"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/engine"
)

type renderingFunc func(context.Context, renderer.Request) (renderer.Result, error)

func (f renderingFunc) Render(ctx context.Context, request renderer.Request) (renderer.Result, error) {
	return f(ctx, request)
}

func TestRuntime_ExplicitOwnership(t *testing.T) {
	c := qt.New(t)
	var received renderer.Request
	service := renderingFunc(func(_ context.Context, request renderer.Request) (renderer.Result, error) {
		received = request
		return renderer.Result{Complete: true, Fragments: []string{"selected"}}, nil
	})
	providers := []engine.Provider{{ID: "example.org/custom", Targets: []engine.Target{
		{Name: "custom", Aliases: []string{"alternate"}, Rendering: service},
		{Name: "offline-only"},
	}}}
	runtime, err := engine.New(providers...)
	c.Assert(err, qt.IsNil)
	providers[0].Targets[0].Name = "changed"
	providers[0].Targets[0].Aliases[0] = "changed"
	providers[0].Targets[0].Rendering = nil
	result, err := runtime.Render(context.Background(), renderer.Request{Target: " ALTERNATE ", Nodes: []ast.Node{&ast.StatementList{}}})
	c.Assert(err, qt.IsNil)
	c.Assert(result.SQL(), qt.Equals, "selected")
	c.Assert(received.Target, qt.Equals, "custom")
	c.Assert(runtime.Targets(), qt.DeepEquals, []string{"custom", "offline-only"})
	names := runtime.Targets()
	names[0] = "mutated"
	c.Assert(runtime.Targets(), qt.DeepEquals, []string{"custom", "offline-only"})
}

func TestRuntime_NoImplicitProviders(t *testing.T) {
	for _, runtime := range []*engine.Runtime{nil, {}, mustRuntime(qt.New(t))} {
		t.Run("empty", func(t *testing.T) {
			c := qt.New(t)
			result, err := runtime.Render(context.Background(), renderer.Request{Target: "postgres"})
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedDialect)
			c.Assert(result, qt.DeepEquals, renderer.Result{})
			c.Assert(runtime.Targets(), qt.HasLen, 0)
		})
	}
}

func TestNew_InvalidRegistration(t *testing.T) {
	var nilRenderer renderingFunc
	cases := []struct {
		name      string
		providers []engine.Provider
	}{
		{name: "missing identity", providers: []engine.Provider{{}}},
		{name: "unqualified identity", providers: []engine.Provider{{ID: "custom"}}},
		{name: "empty identity component", providers: []engine.Provider{{ID: "example.org//custom"}}},
		{name: "duplicate owner", providers: []engine.Provider{{ID: "example.org/custom"}, {ID: "example.org/custom"}}},
		{name: "empty target", providers: []engine.Provider{{ID: "example.org/custom", Targets: []engine.Target{{}}}}},
		{name: "uppercase target", providers: []engine.Provider{{ID: "example.org/custom", Targets: []engine.Target{{Name: "Custom"}}}}},
		{name: "invalid alias", providers: []engine.Provider{{ID: "example.org/custom", Targets: []engine.Target{{Name: "custom", Aliases: []string{"bad alias"}}}}}},
		{name: "duplicate alias", providers: []engine.Provider{{ID: "example.org/custom", Targets: []engine.Target{{Name: "custom", Aliases: []string{"custom"}}}}}},
		{name: "typed nil service", providers: []engine.Provider{{ID: "example.org/custom", Targets: []engine.Target{{Name: "custom", Rendering: nilRenderer}}}}},
		{name: "conflicting targets", providers: []engine.Provider{
			{ID: "example.org/first", Targets: []engine.Target{{Name: "custom"}}},
			{ID: "example.org/second", Targets: []engine.Target{{Name: "custom"}}},
		}},
		{name: "alias shadows target", providers: []engine.Provider{
			{ID: "example.org/first", Targets: []engine.Target{{Name: "custom"}}},
			{ID: "example.org/second", Targets: []engine.Target{{Name: "second", Aliases: []string{"custom"}}}},
		}},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			runtime, err := engine.New(tt.providers...)
			c.Assert(err, qt.ErrorIs, engine.ErrInvalidRegistration)
			c.Assert(runtime, qt.IsNil)
		})
	}
}

func TestRuntime_UnavailableRendering(t *testing.T) {
	c := qt.New(t)
	runtime := mustRuntime(c, engine.Provider{ID: "example.org/custom", Targets: []engine.Target{{Name: "custom"}}})
	result, err := runtime.Render(context.Background(), renderer.Request{Target: "custom"})
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(result, qt.DeepEquals, renderer.Result{})
}

func TestRuntime_ServiceFailureHasNoPartialSQL(t *testing.T) {
	c := qt.New(t)
	failure := errors.New("provider connection lost")
	service := renderingFunc(func(context.Context, renderer.Request) (renderer.Result, error) {
		return renderer.Result{Complete: true, Fragments: []string{"partial"}}, failure
	})
	runtime := mustRuntime(c, renderingProvider(service))
	result, err := runtime.Render(context.Background(), renderer.Request{Target: "custom"})
	c.Assert(err, qt.ErrorIs, failure)
	c.Assert(result, qt.DeepEquals, renderer.Result{})
}

func TestRuntime_CancellationBeforeDispatch(t *testing.T) {
	c := qt.New(t)
	calls := 0
	service := renderingFunc(func(context.Context, renderer.Request) (renderer.Result, error) {
		calls++
		return renderer.Result{Complete: true, Fragments: []string{"unexpected"}}, nil
	})
	runtime := mustRuntime(c, renderingProvider(service))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	result, err := runtime.Render(ctx, renderer.Request{Target: "custom"})
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(result, qt.DeepEquals, renderer.Result{})
	c.Assert(calls, qt.Equals, 0)
}

func TestRuntime_CancellationDuringDispatch(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	service := renderingFunc(func(context.Context, renderer.Request) (renderer.Result, error) {
		cancel()
		return renderer.Result{Complete: true, Fragments: []string{"discarded"}}, nil
	})
	runtime := mustRuntime(c, renderingProvider(service))
	result, err := runtime.Render(ctx, renderer.Request{Target: "custom"})
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(result, qt.DeepEquals, renderer.Result{})
}

func TestRuntime_RequestSliceIsolation(t *testing.T) {
	c := qt.New(t)
	node := &ast.StatementList{}
	nodes := []ast.Node{node}
	service := renderingFunc(func(_ context.Context, request renderer.Request) (renderer.Result, error) {
		request.Nodes[0] = nil
		return renderer.Result{Complete: true, Fragments: []string{""}}, nil
	})
	runtime := mustRuntime(c, renderingProvider(service))
	_, err := runtime.Render(context.Background(), renderer.Request{Target: "custom", Nodes: nodes})
	c.Assert(err, qt.IsNil)
	c.Assert(nodes[0], qt.Equals, node)
}

func renderingProvider(service renderer.Service) engine.Provider {
	return engine.Provider{ID: "example.org/custom", Targets: []engine.Target{{Name: "custom", Rendering: service}}}
}

func mustRuntime(c *qt.C, providers ...engine.Provider) *engine.Runtime {
	c.Helper()
	runtime, err := engine.New(providers...)
	c.Assert(err, qt.IsNil)
	return runtime
}

func TestRuntime_ContextPropagation(t *testing.T) {
	c := qt.New(t)
	type requestKey struct{}
	ctx := context.WithValue(context.Background(), requestKey{}, "request")
	var received any
	service := renderingFunc(func(ctx context.Context, _ renderer.Request) (renderer.Result, error) {
		received = ctx.Value(requestKey{})
		return renderer.Result{Complete: true}, nil
	})
	runtime := mustRuntime(c, renderingProvider(service))
	_, err := runtime.Render(ctx, renderer.Request{Target: "custom"})
	c.Assert(err, qt.IsNil)
	c.Assert(received, qt.Equals, "request")
}

func TestRuntimeRejectsIncompleteRenderingReplies(t *testing.T) {
	c := qt.New(t)
	service := renderingFunc(func(context.Context, renderer.Request) (renderer.Result, error) {
		return renderer.Result{Complete: true, Fragments: []string{"prefix"}}, nil
	})
	runtime := mustRuntime(c, renderingProvider(service))
	result, err := runtime.Render(t.Context(), renderer.Request{
		Target: "custom", Nodes: []ast.Node{ast.NewRawSQL("first"), ast.NewRawSQL("second")},
	})
	c.Assert(err, qt.ErrorIs, renderer.ErrInvalidResult)
	c.Assert(result, qt.DeepEquals, renderer.Result{})
}
