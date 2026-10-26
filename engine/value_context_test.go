package engine_test

import (
	"context"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/engine"
)

type databaseContextFunc func(context.Context, schemaext.DatabaseContextRequest) (schemaext.DatabaseContext, error)

func (f databaseContextFunc) ResolveDatabaseContext(ctx context.Context, request schemaext.DatabaseContextRequest) (schemaext.DatabaseContext, error) {
	return f(ctx, request)
}

type valueContextFunc func(context.Context, schemaext.ValueContextRequest) ([]schemaext.Value, error)

func (f valueContextFunc) BindValueContexts(ctx context.Context, request schemaext.ValueContextRequest) ([]schemaext.Value, error) {
	return f(ctx, request)
}

func contextProvider(service valueContextFunc) engine.Provider {
	p := conversionProvider(conversionFunc(func(_ context.Context, r schemaext.ConversionRequest) ([]schemaext.Value, error) {
		return r.Values, nil
	}))
	p.ContextBindings = []engine.ContextBinding{{Target: "custom", Kinds: []schemaext.Kind{conversionFirst, conversionSecond}, Service: service}}
	return p
}

func TestDatabaseContextUsesOnlyTheSelectedTargetAndChecksReceipts(t *testing.T) {
	t.Parallel()
	c := qt.New(t)
	bound := schemaext.DatabaseContext{Database: "/db/realm", SourceDatabase: "/db"}
	calls := 0
	resolve := databaseContextFunc(func(_ context.Context, request schemaext.DatabaseContextRequest) (schemaext.DatabaseContext, error) {
		calls++
		c.Assert(request.Target, qt.Equals, "custom")
		c.Assert(request.Address, qt.Equals, "opaque://selected")
		return bound, nil
	})
	runtime := mustRuntime(c, engine.Provider{ID: "example.org/context", Targets: []engine.Target{
		{Name: "custom", Aliases: []string{"alternate"}, DatabaseContext: resolve}, {Name: "plain"},
	}})
	got, err := runtime.ResolveDatabaseContext(c.Context(), schemaext.DatabaseContextRequest{Target: " ALTERNATE ", Address: "opaque://selected"})
	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.Equals, bound)
	got, err = runtime.ResolveDatabaseContext(c.Context(), schemaext.DatabaseContextRequest{Target: "plain"})
	c.Assert(err, qt.IsNil)
	c.Assert(got.IsZero(), qt.IsTrue)
	_, err = runtime.ResolveDatabaseContext(c.Context(), schemaext.DatabaseContextRequest{Target: "unknown"})
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedDialect)
	c.Assert(calls, qt.Equals, 1)
	bound = schemaext.DatabaseContext{}
	_, err = runtime.ResolveDatabaseContext(c.Context(), schemaext.DatabaseContextRequest{Target: "custom", Address: "opaque://selected"})
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
}

func TestValueContextBindingBatchesAndPreservesSnapshotOwnership(t *testing.T) {
	t.Parallel()
	c := qt.New(t)
	var held schemaext.ValueContextRequest
	calls := 0
	service := valueContextFunc(func(_ context.Context, request schemaext.ValueContextRequest) ([]schemaext.Value, error) {
		calls++
		held = request
		for _, value := range request.Values {
			value.(*conversionValue).Number *= 2
		}
		return request.Values, nil
	})
	p := contextProvider(service)
	runtime := mustRuntime(c, p)
	p.ContextBindings[0].Kinds[0] = "example.org/changed"
	input := []schemaext.Value{&conversionValue{ID: conversionSecond, Number: 3}, &conversionValue{ID: conversionFirst, Number: 5}}
	result, err := runtime.BindValueContexts(c.Context(), schemaext.ValueContextRequest{Target: "alternate", Context: schemaext.DatabaseContext{Database: "/db/realm", SourceDatabase: "/db"}, Representation: schemaext.Desired, Values: input})
	c.Assert(err, qt.IsNil)
	c.Assert(calls, qt.Equals, 1)
	c.Assert(held.Target, qt.Equals, "custom")
	c.Assert(held.Context.SourceDatabase, qt.Equals, "/db")
	c.Assert(input[0].(*conversionValue).Number, qt.Equals, 3)
	c.Assert(result[0].(*conversionValue).Number, qt.Equals, 6)
	c.Assert(result[1].(*conversionValue).Number, qt.Equals, 10)
	held.Values[0].(*conversionValue).Number = 100
	c.Assert(result[0].(*conversionValue).Number, qt.Equals, 6)
}

func TestValueContextBindingRefusesIncompleteRepliesAndCancellation(t *testing.T) {
	t.Parallel()
	failure := errors.New("binding refused")
	for _, test := range []struct {
		name    string
		respond func(context.CancelFunc, schemaext.ValueContextRequest) ([]schemaext.Value, error)
		want    error
	}{
		{name: "missing", respond: func(_ context.CancelFunc, r schemaext.ValueContextRequest) ([]schemaext.Value, error) {
			return r.Values[:1], nil
		}, want: schemaext.ErrInvalidValue},
		{name: "reordered", respond: func(_ context.CancelFunc, r schemaext.ValueContextRequest) ([]schemaext.Value, error) {
			return []schemaext.Value{r.Values[1], r.Values[0]}, nil
		}, want: schemaext.ErrInvalidValue},
		{name: "failed", respond: func(_ context.CancelFunc, r schemaext.ValueContextRequest) ([]schemaext.Value, error) {
			return r.Values[:1], failure
		}, want: failure},
		{name: "canceled", respond: func(cancel context.CancelFunc, r schemaext.ValueContextRequest) ([]schemaext.Value, error) {
			cancel()
			return r.Values, nil
		}, want: context.Canceled},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)
			ctx, cancel := context.WithCancel(c.Context())
			c.Cleanup(cancel)
			service := valueContextFunc(func(_ context.Context, request schemaext.ValueContextRequest) ([]schemaext.Value, error) {
				return test.respond(cancel, request)
			})
			runtime := mustRuntime(c, contextProvider(service))
			result, err := runtime.BindValueContexts(ctx, schemaext.ValueContextRequest{Target: "custom", Context: schemaext.DatabaseContext{Database: "/db", SourceDatabase: "/db"}, Representation: schemaext.Desired, Values: []schemaext.Value{&conversionValue{ID: conversionFirst}, &conversionValue{ID: conversionSecond}}})
			c.Assert(err, qt.ErrorIs, test.want)
			c.Assert(result, qt.IsNil)
		})
	}
}

func TestValueContextBindingRegistrationIsExclusive(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		mutate func(*engine.Provider)
	}{
		{name: "duplicate kind", mutate: func(p *engine.Provider) {
			p.ContextBindings[0].Kinds = append(p.ContextBindings[0].Kinds, conversionFirst)
		}},
		{name: "duplicate owner", mutate: func(p *engine.Provider) { p.ContextBindings = append(p.ContextBindings, p.ContextBindings[0]) }},
		{name: "unknown kind", mutate: func(p *engine.Provider) { p.ContextBindings[0].Kinds = []schemaext.Kind{"example.org/absent"} }},
		{name: "alias", mutate: func(p *engine.Provider) { p.ContextBindings[0].Target = "alternate" }},
		{name: "typed nil", mutate: func(p *engine.Provider) { p.ContextBindings[0].Service = valueContextFunc(nil) }},
		{name: "typed nil resolver", mutate: func(p *engine.Provider) { p.Targets[0].DatabaseContext = databaseContextFunc(nil) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)
			p := contextProvider(valueContextFunc(func(_ context.Context, r schemaext.ValueContextRequest) ([]schemaext.Value, error) {
				return r.Values, nil
			}))
			test.mutate(&p)
			_, err := engine.New(p)
			c.Assert(err, qt.ErrorIs, engine.ErrInvalidRegistration)
		})
	}
}
