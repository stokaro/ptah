package engine_test

import (
	"context"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemapreparation"
	"ptah.run/engine"
)

type preparationFunc func(context.Context, schemapreparation.Request) (schemapreparation.Result, error)

func (f preparationFunc) PrepareTables(ctx context.Context, request schemapreparation.Request) (schemapreparation.Result, error) {
	return f(ctx, request)
}

func preparationProvider(service schemapreparation.Service) engine.Provider {
	return engine.Provider{ID: "example.org/preparation", Targets: []engine.Target{{Name: "custom", Aliases: []string{"alternate"}, Preparation: service}}}
}

func preparationRequest() schemapreparation.Request {
	semantics := identifier.ForDialect("postgres")
	return schemapreparation.Request{Target: "alternate", Identifiers: semantics, Tables: []schemapreparation.Table{{
		Subject: objectidentity.NewBuilder(semantics).TableParts("", "events"),
		Desired: schemacapture.TableDeclaration{
			Table:  schemamodel.Table{Name: "events", StructName: "Event"},
			Fields: []schemamodel.Field{{Name: "id", StructName: "Event", Type: "BIGINT"}},
		},
		Current:          schemacapture.TableObservation{Table: catalog.Table{Name: "events", Columns: []catalog.Column{{Name: "id", DataType: "BIGINT"}}}},
		CurrentKnowledge: schemaext.Knowledge{State: schemaext.Complete},
	}}}
}

func TestPreparation_SelectedAndCapturedIndependently(t *testing.T) {
	c := qt.New(t)
	var received schemapreparation.Request
	service := preparationFunc(func(_ context.Context, request schemapreparation.Request) (schemapreparation.Result, error) {
		received = request
		request.Tables[0].Desired.Fields[0].Primary = true
		request.Tables[0].ColumnPrimaryKeysPrepared = true
		return schemapreparation.Result{Complete: true, Tables: request.Tables}, nil
	})
	provider := preparationProvider(service)
	runtime := mustRuntime(c, provider)
	provider.Targets[0].Preparation = nil
	source := preparationRequest()
	result, err := runtime.PrepareTables(t.Context(), source)
	c.Assert(err, qt.IsNil)
	c.Assert(received.Target, qt.Equals, "custom")
	c.Assert(result.Complete, qt.IsTrue)
	c.Assert(result.Tables[0].Desired.Fields[0].Primary, qt.IsTrue)
	c.Assert(source.Tables[0].Desired.Fields[0].Primary, qt.IsFalse)
	received.Tables[0].Desired.Fields[0].Name = "changed"
	c.Assert(result.Tables[0].Desired.Fields[0].Name, qt.Equals, "id")
}

func TestPreparation_RejectsMalformedReplies(t *testing.T) {
	cases := []struct {
		name   string
		change func(*schemapreparation.Result)
	}{
		{name: "incomplete", change: func(r *schemapreparation.Result) { r.Complete = false }},
		{name: "missing table", change: func(r *schemapreparation.Result) { r.Tables = nil }},
		{name: "changed subject spelling", change: func(r *schemapreparation.Result) { r.Tables[0].Subject.Name.Source = "changed" }},
		{name: "renamed declaration", change: func(r *schemapreparation.Result) { r.Tables[0].Desired.Table.Name = "renamed" }},
		{name: "renamed column", change: func(r *schemapreparation.Result) { r.Tables[0].Desired.Fields[0].Name = "renamed" }},
		{name: "removed column", change: func(r *schemapreparation.Result) { r.Tables[0].Desired.Fields = nil }},
		{name: "changed type", change: func(r *schemapreparation.Result) { r.Tables[0].Desired.Fields[0].Type = "TEXT" }},
		{name: "changed observation", change: func(r *schemapreparation.Result) { r.Tables[0].Current.Table.Columns[0].IsPrimaryKey = true }},
		{name: "changed knowledge", change: func(r *schemapreparation.Result) { r.Tables[0].CurrentKnowledge.State = schemaext.Absent }},
		{name: "missing column receipt", change: func(r *schemapreparation.Result) { r.Tables[0].Desired.Fields[0].Primary = true }},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			service := preparationFunc(func(_ context.Context, request schemapreparation.Request) (schemapreparation.Result, error) {
				result := schemapreparation.Result{Complete: true, Tables: request.Tables}
				test.change(&result)
				return result, nil
			})
			runtime := mustRuntime(c, preparationProvider(service))
			result, err := runtime.PrepareTables(t.Context(), preparationRequest())
			c.Assert(err, qt.ErrorIs, schemapreparation.ErrInvalid)
			c.Assert(result, qt.DeepEquals, schemapreparation.Result{})
		})
	}
}

func TestPreparation_ProviderFailureDiscardsPartialResult(t *testing.T) {
	c := qt.New(t)
	failure := errors.New("provider failed")
	service := preparationFunc(func(_ context.Context, request schemapreparation.Request) (schemapreparation.Result, error) {
		return schemapreparation.Result{Complete: true, Tables: request.Tables}, failure
	})
	runtime := mustRuntime(c, preparationProvider(service))
	result, err := runtime.PrepareTables(t.Context(), preparationRequest())
	c.Assert(err, qt.ErrorIs, failure)
	c.Assert(result, qt.DeepEquals, schemapreparation.Result{})
}

func TestPreparation_CancellationDiscardsReply(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	service := preparationFunc(func(_ context.Context, request schemapreparation.Request) (schemapreparation.Result, error) {
		cancel()
		return schemapreparation.Result{Complete: true, Tables: request.Tables}, nil
	})
	runtime := mustRuntime(c, preparationProvider(service))
	result, err := runtime.PrepareTables(ctx, preparationRequest())
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(result, qt.DeepEquals, schemapreparation.Result{})
}

func TestPreparation_RequiresSelectedService(t *testing.T) {
	c := qt.New(t)
	runtime := mustRuntime(c, preparationProvider(nil))
	result, err := runtime.PrepareTables(t.Context(), preparationRequest())
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(result, qt.DeepEquals, schemapreparation.Result{})
}

func TestPreparation_RejectsTypedNilRegistration(t *testing.T) {
	c := qt.New(t)
	var service preparationFunc
	runtime, err := engine.New(preparationProvider(service))
	c.Assert(err, qt.ErrorIs, engine.ErrInvalidRegistration)
	c.Assert(runtime, qt.IsNil)
}
