package engine_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
)

func capturedTypes() featureplan.TypeContext {
	builder := objectidentity.NewBuilder(identifier.ForDialect("oracle"))
	return featureplan.TypeContext{
		Desired: []schemaext.DefinedType{
			{ID: builder.SchemaScopedParts(objectidentity.KindComposite, `"app"`, `"Zip"`), References: []string{`"app"."Postal"`, "NUMBER(8, 2)"}},
			{ID: builder.SchemaScopedParts(objectidentity.KindDomain, "APP", "UNCHANGED"), References: []string{"VARCHAR2(8 CHAR)"}},
		},
		Current: []schemaext.DefinedType{
			{ID: builder.SchemaScopedParts(objectidentity.KindComposite, `"app"`, `"Zip"`), References: []string{`"app"."Postal"`}},
		},
	}
}

func copiedTypeFacts(types []schemaext.DefinedType) []schemaext.DefinedType {
	result := make([]schemaext.DefinedType, len(types))
	for i, defined := range types {
		result[i] = defined.Clone()
	}
	return result
}

// Each batch sees all captured definitions, even though its changes belong to
// one service. Mutating a callback's data reaches neither another callback nor
// the caller. No codec for the context types is registered.
func TestPlanningTypeContextIsolatesBatches(t *testing.T) {
	c := qt.New(t)
	var received []featureplan.TypeContext
	var retained []featureplan.TypeContext
	service := planningFunc(func(ctx context.Context, request featureplan.Request) (featureplan.Result, error) {
		received = append(received, featureplan.TypeContext{Desired: copiedTypeFacts(request.Types.Desired), Current: copiedTypeFacts(request.Types.Current)})
		retained = append(retained, request.Types)
		result, err := plannedFixture(ctx, request)
		request.Types.Desired[0].References[0] = "mutated"
		request.Types.Current[0].References[0] = "mutated"
		request.Types.Desired[1].ID.Name.Source = "mutated"
		return result, err
	})
	p := planningProvider(service)
	registration := p.Planning[0]
	p.Planning = nil
	for _, kind := range []schemaext.Kind{conversionFirst, conversionSecond} {
		batch := registration
		batch.Kinds = []schemaext.Kind{kind}
		p.Planning = append(p.Planning, batch)
	}
	request := planningRequest()
	request.Types = capturedTypes()
	result, err := mustRuntime(c, p).PlanFeatures(t.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Changes, qt.HasLen, len(request.Changes))
	c.Assert(received, qt.DeepEquals, []featureplan.TypeContext{capturedTypes(), capturedTypes()})
	c.Assert(request.Types, qt.DeepEquals, capturedTypes())
	retained[0].Desired[0].References[0] = "first callback only"
	retained[0].Current[0].References[0] = "first callback only"
	c.Assert(retained[1].Desired[0].References[0], qt.Equals, "mutated")
	c.Assert(retained[1].Current[0].References[0], qt.Equals, "mutated")
	c.Assert(request.Types, qt.DeepEquals, capturedTypes())
}

func invalidPlanningTypes() []struct {
	name  string
	types []schemaext.DefinedType
} {
	valid := capturedTypes().Desired[0]
	wrongKind := valid.Clone()
	wrongKind.ID.Kind = objectidentity.KindTable
	noSource := valid.Clone()
	noSource.ID.Name.Source = ""
	noKey := valid.Clone()
	noKey.ID.Name.Normalized = ""
	alias := valid.Clone()
	alias.ID.Name.Source = "different spelling with the same key"
	return []struct {
		name  string
		types []schemaext.DefinedType
	}{
		{"zero type", []schemaext.DefinedType{{}}},
		{"wrong kind", []schemaext.DefinedType{wrongKind}},
		{"missing source", []schemaext.DefinedType{noSource}},
		{"missing key", []schemaext.DefinedType{noKey}},
		{"duplicate identity", []schemaext.DefinedType{valid, valid}},
		{"conflicting spelling", []schemaext.DefinedType{valid, alias}},
	}
}

func TestPlanningRefusesInvalidTypeContextBeforeDispatch(t *testing.T) {
	for _, test := range invalidPlanningTypes() {
		for _, side := range []string{"desired", "current"} {
			t.Run(test.name+"/"+side, func(t *testing.T) {
				c := qt.New(t)
				calls := 0
				runtime := mustRuntime(c, planningProvider(planningFunc(func(ctx context.Context, request featureplan.Request) (featureplan.Result, error) {
					calls++
					return plannedFixture(ctx, request)
				})))
				request := planningRequest()
				sides := map[string]*[]schemaext.DefinedType{"desired": &request.Types.Desired, "current": &request.Types.Current}
				*sides[side] = test.types
				result, err := runtime.PlanFeatures(t.Context(), request)
				c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
				c.Assert(result, qt.DeepEquals, featureplan.Result{})
				c.Assert(calls, qt.Equals, 0)
			})
		}
	}
}

func TestDeclarationsTypeContextIsolatesBatches(t *testing.T) {
	c := qt.New(t)
	var received [][]schemaext.DefinedType
	var retained [][]schemaext.DefinedType
	p := declarationProvider(declarationFunc(func(ctx context.Context, request featureplan.DeclarationRequest) (featureplan.DeclarationResult, error) {
		received = append(received, copiedTypeFacts(request.Types))
		retained = append(retained, request.Types)
		request.Types[0].References[0] = "mutated"
		return declaredFixture(ctx, request)
	}))
	registration := p.Declarations[0]
	p.Declarations = nil
	for _, kind := range []schemaext.Kind{conversionFirst, conversionSecond} {
		batch := registration
		batch.Kinds = []schemaext.Kind{kind}
		p.Declarations = append(p.Declarations, batch)
	}
	request := declarationRequest()
	request.Types = capturedTypes().Desired
	result, err := mustRuntime(c, p).PlanDeclarations(t.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Declarations, qt.HasLen, len(request.Objects))
	c.Assert(received, qt.DeepEquals, [][]schemaext.DefinedType{capturedTypes().Desired, capturedTypes().Desired})
	c.Assert(request.Types, qt.DeepEquals, capturedTypes().Desired)
	retained[0][0].References[0] = "first callback only"
	c.Assert(retained[1][0].References[0], qt.Equals, "mutated")
	c.Assert(request.Types, qt.DeepEquals, capturedTypes().Desired)
}

func TestDeclarationsRefusesInvalidTypeContextBeforeDispatch(t *testing.T) {
	for _, test := range invalidPlanningTypes() {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			calls := 0
			p := declarationProvider(declarationFunc(func(ctx context.Context, request featureplan.DeclarationRequest) (featureplan.DeclarationResult, error) {
				calls++
				return declaredFixture(ctx, request)
			}))
			request := declarationRequest()
			request.Types = test.types
			result, err := mustRuntime(c, p).PlanDeclarations(t.Context(), request)
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(result, qt.DeepEquals, featureplan.DeclarationResult{})
			c.Assert(calls, qt.Equals, 0)
		})
	}
}
