package engine_test

import (
	"context"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/featureplan"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemavalidation"
	"ptah.run/engine"
)

func declarationRefusal(_ context.Context, request featureplan.DeclarationRequest) (featureplan.DeclarationResult, error) {
	return featureplan.DeclarationResult{Complete: true, Diagnostics: []featureplan.DeclarationDiagnostic{{Object: new(0),
		Problem: schemavalidation.Diagnostic{Code: schemavalidation.InvalidSchema, Kind: string(request.Objects[0].Value.Kind()), Message: "creation refused"}}}}, nil
}

func TestDeclarationsAggregateRefusalsWithoutSuccessfulPrefixes(t *testing.T) {
	c := qt.New(t)
	p := declarationProvider(declarationFunc(declarationRefusal))
	p.Declarations[0].Kinds = []schemaext.Kind{conversionFirst}
	p.Declarations = append(p.Declarations, engine.DeclarationPlanning{Target: "custom", Kinds: []schemaext.Kind{conversionSecond},
		OperationKinds: []schemaext.Kind{planningOperationKind}, Service: declarationFunc(declaredFixture)})
	request := declarationRequest()
	result, err := mustRuntime(c, p).PlanDeclarations(t.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Complete, qt.IsTrue)
	c.Assert(result.Diagnostics, qt.HasLen, 1)
	c.Assert(*result.Diagnostics[0].Object, qt.Equals, 1)
	c.Assert(result.Contributions, qt.HasLen, 0)
	c.Assert(result.Declarations, qt.HasLen, 0)
	c.Assert(result.Err(request), qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
}

func TestDeclarationsRejectMalformedRefusals(t *testing.T) {
	for _, test := range []struct {
		name string
		edit func(*featureplan.DeclarationResult)
	}{
		{"incomplete refusal", func(r *featureplan.DeclarationResult) { r.Complete = false }},
		{"negative index", func(r *featureplan.DeclarationResult) { r.Diagnostics[0].Object = new(-1) }},
		{"past end", func(r *featureplan.DeclarationResult) { r.Diagnostics[0].Object = new(3) }},
		{"changed input kind", func(r *featureplan.DeclarationResult) { r.Diagnostics[0].Problem.Kind = string(conversionFirst) }},
		{"empty message", func(r *featureplan.DeclarationResult) { r.Diagnostics[0].Problem.Message = "" }},
		{"unknown code", func(r *featureplan.DeclarationResult) { r.Diagnostics[0].Problem.Code = "invented" }},
		{"successful prefix", func(r *featureplan.DeclarationResult) {
			r.Declarations = []featureplan.DeclarationPlan{{Strategy: "partial"}}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			p := declarationProvider(declarationFunc(func(ctx context.Context, request featureplan.DeclarationRequest) (featureplan.DeclarationResult, error) {
				reply, err := declarationRefusal(ctx, request)
				test.edit(&reply)
				return reply, err
			}))
			result, err := mustRuntime(c, p).PlanDeclarations(t.Context(), declarationRequest())
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(result, qt.DeepEquals, featureplan.DeclarationResult{})
		})
	}
}

func TestDeclarationsDiscardFailureAndCancellation(t *testing.T) {
	failure := errors.New("provider failed")
	for _, test := range []struct {
		name string
		err  error
		stop func(context.CancelFunc)
		want error
	}{
		{"failure with output", failure, func(context.CancelFunc) {}, failure},
		{"canceled after output", nil, func(cancel context.CancelFunc) { cancel() }, context.Canceled},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			p := declarationProvider(declarationFunc(func(ctx context.Context, request featureplan.DeclarationRequest) (featureplan.DeclarationResult, error) {
				reply, err := declaredFixture(ctx, request)
				c.Assert(err, qt.IsNil)
				test.stop(cancel)
				return reply, test.err
			}))
			result, err := mustRuntime(c, p).PlanDeclarations(ctx, declarationRequest())
			c.Assert(err, qt.ErrorIs, test.want)
			c.Assert(result, qt.DeepEquals, featureplan.DeclarationResult{})
		})
	}
}
