package modelast_test

import (
	"context"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/modelast"
)

type failedDeclarationRuntime struct{ failure error }

func (failedDeclarationRuntime) Codecs() schemaext.Registry { return schemaext.Registry{} }

func (r failedDeclarationRuntime) PlanDeclarations(context.Context, featureplan.DeclarationRequest) (featureplan.DeclarationResult, error) {
	return featureplan.DeclarationResult{}, r.failure
}

func TestStandaloneLoweringPreservesServiceFailureWithoutVisiting(t *testing.T) {
	c := qt.New(t)
	failure := errors.New("provider unavailable")
	ref := objectidentity.NewBuilder(identifier.ForDialect("postgres")).SchemaScopedParts(objectidentity.Kind(loweringKind), "public", "owned")
	objects, err := schemaext.NewObjects(schemaext.Object{Ref: ref, Value: &loweringValue{Name: "retained"}})
	c.Assert(err, qt.IsNil)
	database := schemamodel.Database{Schemas: []schemamodel.Schema{{Name: "public"}}, FeatureObjects: objects}
	visited := 0
	err = modelast.WalkDatabase(database, "postgres", func(ast.Node) error { visited++; return nil },
		modelast.Lowering{Context: t.Context(), Runtime: failedDeclarationRuntime{failure: failure}})
	c.Assert(err, qt.ErrorIs, failure)
	var serviceFailure *modelast.DeclarationServiceError
	c.Assert(err, qt.ErrorAs, &serviceFailure)
	c.Assert(visited, qt.Equals, 0)
}

// probeStatement is an owner's statement the walk visits by name.
type probeStatement struct{ Name string }

func (*probeStatement) Kind() schemaext.Kind { return "example.org/lowering-statement" }

func (p *probeStatement) CloneExtension() ast.ExtensionPayload { return &probeStatement{Name: p.Name} }

// handoffRuntime contributes two owners' statements on one subject, neither
// ordered against the other: one reads what the other creates, and the
// reader's name sorts first.
type handoffRuntime struct{}

func (handoffRuntime) Codecs() schemaext.Registry { return schemaext.Registry{} }

func (handoffRuntime) PlanDeclarations(context.Context, featureplan.DeclarationRequest) (featureplan.DeclarationResult, error) {
	subject := objectidentity.NewBuilder(identifier.ForDialect("postgres")).TableParts("public", "shared")
	contribution := func(name string, action plangraph.Action) plangraph.Contribution[featureplan.Operation] {
		id := plangraph.StepID{Owner: "example.org/lowering", Name: name}
		return plangraph.Contribution[featureplan.Operation]{Owner: id.Owner, Steps: []plangraph.Step[featureplan.Operation]{{ID: id,
			Payload: featureplan.Operation{Role: ast.StatementExtension, Payload: &probeStatement{Name: name}},
			Effects: []plangraph.Effect{{Subject: subject, Action: action}}}}}
	}
	return featureplan.DeclarationResult{Complete: true, Contributions: []plangraph.Contribution[featureplan.Operation]{
		contribution("a-read", plangraph.Read), contribution("z-create", plangraph.Create),
	}}, nil
}

// TestStandaloneLoweringOrdersOwnersByTheirEffects writes a statement that
// creates an object before another owner's statement that reads it. Neither
// owner orders its statement against the other's, so the walk derives the
// order from their effects, as every planning host does.
func TestStandaloneLoweringOrdersOwnersByTheirEffects(t *testing.T) {
	c := qt.New(t)
	ref := objectidentity.NewBuilder(identifier.ForDialect("postgres")).SchemaScopedParts(objectidentity.Kind(loweringKind), "public", "owned")
	database := schemamodel.Database{FeatureObjects: must.Must(schemaext.NewObjects(schemaext.Object{Ref: ref, Value: &loweringValue{Name: "owned"}}))}
	var visited []ast.Node
	err := modelast.WalkDatabase(database, "postgres", func(node ast.Node) error {
		visited = append(visited, node)
		return nil
	}, modelast.Lowering{Context: t.Context(), Runtime: handoffRuntime{}})
	c.Assert(err, qt.IsNil)
	c.Assert(visited, qt.DeepEquals, []ast.Node{
		&ast.ExtensionStatement{Payload: &probeStatement{Name: "z-create"}},
		&ast.ExtensionStatement{Payload: &probeStatement{Name: "a-read"}},
	})
}

func TestCommonWalkHonorsCancellationDuringVisitors(t *testing.T) {
	for _, test := range []struct {
		name    string
		schemas []schemamodel.Schema
	}{
		{"between statements", []schemamodel.Schema{{Name: "one"}, {Name: "two"}}},
		{"last statement", []schemamodel.Schema{{Name: "one"}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			visited := 0
			err := modelast.WalkDatabase(schemamodel.Database{Schemas: test.schemas}, "postgres", func(ast.Node) error {
				visited++
				cancel()
				return nil
			}, modelast.Lowering{Context: ctx})
			c.Assert(err, qt.ErrorIs, context.Canceled)
			c.Assert(visited, qt.Equals, 1)
		})
	}
}
