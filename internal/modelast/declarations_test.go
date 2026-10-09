package modelast_test

import (
	"context"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
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
