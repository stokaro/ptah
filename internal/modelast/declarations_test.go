package modelast_test

import (
	"context"
	"errors"
	"fmt"
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

func (failedDeclarationRuntime) Codecs() schemaext.Registry               { return schemaext.Registry{} }
func (failedDeclarationRuntime) DeclaresKind(string, schemaext.Kind) bool { return false }

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

func (handoffRuntime) Codecs() schemaext.Registry               { return schemaext.Registry{} }
func (handoffRuntime) DeclaresKind(string, schemaext.Kind) bool { return false }

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

// phasedDeclarationRuntime plans one standalone statement in phase.
type phasedDeclarationRuntime struct{ phase featureplan.Phase }

func (phasedDeclarationRuntime) Codecs() schemaext.Registry               { return schemaext.Registry{} }
func (phasedDeclarationRuntime) DeclaresKind(string, schemaext.Kind) bool { return false }

func (r phasedDeclarationRuntime) PlanDeclarations(context.Context, featureplan.DeclarationRequest) (featureplan.DeclarationResult, error) {
	step := plangraph.Step[featureplan.Operation]{ID: plangraph.StepID{Owner: "example.org/lowering", Name: "late"},
		Payload: featureplan.Operation{Role: ast.StatementExtension, Payload: &phasedStatement{}, Phase: r.phase}}
	return featureplan.DeclarationResult{Complete: true, Contributions: []plangraph.Contribution[featureplan.Operation]{
		{Owner: step.ID.Owner, Steps: []plangraph.Step[featureplan.Operation]{step}},
	}}, nil
}

type phasedStatement struct{}

func (*phasedStatement) Kind() schemaext.Kind                 { return "example.org/lowering-statement" }
func (*phasedStatement) CloneExtension() ast.ExtensionPayload { return &phasedStatement{} }

// TestStandaloneLoweringRefusesAPhase pins that a whole-schema render refuses
// an operation that asks for a phase it has no window for: it orders such a
// step by its dependencies alone, so ignoring the phase would place the
// statement where its owner did not ask for it.
func TestStandaloneLoweringRefusesAPhase(t *testing.T) {
	c := qt.New(t)
	ref := objectidentity.NewBuilder(identifier.ForDialect("postgres")).SchemaScopedParts(objectidentity.Kind(loweringKind), "public", "owned")
	objects, err := schemaext.NewObjects(schemaext.Object{Ref: ref, Value: &loweringValue{Name: "retained"}})
	c.Assert(err, qt.IsNil)
	database := schemamodel.Database{Schemas: []schemamodel.Schema{{Name: "public"}}, FeatureObjects: objects}
	visited := 0

	err = modelast.WalkDatabase(database, "postgres", func(ast.Node) error { visited++; return nil },
		modelast.Lowering{Context: t.Context(), Runtime: phasedDeclarationRuntime{phase: featureplan.PhaseDependent}})

	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(err, qt.ErrorMatches, `.*declaration step example.org/lowering/late asks for the "dependent" phase.*`)
	c.Assert(visited, qt.Equals, 0)
}

// renderedKinds names each statement a walk visited: a schema by its name, a
// table, or an owner's statement.
func renderedKinds(nodes []ast.Node) []string {
	kinds := make([]string, 0, len(nodes))
	for _, node := range nodes {
		switch typed := node.(type) {
		case *ast.CreateSchemaNode:
			kinds = append(kinds, "schema "+typed.Name)
		case *ast.CreateTableNode:
			kinds = append(kinds, "table")
		case *ast.ExtensionStatement:
			kinds = append(kinds, "owner")
		default:
			kinds = append(kinds, fmt.Sprintf("%T", node))
		}
	}
	return kinds
}

// TestStandaloneLoweringPlacesAPrerequisite pins the one phase a whole-schema
// render has a window for: an object every other object may use, such as an
// extension, is created after the schemas and before everything else, where
// its dependencies alone would place it after the common statements.
func TestStandaloneLoweringPlacesAPrerequisite(t *testing.T) {
	tests := []struct {
		name    string
		phase   featureplan.Phase
		schemas []schemamodel.Schema
		want    []string
	}{
		{name: "after the schemas", phase: featureplan.PhasePrerequisite, schemas: []schemamodel.Schema{{Name: "app"}, {Name: "ext"}},
			want: []string{"schema app", "schema ext", "owner", "table"}},
		{name: "first, when the render creates no schema", phase: featureplan.PhasePrerequisite, want: []string{"owner", "table"}},
		{name: "control: a step without a phase follows its dependencies alone", phase: featureplan.PhaseDefault,
			schemas: []schemamodel.Schema{{Name: "app"}, {Name: "ext"}}, want: []string{"owner", "schema app", "schema ext", "table"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ref := objectidentity.NewBuilder(identifier.ForDialect("postgres")).SchemaScopedParts(objectidentity.Kind(loweringKind), "", "owned")
			database := schemamodel.Database{Schemas: test.schemas,
				Tables:         []schemamodel.Table{{StructName: "Order", Name: "orders"}},
				Fields:         []schemamodel.Field{{StructName: "Order", Name: "id", Type: "INTEGER", Primary: true}},
				FeatureObjects: must.Must(schemaext.NewObjects(schemaext.Object{Ref: ref, Value: &loweringValue{Name: "owned"}}))}
			var visited []ast.Node

			err := modelast.WalkDatabase(database, "postgres", func(node ast.Node) error {
				visited = append(visited, node)
				return nil
			}, modelast.Lowering{Context: t.Context(), Runtime: phasedDeclarationRuntime{phase: test.phase}})

			c.Assert(err, qt.IsNil)
			c.Assert(renderedKinds(visited), qt.DeepEquals, test.want)
		})
	}
}

// TestStandaloneLoweringCreatesTheSchemaAnObjectIsPlacedIn pins that a render
// creates the schema an owner's object names, as it creates the schema an
// extension names, and creates none for the target's default schema.
func TestStandaloneLoweringCreatesTheSchemaAnObjectIsPlacedIn(t *testing.T) {
	tests := []struct {
		name   string
		schema string
		want   []string
	}{
		{name: "a named schema", schema: "ext", want: []string{"schema ext", "owner", "table"}},
		{name: "the default schema", schema: "public", want: []string{"owner", "table"}},
		{name: "no schema", want: []string{"owner", "table"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ref := objectidentity.NewBuilder(identifier.ForDialect("postgres")).SchemaScopedParts(objectidentity.Kind(loweringKind), "", "owned")
			database := schemamodel.Database{
				Tables:         []schemamodel.Table{{StructName: "Order", Name: "orders"}},
				Fields:         []schemamodel.Field{{StructName: "Order", Name: "id", Type: "INTEGER", Primary: true}},
				FeatureObjects: must.Must(schemaext.NewObjects(schemaext.Object{Ref: ref, Value: &loweringValue{Name: "owned", Schema: test.schema}}))}
			var visited []ast.Node

			err := modelast.WalkDatabase(database, "postgres", func(node ast.Node) error {
				visited = append(visited, node)
				return nil
			}, modelast.Lowering{Context: t.Context(), Runtime: phasedDeclarationRuntime{phase: featureplan.PhasePrerequisite}})

			c.Assert(err, qt.IsNil)
			c.Assert(renderedKinds(visited), qt.DeepEquals, test.want)
		})
	}
}

// childRuntime plans each declaration as one statement after every common
// step, and declares the child kind when declares is set.
type childRuntime struct{ declares bool }

func (childRuntime) Codecs() schemaext.Registry { return schemaext.Registry{} }
func (r childRuntime) DeclaresKind(_ string, kind schemaext.Kind) bool {
	return r.declares && kind == loweringKind
}

func (childRuntime) PlanDeclarations(_ context.Context, request featureplan.DeclarationRequest) (featureplan.DeclarationResult, error) {
	contribution := plangraph.Contribution[featureplan.Operation]{Owner: "example.org/lowering"}
	result := featureplan.DeclarationResult{Complete: true}
	for _, object := range request.Objects {
		id := plangraph.StepID{Owner: contribution.Owner, Name: object.Ref.Name.Source}
		contribution.Steps = append(contribution.Steps, plangraph.Step[featureplan.Operation]{ID: id,
			Payload: featureplan.Operation{Role: ast.StatementExtension, Payload: &probeStatement{Name: object.Ref.Name.Source}}})
		for _, common := range request.CommonSteps {
			contribution.Dependencies = append(contribution.Dependencies, plangraph.Dependency{Before: common.ID, After: id})
		}
		result.Declarations = append(result.Declarations, featureplan.DeclarationPlan{Subject: object.Ref, Strategy: "create the child", Steps: []plangraph.StepID{id}})
	}
	result.Contributions = []plangraph.Contribution[featureplan.Operation]{contribution}
	return result, nil
}

// visitedShape names the statements a walk visits: a table with the number of
// children its CREATE TABLE carries, and an owner's statement by its name.
func visitedShape(nodes []ast.Node) []string {
	var shape []string
	for _, node := range nodes {
		switch typed := node.(type) {
		case *ast.CreateTableNode:
			shape = append(shape, fmt.Sprintf("create table %s carrying %d", typed.Name, typed.OwnedObjects.Len()))
		case *ast.ExtensionStatement:
			shape = append(shape, "statement "+typed.Payload.(*probeStatement).Name)
		}
	}
	return shape
}

// TestTableChildLowering pins where a whole-schema render puts a table's named
// child. A child whose kind the runtime declares is its owner's to create: it
// is planned as a declaration, scheduled after the common steps it follows,
// and its table's CREATE TABLE does not carry it. Any other child is created
// with its table.
func TestTableChildLowering(t *testing.T) {
	tests := []struct {
		name     string
		declares bool
		want     []string
	}{
		{name: "a declared child", declares: true, want: []string{"create table public.orders carrying 0", "statement tenant"}},
		{name: "any other child", want: []string{"create table public.orders carrying 1"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			table := objectidentity.NewBuilder(identifier.ForDialect("postgres")).TableParts("public", "orders")
			child := objectidentity.ID{Kind: objectidentity.Kind(loweringKind), Schema: table.Schema, Parent: table.Name,
				Name: objectidentity.Part{Source: "tenant", Normalized: "tenant"}}
			database := schemamodel.Database{
				Tables:         []schemamodel.Table{{StructName: "Order", Name: "orders", Schema: "public"}},
				Fields:         []schemamodel.Field{{StructName: "Order", Name: "id", Type: "INTEGER", Primary: true}},
				FeatureObjects: must.Must(schemaext.NewObjects(schemaext.Object{Ref: child, Value: &loweringValue{Name: "tenant"}})),
			}
			var visited []ast.Node

			err := modelast.WalkDatabase(database, "postgres", func(node ast.Node) error {
				visited = append(visited, node)
				return nil
			}, modelast.Lowering{Context: t.Context(), Runtime: childRuntime{declares: test.declares}})

			c.Assert(err, qt.IsNil)
			c.Assert(visitedShape(visited), qt.DeepEquals, test.want)
		})
	}
}
