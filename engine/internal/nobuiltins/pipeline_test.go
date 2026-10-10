// Package nobuiltins_test drives the public schema pipeline through a runtime
// that holds one synthetic provider and nothing from engine/builtin.
//
// The directory holds only this test so the test binary links exactly what the
// test imports. A test beside the engine package would share its binary with
// tests that link the bundled providers, and could not show they are absent.
package nobuiltins_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemapreparation"
	"ptah.run/core/schemaprojection"
	"ptah.run/core/schemavalidation"
	"ptah.run/engine"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

// The target keeps a name the planner package plans for, so the comparison,
// the planner's feature host and the renderer are the shipped ones. Every
// service behind the name is the synthetic provider's.
const target = "postgres"

const (
	levelKind       schemaext.Kind = "example.org/widget/level"
	levelChangeKind schemaext.Kind = "example.org/widget/level-change"
	setLevelKind    schemaext.Kind = "example.org/widget/set-level"
	owner                          = "example.org/widget"
)

// level is a table facet: the widget level a table runs at.
type level struct {
	Value string `json:"value"`
}

func (*level) Kind() schemaext.Kind { return levelKind }
func (v *level) Clone() schemaext.Value {
	cloned := *v
	return &cloned
}
func (v *level) Equal(other schemaext.Value) bool {
	w, ok := other.(*level)
	return ok && w != nil && *v == *w
}

// levelChange is the owner's change between two levels of one table.
type levelChange struct {
	Before string `json:"before"`
	After  string `json:"after"`
}

func (*levelChange) Kind() schemaext.Kind { return levelChangeKind }
func (c *levelChange) CloneChange() schemaext.ChangeValue {
	cloned := *c
	return &cloned
}

// setLevel is the owner's operation that applies a change.
type setLevel struct {
	Table string `json:"table"`
	Level string `json:"level"`
}

func (*setLevel) Kind() schemaext.Kind { return setLevelKind }
func (p *setLevel) CloneExtension() ast.ExtensionPayload {
	cloned := *p
	return &cloned
}

func jsonCodec[P interface {
	schemaext.Payload
	*T
}, T any](representation schemaext.Representation, clone func(P) schemaext.Payload) schemaext.Codec {
	encode := func(payload schemaext.Payload) (json.RawMessage, error) { return json.Marshal(payload) }
	return schemaext.Codec{
		Prototype: P(new(T)), Representation: representation, Version: 1,
		Definition: json.RawMessage(`{"type":"object"}`),
		Clone: func(payload schemaext.Payload) (schemaext.Payload, error) {
			typed, ok := payload.(P)
			if !ok {
				return nil, fmt.Errorf("unexpected payload %T", payload)
			}
			return clone(typed), nil
		},
		Encode: encode, Canonical: encode,
		Decode: func(data json.RawMessage) (schemaext.Payload, error) { return schemaext.DecodeJSON[P](data) },
	}
}

// widget is the synthetic provider's services.
type widget struct {
	extensions renderer.Extensions
}

func (widget) CompareFacets(ctx context.Context, request schemaext.FacetComparisonRequest) (schemaext.FacetComparisonResult, error) {
	if err := ctx.Err(); err != nil {
		return schemaext.FacetComparisonResult{}, err
	}
	result := schemaext.FacetComparisonResult{Complete: true, Desired: request.Desired}
	current := make(map[objectidentity.Key]string)
	for _, record := range request.Current.Records {
		value, found, err := schemaext.FacetAs[*level](record.Values, levelKind)
		if err != nil {
			return schemaext.FacetComparisonResult{}, err
		}
		if found {
			current[record.Subject.Key()] = value.Value
		}
	}
	for _, record := range request.Desired.Records {
		value, found, err := schemaext.FacetAs[*level](record.Values, levelKind)
		if err != nil {
			return schemaext.FacetComparisonResult{}, err
		}
		if !found || current[record.Subject.Key()] == value.Value {
			continue
		}
		result.Changes = append(result.Changes, schemaext.FacetChange{Kind: levelKind, Change: schemaext.ChangeRecord{
			Subject: record.Subject, Value: &levelChange{Before: current[record.Subject.Key()], After: value.Value},
		}})
	}
	return result, nil
}

func (widget) PlanFeatures(ctx context.Context, request featureplan.Request) (featureplan.Result, error) {
	if err := ctx.Err(); err != nil {
		return featureplan.Result{}, err
	}
	result := featureplan.Result{Complete: true}
	for index, record := range request.Changes {
		change, ok := record.Value.(*levelChange)
		if !ok {
			return featureplan.Result{}, fmt.Errorf("%w: unexpected change %T", schemaext.ErrInvalidValue, record.Value)
		}
		id := plangraph.StepID{Owner: owner, Name: fmt.Sprintf("level/%06d", index)}
		// The table is the host's to write. The owner writes its own setting
		// on the table and only reads the table itself.
		setting := record.Subject
		setting.Kind = objectidentity.Kind(levelKind)
		result.Contributions = append(result.Contributions, plangraph.Contribution[featureplan.Operation]{Owner: owner, Steps: []plangraph.Step[featureplan.Operation]{{
			ID:      id,
			Payload: featureplan.Operation{Role: ast.StatementExtension, Payload: &setLevel{Table: record.Subject.Name.Source, Level: change.After}},
			Effects: []plangraph.Effect{{Subject: record.Subject, Action: plangraph.Read}, {Subject: setting, Action: plangraph.Alter}},
			Impact:  schemaext.Effect{Impact: schemaext.Behavioral, Reason: "the table runs at another widget level"},
		}}})
		result.Changes = append(result.Changes, featureplan.ChangePlan{Subject: record.Subject, Kind: levelChangeKind, Strategy: "set the level in place", Steps: []plangraph.StepID{id}})
	}
	return result, nil
}

func (w widget) Render(ctx context.Context, request renderer.Request) (renderer.Result, error) {
	result := renderer.Result{Complete: true}
	for index, node := range request.Nodes {
		if err := ctx.Err(); err != nil {
			return renderer.Result{}, err
		}
		fragment, err := w.render(request.Target, node)
		if err != nil {
			return renderer.Result{}, err
		}
		if fragment == "" {
			// A refused batch carries the refusal and no output.
			return renderer.Result{Complete: true, Diagnostics: []renderer.Diagnostic{{Input: new(index), Problem: schemavalidation.Diagnostic{
				Code: schemavalidation.UnsupportedFeature, Kind: "node", Feature: fmt.Sprintf("%T", node), Message: fmt.Sprintf("the widget target does not render %T", node),
			}}}}, nil
		}
		result.Fragments = append(result.Fragments, fragment+"\n")
	}
	return result, nil
}

func (w widget) render(target string, node ast.Node) (string, error) {
	switch node := node.(type) {
	case *ast.CommentNode:
		return "-- " + node.Text, nil
	case *ast.ExtensionStatement:
		statements, err := w.extensions.Render(renderer.ExtensionContext{Target: target}, ast.StatementExtension, node.Payload)
		return strings.Join(statements, "\n"), err
	case *ast.AlterTableNode:
		var clauses []string
		for _, operation := range node.Operations {
			add, ok := operation.(*ast.AddColumnOperation)
			if !ok {
				return "", nil
			}
			clauses = append(clauses, "ADD COLUMN "+add.Column.Name+" "+add.Column.Type)
		}
		return "ALTER TABLE " + node.Name + " " + strings.Join(clauses, ", ") + ";", nil
	default:
		return "", nil
	}
}

func syntheticRuntime(c *qt.C) *engine.Runtime {
	c.Helper()
	extensions, err := renderer.NewExtensions(renderer.TypedHandler(&setLevel{}, ast.StatementExtension,
		func(_ renderer.ExtensionContext, operation *setLevel) error {
			if operation.Level == "" {
				return fmt.Errorf("%w: a widget level needs a value", ptaherr.ErrInvalidSchemaDiff)
			}
			return nil
		},
		func(_ renderer.ExtensionContext, operation *setLevel) ([]string, error) {
			return []string{"ALTER WIDGET " + operation.Table + " SET LEVEL " + operation.Level + ";"}, nil
		}))
	c.Assert(err, qt.IsNil)
	service := widget{extensions: extensions}
	cloneLevel := func(v *level) schemaext.Payload { return v.Clone() }
	runtime, err := engine.New(engine.Provider{
		ID: owner,
		Codecs: []schemaext.Codec{
			jsonCodec[*level](schemaext.Desired, cloneLevel),
			jsonCodec[*level](schemaext.Observed, cloneLevel),
			jsonCodec[*levelChange](schemaext.Change, func(v *levelChange) schemaext.Payload { return v.CloneChange() }),
			jsonCodec[*setLevel](schemaext.Operation, func(v *setLevel) schemaext.Payload { return v.CloneExtension() }),
		},
		Targets: []engine.Target{{Name: target, Rendering: service, Preparation: schemapreparation.Identity{}, Creations: schemaprojection.IdentityCreations{}}},
		FacetComparisons: []engine.FacetComparison{{Target: target, OwnerKinds: []objectidentity.Kind{objectidentity.KindTable},
			Kinds: []schemaext.Kind{levelKind}, ChangeKinds: []schemaext.Kind{levelChangeKind}, Service: service}},
		Planning: []engine.Planning{{Target: target, Kinds: []schemaext.Kind{levelChangeKind}, OperationKinds: []schemaext.Kind{setLevelKind}, Service: service}},
	})
	c.Assert(err, qt.IsNil)
	return runtime
}

func facets(c *qt.C, value string) schemaext.Facets {
	c.Helper()
	values, err := schemaext.NewFacets(&level{Value: value})
	c.Assert(err, qt.IsNil)
	return values
}

// TestPipeline_ComparesPlansAndRendersThroughASyntheticProvider compares a
// declaration with an observed table, plans the difference and renders it, with
// every feature service the synthetic provider's. The table gains a column,
// which the planner plans and the provider renders, and changes its widget
// level, which the provider compares, plans and renders.
func TestPipeline_ComparesPlansAndRendersThroughASyntheticProvider(t *testing.T) {
	c := qt.New(t)
	runtime := syntheticRuntime(c)
	desired := &schemamodel.Database{Tables: []schemamodel.Table{{Name: "items", StructName: "Items", Facets: facets(c, "high")}},
		Fields: []schemamodel.Field{
			{StructName: "Items", Name: "id", Type: "INTEGER", Nullable: false},
			{StructName: "Items", Name: "label", Type: "TEXT", Nullable: true},
		}}
	current := &catalog.Database{Tables: []catalog.Table{{Name: "items", Facets: facets(c, "low"),
		Columns: []catalog.Column{{Name: "id", DataType: "INTEGER", IsNullable: "NO"}}}}}

	diff, err := schemadiff.CompareWithDialect(t.Context(), desired, current, target, runtime)
	c.Assert(err, qt.IsNil)
	statements, err := planner.GenerateSchemaDiffSQLStatements(t.Context(), runtime, diff, target)
	c.Assert(err, qt.IsNil)

	c.Assert(runtime.Targets(), qt.DeepEquals, []string{target})
	c.Assert(diff.TablesModified, qt.HasLen, 1)
	c.Assert(diff.TablesModified[0].FeatureChanges, qt.HasLen, 1)
	c.Assert(statements, qt.DeepEquals, []string{
		"-- Add/modify columns for table: items\nALTER TABLE items ADD COLUMN label TEXT",
		"ALTER WIDGET items SET LEVEL high",
	})
}

// TestPipeline_TheTestBinaryLinksNoBundledProvider is the control on the claim
// the other test makes: without it, a helper importing engine/builtin would
// supply the bundled providers and the pipeline test could pass on them.
func TestPipeline_TheTestBinaryLinksNoBundledProvider(t *testing.T) {
	c := qt.New(t)
	output, err := exec.CommandContext(t.Context(), "go", "list", "-test", "-deps", "-f", "{{.ImportPath}}", ".").Output()
	c.Assert(err, qt.IsNil)
	packages := strings.Fields(string(output))

	c.Assert(packages, qt.Contains, "ptah.run/engine")
	c.Assert(packages, qt.Contains, "ptah.run/migration/schemadiff")
	c.Assert(packages, qt.Contains, "ptah.run/migration/planner")
	for _, dependency := range packages {
		c.Assert(dependency == "ptah.run/engine/builtin" || strings.HasPrefix(dependency, "ptah.run/engine/builtin/"), qt.IsFalse,
			qt.Commentf("the test binary links %s", dependency))
	}
}
