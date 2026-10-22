package providerhost

// White-box testing required: the native CREATE placeholder and captured owner
// dispatch are private adapter boundaries. Subprocess tests cover the transport.

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/difftypes"
	"ptah.run/core/featureplan"
	"ptah.run/core/plangraph"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/engine"
)

type creationOperation struct{ name string }

func (*creationOperation) Kind() schemaext.Kind { return "example.org/fixture/creation" }
func (o *creationOperation) CloneExtension() ast.ExtensionPayload {
	return &creationOperation{name: o.name}
}

type creationRecorder struct {
	*engine.Runtime
	request                 featureplan.Request
	omitRewrite, omitEffect bool
}

func (r *creationRecorder) PlanFeatures(_ context.Context, request featureplan.Request) (featureplan.Result, error) {
	r.request = request
	result := featureplan.Result{Complete: true}
	if r.omitRewrite {
		return result, nil
	}
	contribution := plangraph.Contribution[featureplan.Operation]{Owner: "example.org/fixture"}
	for _, common := range request.CommonSteps {
		if common.Parent.Name.Source == "" {
			continue
		}
		id := plangraph.StepID{Owner: contribution.Owner, Name: common.Parent.Name.Source}
		effects := []plangraph.Effect{{Subject: common.Parent, Action: plangraph.Create}}
		if r.omitEffect {
			effects = nil
		}
		contribution.Steps = append(contribution.Steps, plangraph.Step[featureplan.Operation]{ID: id,
			Payload: featureplan.Operation{Role: ast.StatementExtension, Payload: &creationOperation{name: id.Name}},
			Effects: effects, Transaction: plangraph.TransactionForbidden, Impact: schemaext.Effect{Impact: schemaext.Additive, Reason: "Creates the captured table"}})
		if !r.omitRewrite {
			result.Rewrites = append(result.Rewrites, plangraph.Rewrite{Sources: []plangraph.StepID{common.ID}, Replacement: id})
		}
	}
	result.Contributions = []plangraph.Contribution[featureplan.Operation]{contribution}
	return result, nil
}

func nativeCreationDiff() *difftypes.SchemaDiff {
	rules := identifier.Semantics{TableNames: identifier.ComparisonExact, ColumnNames: identifier.ComparisonExact, IndexNames: identifier.ComparisonExact, IndexNamespace: identifier.IndexNamespaceTable}
	return &difftypes.SchemaDiff{IdentifierSemantics: &rules, TablesAdded: []difftypes.TableCreation{
		{Name: "child", Table: schemamodel.Table{Name: "child", PrimaryKey: []string{"second", "first"}, Comment: "keep the complete declaration"},
			Fields:      []schemamodel.Field{{Name: "first", Type: "Uint64", Comment: "native"}, {Name: "second", Type: "Utf8"}},
			Constraints: []schemamodel.Constraint{{Name: "positive", Type: "CHECK", CheckExpression: "first > 0"}}, DependsOn: []string{"parent"}},
		{Name: "parent", Table: schemamodel.Table{Name: "parent"}, Fields: []schemamodel.Field{{Name: "id", Type: "Uint64"}}},
	}}
}

func TestNativeCreationRewritesRetainCapturedFactsAndOrder(t *testing.T) {
	c := qt.New(t)
	base, err := engine.New()
	c.Assert(err, qt.IsNil)
	runtime := &creationRecorder{Runtime: base}
	diff := nativeCreationDiff()
	nodes, err := (commonPlanner{target: "fixture", assessParents: true}).GenerateMigrationAST(c.Context(), runtime, diff)
	c.Assert(err, qt.IsNil)
	c.Assert(nodes, qt.HasLen, 2)
	c.Assert(nodes[0].(*ast.ExtensionStatement).Payload.(*creationOperation).name, qt.Equals, "parent")
	c.Assert(nodes[1].(*ast.ExtensionStatement).Payload.(*creationOperation).name, qt.Equals, "child")
	captured := runtime.request.Tables[0].Desired
	c.Assert(captured.Table.PrimaryKey, qt.DeepEquals, []string{"second", "first"})
	c.Assert(captured.Table.Comment, qt.Equals, "keep the complete declaration")
	c.Assert(captured.Fields, qt.DeepEquals, diff.TablesAdded[0].Fields)
	c.Assert(captured.Constraints, qt.DeepEquals, diff.TablesAdded[0].Constraints)
	runtime.request.Tables[0].Desired.Fields[0].Type = "mutated"
	c.Assert(diff.TablesAdded[0].Fields[0].Type, qt.Equals, "Uint64")
}

func TestNativeCreationRequiresCompleteReplacement(t *testing.T) {
	for _, test := range []struct {
		name            string
		rewrite, effect bool
		want            error
	}{
		{"unclaimed creation", true, false, ptaherr.ErrUnsupportedFeature},
		{"replacement loses footprint", false, true, plangraph.ErrInvalid},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			base, err := engine.New()
			c.Assert(err, qt.IsNil)
			runtime := &creationRecorder{Runtime: base, omitRewrite: test.rewrite, omitEffect: test.effect}
			nodes, err := (commonPlanner{target: "fixture", assessParents: true}).GenerateMigrationAST(c.Context(), runtime, nativeCreationDiff())
			c.Assert(err, qt.ErrorIs, test.want)
			c.Assert(nodes, qt.IsNil)
		})
	}
}
