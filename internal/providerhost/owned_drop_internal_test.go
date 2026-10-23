package providerhost

// White-box testing required: the host's removal placeholder and the captured
// planning request are private adapter state. The subprocess tests in
// integration/providercli cover the transport.

import (
	"context"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/core/difftypes"
	"ptah.run/core/featureplan"
	"ptah.run/core/plangraph"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemaext"
	"ptah.run/engine"
)

// dropClaimer plays a provider that owns the table lifecycle. It replaces
// every common table removal with an owned operation, unless left unclaimed.
type dropClaimer struct {
	*engine.Runtime
	request featureplan.Request
	leave   bool
}

func (r *dropClaimer) PlanFeatures(_ context.Context, request featureplan.Request) (featureplan.Result, error) {
	r.request = request
	result := featureplan.Result{Complete: true}
	contribution := plangraph.Contribution[featureplan.Operation]{Owner: "example.org/fixture"}
	for _, common := range request.CommonSteps {
		if r.leave || len(common.Effects) != 1 || common.Effects[0].Action != plangraph.Drop {
			continue
		}
		id := plangraph.StepID{Owner: contribution.Owner, Name: common.ID.Name}
		contribution.Steps = append(contribution.Steps, plangraph.Step[featureplan.Operation]{ID: id,
			Payload:     featureplan.Operation{Role: ast.StatementExtension, Payload: &creationOperation{name: id.Name}},
			Effects:     common.Effects,
			Transaction: plangraph.TransactionForbidden, Impact: schemaext.Effect{Impact: schemaext.Destructive, Reason: "Drops a table"}})
		result.Rewrites = append(result.Rewrites, plangraph.Rewrite{Sources: []plangraph.StepID{common.ID}, Replacement: id})
	}
	result.Contributions = []plangraph.Contribution[featureplan.Operation]{contribution}
	return result, nil
}

func ownedDropDiff() *difftypes.SchemaDiff {
	rules := identifier.Semantics{DefaultSchema: "main", TableNames: identifier.ComparisonExact, ColumnNames: identifier.ComparisonExact, IndexNames: identifier.ComparisonExact, IndexNamespace: identifier.IndexNamespaceTable}
	return &difftypes.SchemaDiff{IdentifierSemantics: &rules, TablesRemoved: difftypes.TableRemovals{{
		Name:    `main.records`,
		Current: schemacapture.TableObservation{Table: catalog.Table{Name: "records", Schema: "main"}},
	}}}
}

func TestOwnedLifecycleRemovedTableReachesOwner_HappyPath(t *testing.T) {
	c := qt.New(t)
	base, err := engine.New()
	c.Assert(err, qt.IsNil)
	runtime := &dropClaimer{Runtime: base}
	nodes, err := ownedPlanner().GenerateMigrationAST(c.Context(), runtime, ownedDropDiff())
	c.Assert(err, qt.IsNil)
	c.Assert(nodes, qt.HasLen, 1)
	c.Assert(nodes[0], qt.Satisfies, func(n ast.Node) bool { _, ok := n.(*ast.ExtensionStatement); return ok })

	// The owner receives the removed table as captured, and one drop step on it.
	c.Assert(runtime.request.Tables, qt.HasLen, 1)
	table := runtime.request.Tables[0]
	c.Assert(table.Action, qt.Equals, featureplan.DropTable)
	c.Assert(table.Current.Table.Name, qt.Equals, "records")
	steps := runtime.request.CommonSteps
	dropIndex := slices.IndexFunc(steps, func(step featureplan.CommonStep) bool {
		return len(step.Effects) == 1 && step.Effects[0].Action == plangraph.Drop
	})
	c.Assert(dropIndex, qt.Not(qt.Equals), -1)
	c.Assert(steps[dropIndex].Parent, qt.DeepEquals, table.Subject)
	c.Assert(steps[dropIndex].Effects, qt.DeepEquals, []plangraph.Effect{{Subject: table.Subject, Action: plangraph.Drop}})
}

func TestOwnedLifecycleUnclaimedTableRemoval_FailurePath(t *testing.T) {
	c := qt.New(t)
	base, err := engine.New()
	c.Assert(err, qt.IsNil)
	nodes, err := ownedPlanner().GenerateMigrationAST(c.Context(), &dropClaimer{Runtime: base, leave: true}, ownedDropDiff())
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, `.*unclaimed provider table removal main\.records.*`)
	c.Assert(nodes, qt.IsNil)
}

func TestOwnedLifecycleWithoutParentKindsRefusesRemovedTable_FailurePath(t *testing.T) {
	c := qt.New(t)
	nodes, err := (commonPlanner{target: "fixture"}).GenerateMigrationAST(c.Context(), nil, ownedDropDiff())
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(nodes, qt.IsNil)
}

func TestOwnedLifecycleRemovedTableWithoutCapture_FailurePath(t *testing.T) {
	c := qt.New(t)
	base, err := engine.New()
	c.Assert(err, qt.IsNil)
	diff := ownedDropDiff()
	diff.TablesRemoved[0].Current = schemacapture.TableObservation{}
	nodes, err := ownedPlanner().GenerateMigrationAST(c.Context(), &dropClaimer{Runtime: base}, diff)
	c.Assert(err, qt.ErrorMatches, `provider planner requires captured table state`)
	c.Assert(nodes, qt.IsNil)
}
