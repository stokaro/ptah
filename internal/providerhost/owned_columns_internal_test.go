package providerhost

// White-box testing required: the host's column placeholder and the captured
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
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/engine"
)

// columnClaimer plays a provider that owns the table lifecycle. It replaces
// every common column step with an owned operation, unless left unclaimed.
type columnClaimer struct {
	*engine.Runtime
	request featureplan.Request
	leave   map[string]bool
}

func (r *columnClaimer) PlanFeatures(_ context.Context, request featureplan.Request) (featureplan.Result, error) {
	r.request = request
	result := featureplan.Result{Complete: true}
	contribution := plangraph.Contribution[featureplan.Operation]{Owner: "example.org/fixture"}
	for _, common := range request.CommonSteps {
		if common.Parent.Name.Source == "" || len(common.Effects) != 2 || r.leave[common.Effects[1].Subject.Name.Source] {
			continue
		}
		id := plangraph.StepID{Owner: contribution.Owner, Name: common.ID.Name}
		contribution.Steps = append(contribution.Steps, plangraph.Step[featureplan.Operation]{ID: id,
			Payload:     featureplan.Operation{Role: ast.StatementExtension, Payload: &creationOperation{name: id.Name}},
			Effects:     common.Effects,
			Transaction: plangraph.TransactionForbidden, Impact: schemaext.Effect{Impact: schemaext.Additive, Reason: "Changes a column"}})
		result.Rewrites = append(result.Rewrites, plangraph.Rewrite{Sources: []plangraph.StepID{common.ID}, Replacement: id})
	}
	result.Contributions = []plangraph.Contribution[featureplan.Operation]{contribution}
	return result, nil
}

func ownedColumnDiff(c *qt.C, added, removed []schemamodel.Field) *difftypes.SchemaDiff {
	rules := identifier.Semantics{DefaultSchema: "main", TableNames: identifier.ComparisonExact, ColumnNames: identifier.ComparisonExact, IndexNames: identifier.ComparisonExact, IndexNamespace: identifier.IndexNamespaceTable}
	table := schemamodel.Table{Name: "records", Schema: "main"}
	return &difftypes.SchemaDiff{IdentifierSemantics: &rules, TablesModified: []difftypes.TableDiff{{
		TableName:      `main.records`,
		Desired:        schemacapture.TableDeclaration{Table: table, Fields: added},
		Current:        schemacapture.TableObservation{Table: catalog.Table{Name: "records", Schema: "main"}},
		ColumnsAdded:   added,
		ColumnsRemoved: removed,
	}}}
}

func ownedPlanner() commonPlanner {
	return commonPlanner{target: "fixture", assessParents: true}
}

func TestOwnedLifecycleAlteredTableReachesOwner_HappyPath(t *testing.T) {
	c := qt.New(t)
	base, err := engine.New()
	c.Assert(err, qt.IsNil)
	runtime := &columnClaimer{Runtime: base}
	facets := planningExcludedFacet(c)
	rich := schemamodel.Field{Name: "added", Type: "TEXT", Default: "x", DefaultSet: true, Comment: "kept", Collate: "NOCASE", Facets: facets}
	diff := ownedColumnDiff(c, []schemamodel.Field{rich}, []schemamodel.Field{{Name: "gone", Type: "TEXT", Nullable: true}})
	nodes, err := ownedPlanner().GenerateMigrationAST(c.Context(), runtime, diff)
	c.Assert(err, qt.IsNil)
	c.Assert(nodes, qt.HasLen, 2)
	for _, node := range nodes {
		c.Assert(node, qt.Satisfies, func(n ast.Node) bool { _, ok := n.(*ast.ExtensionStatement); return ok })
	}

	// The altered table reaches the owner with both captured states.
	c.Assert(runtime.request.Tables, qt.HasLen, 1)
	table := runtime.request.Tables[0]
	c.Assert(table.Action, qt.Equals, featureplan.AlterTable)
	c.Assert(table.Desired.Table.Name, qt.Equals, "records")
	c.Assert(table.Current.Table.Name, qt.Equals, "records")

	builder := objectidentity.NewBuilder(runtime.request.Identifiers)
	steps := runtime.request.CommonSteps
	addedIndex := slices.IndexFunc(steps, func(step featureplan.CommonStep) bool { return step.AddedColumn != nil })
	droppedIndex := slices.IndexFunc(steps, func(step featureplan.CommonStep) bool {
		return len(step.Effects) == 2 && step.Effects[1].Action == plangraph.Drop
	})
	c.Assert(addedIndex, qt.Not(qt.Equals), -1)
	c.Assert(droppedIndex, qt.Not(qt.Equals), -1)
	added, dropped := &steps[addedIndex], &steps[droppedIndex]
	c.Assert(added.Parent, qt.DeepEquals, table.Subject)
	c.Assert(dropped.Parent, qt.DeepEquals, table.Subject)

	// A column step reads its table and creates or drops one column of it.
	column := func(name string) objectidentity.ID {
		return planningChild(table.Subject, builder.ColumnParts("main", "records", name))
	}
	c.Assert(added.Effects, qt.DeepEquals, []plangraph.Effect{{Subject: table.Subject, Action: plangraph.Read}, {Subject: column("added"), Action: plangraph.Create}})
	c.Assert(dropped.Effects, qt.DeepEquals, []plangraph.Effect{{Subject: table.Subject, Action: plangraph.Read}, {Subject: column("gone"), Action: plangraph.Drop}})

	// The operand keeps what the common-table whitelist would refuse.
	c.Assert(added.AddedColumn.Default, qt.IsNotNil)
	c.Assert(added.AddedColumn.Default.Value, qt.Equals, "x")
	c.Assert(added.AddedColumn.Comment, qt.Equals, "kept")
	c.Assert(added.AddedColumn.Collate, qt.Equals, "NOCASE")
	c.Assert(added.AddedColumn.Facets.TargetScope("example.org/fixture/value"), qt.DeepEquals, []string{"other"})
}

func TestOwnedLifecycleAlteredTableKeepsParentAction_HappyPath(t *testing.T) {
	c := qt.New(t)
	diff := ownedColumnDiff(c, []schemamodel.Field{{Name: "added", Type: "TEXT"}}, nil)
	request, _, err := ownedPlanner().planningContext(diff)
	c.Assert(err, qt.IsNil)
	c.Assert(request.Tables, qt.HasLen, 1)
	c.Assert(request.Tables[0].Action, qt.Equals, featureplan.AlterTable)
}

func TestOwnedLifecycleUnclaimedColumnChange_FailurePath(t *testing.T) {
	for _, row := range []struct {
		name         string
		added, drops []schemamodel.Field
		leave        string
		want         string
	}{
		{"addition", []schemamodel.Field{{Name: "added", Type: "TEXT"}}, nil, "added",
			`.*unclaimed provider addition of column added to table main\.records.*`},
		{"removal", nil, []schemamodel.Field{{Name: "gone", Type: "TEXT"}}, "gone",
			`.*unclaimed provider removal of column gone from table main\.records.*`},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			base, err := engine.New()
			c.Assert(err, qt.IsNil)
			runtime := &columnClaimer{Runtime: base, leave: map[string]bool{row.leave: true}}
			nodes, err := ownedPlanner().GenerateMigrationAST(c.Context(), runtime, ownedColumnDiff(c, row.added, row.drops))
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, row.want)
			c.Assert(nodes, qt.IsNil)
		})
	}
}

func TestOwnedLifecycleKeepsRefusals_FailurePath(t *testing.T) {
	modified := func(mutate func(*difftypes.TableDiff)) *difftypes.SchemaDiff {
		diff := ownedColumnDiff(qt.New(t), []schemamodel.Field{{Name: "added", Type: "TEXT"}}, nil)
		mutate(&diff.TablesModified[0])
		return diff
	}
	for _, row := range []struct {
		name string
		diff *difftypes.SchemaDiff
		want string
	}{
		{"modified column", modified(func(d *difftypes.TableDiff) {
			d.ColumnsModified = []difftypes.ColumnDiff{{ColumnName: "added"}}
		}), `.*TableDiff\.ColumnsModified.*`},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			base, err := engine.New()
			c.Assert(err, qt.IsNil)
			nodes, err := ownedPlanner().GenerateMigrationAST(c.Context(), &columnClaimer{Runtime: base}, row.diff)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, "(?s)"+row.want)
			c.Assert(nodes, qt.IsNil)
		})
	}
}

func TestOwnedLifecycleWithoutParentKindsRefusesAlteredTable_FailurePath(t *testing.T) {
	c := qt.New(t)
	nodes, err := (commonPlanner{target: "fixture"}).GenerateMigrationAST(c.Context(), nil, ownedColumnDiff(c, []schemamodel.Field{{Name: "added", Type: "TEXT"}}, nil))
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(nodes, qt.IsNil)
}
