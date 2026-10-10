package tsplan_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/timescaledb/tsplan"
	"ptah.run/dialect/timescaledb/tsschema"
)

var postgres = identifier.ForDialect("postgres")

func commonStep(name string, kind objectidentity.Kind, relation string) featureplan.CommonStep {
	subject := objectidentity.NewBuilder(postgres).TableParts("", relation)
	subject.Kind = kind
	return featureplan.CommonStep{ID: plangraph.StepID{Owner: "ptah.run/schema-creation", Name: name},
		Effects: []plangraph.Effect{{Subject: subject, Action: plangraph.Create}}}
}

func declaration(steps ...featureplan.CommonStep) featureplan.DeclarationRequest {
	return featureplan.DeclarationRequest{Target: "postgres", Identifiers: postgres, CommonSteps: steps,
		Objects: []schemaext.Object{tsschema.DesiredContinuousAggregateObject("", "hourly", tsschema.DesiredContinuousAggregate{Body: "SELECT 1"})}}
}

// TestPlanDeclarations_CreatesAnAggregateBetweenTablesAndViews pins the
// declaration order: an aggregate reads hypertables, which a render creates
// with their tables, and a view may read an aggregate. So the aggregate
// follows the common steps before the first view and precedes that view; a
// render with no view puts it last.
func TestPlanDeclarations_CreatesAnAggregateBetweenTablesAndViews(t *testing.T) {
	table := commonStep("common/000000", objectidentity.KindTable, "readings")
	view := commonStep("common/000001", objectidentity.KindView, "recent")
	matview := commonStep("common/000002", objectidentity.KindMatView, "daily")
	tests := []struct {
		name   string
		common []featureplan.CommonStep
		want   func(plangraph.StepID) []plangraph.Dependency
	}{
		{name: "tables only", common: []featureplan.CommonStep{table}, want: func(id plangraph.StepID) []plangraph.Dependency {
			return []plangraph.Dependency{{Before: table.ID, After: id}}
		}},
		{name: "a view after the tables", common: []featureplan.CommonStep{table, view, matview}, want: func(id plangraph.StepID) []plangraph.Dependency {
			return []plangraph.Dependency{{Before: table.ID, After: id}, {Before: id, After: view.ID}}
		}},
		{name: "a materialized view first", common: []featureplan.CommonStep{matview, table}, want: func(id plangraph.StepID) []plangraph.Dependency {
			return []plangraph.Dependency{{Before: id, After: matview.ID}}
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result, err := tsplan.Service{}.PlanDeclarations(t.Context(), declaration(test.common...))

			c.Assert(err, qt.IsNil)
			c.Assert(result.Diagnostics, qt.HasLen, 0)
			c.Assert(result.Declarations, qt.HasLen, 1)
			c.Assert(result.Contributions, qt.HasLen, 1)
			c.Assert(result.Contributions[0].Dependencies, qt.DeepEquals, test.want(result.Declarations[0].Steps[0]))
		})
	}
}

// TestPlanDeclarations_RefusesARelationThatTakesItsName pins the collision a
// continuous aggregate has with every relation: it holds its name as one, so a
// document that also creates a table, view or materialized view under it
// cannot apply.
func TestPlanDeclarations_RefusesARelationThatTakesItsName(t *testing.T) {
	tests := []struct {
		name string
		kind objectidentity.Kind
	}{
		{name: "a table", kind: objectidentity.KindTable},
		{name: "a view", kind: objectidentity.KindView},
		{name: "a materialized view", kind: objectidentity.KindMatView},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result, err := tsplan.Service{}.PlanDeclarations(t.Context(), declaration(commonStep("common/000000", test.kind, "hourly")))

			c.Assert(err, qt.IsNil)
			c.Assert(result.Complete, qt.IsTrue)
			c.Assert(result.Contributions, qt.HasLen, 0)
			c.Assert(result.Diagnostics, qt.HasLen, 1)
			c.Assert(result.Diagnostics[0].Problem.Kind, qt.Equals, string(tsschema.ContinuousAggregateKind))
			c.Assert(result.Diagnostics[0].Problem.Message, qt.Matches, `a declared relation takes the name hourly, which a TimescaleDB continuous aggregate holds as a relation.*`)
		})
	}
}

// TestPlan_RefusesAnotherTargetFamily pins that both entry points refuse a
// target outside the PostgreSQL family before planning anything.
func TestPlan_RefusesAnotherTargetFamily(t *testing.T) {
	c := qt.New(t)
	request := declaration()
	request.Target = "mysql"

	declared, declarationErr := tsplan.Service{}.PlanDeclarations(t.Context(), request)
	planned, planErr := tsplan.Service{}.PlanFeatures(t.Context(), featureplan.Request{Target: "mysql"})

	c.Assert(declarationErr, qt.ErrorIs, ptaherr.ErrUnsupportedDialect)
	c.Assert(planErr, qt.ErrorIs, ptaherr.ErrUnsupportedDialect)
	c.Assert(declared.Complete, qt.IsFalse)
	c.Assert(planned.Complete, qt.IsFalse)
}

// TestPlanFeatures_AccountsForAHypertableThroughItsTable pins the receipt a
// table operation gets: a dropped table takes its hypertable with it, and a
// surviving one keeps it.
func TestPlanFeatures_AccountsForAHypertableThroughItsTable(t *testing.T) {
	table := objectidentity.NewBuilder(postgres).TableParts("", "readings")
	tests := []struct {
		name   string
		action featureplan.ParentAction
		want   string
	}{
		{name: "a dropped table", action: featureplan.DropTable, want: "remove any hypertable and its chunks with the table"},
		{name: "a surviving table", action: featureplan.AlterTable, want: "keep any hypertable unless a planned change in this plan replaces it"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result, err := tsplan.Service{}.PlanFeatures(t.Context(), featureplan.Request{Target: "postgres", Identifiers: postgres,
				ParentKinds: []schemaext.Kind{tsschema.HypertableKind},
				Tables:      []featureplan.Table{{Subject: table}, {Subject: table, Action: test.action}}})

			c.Assert(err, qt.IsNil)
			c.Assert(result.Diagnostics, qt.HasLen, 0)
			c.Assert(result.Parents, qt.DeepEquals, []featureplan.ParentPlan{{
				Subject: table, Kind: tsschema.HypertableKind, Action: test.action, Strategy: test.want,
			}})
		})
	}
}

// TestPlanFeatures_RefusesATableRebuildOfAHypertable is the other side: a
// rebuild recreates the table as an ordinary one, so it is refused against
// the table that asked, and a parent model other than the hypertable is not
// TimescaleDB's to assess.
func TestPlanFeatures_RefusesATableRebuildOfAHypertable(t *testing.T) {
	c := qt.New(t)
	table := objectidentity.NewBuilder(postgres).TableParts("", "readings")

	result, err := tsplan.Service{}.PlanFeatures(t.Context(), featureplan.Request{Target: "postgres", Identifiers: postgres,
		ParentKinds: []schemaext.Kind{tsschema.HypertableKind},
		Tables:      []featureplan.Table{{Subject: table, Action: featureplan.RebuildTable}}})

	c.Assert(err, qt.IsNil)
	c.Assert(result.Parents, qt.HasLen, 0)
	c.Assert(result.Diagnostics, qt.HasLen, 1)
	c.Assert(result.Diagnostics[0].Parent, qt.DeepEquals, new(0))
	c.Assert(result.Diagnostics[0].Problem.Message, qt.Equals, `TimescaleDB has no plan for a hypertable through parent action "rebuild-table"`)

	_, err = tsplan.Service{}.PlanFeatures(t.Context(), featureplan.Request{Target: "postgres", Identifiers: postgres,
		ParentKinds: []schemaext.Kind{tsschema.ContinuousAggregateKind}})
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
}
