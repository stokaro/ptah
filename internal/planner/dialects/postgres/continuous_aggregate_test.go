package postgres_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/internal/planner/dialects/postgres"
	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
)

// TestPlanner_ReplacesAContinuousAggregateInThatOrder pins the shape a changed
// aggregate takes, and the order of the two halves.
//
// There is no CREATE OR REPLACE for one -- measured on TimescaleDB 2.29.2,
// `CREATE OR REPLACE MATERIALIZED VIEW` is `syntax error at or near
// "MATERIALIZED"` -- so a modification is a drop and a create. The order is the
// assertion rather than an incidental detail: the plan runs additions before
// removals, so a modification expressed as one entry in each list would come
// out create-then-drop and end with no aggregate at all.
func TestPlanner_ReplacesAContinuousAggregateInThatOrder(t *testing.T) {
	c := qt.New(t)
	declared := &schemamodel.Database{ContinuousAggregates: []schemamodel.ContinuousAggregate{
		{Name: "hourly", Schema: "public", Body: "SELECT 2"},
	}}

	nodes, err := postgres.New().GenerateMigrationASTChecked(&difftypes.SchemaDiff{
		ContinuousAggregatesModified: []difftypes.ContinuousAggregateDiff{{
			Name: "public.hourly", OldBody: "SELECT 1", NewBody: "SELECT 2",
		}},
	}, declared)

	c.Assert(err, qt.IsNil)
	c.Assert(continuousAggregateVerbs(nodes), qt.DeepEquals, []string{"drop:hourly", "create:hourly"})
}

// TestPlanner_DropsAnUndeclaredContinuousAggregate pins the removal, which is
// the direction a hypertable does not have.
func TestPlanner_DropsAnUndeclaredContinuousAggregate(t *testing.T) {
	c := qt.New(t)

	nodes, err := postgres.New().GenerateMigrationASTChecked(
		&difftypes.SchemaDiff{ContinuousAggregatesRemoved: []string{"public.hourly"}},
		&schemamodel.Database{})

	c.Assert(err, qt.IsNil)
	c.Assert(continuousAggregateVerbs(nodes), qt.DeepEquals, []string{"drop:public.hourly"})
}

// TestPlanner_CreatesAnAggregateAfterTheHypertableItReads pins the ordering
// between the two families.
//
// Measured on 2.29.2, WITH (timescaledb.continuous) over an ordinary table
// answers `invalid continuous aggregate view`, so an aggregate planned before
// the create_hypertable call that partitions the table it reads would fail the
// migration it belongs to.
func TestPlanner_CreatesAnAggregateAfterTheHypertableItReads(t *testing.T) {
	c := qt.New(t)
	declared := &schemamodel.Database{
		Hypertables: []schemamodel.Hypertable{{Table: "readings", Column: "time"}},
		ContinuousAggregates: []schemamodel.ContinuousAggregate{
			{Name: "hourly", Body: "SELECT 1"},
		},
	}

	nodes, err := postgres.New().GenerateMigrationASTChecked(&difftypes.SchemaDiff{
		HypertablesAdded:          []string{"readings"},
		ContinuousAggregatesAdded: []string{"hourly"},
	}, declared)

	c.Assert(err, qt.IsNil)
	c.Assert(timescaleObjectOrder(nodes), qt.DeepEquals, []string{"hypertable:readings", "aggregate:hourly"})
}

// continuousAggregateVerbs names the aggregate statements a plan carries, in
// the order it carries them.
func continuousAggregateVerbs(nodes []ast.Node) []string {
	var verbs []string
	for _, node := range nodes {
		verbs = append(verbs, continuousAggregateVerb(node)...)
	}
	return verbs
}

func continuousAggregateVerb(node ast.Node) []string {
	switch typed := node.(type) {
	case *ast.CreateContinuousAggregateNode:
		return []string{"create:" + typed.Name}
	case *ast.DropContinuousAggregateNode:
		return []string{"drop:" + typed.Name}
	default:
		return nil
	}
}

// timescaleObjectOrder names the two TimescaleDB statements a plan carries, in
// the order it carries them.
func timescaleObjectOrder(nodes []ast.Node) []string {
	var order []string
	for _, node := range nodes {
		order = append(order, timescaleObjectLabel(node)...)
	}
	return order
}

func timescaleObjectLabel(node ast.Node) []string {
	switch typed := node.(type) {
	case *ast.CreateHypertableNode:
		return []string{"hypertable:" + typed.Table}
	case *ast.CreateContinuousAggregateNode:
		return []string{"aggregate:" + typed.Name}
	default:
		return nil
	}
}
