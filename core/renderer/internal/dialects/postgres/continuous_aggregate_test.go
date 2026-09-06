package postgres_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/renderer/internal/dialects/postgres"
)

// TestVisitCreateContinuousAggregate_WritesWhatTheExtensionOwns pins the shape
// of the statement.
//
// Every part below is measured on TimescaleDB 2.29.2 / PostgreSQL 17.11:
//
//	CREATE MATERIALIZED VIEW … WITH (timescaledb.continuous) AS … WITH NO DATA
//	                                                       -> CREATE MATERIALIZED VIEW
//	the same without WITH NO DATA, inside a transaction
//	     -> ERROR: CREATE MATERIALIZED VIEW ... WITH DATA cannot run inside a transaction block
//	timescaledb.materialized_only = true                   -> the catalog reports materialized_only t
func TestVisitCreateContinuousAggregate_WritesWhatTheExtensionOwns(t *testing.T) {
	tests := []struct {
		name string
		node *ast.CreateContinuousAggregateNode
		want []string
	}{
		{
			name: "the default",
			node: ast.NewCreateContinuousAggregate("hourly", "SELECT 1"),
			want: []string{
				`CREATE MATERIALIZED VIEW "hourly" WITH (timescaledb.continuous) AS`,
				"SELECT 1",
				"WITH NO DATA",
			},
		},
		{
			// The body a description read from a server carries: the catalog's
			// `view_definition` ends in a semicolon, and writing it in would
			// put the terminator BEFORE `WITH NO DATA`. The server answers
			// `syntax error at or near "WITH"`, so a document Ptah inspected
			// could not be applied by Ptah.
			name: "a body carrying the catalog's terminator",
			node: ast.NewCreateContinuousAggregate("hourly", "SELECT 1;"),
			want: []string{"SELECT 1\nWITH NO DATA"},
		},
		{
			name: "in a schema, materialized only",
			node: ast.NewCreateContinuousAggregate("hourly", "SELECT 1").
				SetSchema("metrics").SetMaterializedOnly(new(true)),
			want: []string{
				`CREATE MATERIALIZED VIEW "metrics"."hourly" ` +
					`WITH (timescaledb.continuous, timescaledb.materialized_only = true) AS`,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			renderer := postgres.NewWithCapabilities(
				capability.Postgres17().With(capability.ContinuousAggregates, true), platform.Postgres)

			out, err := renderer.Render(test.node)

			c.Assert(err, qt.IsNil)
			for _, want := range test.want {
				c.Assert(out, qt.Contains, want)
			}
		})
	}
}

// TestVisitDropContinuousAggregate_TakesTheVerbTheServerNames pins the removal.
//
// DROP VIEW is refused outright: `cannot drop continuous aggregate using DROP
// VIEW. HINT: Use DROP MATERIALIZED VIEW to drop a continuous aggregate.` A
// plan that emitted it could never apply, and every run would report the same
// pending change.
func TestVisitDropContinuousAggregate_TakesTheVerbTheServerNames(t *testing.T) {
	c := qt.New(t)
	renderer := postgres.NewWithCapabilities(
		capability.Postgres17().With(capability.ContinuousAggregates, true), platform.Postgres)

	out, err := renderer.Render(ast.NewDropContinuousAggregate("hourly").SetIfExists())

	c.Assert(err, qt.IsNil)
	c.Assert(out, qt.Contains, `DROP MATERIALIZED VIEW IF EXISTS "hourly"`)
	c.Assert(out, qt.Not(qt.Contains), "DROP VIEW")
}

// TestVisitContinuousAggregate_SkipsWhereTheExtensionIsAbsent is the other half
// of the capability.
//
// A PostgreSQL target without TimescaleDB has no `timescaledb.continuous`
// option, and a plan carrying one would fail at apply time on `unrecognized
// parameter "timescaledb.continuous"` instead of saying so in the plan an
// operator reviews.
func TestVisitContinuousAggregate_SkipsWhereTheExtensionIsAbsent(t *testing.T) {
	c := qt.New(t)
	renderer := postgres.NewWithCapabilities(capability.Postgres17(), platform.Postgres)

	out, err := renderer.Render(ast.NewCreateContinuousAggregate("hourly", "SELECT 1"))

	c.Assert(err, qt.IsNil)
	c.Assert(out, qt.Contains, "continuous aggregate hourly is not supported by this target; skipped.")
	c.Assert(out, qt.Not(qt.Contains), "timescaledb.continuous")
}
