package generator

// White-box testing required: the down direction is built by reversing a diff
// through unexported helpers, and the reversal is what this pins -- the public
// API only exposes the SQL that comes out of the whole pipeline.

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/migration/schemadiff"
	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
)

// TestGenerateDownMigration_ContinuousAggregate pins both directions of the
// rollback, and that the restored one carries the body the DATABASE had.
//
// The down direction of a create is a DROP MATERIALIZED VIEW, which is the
// server's own verb: DROP VIEW answers `cannot drop continuous aggregate using
// DROP VIEW`. The down direction of a drop rebuilds the aggregate from the
// pre-change read, which is the only place the definition still exists -- the
// desired schema stopped naming it, and the aggregate itself is gone
// (stokaro/ptah#1026).
func TestGenerateDownMigration_ContinuousAggregate(t *testing.T) {
	tests := []struct {
		name    string
		diff    *difftypes.SchemaDiff
		want    []string
		notWant []string
	}{
		{
			name: "rolling back a create drops it",
			diff: &difftypes.SchemaDiff{ContinuousAggregatesAdded: difftypes.ContinuousAggregateChanges{{Name: "public.hourly"}}},
			want: []string{`DROP MATERIALIZED VIEW IF EXISTS "public"."hourly"`},
			// DROP VIEW would be refused by the server, so a rollback carrying
			// it could not run at all.
			notWant: []string{"DROP VIEW"},
		},
		{
			name: "rolling back a drop restores the body the database had",
			// Carried in the shape the comparison produces for a removal: the
			// schema and name apart, and the body the database reported. The
			// reversal renders from this rather than reading the database
			// again, which is what makes the body the change's own.
			diff: &difftypes.SchemaDiff{ContinuousAggregatesRemoved: difftypes.ContinuousAggregateChanges{{
				Schema: "public",
				Name:   "hourly",
				Body:   "SELECT time_bucket('01:00:00'::interval, \"time\") FROM readings",
			}}},
			want: []string{
				`CREATE MATERIALIZED VIEW "public"."hourly" WITH (timescaledb.continuous`,
				"SELECT time_bucket('01:00:00'::interval, \"time\") FROM readings",
				"WITH NO DATA",
			},
		},
	}

	database := &catalog.Database{
		ContinuousAggregates: []catalog.ContinuousAggregate{{
			Schema: "public", Name: "hourly",
			HypertableSchema: "public", HypertableName: "readings",
			Definition: "SELECT time_bucket('01:00:00'::interval, \"time\") FROM readings",
		}},
	}
	desired := &schemamodel.Database{ContinuousAggregates: []schemamodel.ContinuousAggregate{{
		Name: "hourly", Schema: "public", Body: "SELECT 1",
	}}}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			sql, err := generateDownMigrationSQL(
				test.diff,
				desired,
				database,
				platform.Postgres,
				capability.Postgres17().With(capability.ContinuousAggregates, true),
			)

			c.Assert(err, qt.IsNil)
			for _, want := range test.want {
				c.Assert(sql, qt.Contains, want)
			}
			for _, notWant := range test.notWant {
				c.Assert(sql, qt.Not(qt.Contains), notWant)
			}
			c.Assert(strings.TrimSpace(sql), qt.Not(qt.Equals), "")
		})
	}
}

// TestGenerateDownMigration_ContinuousAggregateBodyComesFromTheComparison
// drives the whole path rather than a hand-built diff.
//
// The test above supplies the change itself, so it measures the planner and not
// where the body came from. The body is carried by the COMPARISON now
// (stokaro/ptah#2315): a removal describes the aggregate the database reported,
// and the reversal renders that description instead of reading the database
// again. Blanking `Body` in the carry leaves every hand-built fixture green,
// which is why this one starts from two schemas.
func TestGenerateDownMigration_ContinuousAggregateBodyComesFromTheComparison(t *testing.T) {
	c := qt.New(t)

	const definition = "SELECT time_bucket('01:00:00'::interval, \"time\") FROM readings"

	// The desired schema does not name the aggregate; the database has it.
	// That is what puts it in ContinuousAggregatesRemoved.
	desired := &schemamodel.Database{}
	database := &catalog.Database{
		ContinuousAggregates: []catalog.ContinuousAggregate{{
			Schema: "public", Name: "hourly",
			HypertableSchema: "public", HypertableName: "readings",
			Definition: definition,
		}},
	}

	upDiff := schemadiff.CompareWithDialect(desired, database, platform.Postgres)
	c.Assert(upDiff.ContinuousAggregatesRemoved.Names(), qt.DeepEquals, []string{"public.hourly"})

	sql, err := generateDownMigrationSQL(
		upDiff,
		desired,
		database,
		platform.Postgres,
		capability.Postgres17().With(capability.ContinuousAggregates, true),
	)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, definition,
		qt.Commentf("the rollback rebuilds the aggregate from the body the comparison carried"))
}
