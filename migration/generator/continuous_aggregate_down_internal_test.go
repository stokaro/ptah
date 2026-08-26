package generator

// White-box testing required: the down direction is built by reversing a diff
// through unexported helpers, and the reversal is what this pins -- the public
// API only exposes the SQL that comes out of the whole pipeline.

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/capability"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
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
			diff: &difftypes.SchemaDiff{ContinuousAggregatesAdded: []string{"public.hourly"}},
			want: []string{`DROP MATERIALIZED VIEW IF EXISTS "public"."hourly"`},
			// DROP VIEW would be refused by the server, so a rollback carrying
			// it could not run at all.
			notWant: []string{"DROP VIEW"},
		},
		{
			name: "rolling back a drop restores the body the database had",
			diff: &difftypes.SchemaDiff{ContinuousAggregatesRemoved: []string{"public.hourly"}},
			want: []string{
				`CREATE MATERIALIZED VIEW "public"."hourly" WITH (timescaledb.continuous`,
				"SELECT time_bucket('01:00:00'::interval, \"time\") FROM readings",
				"WITH NO DATA",
			},
		},
	}

	database := &dbschematypes.DBSchema{
		ContinuousAggregates: []dbschematypes.DBContinuousAggregate{{
			Schema: "public", Name: "hourly",
			HypertableSchema: "public", HypertableName: "readings",
			Definition: "SELECT time_bucket('01:00:00'::interval, \"time\") FROM readings",
		}},
	}
	generated := &goschema.Database{ContinuousAggregates: []goschema.ContinuousAggregate{{
		Name: "hourly", Schema: "public", Body: "SELECT 1",
	}}}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			sql, err := generateDownMigrationSQL(
				test.diff,
				generated,
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
