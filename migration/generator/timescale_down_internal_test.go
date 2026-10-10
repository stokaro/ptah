package generator

// White-box testing required: the down direction is built by reversing a diff
// through unexported helpers, and the reversal is what this pins -- the public
// API only exposes the SQL that comes out of the whole pipeline.

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/timescaledb/tsdiff"
	"ptah.run/dialect/timescaledb/tsschema"
	"ptah.run/engine/builtin"
	"ptah.run/migration/schemadiff"
	"ptah.run/migration/schemadiff/difftypes"
)

// timescaleCapabilities is a PostgreSQL target with both TimescaleDB keys.
func timescaleCapabilities() capability.Capabilities {
	return capability.Postgres17().With(capability.Hypertables, true).With(capability.ContinuousAggregates, true)
}

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
	const definition = "SELECT time_bucket('01:00:00'::interval, \"time\") FROM readings"
	tests := []struct {
		name   string
		change tsdiff.ContinuousAggregate
		want   string
	}{
		{
			name:   "rolling back a create drops it",
			change: tsdiff.ContinuousAggregate{After: &tsschema.DesiredContinuousAggregate{Body: "SELECT 1"}},
			want:   "DROP MATERIALIZED VIEW IF EXISTS \"public\".\"hourly\";",
		},
		{
			// Carried in the shape the comparison produces for a removal: the
			// definition the database reported. The reversal renders from this
			// rather than reading the database again, which is what makes the
			// body the change's own.
			name:   "rolling back a drop restores the body the database had",
			change: tsdiff.ContinuousAggregate{Before: &tsschema.ObservedContinuousAggregate{Definition: definition + ";", MaterializedOnly: new(true), HypertableName: "readings"}},
			want: "CREATE MATERIALIZED VIEW \"public\".\"hourly\" WITH (timescaledb.continuous, timescaledb.materialized_only = true) AS\n" +
				definition + "\nWITH NO DATA",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := &difftypes.SchemaDiff{FeatureChanges: []schemaext.ChangeRecord{{
				Subject: tsschema.ContinuousAggregateRefWith(identifier.ForDialect(platform.Postgres), "public", "hourly"),
				Value:   &test.change,
			}}}

			sql, err := generateDownMigrationSQL(t.Context(), must.Must(builtin.New()),
				diff, &schemamodel.Database{}, &catalog.Database{}, platform.Postgres, timescaleCapabilities())

			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Contains, test.want)
			c.Assert(sql, qt.Not(qt.Contains), "DROP VIEW")
		})
	}
}

// TestGenerateDownMigration_ContinuousAggregateBodyComesFromTheComparison
// drives the whole path rather than a hand-built diff.
//
// The test above supplies the change itself, so it measures the planner and not
// where the body came from. The body is carried by the COMPARISON
// (stokaro/ptah#2315): a removal describes the aggregate the database reported,
// and the reversal renders that description instead of reading the database
// again. Blanking the definition in the carry leaves every hand-built fixture
// green, which is why this one starts from two schemas.
func TestGenerateDownMigration_ContinuousAggregateBodyComesFromTheComparison(t *testing.T) {
	c := qt.New(t)
	const definition = "SELECT time_bucket('01:00:00'::interval, \"time\") FROM readings"
	// The desired schema does not name the aggregate and says it describes
	// every aggregate; the database has one. That is what plans its removal.
	desired := &schemamodel.Database{FeatureCoverage: must.Must(tsschema.CompleteCoverage(schemaext.Desired))}
	database := &catalog.Database{
		FeatureObjects: must.Must(schemaext.NewObjects(tsschema.ObservedContinuousAggregateObject("public", "hourly",
			tsschema.ObservedContinuousAggregate{Definition: definition, HypertableSchema: "public", HypertableName: "readings"}))),
		FeatureCoverage: must.Must(tsschema.CompleteCoverage(schemaext.Observed)),
	}

	upDiff := must.Must(schemadiff.CompareWithDialect(t.Context(), desired, database, platform.Postgres, must.Must(builtin.New())))
	c.Assert(upDiff.FeatureChanges, qt.HasLen, 1)

	sql, err := generateDownMigrationSQL(t.Context(), must.Must(builtin.New()),
		upDiff, desired, database, platform.Postgres, timescaleCapabilities())

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, "CREATE MATERIALIZED VIEW \"public\".\"hourly\" WITH (timescaledb.continuous) AS\n"+definition+"\nWITH NO DATA",
		qt.Commentf("the rollback rebuilds the aggregate from the body the comparison carried"))
}

// TestGenerateDownMigration_ModifiedAggregateIsRecreatedFromThePriorDeclaration
// pins the half of a modification the declared body does not carry.
//
// A modification is a drop and a create, and the create renders from the
// aggregate the change carries -- its body and its option. Reversing only the
// direction without the operands would have the rollback recreate the
// definition it is undoing.
func TestGenerateDownMigration_ModifiedAggregateIsRecreatedFromThePriorDeclaration(t *testing.T) {
	c := qt.New(t)
	const priorDefinition = "SELECT time_bucket('30 min', ts) FROM readings"
	const declaredBody = "SELECT time_bucket('1 hour', ts) FROM readings"
	desired := &schemamodel.Database{
		FeatureObjects: must.Must(schemaext.NewObjects(tsschema.DesiredContinuousAggregateObject("public", "hourly",
			tsschema.DesiredContinuousAggregate{StructName: "Hourly", Body: declaredBody, MaterializedOnly: new(true)}))),
		FeatureCoverage: must.Must(tsschema.CompleteCoverage(schemaext.Desired)),
	}
	database := &catalog.Database{
		FeatureObjects: must.Must(schemaext.NewObjects(tsschema.ObservedContinuousAggregateObject("public", "hourly",
			tsschema.ObservedContinuousAggregate{Definition: priorDefinition, MaterializedOnly: new(false), HypertableSchema: "public", HypertableName: "readings"}))),
		FeatureCoverage: must.Must(tsschema.CompleteCoverage(schemaext.Observed)),
	}
	upDiff := must.Must(schemadiff.CompareWithDialect(t.Context(), desired, database, platform.Postgres, must.Must(builtin.New())))
	c.Assert(upDiff.FeatureChanges, qt.HasLen, 1)

	up, err := generateUpMigrationSQL(context.Background(), must.Must(builtin.New()), upDiff, desired, platform.Postgres, timescaleCapabilities())
	c.Assert(err, qt.IsNil)
	down, err := generateDownMigrationSQL(t.Context(), must.Must(builtin.New()), upDiff, desired, database, platform.Postgres, timescaleCapabilities())
	c.Assert(err, qt.IsNil)

	c.Assert(up, qt.Contains, "DROP MATERIALIZED VIEW IF EXISTS \"public\".\"hourly\";\nCREATE MATERIALIZED VIEW \"public\".\"hourly\" "+
		"WITH (timescaledb.continuous, timescaledb.materialized_only = true) AS\n"+declaredBody)
	c.Assert(down, qt.Contains, "DROP MATERIALIZED VIEW IF EXISTS \"public\".\"hourly\";\nCREATE MATERIALIZED VIEW \"public\".\"hourly\" "+
		"WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS\n"+priorDefinition,
		qt.Commentf("the rollback recreates the definition the database held"))
	c.Assert(down, qt.Not(qt.Contains), declaredBody, qt.Commentf("the rollback must not recreate the body it is undoing"))
}

// TestGenerateDownMigration_AHypertableHasNoRollback pins the reversal of a
// table partitioned in place: TimescaleDB has no statement that turns a
// hypertable back into an ordinary table, so the rollback refuses rather than
// writing a down migration that leaves the table partitioned and calls it
// undone.
func TestGenerateDownMigration_AHypertableHasNoRollback(t *testing.T) {
	c := qt.New(t)
	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "R", Name: "readings",
			Facets: must.Must(schemaext.NewFacets(&tsschema.DesiredHypertable{Column: "time"}))}},
		Fields:          []schemamodel.Field{{StructName: "R", Name: "time", Type: "TIMESTAMPTZ"}},
		FeatureCoverage: must.Must(tsschema.CompleteCoverage(schemaext.Desired)),
	}
	database := &catalog.Database{
		Tables:          []catalog.Table{{Name: "readings", Columns: []catalog.Column{{Name: "time", DataType: "timestamp with time zone", UDTName: "timestamptz"}}}},
		FeatureCoverage: must.Must(tsschema.CompleteCoverage(schemaext.Observed)),
	}
	upDiff := must.Must(schemadiff.CompareWithDialect(t.Context(), desired, database, platform.Postgres, must.Must(builtin.New())))
	c.Assert(upDiff.TablesModified, qt.HasLen, 1)

	down, err := generateDownMigrationSQL(t.Context(), must.Must(builtin.New()), upDiff, desired, database, platform.Postgres, timescaleCapabilities())

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, `(?s).*readings is a hypertable and the desired schema does not declare one; TimescaleDB has no statement that turns a hypertable back into an ordinary table.*`)
	c.Assert(down, qt.Equals, "")
}
