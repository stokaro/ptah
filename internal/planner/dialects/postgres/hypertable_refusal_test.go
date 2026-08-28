package postgres_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/internal/planner/dialects/postgres"
	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
)

// TestPlanner_RefusesWhatTimescaleDBCannotUndo pins the two divergences that
// have no statement, and pins that they are REFUSALS rather than silence.
//
// Measured on TimescaleDB 2.29.2: `drop_hypertable` answers `function
// drop_hypertable(unknown) does not exist`, and no call repartitions an
// existing hypertable. Planning nothing would leave the table partitioned, the
// description saying otherwise, and an operator reading "no changes" believing
// the two agree — a divergence that is permanent and invisible at once
// (stokaro/ptah#1026).
func TestPlanner_RefusesWhatTimescaleDBCannotUndo(t *testing.T) {
	tests := []struct {
		name string
		diff *difftypes.SchemaDiff
		want string
	}{
		{
			name: "the declaration stops naming it",
			diff: &difftypes.SchemaDiff{HypertablesRemoved: difftypes.HypertableChanges{{Table: "public.readings"}}},
			want: "no statement that turns a hypertable back into an ordinary table",
		},
		{
			name: "the declaration moves the dimension",
			diff: &difftypes.SchemaDiff{HypertablesModified: []difftypes.HypertableDiff{{
				Table: "public.readings", OldColumn: "time", NewColumn: "recorded_at",
			}}},
			want: "no statement that repartitions an existing hypertable",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			_, err := postgres.New().GenerateMigrationAST(test.diff, &schemamodel.Database{})

			c.Assert(err, qt.IsNotNil)
			c.Assert(err.Error(), qt.Contains, test.want)
			// The name is in the message, because an operator with several
			// hypertables needs to know which one to write the migration for.
			c.Assert(err.Error(), qt.Contains, "readings")
		})
	}
}

// TestPlanner_PlansAnAddedHypertable is the control the refusals need: the
// direction that HAS a statement still produces one.
func TestPlanner_PlansAnAddedHypertable(t *testing.T) {
	c := qt.New(t)
	declared := &schemamodel.Database{Hypertables: []schemamodel.Hypertable{
		{Table: "public.readings", Column: "time"},
	}}

	nodes, err := postgres.New().GenerateMigrationAST(
		&difftypes.SchemaDiff{HypertablesAdded: difftypes.HypertableChanges{{Table: "public.readings"}}}, declared)

	c.Assert(err, qt.IsNil)
	c.Assert(nodes, qt.Not(qt.HasLen), 0)
}
