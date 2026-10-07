package postgres_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/internal/planner/dialects/postgres"
	"ptah.run/migration/schemadiff/difftypes"
)

// TestPlanner_AColumnTakesItsKeyUnderTheComparisonsName adds a column's own
// UNIQUE under the name the diff carries, which the comparison derived from
// the names the desired state holds (stokaro/ptah#3859). Measured on
// PostgreSQL 18.6, `ALTER TABLE c ADD CONSTRAINT c_x_key UNIQUE (x)` beside a
// unique index c_x_key is refused with `relation "c_x_key" already exists`,
// and Atlas CE v1.3.0 writes c_x_key1. A diff built by hand carries no name,
// and the name the server tries first stands in.
func TestPlanner_AColumnTakesItsKeyUnderTheComparisonsName(t *testing.T) {
	tests := []struct {
		name  string
		names map[string]string
		want  string
	}{
		{
			name:  "the name the diff carries",
			names: map[string]string{"x": "c_x_key1"},
			want:  `ALTER TABLE "c" ADD CONSTRAINT "c_x_key1" UNIQUE ("x");`,
		},
		{
			name: "no name carried",
			want: `ALTER TABLE "c" ADD CONSTRAINT "c_x_key" UNIQUE ("x");`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := &difftypes.SchemaDiff{TablesModified: []difftypes.TableDiff{{
				TableName: "c",
				ColumnsModified: []difftypes.ColumnDiff{{
					ColumnName: "x",
					Changes:    map[string]string{"unique": "false -> true"},
					Desired:    schemamodel.Field{StructName: "C", Name: "x", Type: "integer", Nullable: true, Unique: true},
				}},
				ColumnKeyNames: test.names,
			}}}
			caps := capability.Postgres17()

			nodes, err := postgres.NewForDialect(platform.Postgres, caps).GenerateMigrationAST(diff)
			c.Assert(err, qt.IsNil)
			sql, err := builtin.RenderSQLWithCapabilities(platform.Postgres, caps, nodes...)

			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Contains, test.want)
		})
	}
}
