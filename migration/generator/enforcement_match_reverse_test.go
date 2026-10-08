package generator_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/migration/generator"
	"ptah.run/migration/schemadiff/difftypes"
)

// postgres18Rollback renders the rollback of diff against current on
// PostgreSQL 18, the line that keeps NOT ENFORCED.
func postgres18Rollback(c *qt.C, diff *difftypes.SchemaDiff, current *catalog.Database) string {
	c.Helper()
	plan, err := generator.PlanBidirectionalSchemaDiff(c.Context(),
		generator.BidirectionalSchemaPlanOptions{Runtime: must.Must(builtin.New()), Diff: diff,
			DesiredSchema: &schemamodel.Database{},
			CurrentSchema: current,
			Dialect:       platform.Postgres,
			Capabilities:  capability.Postgres18(),
		})
	c.Assert(err, qt.IsNil)
	sql, err := builtin.RenderSQLWithCapabilities(platform.Postgres, capability.Postgres18(), plan.Reverse.Nodes...)
	c.Assert(err, qt.IsNil)
	return sql
}

// A rollback that adds back a constraint the forward change dropped adds it as
// the database the change started from held it: a CHECK the server did not
// check, and a foreign key with its MATCH type, enforcement and deferral
// (stokaro/ptah#3853). Put back without them, the rollback builds another
// constraint.
func TestPlanBidirectionalSchemaDiff_DroppedConstraintComesBackWithItsClauses(t *testing.T) {
	clause, parent, column := "n > 0", "parents", "id"
	method, elements, predicate := "gist", "room WITH =, during WITH &&", "active"
	tests := []struct {
		name       string
		constraint catalog.Constraint
		want       string
	}{
		{
			name:       "an unvalidated check",
			constraint: catalog.Constraint{Name: "orders_n_check", TableName: "orders", Schema: "app", Type: "CHECK", CheckClause: &clause, NotValid: true},
			want:       `ADD CONSTRAINT "orders_n_check" CHECK (n > 0) NOT VALID;`,
		},
		{
			name:       "an unvalidated foreign key",
			constraint: catalog.Constraint{Name: "orders_p_fkey", TableName: "orders", Schema: "app", Type: "FOREIGN KEY", ColumnName: "p", ColumnNames: []string{"p"}, ForeignTable: &parent, ForeignColumn: &column, ForeignColumns: []string{"id"}, NotValid: true},
			want:       `FOREIGN KEY ("p") REFERENCES "parents"("id") NOT VALID;`,
		},
		{
			name:       "a partial deferred exclusion",
			constraint: catalog.Constraint{Name: "orders_period_excl", TableName: "orders", Schema: "app", Type: "EXCLUDE", UsingMethod: &method, ExcludeElements: &elements, WhereCondition: &predicate, Deferrable: true, Initially: "deferred"},
			want:       `EXCLUDE USING gist (room WITH =, during WITH &&) WHERE (active) DEFERRABLE INITIALLY DEFERRED;`,
		},
		{
			name: "a CHECK not enforced",
			constraint: catalog.Constraint{
				Name: "orders_n_check", TableName: "orders", Schema: "app", Type: "CHECK",
				CheckClause: &clause, NotEnforced: true,
			},
			want: `ADD CONSTRAINT "orders_n_check" CHECK (n > 0) NOT ENFORCED;`,
		},
		{
			name: "a foreign key",
			constraint: catalog.Constraint{
				Name: "orders_p_fkey", TableName: "orders", Schema: "app", Type: "FOREIGN KEY",
				ColumnNames: []string{"p"}, ColumnName: "p", ForeignTable: &parent, ForeignColumn: &column,
				ForeignColumns: []string{"id"}, Match: "FULL", NotEnforced: true, Deferrable: true,
			},
			want: `FOREIGN KEY ("p") REFERENCES "parents"("id") MATCH FULL DEFERRABLE NOT ENFORCED;`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := &difftypes.SchemaDiff{ConstraintsRemoved: difftypes.ConstraintRemovals{{
				Name: test.constraint.Name, TableName: "app.orders", Type: test.constraint.Type,
			}}}

			sql := postgres18Rollback(c, diff, &catalog.Database{
				Tables: []catalog.Table{
					{Name: "orders", Schema: "app", Columns: []catalog.Column{{Name: "p", DataType: "integer", IsNullable: "YES"}, {Name: "n", DataType: "integer", IsNullable: "YES"}}},
					{Name: "parents", Columns: []catalog.Column{{Name: "id", DataType: "integer", IsPrimaryKey: true, IsNullable: "NO"}}},
				},
				Constraints: []catalog.Constraint{test.constraint},
			})

			c.Assert(sql, qt.Contains, test.want, qt.Commentf("rollback:\n%s", sql))
		})
	}
}
