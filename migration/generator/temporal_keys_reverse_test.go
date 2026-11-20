package generator_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/difftypes"
)

// A rollback that adds back a temporal key the forward change dropped adds it as
// the database the change started from held it: WITHOUT OVERLAPS on a UNIQUE
// and PERIOD on a foreign key (stokaro/ptah#6330). Put back without them, the
// rollback builds a key that accepts the rows the original refused.
func TestPlanBidirectionalSchemaDiff_DroppedTemporalKeyComesBackWithItsClause(t *testing.T) {
	parent, column := "parents", "id"
	tests := []struct {
		name       string
		constraint catalog.Constraint
		want       string
	}{
		{
			name: "a unique constraint",
			constraint: catalog.Constraint{
				Name: "orders_uq", TableName: "orders", Schema: "app", Type: "UNIQUE", ColumnName: "p",
				ColumnNames: []string{"p", "valid_at"}, WithoutOverlaps: true,
			},
			want: `ADD CONSTRAINT "orders_uq" UNIQUE ("p", "valid_at" WITHOUT OVERLAPS);`,
		},
		{
			name: "a foreign key",
			constraint: catalog.Constraint{
				Name: "orders_p_fkey", TableName: "orders", Schema: "app", Type: "FOREIGN KEY", ColumnName: "p",
				ColumnNames: []string{"p", "valid_at"}, ForeignTable: &parent, ForeignColumn: &column,
				ForeignColumns: []string{"id", "valid_at"}, Period: true,
			},
			want: `FOREIGN KEY ("p", PERIOD "valid_at") REFERENCES "parents"("id", PERIOD "valid_at");`,
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
					{Name: "orders", Schema: "app", Columns: []catalog.Column{
						{Name: "p", DataType: "integer", IsNullable: "YES"},
						{Name: "valid_at", DataType: "daterange", IsNullable: "YES"},
					}},
					{Name: "parents", Columns: []catalog.Column{
						{Name: "id", DataType: "integer", IsPrimaryKey: true, IsNullable: "NO"},
						{Name: "valid_at", DataType: "daterange", IsPrimaryKey: true, IsNullable: "NO"},
					}},
				},
				Constraints: []catalog.Constraint{test.constraint},
			})

			c.Assert(sql, qt.Contains, test.want, qt.Commentf("rollback:\n%s", sql))
		})
	}
}
