package mysql_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/difftypes"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
)

// heldOrderDiff changes the default collation of t, which holds its columns
// as id, zeta, alpha and mid, and resets the three it lists in name order, as
// the comparison reports them, with held as the columns the read holds.
func heldOrderDiff(held []catalog.Column) *difftypes.SchemaDiff {
	reset := func(name string) difftypes.ColumnDiff {
		return difftypes.ColumnDiff{
			ColumnName: name,
			Changes:    map[string]string{"collation": "utf8mb4_bin -> utf8mb4_general_ci"},
			Desired:    schemamodel.Field{Name: name, StructName: "T", Type: "VARCHAR(20)", Nullable: true},
		}
	}
	diff := &difftypes.SchemaDiff{TablesModified: []difftypes.TableDiff{{
		TableName:       "t",
		CollationChange: &difftypes.CollationChange{Current: "utf8mb4_bin", Desired: "utf8mb4_general_ci"},
		ColumnsModified: []difftypes.ColumnDiff{reset("alpha"), reset("mid"), reset("zeta")},
	}}}
	diff.TablesModified[0].Current.Table.Columns = held
	return diff
}

// TestPlanner_MySQLFamilyPlansTheColumnsOfATableCollationInHeldOrder writes
// the columns a changed default collation resets in the order the table holds
// them, as the community binary writes them, with the comments that describe
// them in the same order. Where the read lists no columns, as the control,
// they stay in the order the comparison reports.
func TestPlanner_MySQLFamilyPlansTheColumnsOfATableCollationInHeldOrder(t *testing.T) {
	held := []catalog.Column{{Name: "id"}, {Name: "ZETA"}, {Name: "alpha"}, {Name: "mid"}}
	for _, row := range []struct {
		name string
		held []catalog.Column
		want string
	}{
		{name: "held order", held: held, want: "-- Modify table: t\n" +
			"-- Modify column t.zeta: collation: utf8mb4_bin -> utf8mb4_general_ci\n" +
			"-- Modify column t.alpha: collation: utf8mb4_bin -> utf8mb4_general_ci\n" +
			"-- Modify column t.mid: collation: utf8mb4_bin -> utf8mb4_general_ci\n" +
			"ALTER TABLE `t` COLLATE utf8mb4_general_ci, MODIFY COLUMN `zeta` VARCHAR(20), " +
			"MODIFY COLUMN `alpha` VARCHAR(20), MODIFY COLUMN `mid` VARCHAR(20);\n\n"},
		{name: "no columns held", want: "-- Modify table: t\n" +
			"-- Modify column t.alpha: collation: utf8mb4_bin -> utf8mb4_general_ci\n" +
			"-- Modify column t.mid: collation: utf8mb4_bin -> utf8mb4_general_ci\n" +
			"-- Modify column t.zeta: collation: utf8mb4_bin -> utf8mb4_general_ci\n" +
			"ALTER TABLE `t` COLLATE utf8mb4_general_ci, MODIFY COLUMN `alpha` VARCHAR(20), " +
			"MODIFY COLUMN `mid` VARCHAR(20), MODIFY COLUMN `zeta` VARCHAR(20);\n\n"},
	} {
		for _, test := range mysqlFamilyPlannerCases() {
			t.Run(row.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)

				nodes, err := test.planner.GenerateMigrationAST(c.Context(), must.Must(builtin.New()), heldOrderDiff(row.held))
				c.Assert(err, qt.IsNil)
				rendered, err := builtin.RenderSQL(platform.MySQL, nodes...)

				c.Assert(err, qt.IsNil)
				c.Assert(rendered, qt.Equals, row.want)
			})
		}
	}
}
