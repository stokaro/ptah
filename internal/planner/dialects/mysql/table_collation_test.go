package mysql_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/difftypes"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
)

// tableCollationDiff changes the default collation of users and resets its
// column email, which declares none, to the collation it takes from the table.
func tableCollationDiff() *difftypes.SchemaDiff {
	return &difftypes.SchemaDiff{TablesModified: []difftypes.TableDiff{{
		TableName:       "users",
		CollationChange: &difftypes.CollationChange{Current: "utf8mb4_bin", Desired: "utf8mb4_general_ci"},
		ColumnsModified: []difftypes.ColumnDiff{{
			ColumnName: "email",
			Changes:    map[string]string{"collation": "utf8mb4_bin -> utf8mb4_general_ci"},
			Desired:    schemamodel.Field{Name: "email", StructName: "User", Type: "VARCHAR(255)", Nullable: true},
		}},
	}}}
}

// A changed default collation is planned before the statements of the table's
// columns. MySQL resolves a column restated without a collation from the
// table's default at the moment its statement runs, so the reverse order
// leaves the column on the old collation and the next plan finds it again.
func TestPlanner_MySQLFamilyPlansATableCollationBeforeItsColumns(t *testing.T) {
	for _, test := range mysqlFamilyPlannerCases() {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			nodes, err := test.planner.GenerateMigrationAST(c.Context(), must.Must(builtin.New()), tableCollationDiff())
			c.Assert(err, qt.IsNil)
			rendered, err := builtin.RenderSQL(platform.MySQL, nodes...)

			c.Assert(err, qt.IsNil)
			c.Assert(rendered, qt.Matches, "(?s).*ALTER TABLE `users` COLLATE utf8mb4_general_ci;\n"+
				"(-- [^\n]*\n)*ALTER TABLE `users` MODIFY COLUMN `email` VARCHAR\\(255\\);.*")
		})
	}
}

// The control: a table with no collation change gets no table COLLATE
// statement.
func TestPlanner_MySQLFamilyPlansNoTableCollationWithoutAChange(t *testing.T) {
	for _, test := range mysqlFamilyPlannerCases() {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := tableCollationDiff()
			diff.TablesModified[0].CollationChange = nil

			nodes, err := test.planner.GenerateMigrationAST(c.Context(), must.Must(builtin.New()), diff)
			c.Assert(err, qt.IsNil)
			rendered, err := builtin.RenderSQL(platform.MySQL, nodes...)

			c.Assert(err, qt.IsNil)
			c.Assert(rendered, qt.Not(qt.Contains), " COLLATE ")
			c.Assert(rendered, qt.Contains, "ALTER TABLE `users` MODIFY COLUMN `email` VARCHAR(255);")
		})
	}
}
