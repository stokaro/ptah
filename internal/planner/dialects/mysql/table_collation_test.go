package mysql_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
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

// A changed default collation is planned in one statement with the reset of
// the column that takes it, as the community binary writes it, with the
// comment that describes the reset ahead of it. MySQL resolves a column
// restated without a collation from the table's default, so a reset planned
// on its own ahead of the table's change would leave the column on the old
// collation.
func TestPlanner_MySQLFamilyPlansATableCollationWithItsColumns(t *testing.T) {
	for _, test := range mysqlFamilyPlannerCases() {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			nodes, err := test.planner.GenerateMigrationAST(c.Context(), must.Must(builtin.New()), tableCollationDiff())
			c.Assert(err, qt.IsNil)
			rendered, err := builtin.RenderSQL(platform.MySQL, nodes...)

			c.Assert(err, qt.IsNil)
			c.Assert(rendered, qt.Equals, "-- Modify table: users\n"+
				"-- Modify column users.email: collation: utf8mb4_bin -> utf8mb4_general_ci\n"+
				"ALTER TABLE `users` COLLATE utf8mb4_general_ci, MODIFY COLUMN `email` VARCHAR(255);\n\n")
		})
	}
}

// A change of the table's character set names it in the statement, and the
// reset of the column it converts carries both character sets, which
// migration/safety rates. One statement is what keeps a value the server
// cannot convert from leaving the table half changed.
func TestPlanner_MySQLFamilyPlansATableCharsetWithItsColumns(t *testing.T) {
	for _, test := range mysqlFamilyPlannerCases() {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := tableCollationDiff()
			diff.TablesModified[0].CollationChange = &difftypes.CollationChange{Current: "utf8mb4_bin", Desired: "latin1_bin",
				Charset: &difftypes.CharsetChange{Current: "utf8mb4", Desired: "latin1"}}
			diff.TablesModified[0].ColumnsModified[0].Changes = map[string]string{"collation": "utf8mb4_bin -> latin1_bin"}
			diff.TablesModified[0].ColumnsModified[0].CharsetChange = &difftypes.CharsetChange{Current: "utf8mb4", Desired: "latin1"}

			nodes, err := test.planner.GenerateMigrationAST(c.Context(), must.Must(builtin.New()), diff)
			c.Assert(err, qt.IsNil)
			rendered, err := builtin.RenderSQL(platform.MySQL, nodes...)

			c.Assert(err, qt.IsNil)
			c.Assert(rendered, qt.Contains, "ALTER TABLE `users` CHARSET latin1, COLLATE latin1_bin, MODIFY COLUMN `email` VARCHAR(255);\n")
			modify := nodes[len(nodes)-1].(*ast.AlterTableNode).Operations[1].(*ast.ModifyColumnOperation)
			c.Assert([]string{modify.PreviousCharset, modify.Charset}, qt.DeepEquals, []string{"utf8mb4", "latin1"})
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
