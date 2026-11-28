package mysqllike_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
)

// A trigger lives in its table's database, and a whole-server session selects
// no database, so a trigger statement names the database whenever its table
// is named with one. MySQL 8.4 and MariaDB 11.8 refuse an unqualified CREATE
// TRIGGER or DROP TRIGGER there with Error 1046.
func TestVisitNode_TriggerNameTakesTheDatabaseOfItsTable(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		node    ast.Node
		want    string
	}{
		{
			name: "drop, qualified table", dialect: "mysql",
			node: ast.NewDropTrigger("trg", "gone.u"),
			want: "DROP TRIGGER `gone`.`trg`;\n",
		},
		{
			name: "drop if exists, qualified table", dialect: "mariadb",
			node: ast.NewDropTrigger("trg", "gone.u").SetIfExists(),
			want: "DROP TRIGGER IF EXISTS `gone`.`trg`;\n",
		},
		{
			name: "drop, bare table", dialect: "mysql",
			node: ast.NewDropTrigger("trg", "u"),
			want: "DROP TRIGGER `trg`;\n",
		},
		{
			name: "create, qualified table", dialect: "mysql",
			node: ast.NewCreateTrigger("trg", "gone.u").SetTiming("BEFORE").SetEvent("INSERT").SetBody("SET NEW.c = 1"),
			want: "CREATE TRIGGER `gone`.`trg` BEFORE INSERT ON `gone`.`u` FOR EACH ROW SET NEW.c = 1;\n",
		},
		{
			name: "create, bare table", dialect: "mariadb",
			node: ast.NewCreateTrigger("trg", "u").SetTiming("BEFORE").SetEvent("INSERT").SetBody("SET NEW.c = 1"),
			want: "CREATE TRIGGER `trg` BEFORE INSERT ON `u` FOR EACH ROW SET NEW.c = 1;\n",
		},
		{
			name: "replace without CREATE OR REPLACE, qualified table", dialect: "mysql",
			node: ast.NewCreateTrigger("trg", "gone.u").SetTiming("BEFORE").SetEvent("INSERT").SetBody("SET NEW.c = 1").SetReplace(),
			want: "DROP TRIGGER IF EXISTS `gone`.`trg`;\n" +
				"CREATE TRIGGER `gone`.`trg` BEFORE INSERT ON `gone`.`u` FOR EACH ROW SET NEW.c = 1;\n",
		},
		{
			name: "a dot in the database name stays in the database part", dialect: "mysql",
			node: ast.NewDropTrigger("trg", "`a.b`.u"),
			want: "DROP TRIGGER `a.b`.`trg`;\n",
		},
	}
	for _, test := range tests {
		t.Run(test.dialect+"/"+test.name, func(t *testing.T) {
			c := qt.New(t)
			r := newRenderer(test.dialect)

			c.Assert(r.VisitNode(test.node), qt.IsNil)
			c.Assert(r.Output(), qt.Equals, test.want)
		})
	}
}
