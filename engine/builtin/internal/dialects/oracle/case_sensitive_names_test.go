package oracle_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
)

// TestCaseSensitiveNames_StayQuoted renders statements that name objects the
// comparison read from an Oracle catalog. A name created quoted, such as a
// table stored as orders, reaches the renderer quoted, and it has to stay
// quoted: written bare it folds to ORDERS, which is another table or none, and
// the DROP answered ORA-00942 (stokaro/ptah#6243). A name that is its own
// upper-case fold is written bare whether it arrives quoted or not, and so is
// every unquoted name.
func TestCaseSensitiveNames_StayQuoted(t *testing.T) {
	tests := []struct {
		name string
		node ast.Node
		want string
	}{
		{
			name: "a dropped table and its schema",
			node: ast.NewDropTable(`APP."orders"`),
			want: `DROP TABLE APP."orders" PURGE;`,
		},
		{
			name: "a quoted schema that is not its fold",
			node: ast.NewDropTable(`"app"."orders"`),
			want: `DROP TABLE "app"."orders" PURGE;`,
		},
		{
			name: "a quoted name that is its own fold is written bare",
			node: ast.NewDropTable(`"APP"."ORDERS"`),
			want: `DROP TABLE APP.ORDERS PURGE;`,
		},
		{
			name: "an unquoted name is written bare",
			node: ast.NewDropTable(`app.orders`),
			want: `DROP TABLE app.orders PURGE;`,
		},
		{
			name: "a dropped column",
			node: &ast.AlterTableNode{Name: "ORDERS", Operations: []ast.AlterOperation{&ast.DropColumnOperation{ColumnName: `"qty"`}}},
			want: `ALTER TABLE ORDERS DROP COLUMN "qty";`,
		},
		{
			name: "a dropped constraint of a quoted table",
			node: &ast.AlterTableNode{Name: `"orders"`, Operations: []ast.AlterOperation{&ast.DropConstraintOperation{ConstraintName: `"ck_qty"`}}},
			want: `ALTER TABLE "orders" DROP CONSTRAINT "ck_qty";`,
		},
		{
			name: "a renamed column",
			node: &ast.AlterTableNode{Name: "ORDERS", Operations: []ast.AlterOperation{&ast.RenameColumnOperation{OldName: `"qty"`, NewName: "quantity"}}},
			want: `ALTER TABLE ORDERS RENAME COLUMN "qty" TO quantity;`,
		},
		{
			name: "a dropped index",
			node: ast.NewDropIndex(`"idx_qty"`),
			want: `DROP INDEX "idx_qty";`,
		},
		{
			name: "a keyword keeps the quotes it always had",
			node: &ast.AlterTableNode{Name: "ORDERS", Operations: []ast.AlterOperation{&ast.DropColumnOperation{ColumnName: "size"}}},
			want: `ALTER TABLE ORDERS DROP COLUMN "size";`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			sql := render(c, capability.ForDialect(platform.Oracle), test.node)

			c.Assert(sql, qt.Contains, test.want)
		})
	}
}
