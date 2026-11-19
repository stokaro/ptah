package mysqllike_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/engine/builtin/internal/dialects/internal/bufwriter"
	"ptah.run/engine/builtin/internal/dialects/mysqllike"
)

// An expression default is written in parentheses, which MySQL requires around
// one: `DEFAULT 1 + 2` is ERROR 1064 and `DEFAULT now()` on a varchar ERROR
// 1067. What the server takes bare is written bare, so the catalog reports a
// literal as a literal and a current-time default as it did.
func TestVisitNode_CreateTableEnclosesAnExpressionDefault(t *testing.T) {
	tests := []struct {
		name, columnType, expression string
		want                         string
	}{
		{name: "an operation", columnType: "VARCHAR(30)", expression: "1 + 2", want: "`c` VARCHAR(30) DEFAULT (1 + 2)"},
		{name: "a call on a string column", columnType: "VARCHAR(30)", expression: "now()", want: "`c` VARCHAR(30) DEFAULT (now())"},
		{name: "a call", columnType: "VARCHAR(36)", expression: "uuid()", want: "`c` VARCHAR(36) DEFAULT (uuid())"},
		{name: "two groups", columnType: "INT", expression: "(1) + (2)", want: "`c` INT DEFAULT ((1) + (2))"},
		{name: "an enclosed expression", columnType: "VARCHAR(30)", expression: "(1 + 2)", want: "`c` VARCHAR(30) DEFAULT (1 + 2)"},
		{name: "a current-time function on a timestamp", columnType: "TIMESTAMP", expression: "CURRENT_TIMESTAMP", want: "`c` TIMESTAMP DEFAULT CURRENT_TIMESTAMP"},
		{name: "a call to now on a datetime", columnType: "DATETIME", expression: "NOW()", want: "`c` DATETIME DEFAULT NOW()"},
		{name: "a number", columnType: "INT", expression: "-1", want: "`c` INT DEFAULT -1"},
		{name: "a boolean", columnType: "BOOLEAN", expression: "true", want: "`c` BOOLEAN DEFAULT true"},
		{name: "a quoted string", columnType: "VARCHAR(30)", expression: "'x'", want: "`c` VARCHAR(30) DEFAULT 'x'"},
		{name: "NULL", columnType: "VARCHAR(30)", expression: "NULL", want: "`c` VARCHAR(30) DEFAULT NULL"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			column := ast.NewColumn("c", test.columnType)
			column.SetDefaultExpression(test.expression)

			got := renderNode(c, &ast.CreateTableNode{Name: "notes", Columns: []*ast.ColumnNode{column}})

			c.Assert(got, qt.Contains, test.want)
		})
	}
}

// The column an ALTER rewrites takes the same parentheses: a plan that changes
// a literal default to the expression writes MODIFY COLUMN.
func TestVisitNode_ModifyColumnEnclosesAnExpressionDefault(t *testing.T) {
	c := qt.New(t)
	column := ast.NewColumn("c", "VARCHAR(30)")
	column.SetDefaultExpression("1 + 2")

	got := renderNode(c, &ast.AlterTableNode{
		Name:       "notes",
		Operations: []ast.AlterOperation{&ast.ModifyColumnOperation{Column: column}},
	})

	c.Assert(got, qt.Contains, "MODIFY COLUMN `c` VARCHAR(30) DEFAULT (1 + 2);")
}

func renderNode(c *qt.C, node ast.Node) string {
	c.Helper()
	writer := &bufwriter.Writer{}
	renderer := mysqllike.NewWithCapabilities("mysql", writer, capability.ForDialect("mysql"))

	c.Assert(renderer.VisitNode(node), qt.IsNil)

	return renderer.GetOutput()
}
