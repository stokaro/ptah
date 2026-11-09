package parser_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/internal/parser"
)

func TestParseColumnType_OracleTimestampWithLocalTimeZone(t *testing.T) {
	tests := []struct {
		name    string
		typeSQL string
	}{
		{name: "precision zero", typeSQL: "TIMESTAMP(0) WITH LOCAL TIME ZONE"},
		{name: "default precision", typeSQL: "TIMESTAMP WITH LOCAL TIME ZONE"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			statements, err := parser.NewParser(
				"CREATE TABLE t (a "+test.typeSQL+", b INTEGER)",
				parser.WithDialect(platform.Oracle),
			).Parse()

			c.Assert(err, qt.IsNil)
			c.Assert(statements.Statements, qt.HasLen, 1)
			table, ok := statements.Statements[0].(*ast.CreateTableNode)
			c.Assert(ok, qt.IsTrue)
			c.Assert(table.Columns, qt.HasLen, 2)
			c.Assert(table.Columns[0].Type, qt.Equals, test.typeSQL)
			c.Assert(table.Columns[1].Name, qt.Equals, "b")
		})
	}
}

func TestParseColumnType_PostgresRejectsOracleLocalTimeZone(t *testing.T) {
	c := qt.New(t)
	_, err := parser.NewParser(
		"CREATE TABLE t (a TIMESTAMP(0) WITH LOCAL TIME ZONE)",
		parser.WithDialect(platform.Postgres),
	).Parse()

	c.Assert(err, qt.IsNotNil)
}
