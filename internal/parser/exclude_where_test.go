package parser_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/internal/parser"
)

// TestParse_ExcludeWhereKeepsItsOperators reads an EXCLUDE's WHERE clause as it
// was written. The lexer reads an operator one character at a time, and a
// space put between every two tokens turns `n >= 0` into `n > = 0` and `t <>
// 'x'` into `t < > 'x'`: conditions PostgreSQL 18.6 refuses with a syntax
// error, so a plan that added the constraint failed, and the server probe
// that compares it could never parse it (stokaro/ptah#3767).
func TestParse_ExcludeWhereKeepsItsOperators(t *testing.T) {
	tests := []struct {
		name  string
		where string
		want  string
	}{
		{name: "two-character comparisons", where: "n >= 0 AND t <> 'x'", want: "n >= 0 AND t <> 'x'"},
		{name: "a cast", where: "t::text <> ''", want: "t::text <> ''"},
		{name: "a run of whitespace", where: "n  >=\n 0", want: "n >= 0"},
		{name: "a nested condition", where: "(n > 0) OR (t IS NULL)", want: "(n > 0) OR (t IS NULL)"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := parser.NewParser(
				"CREATE TABLE ex (n int, t text, EXCLUDE USING btree (n WITH =) WHERE (" + test.where + "));",
			).Parse()

			c.Assert(err, qt.IsNil)
			table := statements.Statements[0].(*ast.CreateTableNode)
			c.Assert(table.Constraints, qt.HasLen, 1)
			c.Assert(table.Constraints[0].WhereCondition, qt.Equals, test.want)
		})
	}
}
