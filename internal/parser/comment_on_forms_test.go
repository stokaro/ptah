package parser_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/internal/parser"
)

// Every form pg_dump writes parses, and the target reaches the comment node
// whole: a two-word kind, an argument list and an ON clause each stay in the
// text for the schema reader to resolve (stokaro/ptah#3646).
func TestParser_CommentOnKeepsTheWholeTarget(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "a function with its argument list",
			sql:  `COMMENT ON FUNCTION app.f(a integer, b text) IS 'x';`,
			want: `COMMENT ON FUNCTION app.f(a integer, b text) IS 'x'`,
		},
		{
			name: "a two-word kind",
			sql:  `COMMENT ON MATERIALIZED VIEW app.m IS 'x';`,
			want: `COMMENT ON MATERIALIZED VIEW app.m IS 'x'`,
		},
		{
			name: "a trigger on its table",
			sql:  `COMMENT ON TRIGGER audit ON app.t IS 'x';`,
			want: `COMMENT ON TRIGGER audit ON app.t IS 'x'`,
		},
		{
			name: "a constraint on its table",
			sql:  `COMMENT ON CONSTRAINT positive ON app.t IS 'x';`,
			want: `COMMENT ON CONSTRAINT positive ON app.t IS 'x'`,
		},
		{
			// PostgreSQL takes `is` as a parameter name, unquoted: it is a
			// type_func_name_keyword. The IS that ends the target is the first
			// one outside every parenthesis.
			name: "an argument named is",
			sql:  `COMMENT ON FUNCTION app.f(is integer) IS 'x';`,
			want: `COMMENT ON FUNCTION app.f(is integer) IS 'x'`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := parser.NewParser(test.sql).Parse()

			c.Assert(err, qt.IsNil)
			c.Assert(statements.Statements, qt.HasLen, 1)
			comment, ok := statements.Statements[0].(*ast.CommentNode)
			c.Assert(ok, qt.IsTrue)
			c.Assert(comment.Text, qt.Equals, test.want)
		})
	}
}
