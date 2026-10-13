package parser_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/internal/parser"
)

func TestParser_CommentNullRetainsTargetAndLiteralKind(t *testing.T) {
	tests := []struct{ sql, want string }{
		{"COMMENT ON TABLE t IS NULL;", "COMMENT ON TABLE t IS NULL"},
		{"COMMENT ON TABLE t IS null;", "COMMENT ON TABLE t IS NULL"},
		{"COMMENT ON FUNCTION app.f(is integer) IS NULL;", "COMMENT ON FUNCTION app.f(is integer) IS NULL"},
		{"COMMENT ON MATERIALIZED VIEW app.v IS NULL;", "COMMENT ON MATERIALIZED VIEW app.v IS NULL"},
		{"COMMENT ON TABLE t IS 'NULL';", "COMMENT ON TABLE t IS 'NULL'"},
	}
	for _, test := range tests {
		t.Run(test.sql, func(t *testing.T) {
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
