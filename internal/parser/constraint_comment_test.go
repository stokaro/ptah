package parser_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/internal/parser"
)

func TestParser_ConstraintCommentOperation(t *testing.T) {
	for _, test := range []struct{ sql, table, name, comment string }{
		{`COMMENT ON CONSTRAINT c ON app.t IS 'note';`, "app.t", "c", "note"},
		{`COMMENT ON CONSTRAINT "C.dot" ON "S"."T.dot" IS 'it''s checked';`, `"S"."T.dot"`, `"C.dot"`, "it's checked"},
		{`COMMENT ON CONSTRAINT c ON t IS NULL;`, "t", "c", ""},
		{`COMMENT ON CONSTRAINT c ON t IS 'NULL';`, "t", "c", "NULL"},
		{`comment on constraint c on t is $q$a note$q$;`, "t", "c", "a note"},
	} {
		t.Run(test.sql, func(t *testing.T) {
			c := qt.New(t)
			parsed, err := parser.NewParser(test.sql, parser.WithDialect("postgres")).Parse()
			c.Assert(err, qt.IsNil)
			c.Assert(parsed.Statements, qt.HasLen, 1)
			node, ok := parsed.Statements[0].(*ast.AlterTableNode)
			c.Assert(ok, qt.IsTrue)
			c.Assert(node.Name, qt.Equals, test.table)
			c.Assert(node.Operations, qt.DeepEquals, []ast.AlterOperation{&ast.SetConstraintCommentOperation{Constraint: test.name, Comment: test.comment}})
		})
	}
}

func TestParser_ConstraintCommentRefusesMalformedTarget(t *testing.T) {
	for _, source := range []string{
		`COMMENT ON CONSTRAINT c t IS 'note';`,
		`COMMENT ON CONSTRAINT c ON t extra IS 'note';`,
		`COMMENT ON CONSTRAINT c ON IS 'note';`,
	} {
		t.Run(source, func(t *testing.T) {
			c := qt.New(t)
			_, err := parser.NewParser(source, parser.WithDialect("postgres")).Parse()
			c.Assert(err, qt.ErrorMatches, "(?s).*COMMENT ON CONSTRAINT.*")
		})
	}
}
