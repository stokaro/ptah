package builtin_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/capability"
	"ptah.run/engine/builtin"
	"ptah.run/internal/parser"
	"ptah.run/internal/sqlschema"
)

func TestRenderSQL_ParsedConstraintComment(t *testing.T) {
	for _, test := range []struct{ name, source, want, comment string }{
		{"sets", `CREATE TABLE t (a int, CONSTRAINT c CHECK (a > 0)); COMMENT ON CONSTRAINT c ON t IS 'it''s positive';`, `COMMENT ON CONSTRAINT "c" ON "t" IS 'it''s positive';`, "it's positive"},
		{"clears", `CREATE TABLE t (a int, CONSTRAINT c CHECK (a > 0)); COMMENT ON CONSTRAINT c ON t IS 'positive'; COMMENT ON CONSTRAINT c ON t IS NULL;`, `COMMENT ON CONSTRAINT "c" ON "t" IS NULL;`, ""},
		{"literal NULL", `CREATE TABLE t (a int, CONSTRAINT c CHECK (a > 0)); COMMENT ON CONSTRAINT c ON t IS 'NULL';`, `COMMENT ON CONSTRAINT "c" ON "t" IS 'NULL';`, "NULL"},
		{"after alter", `CREATE TABLE t (a int); ALTER TABLE t ADD CONSTRAINT c CHECK (a > 0); COMMENT ON CONSTRAINT c ON t IS 'positive';`, `COMMENT ON CONSTRAINT "c" ON "t" IS 'positive';`, "positive"},
		{"quoted identity", `CREATE TABLE "T.dot" (a int, CONSTRAINT "C.dot" CHECK (a > 0)); COMMENT ON CONSTRAINT "C.dot" ON "T.dot" IS 'quoted';`, `COMMENT ON CONSTRAINT "C.dot" ON "T.dot" IS 'quoted';`, "quoted"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			parsed, err := parser.NewParser(test.source, parser.WithDialect("postgres")).ParseContext(t.Context())
			c.Assert(err, qt.IsNil)
			rendered, err := builtin.RenderSQLWithCapabilities("postgres", capability.Postgres18(), parsed)
			c.Assert(err, qt.IsNil)
			c.Assert(rendered, qt.Contains, "\n"+test.want+"\n")
			model, _, err := sqlschema.Read([]byte(rendered), "postgres")
			c.Assert(err, qt.IsNil)
			c.Assert(model.Constraints, qt.HasLen, 1)
			c.Assert(model.Constraints[0].Comment, qt.Equals, test.comment)
		})
	}
}

func TestRenderSQL_LexicalCommentStaysInert(t *testing.T) {
	c := qt.New(t)
	parsed, err := parser.NewParser("-- COMMENT ON CONSTRAINT c ON t IS 'inert';\nCREATE TABLE t (a int);", parser.WithDialect("postgres")).ParseContext(t.Context())
	c.Assert(err, qt.IsNil)
	rendered, err := builtin.RenderSQLWithCapabilities("postgres", capability.Postgres18(), parsed)
	c.Assert(err, qt.IsNil)
	c.Assert(rendered, qt.Not(qt.Contains), "\nCOMMENT ON CONSTRAINT")
}
