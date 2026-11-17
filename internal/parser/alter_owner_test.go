package parser_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/internal/parser"
)

// An ownership statement of a kind whose owner the dialect does not manage
// contributes no node: the table before it is the only statement the parse
// returns.
func TestParse_AlterOwnerTo_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name string
		sql  string
	}{
		{name: "table", sql: "ALTER TABLE public.t OWNER TO postgres;"},
		{name: "table, unqualified", sql: "ALTER TABLE t OWNER TO postgres;"},
		{name: "table, ONLY", sql: "ALTER TABLE ONLY public.t OWNER TO postgres;"},
		{name: "table, quoted names", sql: `ALTER TABLE "App"."T" OWNER TO "Role Name";`},
		{name: "schema", sql: "ALTER SCHEMA app OWNER TO postgres;"},
		{name: "type", sql: "ALTER TYPE app.mood OWNER TO postgres;"},
		{name: "domain", sql: "ALTER DOMAIN app.d OWNER TO postgres;"},
		{name: "function with no arguments", sql: "ALTER FUNCTION app.f() OWNER TO postgres;"},
		{name: "function with an argument type", sql: "ALTER FUNCTION app.f(integer) OWNER TO r;"},
		{name: "function with named and multi-word arguments", sql: "ALTER FUNCTION app.g(a integer, b character varying, OUT c timestamp with time zone) OWNER TO r;"},
		{name: "function with an array argument", sql: "ALTER FUNCTION app.h(integer[], app.mood) OWNER TO r;"},
		{name: "function with no argument list", sql: "ALTER FUNCTION app.f OWNER TO r;"},
		{name: "procedure with arguments", sql: "ALTER PROCEDURE app.p(integer) OWNER TO r;"},
		{name: "sequence", sql: "ALTER SEQUENCE app.seq OWNER TO postgres;"},
		{name: "role keyword", sql: "ALTER TABLE t OWNER TO CURRENT_USER;"},
		{name: "no semicolon at the end of the file", sql: "ALTER TABLE t OWNER TO postgres"},
		{name: "lower case", sql: "alter table t owner to postgres;"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := parser.NewParser("CREATE TABLE t (id int);\n"+test.sql+"\n",
				parser.WithDialect("postgres")).Parse()

			c.Assert(err, qt.IsNil)
			c.Assert(statements.Statements, qt.HasLen, 1)
			_, isTable := statements.Statements[0].(*ast.CreateTableNode)
			c.Assert(isTable, qt.IsTrue)
		})
	}
}

// An ownership statement of a view or a materialized view, whose owner
// PostgreSQL's descriptor lists as managed, is a node naming the object and
// the role as the statement writes them; deciding what the role's name is
// belongs to the schema reader.
func TestParse_AlterOwnerTo_ManagedKind_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name, sql string
		want      ast.AlterOwnerNode
	}{
		{name: "view", sql: "ALTER VIEW app.v OWNER TO reporting;", want: ast.AlterOwnerNode{Kind: "VIEW", Name: "app.v", Owner: "reporting"}},
		{name: "view, unqualified", sql: "ALTER VIEW v OWNER TO reporting;", want: ast.AlterOwnerNode{Kind: "VIEW", Name: "v", Owner: "reporting"}},
		{name: "view, quoted names", sql: `ALTER VIEW "App"."V" OWNER TO "Report Role";`, want: ast.AlterOwnerNode{Kind: "VIEW", Name: `"App"."V"`, Owner: `"Report Role"`}},
		{name: "materialized view", sql: "ALTER MATERIALIZED VIEW app.mv OWNER TO reporting;", want: ast.AlterOwnerNode{Kind: "MATERIALIZED VIEW", Name: "app.mv", Owner: "reporting"}},
		{name: "lower case", sql: "alter materialized view mv owner to Reporting;", want: ast.AlterOwnerNode{Kind: "MATERIALIZED VIEW", Name: "mv", Owner: "Reporting"}},
		{name: "role keyword", sql: "ALTER VIEW v OWNER TO CURRENT_USER;", want: ast.AlterOwnerNode{Kind: "VIEW", Name: "v", Owner: "CURRENT_USER"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := parser.NewParser(test.sql+"\n", parser.WithDialect("postgres")).Parse()

			c.Assert(err, qt.IsNil)
			c.Assert(statements.Statements, qt.HasLen, 1)
			node, isOwner := statements.Statements[0].(*ast.AlterOwnerNode)
			c.Assert(isOwner, qt.IsTrue)
			c.Assert(*node, qt.DeepEquals, test.want)
		})
	}
}

// CockroachDB and YugabyteDB manage the owner of a view too, and read the
// statement of no other kind.
func TestParse_AlterOwnerTo_PostgresFamily_HappyPath(t *testing.T) {
	for _, dialect := range []string{"cockroachdb", "yugabytedb"} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)

			statements, err := parser.NewParser("ALTER MATERIALIZED VIEW mv OWNER TO reporting;\n", parser.WithDialect(dialect)).Parse()

			c.Assert(err, qt.IsNil)
			c.Assert(statements.Statements, qt.HasLen, 1)
			node, isOwner := statements.Statements[0].(*ast.AlterOwnerNode)
			c.Assert(isOwner, qt.IsTrue)
			c.Assert(*node, qt.DeepEquals, ast.AlterOwnerNode{Kind: "MATERIALIZED VIEW", Name: "mv", Owner: "reporting"})
		})
	}
}

// A document of no dialect takes the spelling of any engine.
func TestParse_AlterOwnerTo_NoDialect_HappyPath(t *testing.T) {
	c := qt.New(t)

	statements, err := parser.NewParser("ALTER SCHEMA app OWNER TO postgres;\nALTER TABLE app.t OWNER TO postgres;\n").Parse()

	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 0)
}

// A statement that stops short of a role, names something after it, or takes
// another action keeps its refusal.
func TestParse_AlterOwnerTo_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name, sql, wantErr string
	}{
		{name: "table, no TO", sql: "ALTER TABLE t OWNER postgres;", wantErr: `(?s).*expected TO after ALTER TABLE OWNER.*`},
		{name: "table, no role", sql: "ALTER TABLE t OWNER TO;", wantErr: `(?s).*expected the role after ALTER TABLE OWNER TO.*`},
		{name: "table, a number for the role", sql: "ALTER TABLE t OWNER TO 1;", wantErr: `(?s).*expected the role after ALTER TABLE OWNER TO, got "1".*`},
		{name: "table, a bracketed role", sql: "ALTER TABLE t OWNER TO [r];", wantErr: `(?s).*expected the role after ALTER TABLE OWNER TO, got "\[".*`},
		{name: "table, clause after the role", sql: "ALTER TABLE t OWNER TO r SET SCHEMA s;", wantErr: `(?s).*unsupported ALTER TABLE OWNER TO clause: SET`},
		{name: "table, owner in a list of actions", sql: "ALTER TABLE t OWNER TO r, ADD COLUMN b int;", wantErr: `(?s).*expected the end of ALTER TABLE OWNER TO.*`},
		{name: "schema, no role", sql: "ALTER SCHEMA app OWNER TO;", wantErr: `(?s).*expected the role after ALTER SCHEMA OWNER TO.*`},
		{name: "function, unterminated arguments", sql: "ALTER FUNCTION app.f(integer OWNER TO r;", wantErr: `(?s).*routine arguments.*`},
		{name: "function, role missing after the arguments", sql: "ALTER FUNCTION app.f(integer) OWNER TO;", wantErr: `(?s).*expected the role after ALTER FUNCTION OWNER TO.*`},
		{name: "schema, another action", sql: "ALTER SCHEMA app RENAME TO other;", wantErr: `(?s).*unsupported ALTER target: SCHEMA`},
		{name: "type, another action", sql: "ALTER TYPE app.mood ADD VALUE 'x';", wantErr: `(?s).*unsupported ALTER target: TYPE`},
		{name: "sequence, another action", sql: "ALTER SEQUENCE app.seq RENAME TO other;", wantErr: `(?s).*unsupported ALTER target: SEQUENCE`},
		{name: "view, another action", sql: "ALTER VIEW app.v RENAME TO w;", wantErr: `(?s).*unsupported ALTER target: VIEW`},
		{name: "materialized without view", sql: "ALTER MATERIALIZED mv OWNER TO r;", wantErr: `(?s).*unsupported ALTER target: MATERIALIZED`},
		{name: "a kind that is not read", sql: "ALTER COLLATION app.c OWNER TO r;", wantErr: `(?s).*unsupported ALTER target: COLLATION`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := parser.NewParser(test.sql, parser.WithDialect("postgres")).Parse()

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(statements, qt.IsNil)
		})
	}
}

// OWNER TO is not MySQL's, SQLite's or SQL Server's, so a document in one of
// those dialects keeps the refusal it had: reading it there would accept what
// the engine's own server refuses.
func TestParse_AlterOwnerTo_OtherDialects_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name, dialect, sql, wantErr string
	}{
		{name: "mysql table", dialect: "mysql", sql: "ALTER TABLE t OWNER TO r;", wantErr: `(?s).*unsupported ALTER operation: OWNER`},
		{name: "mysql schema", dialect: "mysql", sql: "ALTER SCHEMA app OWNER TO r;", wantErr: `(?s).*unsupported ALTER target: SCHEMA`},
		{name: "sqlite table", dialect: "sqlite", sql: "ALTER TABLE t OWNER TO r;", wantErr: `(?s).*unsupported ALTER operation: OWNER`},
		{name: "sqlserver view", dialect: "sqlserver", sql: "ALTER VIEW v OWNER TO r;", wantErr: `(?s).*unsupported ALTER target: VIEW`},
		{name: "clickhouse table", dialect: "clickhouse", sql: "ALTER TABLE t OWNER TO r;", wantErr: `(?s).*unsupported ALTER operation: OWNER`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := parser.NewParser(test.sql, parser.WithDialect(test.dialect)).Parse()

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(statements, qt.IsNil)
		})
	}
}
