package parser_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/internal/parser"
)

// ALTER SEQUENCE ... OWNED BY, which pg_dump writes for the sequence of every
// serial column, reads into the sequence change that carries the owner, on
// each dialect whose columns own sequences and in a document of no dialect.
// NONE is the keyword, in whatever case it is written.
func TestParse_AlterSequenceOwnedBy_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name, dialect, sql            string
		wantSchema, wantName, wantOwn string
	}{
		{
			name: "pg_dump spelling", dialect: "postgres",
			sql:        "ALTER SEQUENCE public.s_int_id_seq OWNED BY public.s_int.id;",
			wantSchema: "public", wantName: "s_int_id_seq", wantOwn: "public.s_int.id",
		},
		{
			name: "quoted names", dialect: "postgres",
			sql:        `ALTER SEQUENCE public."Order_Id_seq" OWNED BY public."Order"."Id";`,
			wantSchema: "public", wantName: `"Order_Id_seq"`, wantOwn: `public."Order"."Id"`,
		},
		{
			name: "unqualified", dialect: "postgres",
			sql:      "ALTER SEQUENCE s OWNED BY t.id;",
			wantName: "s", wantOwn: "t.id",
		},
		{
			name: "NONE in lower case", dialect: "postgres",
			sql:      "alter sequence s owned by none;",
			wantName: "s", wantOwn: "NONE",
		},
		{
			name: "no semicolon at the end of the file", dialect: "postgres",
			sql:      "ALTER SEQUENCE s OWNED BY t.id",
			wantName: "s", wantOwn: "t.id",
		},
		{
			name: "CockroachDB", dialect: "cockroachdb",
			sql:      "ALTER SEQUENCE s OWNED BY t.id;",
			wantName: "s", wantOwn: "t.id",
		},
		{
			name: "YugabyteDB", dialect: "yugabytedb",
			sql:      "ALTER SEQUENCE s OWNED BY t.id;",
			wantName: "s", wantOwn: "t.id",
		},
		{
			name:     "no dialect",
			sql:      "ALTER SEQUENCE s OWNED BY t.id;",
			wantName: "s", wantOwn: "t.id",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := parser.NewParser(test.sql, parser.WithDialect(test.dialect)).Parse()

			c.Assert(err, qt.IsNil)
			c.Assert(statements.Statements, qt.DeepEquals, []ast.Node{
				ast.NewAlterSequence(test.wantName).SetSchema(test.wantSchema).SetOwnedBy(test.wantOwn),
			})
		})
	}
}

// A statement that stops short of an owner, says more after it, or takes
// another action keeps a refusal, and so does OWNED BY on a dialect whose
// columns own no sequence.
func TestParse_AlterSequenceOwnedBy_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name, dialect, sql, wantErr string
	}{
		{name: "no BY", dialect: "postgres", sql: "ALTER SEQUENCE s OWNED t.id;", wantErr: `(?s).*expected BY after ALTER SEQUENCE s OWNED.*`},
		{name: "no owner", dialect: "postgres", sql: "ALTER SEQUENCE s OWNED BY;", wantErr: `(?s).*OWNED BY target.*`},
		{name: "a clause after the owner", dialect: "postgres", sql: "ALTER SEQUENCE s OWNED BY t.id RESTART;", wantErr: `(?s).*ALTER SEQUENCE OWNED BY.*`},
		{name: "another action", dialect: "postgres", sql: "ALTER SEQUENCE s RESTART WITH 5;", wantErr: `(?s).*unsupported ALTER target: SEQUENCE`},
		{name: "OWNED BY under MariaDB", dialect: "mariadb", sql: "ALTER SEQUENCE s OWNED BY t.id;", wantErr: `(?s).*unsupported ALTER target: SEQUENCE`},
		{name: "OWNED BY under SQL Server", dialect: "sqlserver", sql: "ALTER SEQUENCE s OWNED BY t.id;", wantErr: `(?s).*unsupported ALTER target: SEQUENCE`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := parser.NewParser(test.sql, parser.WithDialect(test.dialect)).Parse()

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(statements, qt.IsNil)
		})
	}
}
