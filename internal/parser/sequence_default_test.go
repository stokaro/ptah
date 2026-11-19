package parser_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
)

// A default that reads a sequence pseudocolumn the dialect's descriptor names
// is kept as the expression written, schema-qualified or not, with each name
// bare or double-quoted, including the quoted form Oracle stores. Read as a
// value, seq.NEXTVAL renders as the string 'seq.NEXTVAL', and a default
// opening with a quoted name is refused at the dot. A string stays a string,
// an argument list makes the words a call, and a name that is no pseudocolumn
// stays a word. PostgreSQL names no pseudocolumn, so seq.NEXTVAL is a word
// there. SQL Server's descriptor names the NEXT VALUE FOR clause, which is
// kept as written with or without parentheses; without the field, NEXT is a
// word and VALUE is refused as a column attribute.
func TestParse_SequenceDefault_HappyPath(t *testing.T) {
	rows := []struct {
		name, dialect, column string
		wantExpression        string
		wantValue             string
	}{
		{name: "Oracle, NEXTVAL", dialect: "oracle", column: "a NUMBER DEFAULT seq.NEXTVAL", wantExpression: "seq.NEXTVAL"},
		{name: "Oracle, CURRVAL", dialect: "oracle", column: "a NUMBER DEFAULT seq.currval", wantExpression: "seq.currval"},
		{
			name: "Oracle, a schema", dialect: "oracle",
			column: "a NUMBER DEFAULT app.seq.NEXTVAL", wantExpression: "app.seq.NEXTVAL",
		},
		{
			name: "Oracle, a quoted sequence", dialect: "oracle",
			column: `a NUMBER DEFAULT "Seq".NEXTVAL`, wantExpression: `"Seq".NEXTVAL`,
		},
		{
			name: "Oracle, a schema and a quoted sequence", dialect: "oracle",
			column: `a NUMBER DEFAULT app."Seq".nextval`, wantExpression: `app."Seq".nextval`,
		},
		{
			name: "Oracle, the stored form", dialect: "oracle",
			column: `a NUMBER DEFAULT "APP"."SEQ"."NEXTVAL"`, wantExpression: `"APP"."SEQ"."NEXTVAL"`,
		},
		{
			name: "Oracle, an operator after it", dialect: "oracle",
			column: "a NUMBER DEFAULT seq.NEXTVAL + 1", wantExpression: "seq.NEXTVAL + 1",
		},
		{
			name: "Oracle, a string", dialect: "oracle",
			column: "a VARCHAR2(20) DEFAULT 'seq.NEXTVAL'", wantValue: "'seq.NEXTVAL'",
		},
		{
			name: "Oracle, a call", dialect: "oracle",
			column: "a NUMBER DEFAULT pkg.nextval(1)", wantExpression: "pkg.nextval(1)",
		},
		{
			name: "Oracle, a quoted pseudocolumn in lower case", dialect: "oracle",
			column: `a NUMBER DEFAULT seq."nextval"`, wantValue: `seq."nextval"`,
		},
		{name: "PostgreSQL, NEXTVAL", dialect: "postgres", column: "a bigint DEFAULT seq.NEXTVAL", wantValue: "seq.NEXTVAL"},
		{
			name: "SQL Server, NEXT VALUE FOR", dialect: "sqlserver",
			column: "a bigint DEFAULT NEXT VALUE FOR seq", wantExpression: "NEXT VALUE FOR seq",
		},
		{
			name: "SQL Server, in lower case with a schema", dialect: "sqlserver",
			column: "a bigint DEFAULT next value for dbo.seq", wantExpression: "next value for dbo.seq",
		},
		{
			name: "SQL Server, bracketed", dialect: "sqlserver",
			column: "a bigint DEFAULT NEXT VALUE FOR [dbo].[seq]", wantExpression: "NEXT VALUE FOR [dbo].[seq]",
		},
		{
			name: "SQL Server, an operator after it", dialect: "sqlserver",
			column: "a bigint DEFAULT NEXT VALUE FOR seq + 1", wantExpression: "NEXT VALUE FOR seq + 1",
		},
		{
			name: "SQL Server, in parentheses", dialect: "sqlserver",
			column: "a bigint DEFAULT (NEXT VALUE FOR seq)", wantExpression: "(NEXT VALUE FOR seq)",
		},
		{
			name: "SQL Server, a string", dialect: "sqlserver",
			column: "a varchar(30) DEFAULT 'NEXT VALUE FOR seq'", wantValue: "'NEXT VALUE FOR seq'",
		},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := parseNiladicDefault(row.dialect, row.column)

			c.Assert(err, qt.IsNil)
			table, ok := statements.Statements[0].(*ast.CreateTableNode)
			c.Assert(ok, qt.IsTrue)
			c.Assert(table.Columns, qt.HasLen, 3)
			c.Assert(table.Columns[1].Default, qt.IsNotNil)
			c.Assert(table.Columns[1].Default.Expression, qt.Equals, row.wantExpression)
			c.Assert(table.Columns[1].Default.Value, qt.Equals, row.wantValue)
		})
	}
}
