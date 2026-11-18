package parser_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/parser"
)

// parseNiladicDefault parses a CREATE TABLE with one column after its id, read
// as dialect.
func parseNiladicDefault(dialect, column string) (*ast.StatementList, error) {
	sql := "CREATE TABLE t (id NUMBER, " + column + ", z NUMBER);"
	return parser.NewParser(sql, parser.WithDialect(dialect), parser.WithSQL(builtintest.Runtime().SQL())).Parse()
}

// A function a default calls without parentheses is kept as the expression
// written, where the dialect's descriptor names it. Read as a value, Oracle's
// SYSDATE would render as the string 'SYSDATE'. A precision is an argument,
// not an empty list, and Oracle calls SESSIONTIMEZONE() as well. PostgreSQL names no such function, so SYSDATE is a word,
// and CockroachDB calls current_date() with parentheses.
func TestParse_NiladicDefault_HappyPath(t *testing.T) {
	rows := []struct {
		name, dialect, column string
		wantExpression        string
		wantValue             string
	}{
		{name: "Oracle, SYSDATE", dialect: "oracle", column: "a DATE DEFAULT SYSDATE", wantExpression: "SYSDATE"},
		{name: "Oracle, sysdate", dialect: "oracle", column: "a DATE DEFAULT sysdate", wantExpression: "sysdate"},
		{name: "Oracle, UID", dialect: "oracle", column: "a NUMBER DEFAULT UID", wantExpression: "UID"},
		{
			name: "Oracle, SESSIONTIMEZONE", dialect: "oracle",
			column: "a VARCHAR2(64) DEFAULT SESSIONTIMEZONE", wantExpression: "SESSIONTIMEZONE",
		},
		{
			name: "Oracle, ORA_INVOKING_USERID", dialect: "oracle",
			column: "a NUMBER DEFAULT ORA_INVOKING_USERID", wantExpression: "ORA_INVOKING_USERID",
		},
		{
			name: "Oracle, a precision", dialect: "oracle",
			column: "a TIMESTAMP DEFAULT SYSTIMESTAMP(3)", wantExpression: "SYSTIMESTAMP(3)",
		},
		{
			name: "Oracle, a current-time keyword with a precision", dialect: "oracle",
			column: "a TIMESTAMP DEFAULT CURRENT_TIMESTAMP(3)", wantExpression: "CURRENT_TIMESTAMP(3)",
		},
		{
			name: "Oracle, SESSIONTIMEZONE()", dialect: "oracle",
			column: "a VARCHAR2(64) DEFAULT SESSIONTIMEZONE()", wantExpression: "SESSIONTIMEZONE()",
		},
		{name: "PostgreSQL, SYSDATE", dialect: "postgres", column: "a text DEFAULT SYSDATE", wantValue: "SYSDATE"},
		{
			name: "CockroachDB, current_date()", dialect: "cockroachdb",
			column: "a date DEFAULT current_date()", wantExpression: "current_date()",
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

// Oracle answers ORA-03077 to SYSDATE() and USER(), ORA-30088 to
// SYSTIMESTAMP() and ORA-04044 to DBTIMEZONE(). The default is refused at the
// parenthesis, and the refusal names the bare spelling as written.
// SESSIONTIMEZONE(), which Oracle calls, passes.
func TestParse_NiladicDefault_FailurePath(t *testing.T) {
	const oracle = `Oracle Free 23\.26 calls it without parentheses and answers ORA-03077, ORA-30088 or ORA-04044 ` +
		`to an empty argument list`
	rows := []struct {
		name, column, wantErr string
	}{
		{
			name: "SYSDATE()", column: "a DATE DEFAULT SYSDATE()",
			wantErr: `line 1, column 50: expected default value: SYSDATE\(\) writes an empty argument list: write DEFAULT SYSDATE; ` + oracle,
		},
		{
			name: "sysdate( )", column: "a DATE DEFAULT sysdate( )",
			wantErr: `line 1, column 50: expected default value: sysdate\(\) writes an empty argument list: write DEFAULT sysdate; ` + oracle,
		},
		{
			name: "CURRENT_DATE()", column: "a DATE DEFAULT CURRENT_DATE()",
			wantErr: `line 1, column 55: expected default value: CURRENT_DATE\(\) writes an empty argument list: write DEFAULT CURRENT_DATE; ` + oracle,
		},
		{
			name: "SYSTIMESTAMP()", column: "a TIMESTAMP DEFAULT SYSTIMESTAMP()",
			wantErr: `line 1, column 60: expected default value: SYSTIMESTAMP\(\) writes an empty argument list: write DEFAULT SYSTIMESTAMP; ` + oracle,
		},
		{
			name: "DBTIMEZONE()", column: "a VARCHAR2(64) DEFAULT DBTIMEZONE()",
			wantErr: `line 1, column 61: expected default value: DBTIMEZONE\(\) writes an empty argument list: write DEFAULT DBTIMEZONE; ` + oracle,
		},
		{
			name: "USER()", column: "a VARCHAR2(128) DEFAULT USER()",
			wantErr: `line 1, column 56: expected default value: USER\(\) writes an empty argument list: write DEFAULT USER; ` + oracle,
		},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := parseNiladicDefault("oracle", row.column)

			c.Assert(err, qt.ErrorMatches, row.wantErr)
			c.Assert(statements, qt.IsNil)
		})
	}
}
