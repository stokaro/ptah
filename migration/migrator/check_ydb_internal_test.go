package migrator

// White-box testing required: the rules are properties of one unexported
// validator, and the exported path that reaches it needs a YDB connection,
// which Ptah cannot open until the YDB driver lands (stokaro/ptah#4015, phase
// 4). The assertion is that the text is refused before any query is built,
// which is the guarantee a connection would not add to.

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

// A YDB check assertion is one read-only SELECT that reaches nothing outside
// the database. Each row is refused by one of the three readings the
// validator joins: the statement count, the YQL read-only proof, and the
// reach scan.
func TestValidateCheckAssertionStatically_YDB_FailurePath(t *testing.T) {
	tests := []struct {
		name      string
		assertion string
		wantErr   string
	}{
		{
			name:      "two statements",
			assertion: "SELECT 1; SELECT 2",
			wantErr:   `check assertion must be one read-only SELECT statement, got 2 statements`,
		},
		{
			name:      "a write",
			assertion: "UPSERT INTO `t` (id) VALUES (1)",
			wantErr:   `check assertion must be a read-only SELECT statement`,
		},
		{
			// The split keeps the string whole, so a semicolon inside it is
			// not a second statement, and the proof finds INTO.
			name:      "INTO RESULT",
			assertion: `SELECT "a;b" AS x INTO RESULT r`,
			wantErr: `check assertion must be one read-only SELECT statement: a YDB statement is proved read-only only ` +
				`when it is one SELECT, and INTO starts or carries a statement that is not a read`,
		},
		{
			name:      "a braced lambda",
			assertion: "SELECT ($x) -> { $y = $x; RETURN $y; }",
			wantErr: `check assertion must be one read-only SELECT statement: a YDB statement is proved read-only only ` +
				`when it is one SELECT, and a semicolon inside it starts another statement or a lambda body`,
		},
		{
			name:      "a UDF",
			assertion: `SELECT Python::f(@@def f(): pass@@)() = 1`,
			wantErr: `check assertion must not use YQL UDF call, which runs a user-defined function module, ` +
				`which may execute a script or reach outside the database`,
		},
		{
			name:      "an external source",
			assertion: "SELECT COUNT(*) = 0 FROM ext.`bucket/path`",
			wantErr: `check assertion must not use YQL external source, which reads a cluster or an external data source, ` +
				`which reaches object storage or another database`,
		},
		{
			// The setting is refused before either reading runs: the check
			// expands only executable comments it recognizes.
			name:      "a translation setting",
			assertion: "--!ansi_lexer\nSELECT 'a\\'; DELETE FROM t; --'",
			wantErr:   `check assertion contains an unrecognized SQL token`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(validateCheckAssertionStatically(test.assertion, "ydb", ""), qt.ErrorMatches, test.wantErr)
		})
	}
}

// Each read ran on YDB 26.2.1.14 and answered one value.
func TestValidateCheckAssertionStatically_YDB_HappyPath(t *testing.T) {
	for _, assertion := range []string{
		"SELECT COUNT(*) = 0 FROM `dir/t` WHERE id > 0",
		"SELECT (SELECT COUNT(*) FROM `dir/t`) = 0",
		"SELECT CASE WHEN EXISTS (SELECT * FROM `dir/t`) THEN 1 ELSE 0 END",
		`SELECT "a;b" = "a;b"`,
	} {
		c := qt.New(t)
		c.Assert(validateCheckAssertionStatically(assertion, "ydbs", ""), qt.IsNil, qt.Commentf("assertion %q", assertion))
	}
}
