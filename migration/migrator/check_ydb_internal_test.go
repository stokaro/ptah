package migrator

// White-box testing required: the rules are properties of one unexported
// validator, and the exported path that reaches it needs a YDB connection,
// which Ptah cannot open. The assertion is that the text is refused before
// any query is built, which is the guarantee a connection would not add to.

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

const (
	ydbCheckNotARead = `check assertion must be one read-only SELECT statement: a YDB statement is proved read-only ` +
		`only when it is one SELECT`
	ydbCheckExternalSource = `check assertion must not use YQL external source, which reads a cluster or an external ` +
		`data source: object storage or another database`
)

// A YDB check assertion is one read-only SELECT that reaches nothing outside
// the database. Each row is refused by one of the three readings the
// validator joins: the statement count, the reach scan, and the YQL
// read-only proof, in that order.
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
			wantErr:   ydbCheckNotARead + `, and INTO starts or carries a statement that is not a read`,
		},
		{
			name:      "a braced lambda",
			assertion: "SELECT ($x) -> { $y = $x; RETURN $y; }",
			wantErr:   ydbCheckNotARead + `, and a semicolon inside it starts another statement or a lambda body`,
		},
		{
			name:      "a UDF",
			assertion: `SELECT Python::f(@@def f(): pass@@)() = 1`,
			wantErr: `check assertion must not use YQL UDF call, which runs a user-defined function module, ` +
				`and a module may execute a script or reach outside the database`,
		},
		{
			name:      "an external source",
			assertion: "SELECT COUNT(*) = 0 FROM ext.`bucket/path`",
			wantErr:   ydbCheckExternalSource,
		},
		{
			// SELECT 1uFROM `dir/t` read `1u FROM` on YDB 26.2.1.14.
			name:      "a source after a suffixed number",
			assertion: "SELECT 1uFROM ext.`bucket/path`",
			wantErr:   ydbCheckExternalSource,
		},
		{
			name:      "a source after a suffixed zero",
			assertion: "SELECT COUNT(*) = 0lFROM ext.`bucket/path`",
			wantErr:   ydbCheckExternalSource,
		},
		{
			name:      "a source after ANY",
			assertion: "SELECT * FROM ANY ext.`bucket/path` AS a JOIN `t` AS b ON a.id = b.id",
			wantErr:   ydbCheckExternalSource,
		},
		{
			name:      "a join after a suffixed number",
			assertion: "SELECT * FROM `t` AS a JOIN `t` AS b ON a.id = b.id + 0uJOIN ext.`p` AS c ON a.id = c.id",
			wantErr:   ydbCheckExternalSource,
		},
		{
			name:      "a COMBINE source",
			assertion: "SELECT 1 UNION ALL COMBINE ext.`p` WITH `t` ON 1 USING EXTERNAL FUNCTION(\"YANDEX-CLOUD\", \"f\")",
			wantErr:   ydbCheckExternalSource,
		},
		{
			name:      "a service prefix",
			assertion: "SELECT * FROM ext:dir.`t`",
			wantErr:   ydbCheckExternalSource,
		},
		{
			name:      "a file read in a join condition",
			assertion: "SELECT * FROM `t` AS a JOIN `t` AS b ON FileContent(\"secret\") = a.v",
			wantErr: `check assertion must not use YQL file or secret function, which reads a file attached to ` +
				`the query, a folder listing or a secret`,
		},
		{
			name:      "a file listing",
			assertion: `SELECT ListLength(Files("dir"))`,
			wantErr: `check assertion must not use YQL file or secret function, which reads a file attached to ` +
				`the query, a folder listing or a secret`,
		},
		{
			name:      "evaluated code in a join condition",
			assertion: "SELECT * FROM `t` AS a JOIN `t` AS b ON EvaluateCode(QuoteCode(1)) = a.v",
			wantErr: `check assertion must not use YQL code or UDF function, which evaluates code built at run ` +
				`time or calls a user-defined function by name`,
		},
		{
			// Measured: PgCall('version') runs, and pg_read_file answers
			// `No access to proc: pg_read_file`.
			name:      "a PostgreSQL function",
			assertion: "SELECT PgCall('pg_read_file', '/etc/passwd'p)",
			wantErr: `check assertion must not use YQL PostgreSQL function, which runs a PostgreSQL function ` +
				`inside YDB, and the server alone decides which functions may run`,
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
		`SELECT JSON_VALUE(CAST(@@{"a": 1}@@ AS Json), "$.a" RETURNING Int32)`,
	} {
		c := qt.New(t)
		c.Assert(validateCheckAssertionStatically(assertion, "ydbs", ""), qt.IsNil, qt.Commentf("assertion %q", assertion))
	}
}
