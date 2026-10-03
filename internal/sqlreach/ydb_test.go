package sqlreach_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/internal/sqlreach"
)

// Each statement reaches past the database a YDB plan or assertion is sent to,
// or changes how the server reads the text so that what it runs cannot be
// judged. Every one is refused on YDB.
func TestScan_YDB_RefusesWhatReachesOutside(t *testing.T) {
	tests := []struct {
		name      string
		statement string
		construct string
	}{
		{name: "an ANSI lexer setting", statement: "--!ansi_lexer\nSELECT 'a\\'; DELETE FROM t; --'", construct: "YQL translation setting"},
		{name: "a syntax setting after whitespace", statement: "  --!syntax_v1\nSELECT 1", construct: "YQL translation setting"},
		{name: "a pragma", statement: `PRAGMA TablePathPrefix = "/local/other"`, construct: "PRAGMA"},
		{name: "a pragma fetching a file", statement: `PRAGMA File("x", "https://example.invalid/x")`, construct: "PRAGMA"},
		{name: "a pragma inside an action", statement: "DEFINE ACTION $a() AS PRAGMA Library(\"l\"); SELECT 1; END DEFINE", construct: "PRAGMA"},
		{name: "a Python UDF", statement: "SELECT Python::f(@@def f(): pass@@)()", construct: "YQL UDF call"},
		{name: "a JavaScript UDF", statement: "SELECT JavaScript::f(@@function f() {}@@)()", construct: "YQL UDF call"},
		{name: "a native UDF module", statement: `SELECT String::Contains("abc", "b")`, construct: "YQL UDF call"},
		{name: "a backticked UDF module", statement: "SELECT `String`::Contains(\"abc\", \"b\")", construct: "YQL UDF call"},
		{name: "a UDF in a lambda", statement: "SELECT ListMap(AsList(1), ($x) -> (Url::Encode($x)))", construct: "YQL UDF call"},
		{name: "a file read", statement: `SELECT FileContent("secrets")`, construct: "YQL file or secret function"},
		{name: "a file listing", statement: `SELECT ListLength(Files("dir"))`, construct: "YQL file or secret function"},
		{name: "a secret", statement: `SELECT SecureParam("token:default")`, construct: "YQL file or secret function"},
		{name: "a folder listing", statement: `SELECT * FROM FOLDER("dir")`, construct: "YQL file or secret function"},
		// calledFunction exempts a name after ON, where DDL names an object.
		// In a YQL join, ON starts a condition, and a call there runs.
		{name: "a file read in a join condition", statement: "SELECT * FROM `t` AS a JOIN `t` AS b ON FileContent(\"secret\") = a.v", construct: "YQL file or secret function"},
		{name: "evaluated code", statement: `SELECT EvaluateCode(QuoteCode(1))`, construct: "YQL code or UDF function"},
		{name: "evaluated code in a join condition", statement: "SELECT * FROM `t` AS a JOIN `t` AS b ON EvaluateCode(QuoteCode(1)) = a.v", construct: "YQL code or UDF function"},
		{name: "any Evaluate builtin", statement: "SELECT EVALUATEEXPR(1)", construct: "YQL code or UDF function"},
		{name: "any Code builtin", statement: "SELECT FuncCode(\"Int32\")", construct: "YQL code or UDF function"},
		{name: "a UDF by name", statement: "SELECT Udf(\"String.Strip\")(\" a \")", construct: "YQL code or UDF function"},
		{name: "a backticked builtin", statement: "SELECT `QuoteCode`(1)", construct: "YQL code or UDF function"},
		{name: "a side effect marker", statement: "SELECT WithSideEffects(1)", construct: "YQL code or UDF function"},
		{name: "a PostgreSQL function", statement: "SELECT PgCall('version')", construct: "YQL PostgreSQL function"},
		{name: "a PostgreSQL range function", statement: "SELECT PgRangeCall('generate_series', 1, 2)", construct: "YQL PostgreSQL function"},
		{name: "any Pg builtin", statement: "SELECT pgconst(1, int4)", construct: "YQL PostgreSQL function"},
		{name: "an external data source", statement: "SELECT * FROM `s3_source`.`bucket/path` WITH (FORMAT = \"csv_with_names\")", construct: "YQL external source"},
		{name: "a cluster", statement: "SELECT * FROM dir.t", construct: "YQL external source"},
		{name: "an external source in a join", statement: "SELECT * FROM `t` AS a JOIN ext.`p` AS b ON a.id = b.id", construct: "YQL external source"},
		// SELECT 1uFROM `dir/t` read `1u FROM` on YDB 26.2.1.14: the suffix
		// ends the number, so the word after it is FROM.
		{name: "a source after a suffixed number", statement: "SELECT 1uFROM ext.`bucket/path`", construct: "YQL external source"},
		{name: "a source after an octal number", statement: "SELECT COUNT(*) = 0o0FROM ext.`p`", construct: "YQL external source"},
		{name: "a join after a suffixed number", statement: "SELECT * FROM `t` AS a JOIN `t` AS b ON a.id = b.id + 0uJOIN ext.`p` AS c ON a.id = c.id", construct: "YQL external source"},
		{name: "a source after ANY", statement: "SELECT * FROM ANY ext.`bucket/path` AS a JOIN `t` AS b ON a.id = b.id", construct: "YQL external source"},
		{name: "a join after ANY", statement: "SELECT * FROM `t` AS a JOIN ANY ext.`p` AS b ON a.id = b.id", construct: "YQL external source"},
		// FROM ext:dir.`t` answered `Unknown service: ext`: the grammar's
		// cluster_expr reads a service before the colon.
		{name: "a service prefix", statement: "SELECT * FROM ext:dir.`t`", construct: "YQL external source"},
		{name: "a cluster asterisk", statement: "SELECT * FROM *.`t`", construct: "YQL external source"},
		{name: "a source after a comma", statement: "SELECT * FROM `t`, ext.`p`", construct: "YQL external source"},
		{name: "a source after a join condition and a comma", statement: "SELECT * FROM `t` AS a JOIN `u` AS b ON a.id = b.id, ext.`p` AS c", construct: "YQL external source"},
		{name: "a source in a subquery list", statement: "SELECT * FROM (SELECT * FROM `t`, ext.`p`)", construct: "YQL external source"},
		{name: "a PROCESS source", statement: "PROCESS STREAM ext.`p` USING $f(TableRow())", construct: "YQL external source"},
		{name: "a REDUCE source after a comma", statement: "REDUCE `t`, ext.`p` ON id USING $f(TableRow())", construct: "YQL external source"},
		{name: "a COMBINE source", statement: "SELECT 1 UNION ALL COMBINE ext.`p` WITH `t` ON 1 USING EXTERNAL FUNCTION(\"YANDEX-CLOUD\", \"f\")", construct: "YQL external source"},
		{name: "a COMBINE second source", statement: "COMBINE `t` WITH ext.`p` ON 1 USING $f()", construct: "YQL external source"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			finding, found, err := sqlreach.Scan(test.statement, platform.YDB)

			c.Assert(err, qt.IsNil)
			c.Assert(found, qt.IsTrue)
			c.Assert(finding.Construct, qt.Equals, test.construct)
		})
	}
}

// The YQL rules recognize a shape, not a word: a directory path, a join on a
// qualified column, a colon in a string, and a pragma spelled inside a string
// or a name are ordinary reads. Each statement ran on YDB 26.2.1.14 or is a
// read of the same shape.
func TestScan_YDB_LeavesOrdinaryReadsAlone(t *testing.T) {
	tests := []struct {
		name      string
		statement string
	}{
		{name: "a table in a directory", statement: "SELECT COUNT(*) AS n FROM `dir/t` WHERE id > 0"},
		{name: "a join on qualified columns", statement: "SELECT a.id FROM `dir/t` AS a JOIN `dir/sub/t2` AS b ON a.id = b.id"},
		{name: "colons in a string", statement: `SELECT "a::b" AS x`},
		{name: "a colon between named arguments", statement: "SELECT AsStruct(1 AS a) AS s"},
		{name: "pragma inside a string", statement: `SELECT "PRAGMA File" AS x`},
		{name: "a bang comment after the head", statement: "SELECT 1 --!ansi_lexer"},
		{name: "a parenthesized lambda", statement: "SELECT ListMap(AsList(1, 2), ($x) -> ($x + 1)) AS l"},
		{name: "qualified columns in a function call", statement: "SELECT COALESCE(a.x, b.y) FROM `t` AS a JOIN `u` AS b ON a.id = b.id"},
		{name: "qualified columns after GROUP BY and ORDER BY", statement: "SELECT a.x FROM `t` AS a GROUP BY a.x, a.y ORDER BY a.x, a.y"},
		{name: "a column named like a file function", statement: "SELECT 1 AS files, zipcode FROM `t`"},
		{name: "an IN list", statement: "SELECT * FROM `t` AS a WHERE a.id IN (1, 2)"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			finding, found, err := sqlreach.Scan(test.statement, platform.YDB)

			c.Assert(err, qt.IsNil)
			c.Assert(found, qt.IsFalse)
			c.Assert(finding, qt.Equals, sqlreach.Finding{})
		})
	}
}

// The YDB rules belong to YDB. On ClickHouse `::` is a cast's neighbor in no
// grammar, and a dotted source is a database and a table: neither is refused
// there by a YDB rule.
func TestScan_YDBRulesStayOnYDB(t *testing.T) {
	for _, statement := range []string{"SELECT * FROM analytics.events", "PRAGMA x"} {
		c := qt.New(t)

		_, found, err := sqlreach.Scan(statement, platform.ClickHouse)

		c.Assert(err, qt.IsNil)
		c.Assert(found, qt.IsFalse, qt.Commentf("statement %q", statement))
	}
}

// Each read ran on YDB 26.2.1.14 and answered rows.
func TestYQLReadOnly_HappyPath(t *testing.T) {
	tests := []struct {
		name      string
		statement string
	}{
		{name: "a filtered count", statement: "SELECT COUNT(*) AS n FROM `dir/t` WHERE id > 0"},
		{name: "a join, a grouping, an order and a limit", statement: "SELECT a.id AS id FROM `dir/t` AS a JOIN `dir/sub/t2` AS b " +
			"ON a.id = b.id GROUP BY a.id ORDER BY id LIMIT 10"},
		{name: "a scalar subquery", statement: "SELECT (SELECT COUNT(*) FROM `dir/t`) = 0 AS empty"},
		{name: "an EXISTS inside a CASE", statement: "SELECT CASE WHEN EXISTS (SELECT * FROM `dir/t`) THEN 1 ELSE 0 END AS e"},
		{name: "a parenthesized lambda", statement: "SELECT ListMap(AsList(1, 2), ($x) -> ($x + 1)) AS l"},
		{name: "a trailing semicolon", statement: "SELECT 1;"},
		{name: "a quoted column spelled like a keyword", statement: "SELECT `update` FROM `dir/t`"},
		{name: "keywords inside strings", statement: `SELECT "DELETE; DROP" AS x, @@INSERT INTO t@@ AS y`},
		// Answered 1 on YDB 26.2.1.14: RETURNING here types a JSON value.
		{name: "JSON_VALUE with RETURNING", statement: `SELECT JSON_VALUE(CAST(@@{"a": 1}@@ AS Json), "$.a" RETURNING Int32)`},
		{name: "suffixed numbers apart from words", statement: "SELECT 1u + 0x1F = 32u AND 1.5f > 0e0"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(sqlreach.YQLReadOnly(test.statement), qt.IsNil)
		})
	}
}

// Every construct that is not a read, and every shape the proof cannot read
// as one SELECT, is refused.
func TestYQLReadOnly_FailurePath(t *testing.T) {
	const notASelect = `a YDB statement is proved read-only only when it is one SELECT`
	const secondStatement = notASelect + `, and a semicolon inside it starts another statement or a lambda body`
	tests := []struct {
		name      string
		statement string
		wantErr   string
	}{
		{name: "nothing", statement: "", wantErr: notASelect},
		{name: "a named expression", statement: "$x = SELECT 1; SELECT * FROM $x", wantErr: notASelect},
		{name: "SHOW CREATE", statement: "SHOW CREATE TABLE `t`", wantErr: notASelect},
		{name: "VALUES", statement: "VALUES (1)", wantErr: notASelect},
		{name: "a second statement", statement: "SELECT 1; SELECT 2", wantErr: secondStatement},
		{name: "a braced lambda", statement: "SELECT ($x) -> { $y = $x; RETURN $y; }", wantErr: secondStatement},
		{name: "an INSERT behind a SELECT", statement: "SELECT 1; INSERT INTO t (id) VALUES (1)", wantErr: secondStatement},
		{name: "INTO RESULT", statement: "SELECT 1 AS x INTO RESULT r", wantErr: notASelect + `, and INTO starts or carries a statement that is not a read`},
		{name: "UPSERT", statement: "SELECT 1 UPSERT INTO t (id) VALUES (1)", wantErr: notASelect + `, and UPSERT starts or carries a statement that is not a read`},
		{name: "REPLACE", statement: "SELECT * FROM (REPLACE INTO t SELECT 1)", wantErr: notASelect + `, and REPLACE starts or carries a statement that is not a read`},
		{name: "UPDATE", statement: "SELECT 1 FROM (UPDATE t SET v = 1)", wantErr: notASelect + `, and UPDATE starts or carries a statement that is not a read`},
		{name: "DELETE", statement: "SELECT 1 FROM (DELETE FROM t)", wantErr: notASelect + `, and DELETE starts or carries a statement that is not a read`},
		{name: "BATCH", statement: "SELECT 1 FROM (BATCH UPDATE t SET v = 1)", wantErr: notASelect + `, and BATCH starts or carries a statement that is not a read`},
		{name: "DDL", statement: "SELECT 1 FROM (ALTER TABLE t ADD COLUMN v Utf8)", wantErr: notASelect + `, and ALTER starts or carries a statement that is not a read`},
		{name: "a grant", statement: "SELECT 1 FROM (GRANT ALL ON `t` TO u)", wantErr: notASelect + `, and GRANT starts or carries a statement that is not a read`},
		{name: "an action", statement: "SELECT 1 FROM (DEFINE ACTION $a() AS SELECT 1; END DEFINE)", wantErr: notASelect + `, and DEFINE starts or carries a statement that is not a read`},
		{name: "DO", statement: "SELECT 1 FROM (DO $a())", wantErr: notASelect + `, and DO starts or carries a statement that is not a read`},
		{name: "EVALUATE", statement: "SELECT 1 FROM (EVALUATE FOR $i IN AsList(1) DO $a($i))", wantErr: notASelect + `, and EVALUATE starts or carries a statement that is not a read`},
		{name: "PROCESS", statement: "SELECT * FROM (PROCESS `t` USING $f(TableRow()))", wantErr: notASelect + `, and PROCESS starts or carries a statement that is not a read`},
		{name: "REDUCE", statement: "SELECT * FROM (REDUCE `t` ON id USING $f(TableRow()))", wantErr: notASelect + `, and REDUCE starts or carries a statement that is not a read`},
		{name: "COMMIT", statement: "SELECT 1 COMMIT", wantErr: notASelect + `, and COMMIT starts or carries a statement that is not a read`},
		{name: "DISCARD", statement: "SELECT 1 FROM (DISCARD SELECT 1)", wantErr: notASelect + `, and DISCARD starts or carries a statement that is not a read`},
		{name: "PRAGMA", statement: "SELECT 1 PRAGMA x", wantErr: notASelect + `, and PRAGMA starts or carries a statement that is not a read`},
		{name: "IMPORT", statement: "SELECT 1 IMPORT lib SYMBOLS $f", wantErr: notASelect + `, and IMPORT starts or carries a statement that is not a read`},
		{name: "an unquoted column spelled like a keyword", statement: "SELECT update FROM `t`", wantErr: notASelect + `, and UPDATE starts or carries a statement that is not a read`},
		// The suffix ends the number where the server ends it, and a word
		// written against a number is refused whatever the word is.
		{name: "a word against a suffixed number", statement: "SELECT 1uFROM ext.`bucket/path`", wantErr: notASelect + `, and FROM is written against the number 1u`},
		{name: "a word against an octal number", statement: "SELECT COUNT(*) = 0o0FROM ext.`p`", wantErr: notASelect + `, and FROM is written against the number 0o0`},
		{name: "a word against an exponent", statement: "SELECT 1e0INTO RESULT r", wantErr: notASelect + `, and INTO is written against the number 1e0`},
		{name: "a word against a plain number", statement: "SELECT 1AS x", wantErr: notASelect + `, and AS is written against the number 1`},
		// A colon names a service, as in FROM ext:dir.`t`, which answered
		// `Unknown service: ext`. Struct and dict literals write one too and
		// are refused with it.
		{name: "a service prefix", statement: "SELECT * FROM ext:dir.`t`", wantErr: notASelect + `, and a colon inside it may name a cluster`},
		{name: "a struct literal", statement: "SELECT <|a: 1|>", wantErr: notASelect + `, and a colon inside it may name a cluster`},
		{name: "COMBINE", statement: "SELECT 1 UNION ALL COMBINE `t` WITH `u` ON 1 USING $f()", wantErr: notASelect + `, and COMBINE starts or carries a statement that is not a read`},
		// The lexer reads past a translation setting, so the rest of the text
		// must still prove itself: here it does not even start with SELECT.
		{name: "a setting heading a write", statement: "--!syntax_v1\nDELETE FROM t", wantErr: notASelect},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(sqlreach.YQLReadOnly(test.statement), qt.ErrorMatches, test.wantErr)
		})
	}
}
