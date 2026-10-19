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
		// The source list of the subquery ends with its parenthesis, so the
		// comma in the call after it joins nothing.
		{name: "a call after a subquery source", statement: "SELECT * FROM (SELECT * FROM `t`) AS s WHERE COALESCE(s.a, s.b) > 0"},
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
