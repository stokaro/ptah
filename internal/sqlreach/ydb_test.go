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
		{name: "a file read", statement: `SELECT FileContent("secrets")`, construct: "YQL file or code function"},
		{name: "evaluated code", statement: `SELECT EvaluateCode(QuoteCode(1))`, construct: "YQL file or code function"},
		{name: "an external data source", statement: "SELECT * FROM `s3_source`.`bucket/path` WITH (FORMAT = \"csv_with_names\")", construct: "YQL external source"},
		{name: "a cluster", statement: "SELECT * FROM dir.t", construct: "YQL external source"},
		{name: "an external source in a join", statement: "SELECT * FROM `t` AS a JOIN ext.`p` AS b ON a.id = b.id", construct: "YQL external source"},
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
