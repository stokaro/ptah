package yqlreach_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/yqlreach"
)

// Each read ran on YDB 26.2.1.14 and answered rows.
func TestReadOnly_HappyPath(t *testing.T) {
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
			c.Assert(yqlreach.ReadOnly(test.statement), qt.IsNil)
		})
	}
}

// Every construct that is not a read, and every shape the proof cannot read
// as one SELECT, is refused.
func TestReadOnly_FailurePath(t *testing.T) {
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
			c.Assert(yqlreach.ReadOnly(test.statement), qt.ErrorMatches, test.wantErr)
		})
	}
}
