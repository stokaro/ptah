package sqllint_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/sqllint"
)

// lintOne lints sql in dialect and returns the findings as `rule severity message`.
func lintOne(c *qt.C, dialect, sql string) []string {
	c.Helper()
	findings, err := sqllint.LintSource(c.Context(), sqllint.Source{Name: "x.sql", SQL: sql}, sqllint.Options{
		SQL: builtintest.Runtime().SQL(), Bodies: builtintest.Runtime(), Dialect: dialect,
		DisabledRules: []string{sqllint.RuleTableWithoutPrimaryKey},
	})
	c.Assert(err, qt.IsNil)
	lines := make([]string, 0, len(findings))
	for _, finding := range findings {
		lines = append(lines, finding.Rule+" "+string(finding.Severity)+" "+finding.Message)
	}
	return lines
}

const (
	notAnalyzed  = "SQL004 info no rule examined "
	nothingRead  = "SQL002 error the SQL parser does not read this statement, which a server may still accept: "
	notALanguage = "SQL001 error no engine Ptah reads has this statement: "
)

// TestLintSource_ValidSQLIsNotAnError holds what the parser reads to an answer no
// default threshold fails on: nothing is wrong with the file, and no rule looked.
func TestLintSource_ValidSQLIsNotAnError(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		sql     string
		want    []string
	}{
		{name: "a dropped view", dialect: platform.SQLite, sql: "DROP VIEW v;", want: []string{notAnalyzed + "DROP VIEW in this file"}},
		{name: "a dropped view on PostgreSQL", dialect: platform.Postgres, sql: "DROP VIEW IF EXISTS v, w CASCADE;", want: []string{notAnalyzed + "DROP VIEW in this file"}},
		{name: "a provider view security clause", dialect: "spanner", sql: "CREATE VIEW v SQL SECURITY INVOKER AS SELECT 1;", want: []string{notAnalyzed + "CREATE VIEW in this file"}},
		{name: "a dropped trigger", dialect: platform.MySQL, sql: "DROP TRIGGER IF EXISTS t;", want: []string{notAnalyzed + "DROP TRIGGER in this file"}},
		{name: "a dropped function", dialect: platform.Postgres, sql: "DROP FUNCTION f(int);", want: []string{notAnalyzed + "DROP FUNCTION in this file"}},
		{name: "a dropped procedure", dialect: platform.Postgres, sql: "DROP PROCEDURE p;", want: []string{notAnalyzed + "DROP PROCEDURE in this file"}},
		{name: "TRUNCATE", dialect: platform.Postgres, sql: "TRUNCATE TABLE t RESTART IDENTITY;", want: []string{notAnalyzed + "TRUNCATE in this file"}},
		{name: "a savepoint", dialect: platform.SQLite, sql: "SAVEPOINT s;\nRELEASE s;", want: []string{notAnalyzed + "SAVEPOINT, RELEASE in this file"}},
		{name: "a lock", dialect: platform.Postgres, sql: "LOCK TABLE t IN ACCESS SHARE MODE;", want: []string{notAnalyzed + "LOCK in this file"}},
		{name: "a rename", dialect: platform.MySQL, sql: "RENAME TABLE a TO b;", want: []string{notAnalyzed + "ALTER TABLE in this file"}},
		{name: "a grant the parser reads", dialect: platform.Postgres, sql: "GRANT SELECT ON t TO r;", want: []string{notAnalyzed + "GRANT PRIVILEGE in this file"}},
		{name: "a batch separator is no statement", dialect: platform.SQLServer, sql: "DROP VIEW v\nGO\n", want: []string{notAnalyzed + "DROP VIEW in this file"}},
		{name: "a file of comments", dialect: platform.Postgres, sql: "-- nothing here\n/* or here */;\n", want: make([]string, 0)},
		{name: "a version-guarded statement", dialect: platform.MySQL, sql: "/*!40101 SET NAMES utf8 */;\n", want: make([]string, 0)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(lintOne(c, test.dialect, test.sql), qt.DeepEquals, test.want)
		})
	}
}

// TestLintSource_WhatNothingReadStaysAnError holds the decision on SQL002: a statement
// nothing read keeps the exit code, because its tail could hold anything, and it says so
// in words that do not call the SQL wrong.
func TestLintSource_WhatNothingReadStaysAnError(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		sql     string
		want    []string
	}{
		{name: "a statement that writes rows", dialect: platform.Postgres, sql: "SELECT @@@;", want: []string{"SQL002 error ptah sql lint does not lint SELECT statements yet"}},
		{name: "a parenthesized query", dialect: platform.Postgres, sql: "(SELECT 1);", want: []string{"SQL002 error ptah sql lint does not lint SELECT statements yet"}},
		{name: "an INSERT", dialect: platform.SQLite, sql: "INSERT INTO t VALUES (1);", want: []string{"SQL002 error ptah sql lint does not lint INSERT statements yet"}},
		{name: "REPLACE, as INSERT", dialect: platform.MySQL, sql: "REPLACE INTO t VALUES (1);", want: []string{"SQL002 error ptah sql lint does not lint REPLACE statements yet"}},
		{name: "UPSERT, as INSERT", dialect: platform.CockroachDB, sql: "UPSERT INTO t VALUES (1);", want: []string{"SQL002 error ptah sql lint does not lint UPSERT statements yet"}},
		{name: "a target the parser does not model", dialect: platform.Postgres, sql: "DROP AGGREGATE a(int);", want: []string{nothingRead + "unsupported DROP target: AGGREGATE"}},
		{name: "a CREATE the parser does not model", dialect: platform.Postgres, sql: "CREATE COLLATION c (locale = 'C');", want: []string{nothingRead + "unsupported CREATE target: COLLATION"}},
		{name: "a word it cannot tell from a mistake", dialect: platform.Postgres, sql: "CREATE TABLE b BOGUS;", want: []string{nothingRead + "unsupported table option: BOGUS"}},
		{name: "valid SQL it cannot tell from one", dialect: platform.ClickHouse, sql: "DROP TABLE t ON CLUSTER c;", want: []string{nothingRead + "unsupported DROP TABLE clause: ON"}},
		{name: "a clause another engine has", dialect: platform.Postgres, sql: "DROP VIEW v SYNC;", want: []string{nothingRead + "unsupported DROP VIEW clause: SYNC"}},
		{name: "a statement the dialect has no form of", dialect: platform.SQLite, sql: "TRUNCATE t;", want: []string{nothingRead + "unsupported SQL statement: TRUNCATE"}},
		{name: "a kind the dialect has no form of", dialect: platform.MySQL, sql: "DROP MATERIALIZED VIEW m;", want: []string{nothingRead + "unsupported DROP target: MATERIALIZED"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(lintOne(c, test.dialect, test.sql), qt.DeepEquals, test.want)
		})
	}
}

// TestLintSource_WhatIsNoSQLIsSaidToBe holds the part the parser can tell apart: words
// no engine opens a statement with, and text that is malformed by any grammar.
func TestLintSource_WhatIsNoSQLIsSaidToBe(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want []string
	}{
		{name: "a statement no engine has", sql: "FOO BAR;", want: []string{notALanguage + "FOO"}},
		{name: "a misspelled keyword", sql: "SELEC 1;", want: []string{notALanguage + "SELEC"}},
		{name: "a misspelled DROP", sql: "DROP TABEL t;", want: []string{notALanguage + "DROP TABEL"}},
		{name: "a misspelled CREATE", sql: "CREATE TABEL t (id int);", want: []string{notALanguage + "CREATE TABEL"}},
		{name: "punctuation for a keyword", sql: "@@@ x;", want: []string{"SQL001 error expected SQL keyword, got Operator"}},
		{name: "a name that is none", sql: "DROP VIEW @@@;", want: []string{"SQL001 error expected view name: expected identifier, got Operator"}},
		{name: "a list where one name is taken", sql: "DROP TRIGGER t, u;", want: []string{"SQL001 error expected the end of DROP TRIGGER, got Operator"}},
		{name: "SQLite refuses a list of tables", sql: "DROP TABLE t, t2;", want: []string{"SQL001 error expected the end of DROP TABLE, got Operator"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(lintOne(c, platform.SQLite, test.sql), qt.DeepEquals, test.want)
		})
	}
}

func TestLintSource_DropTableRefusesTrailingStatement(t *testing.T) {
	c := qt.New(t)
	c.Assert(lintOne(c, platform.Postgres, "DROP TABLE t SELECT 1;"), qt.DeepEquals,
		[]string{nothingRead + "unsupported DROP TABLE clause: SELECT"})
}
