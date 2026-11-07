package sqllint_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/capability"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/sqllint"
)

// The S69 cases of stokaro/ptah#4204 on PostgreSQL: a dollar-quoted string, a
// nested block comment with a semicolon after its inner comment, and an escape
// string, each holding a semicolon and an apostrophe. Each statement is read
// by its first word as PostgreSQL reads it, so the INSERT after the nested
// comment is the INSERT it is (stokaro/ptah#5586). Read by the SQL standard,
// the comment ended at its first */ and the statement opened with the word
// "it", which no rule examined.
func TestLintSource_ReadsAStatementsFirstWordByTheDialect(t *testing.T) {
	c := qt.New(t)
	source := sqllint.Source{Name: "s69.sql", SQL: "CREATE TABLE withpk (id bigint NOT NULL, note text, PRIMARY KEY (id));\n" +
		"INSERT INTO withpk (id, note) VALUES (1, $$it's; fine$$);\n" +
		"/* a /* nested */ it's; a comment */\n" +
		"INSERT INTO withpk (id, note) VALUES (2, E'it\\'s; fine');\n" +
		"CREATE TABLE nopk (a bigint);\n"}

	findings, err := sqllint.LintSource(c.Context(), source, sqllint.Options{SQL: builtintest.Runtime().SQL(), Bodies: builtintest.Runtime(),
		Dialect: "postgres", Capabilities: capability.ForDialect("postgres")})

	c.Assert(err, qt.IsNil)
	c.Assert(findingLines(findings), qt.DeepEquals, []string{
		"2:1 SQL002 ptah sql lint does not lint INSERT statements yet",
		"4:1 SQL002 ptah sql lint does not lint INSERT statements yet",
		`5:14 DDL001 table "nopk" has no primary key`,
	})
}
