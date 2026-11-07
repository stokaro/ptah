package sqllint_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/internal/sqllint"
)

// notRead opens the message of a statement the parser could not tell from valid SQL.
const notRead = "the SQL parser does not read this statement, which a server may still accept: "

// A statement the parser refuses is reported at its place in the file, though the
// parser read the statement alone, and the finding's message is the reason: the
// position the finding states is the only one a reader sees.
func TestParseError_ReportsThePlaceInTheFile(t *testing.T) {
	tests := []struct {
		name       string
		dialect    string
		sql        string
		wantLine   int
		wantColumn int
		wantRule   string
		wantText   string
	}{
		{
			name:       "the first statement",
			dialect:    platform.Postgres,
			sql:        "CREATE TABLE b BOGUS;",
			wantLine:   1,
			wantColumn: 16,
			wantRule:   sqllint.RuleUnsupportedStatement,
			wantText:   notRead + "unsupported table option: BOGUS",
		},
		{
			name:       "a statement after one with a multi-line body",
			dialect:    platform.Postgres,
			sql:        "CREATE TABLE a (\n  id INTEGER PRIMARY KEY,\n  note TEXT DEFAULT 'one\ntwo'\n);\n\n/* a block\n   comment */\nCREATE TABLE b BOGUS;\n",
			wantLine:   9,
			wantColumn: 16,
			wantRule:   sqllint.RuleUnsupportedStatement,
			wantText:   notRead + "unsupported table option: BOGUS",
		},
		{
			name:       "CR line endings",
			dialect:    platform.Postgres,
			sql:        "CREATE TABLE a (\r  id INTEGER PRIMARY KEY\r);\rCREATE TABLE b BOGUS;\r",
			wantLine:   4,
			wantColumn: 16,
			wantRule:   sqllint.RuleUnsupportedStatement,
			wantText:   notRead + "unsupported table option: BOGUS",
		},
		{
			name:       "a multi-byte name and a tab",
			dialect:    platform.Postgres,
			sql:        "CREATE TABLE заказы\tBOGUS;",
			wantLine:   1,
			wantColumn: 21,
			wantRule:   sqllint.RuleUnsupportedStatement,
			wantText:   notRead + "unsupported table option: BOGUS",
		},
		{
			name:       "after a client delimiter directive",
			dialect:    platform.MySQL,
			sql:        "CREATE TABLE a (id INT PRIMARY KEY);\nDELIMITER $$\nCREATE TABLE c (id INT PRIMARY KEY)$$\nDELIMITER ;\nCREATE TABLE b BOGUS;\n",
			wantLine:   5,
			wantColumn: 16,
			wantRule:   sqllint.RuleUnsupportedStatement,
			wantText:   notRead + "unsupported table option: BOGUS",
		},
		{
			name:       "a syntax error",
			dialect:    platform.Postgres,
			sql:        "CREATE TABLE a (id INTEGER PRIMARY KEY);\nCREATE TABLE (id INTEGER);\n",
			wantLine:   2,
			wantColumn: 14,
			wantRule:   sqllint.RuleParseError,
			wantText:   "expected table name: expected identifier, got Operator",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			findings := lintAs(c, test.dialect, test.sql, sqllint.Options{})

			index := slices.IndexFunc(findings, func(finding sqllint.Finding) bool { return finding.Rule == test.wantRule })
			c.Assert(index >= 0, qt.IsTrue, qt.Commentf("%v", findings))
			c.Assert(findings[index].Line, qt.Equals, test.wantLine)
			c.Assert(findings[index].Column, qt.Equals, test.wantColumn)
			c.Assert(findings[index].Message, qt.Equals, test.wantText)
		})
	}
}
