package sqlutil_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/sqlutil"
)

// A comment or default whose text holds a line that reads like a delimiter
// directive reaches the server whole. Each row is one statement followed by a
// second, so a directive mistaken for text-level syntax shows twice: the
// literal loses a line, and the semicolon stops ending the first statement.
func TestSplitStatementsForDialect_DirectiveLookingLiteral_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		first   string
	}{
		{
			name:    "postgres table comment with the Atlas form",
			dialect: "postgres",
			first:   "COMMENT ON TABLE t IS 'c\n-- atlas:delimiter $$\nd'",
		},
		{
			name:    "postgres default with the client form",
			dialect: "postgres",
			first:   "ALTER TABLE t ALTER COLUMN n SET DEFAULT 'a\nDELIMITER $$\nb'",
		},
		{
			name:    "postgres E string with a backslash before the quote",
			dialect: "postgres",
			first:   "COMMENT ON TABLE t IS E'c\\'\nDELIMITER $$\nd'",
		},
		{
			name:    "mysql inline comment with the Atlas form",
			dialect: "mysql",
			first:   "CREATE TABLE t (n TEXT COMMENT 'x\n-- atlas:delimiter $$\ny')",
		},
		{
			name:    "mysql backslash-escaped quote",
			dialect: "mysql",
			first:   "CREATE TABLE t (n TEXT COMMENT 'x\\'\nDELIMITER $$\ny')",
		},
		{
			name:    "sqlite default with the Atlas form",
			dialect: "sqlite",
			first:   "CREATE TABLE t (n TEXT DEFAULT 'x\n-- atlas:delimiter $$\ny')",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := sqlutil.SplitStatementsForDialect(test.dialect, test.first+";\nSELECT 1;\n")

			c.Assert(got, qt.DeepEquals, []string{test.first, "SELECT 1"})
		})
	}
}

func TestSplitStatements_DirectiveLookingLiteral_HappyPath(t *testing.T) {
	c := qt.New(t)
	first := "COMMENT ON TABLE t IS 'c\n-- atlas:delimiter $$\nd'"

	got := sqlutil.SplitStatements(first + ";\nSELECT 1;\n")

	c.Assert(got, qt.DeepEquals, []string{first, "SELECT 1"})
}

// The control: the same two directives written outside a literal still select
// the delimiter.
func TestSplitStatementsForDialect_DirectiveOutsideALiteral_StillHonored(t *testing.T) {
	tests := []struct {
		name   string
		script string
	}{
		{name: "Atlas form", script: "SELECT 'x';\n-- atlas:delimiter $$\nSELECT 1; SELECT 2$$\n"},
		{name: "client form", script: "SELECT 'x';\nDELIMITER $$\nSELECT 1; SELECT 2$$\n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := sqlutil.SplitStatementsForDialect("mysql", test.script)

			c.Assert(got, qt.DeepEquals, []string{"SELECT 'x'", "SELECT 1", "SELECT 2"})
		})
	}
}
