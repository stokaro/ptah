package sqlserverbatch_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/sqlserverbatch"
)

// TestExecText_HappyPath reads the text an EXEC of one string literal runs.
func TestExecText_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name      string
		statement string
		want      string
	}{
		{name: "EXEC of a literal", statement: "EXEC(N'COMMIT')", want: "COMMIT"},
		{name: "EXECUTE of a literal with no N and a semicolon", statement: "execute ( 'it''s' );", want: "it's"},
		{name: "sp_executesql", statement: "EXEC sp_executesql N'ROLLBACK'", want: "ROLLBACK"},
		{name: "sp_executesql with parameters", statement: "EXEC sp_executesql N'SELECT @p', N'@p int', @p = 1", want: "SELECT @p"},
		{name: "sp_executesql in brackets", statement: "EXEC [sys].[sp_executesql] N'COMMIT'", want: "COMMIT"},
		{name: "a qualified sp_executesql with @stmt", statement: "EXEC master.sys.sp_executesql @stmt = N'DROP TABLE dbo.x'", want: "DROP TABLE dbo.x"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, ok := sqlserverbatch.ExecText(test.statement)
			c.Assert(ok, qt.IsTrue)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// TestExecText_FailurePath lists EXEC statements whose text the server
// computes or sends elsewhere, and statements that are no EXEC of text.
func TestExecText_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name      string
		statement string
	}{
		{name: "a variable", statement: "EXEC(@sql)"},
		{name: "a concatenation", statement: "EXEC(N'DROP' + N' TABLE dbo.x')"},
		{name: "a linked server", statement: "EXEC(N'COMMIT') AT remote"},
		{name: "sp_executesql of a variable", statement: "EXEC sp_executesql @sql"},
		{name: "sp_executesql of a concatenation", statement: "EXEC sp_executesql N'DROP ' + N'TABLE dbo.x'"},
		{name: "an N apart from its literal", statement: "EXEC(N 'COMMIT')"},
		{name: "a procedure", statement: "EXEC dbo.seed N'COMMIT'"},
		{name: "a statement that is no EXEC", statement: "SELECT N'COMMIT'"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, ok := sqlserverbatch.ExecText(test.statement)
			c.Assert(ok, qt.IsFalse)
			c.Assert(got, qt.Equals, "")
		})
	}
}
