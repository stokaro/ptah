package lint_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/migration/lint"
)

// A PostgreSQL migration that changes an owner is read, so AC102 has nothing to
// say about it: the parser read the statement and it models no ownership. The
// token-level rules still see the statement, which the ofRule filter leaves out.
func TestAC102_AnOwnerChangeIsRead_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name string
		sql  string
	}{
		{name: "table", sql: "ALTER TABLE t OWNER TO app;"},
		{name: "schema", sql: "ALTER SCHEMA app OWNER TO app;"},
		{name: "function with arguments", sql: "ALTER FUNCTION app.f(integer) OWNER TO app;"},
		{name: "sequence", sql: "ALTER SEQUENCE app.seq OWNER TO app;"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			findings := analyzeUnparsed(c, "postgres", test.sql, "SELECT 1;", lint.UnparsedAll)

			c.Assert(ofRule(findings, "AC102"), qt.HasLen, 0)
		})
	}
}

// OWNER TO is not MySQL's, so a MySQL run still reports the statement as one
// nothing read.
func TestAC102_AnOwnerChangeOnAnotherEngine_FailurePath(t *testing.T) {
	c := qt.New(t)

	found := ofRule(analyzeUnparsed(c, "mysql", "ALTER TABLE t OWNER TO app;", "SELECT 1;", lint.UnparsedAll), "AC102")

	c.Assert(found, qt.HasLen, 1)
	c.Assert(found[0].Message, qt.Contains, "(unsupported ALTER operation: OWNER)")
}

// Another action on a kind the reader takes for OWNER TO is still reported.
func TestAC102_AnotherActionOnTheSameKind_FailurePath(t *testing.T) {
	c := qt.New(t)

	found := ofRule(analyzeUnparsed(c, "postgres", "ALTER SCHEMA app RENAME TO other;", "SELECT 1;", lint.UnparsedAll), "AC102")

	c.Assert(found, qt.HasLen, 1)
	c.Assert(found[0].Message, qt.Contains, "(unsupported ALTER target: SCHEMA)")
}
