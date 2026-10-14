package chsource_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/builtintest"
	"ptah.run/internal/parser"
)

func TestSQLRowPolicyUnsupportedClausesAreNamed(t *testing.T) {
	tests := []struct{ text, reason string }{
		{"CREATE ROW POLICY IF NOT EXISTS p ON orders", "conditional"},
		{"CREATE ROW POLICY p ON CLUSTER c ON orders", "ON CLUSTER"},
		{"CREATE ROW POLICY p ON app.*", "database-wide"},
		{"CREATE ROW POLICY p ON orders IN local_directory", "IN"},
		{"CREATE ROW POLICY p ON orders WITH CHECK id = 1", "WITH"},
		{"CREATE ROW POLICY p ON orders USING id = 1 WITH CHECK id = 1", "WITH"},
		{"CREATE ROW POLICY p ON orders FOR UPDATE USING id = 1", "SELECT only"},
		{"CREATE ROW POLICY p ON orders TO CURRENT_USER", "CURRENT_USER"},
		{"CREATE ROW POLICY p ON orders TO ALL EXCEPT", "empty entry"},
		{"CREATE ROW POLICY p ON orders TO ALL TO NONE", "repeated"},
		{"CREATE ROW POLICY p ON orders USING (id > 0", "unbalanced"},
		{"CREATE ROW POLICY p ON orders USING ()", "filter cannot be empty"},
		{"CREATE ROW POLICY p, q ON orders", "expected ON"},
		{"CREATE ROW POLICY p ON orders, other", "unsupported"},
		{"CREATE ROW POLICY p ON orders AS STRICT", "PERMISSIVE or RESTRICTIVE"},
		{"ALTER ROW POLICY p ON orders RENAME TO q", "RENAME"},
		{"DROP ROW POLICY p ON orders ON CLUSTER c", "unsupported"},
		{"CREATE ROW POLICY p ON orders TO `open", "closed quoted"},
		{"CREATE ROW POLICY p ON orders USING 'open", "closed quoted"},
		{"CREATE ROW POLICY p ON orders TO `a\\n`", "backslash escapes"},
	}
	for _, test := range tests {
		t.Run(test.text, func(t *testing.T) {
			c := qt.New(t)
			statements, err := parser.NewParser(test.text, parser.WithDialect("clickhouse"), parser.WithSQL(builtintest.Runtime().SQL())).Parse()
			c.Assert(err, qt.ErrorMatches, "(?s).*"+test.reason+".*")
			c.Assert(statements, qt.IsNil)
		})
	}
}
