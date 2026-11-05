package guards_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/internal/planner/guards"
)

// TestSettle_LeavesOutAGuardNothingNeeds_HappyPath is a plan no statement of which
// may have removed what a later one names.
func TestSettle_LeavesOutAGuardNothingNeeds_HappyPath(t *testing.T) {
	c := qt.New(t)
	table := ast.NewDropTable("gone").SetIfExists()
	view := ast.NewDropView("v").SetIfExists()
	index := ast.NewDropIndex("i").SetTable("t").SetIfExists()
	created := ast.NewIndex("j", "t", "a")
	created.IfNotExists = true

	guards.Settle([]ast.Node{ast.NewRawSQL("PRAGMA foreign_keys = off;"), table, view, index, created}, guards.Options{
		HarmlessRaw: func(*ast.RawSQLNode) bool { return true },
	})

	c.Assert(table.IfExists, qt.IsFalse)
	c.Assert(view.IfExists, qt.IsFalse)
	c.Assert(index.IfExists, qt.IsFalse)
	c.Assert(created.IfNotExists, qt.IsFalse)
}

// TestSettle_KeepsAGuardAnEarlierStatementNeeds_FailurePath is a statement that may
// have removed the object the next one names, and the raw statement or owner
// operation a planner did not declare harmless.
func TestSettle_KeepsAGuardAnEarlierStatementNeeds_FailurePath(t *testing.T) {
	owner := &ast.ExtensionStatement{}
	tests := []struct {
		name   string
		before ast.Node
		opts   guards.Options
	}{
		{name: "a raw statement nobody declared harmless", before: ast.NewRawSQL("DELETE FROM t")},
		{
			name:   "a raw statement a planner does not declare harmless",
			before: ast.NewRawSQL("DROP VIEW v"),
			opts:   guards.Options{HarmlessRaw: func(*ast.RawSQLNode) bool { return false }},
		},
		{name: "an owner operation nobody declared harmless", before: owner},
		{name: "a cascading drop", before: ast.NewDropTable("other").SetCascade()},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			view := ast.NewDropView("v").SetIfExists()

			guards.Settle([]ast.Node{test.before, view}, test.opts)

			c.Assert(view.IfExists, qt.IsTrue)
		})
	}
}
