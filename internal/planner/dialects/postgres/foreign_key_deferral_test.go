package postgres_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/renderer"
	"go.5x5.cz/ptah/internal/planner/dialects/postgres"
	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
)

// tableQualifiedAdditionSQL plans a foreign key that arrives with its table, the
// route a comparator-detected difference takes.
func tableQualifiedAdditionSQL(c *qt.C, deferrable bool, initially string) string {
	c.Helper()
	diff := &difftypes.SchemaDiff{
		ConstraintsAdded: []string{"fk_child_pid"},
		ConstraintsAddedWithTables: []difftypes.ConstraintAdditionInfo{{
			Name:           "fk_child_pid",
			TableName:      "child",
			Type:           "FOREIGN KEY",
			Columns:        []string{"pid"},
			ForeignTable:   "parent",
			ForeignColumn:  "id",
			ForeignColumns: []string{"id"},
			Deferrable:     deferrable,
			Initially:      initially,
		}},
	}
	nodes, err := postgres.New().GenerateMigrationAST(diff)
	c.Assert(err, qt.IsNil)
	sql, err := renderer.RenderSQL("postgres", nodes...)
	c.Assert(err, qt.IsNil)
	return legacyRenderedSQL(sql)
}

// TestPlanner_AddsAForeignKeysDeferral pins that the clause reaches the
// statement on both routes.
//
// ALTER TABLE ADD CONSTRAINT built the reference from the table, the columns and
// the two referential actions, and nothing else. A declared DEFERRABLE was
// therefore planned on every run and never applied -- and where the change is
// expressed as a drop and an add, the add removed a deferral the database had
// (stokaro/ptah#2216).
func TestPlanner_AddsAForeignKeysDeferral(t *testing.T) {
	tests := []struct {
		name       string
		deferrable bool
		initially  string
		want       string
	}{
		{name: "deferred", deferrable: true, initially: "DEFERRED", want: "DEFERRABLE INITIALLY DEFERRED"},
		{name: "immediate", deferrable: true, initially: "IMMEDIATE", want: "DEFERRABLE INITIALLY IMMEDIATE"},
		{name: "deferrable with no timing", deferrable: true, initially: "", want: "DEFERRABLE"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			// One route now: the addition carries the reference it renders.
			// The other resolved a bare name against the declaration handed to
			// the planner, and that shape is withdrawn (stokaro/ptah#2315).
			c.Assert(tableQualifiedAdditionSQL(c, tt.deferrable, tt.initially), qt.Contains, tt.want)
		})
	}
}

// TestPlanner_LeavesAnImmediateForeignKeyWithoutADeferralClause is the control.
//
// Most foreign keys carry no deferral, and a planner that wrote DEFERRABLE
// unconditionally would satisfy every row above while changing the semantics of
// every key it ever added.
func TestPlanner_LeavesAnImmediateForeignKeyWithoutADeferralClause(t *testing.T) {
	c := qt.New(t)

	c.Assert(tableQualifiedAdditionSQL(c, false, ""), qt.Not(qt.Contains), "DEFERRABLE")
}
