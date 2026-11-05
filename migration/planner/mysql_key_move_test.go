package planner_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/difftypes"
	"ptah.run/core/planext"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/engine/builtin"
	"ptah.run/migration/planner"
)

// movedKeyDiff moves the primary key of parent from id to code.
func movedKeyDiff() *difftypes.SchemaDiff {
	return &difftypes.SchemaDiff{
		ConstraintsAdded: difftypes.ConstraintAdditions{
			{Name: "PRIMARY", TableName: "parent", Type: "PRIMARY KEY", Columns: []string{"code"}},
		},
		ConstraintsRemoved: difftypes.ConstraintRemovals{
			{Name: "PRIMARY", TableName: "parent", Type: "PRIMARY KEY"},
		},
	}
}

// removedForeignKey records the removal of child_fk, a foreign key over parent.id.
func removedForeignKey(diff *difftypes.SchemaDiff) {
	diff.ConstraintsRemoved = append(diff.ConstraintsRemoved,
		difftypes.ConstraintRemovalInfo{Name: "child_fk", TableName: "child", Type: "FOREIGN KEY"})
	diff.ForeignKeysRemovedWithTables = append(diff.ForeignKeysRemovedWithTables, difftypes.ForeignKeyRemovalInfo{
		Name: "child_fk", TableName: "child", Columns: []string{"pid"}, ForeignTable: "parent", ForeignColumns: []string{"id"},
	})
}

// keyMovePlan plans change on dialect in the exact style.
func keyMovePlan(c *qt.C, dialect string, change func(*difftypes.SchemaDiff)) []string {
	c.Helper()
	diff := movedKeyDiff()
	change(diff)
	statements, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(c.Context(), must.Must(builtin.New()), diff, dialect,
		planext.Options{Capabilities: capability.ForDialect(dialect), Statements: planext.StatementStyleExact})
	c.Assert(err, qt.IsNil)
	return statements
}

// TestMovedKeyOrder_MySQLFamily pins the order MySQL requires. It answers `DROP
// PRIMARY KEY, ADD PRIMARY KEY (code)` with Error 1553 while a foreign key stands
// over the old key with nothing else to use, so what the foreign key will use goes
// first, and so does a foreign key the plan drops. MariaDB takes either order and
// gets the same plan.
func TestMovedKeyOrder_MySQLFamily(t *testing.T) {
	const move = "ALTER TABLE `parent` DROP PRIMARY KEY, ADD PRIMARY KEY (`code`)"
	tests := []struct {
		name   string
		change func(*difftypes.SchemaDiff)
		want   []string
	}{
		{
			name:   "nothing else changes",
			change: func(*difftypes.SchemaDiff) {},
			want:   []string{move},
		},
		{
			name: "a UNIQUE the plan adds goes ahead of the move",
			change: func(diff *difftypes.SchemaDiff) {
				diff.ConstraintsAdded = append(diff.ConstraintsAdded, difftypes.ConstraintAdditionInfo{
					Name: "id_key", TableName: "parent", Type: "UNIQUE", Columns: []string{"id"},
				})
			},
			want: []string{"ALTER TABLE `parent` ADD CONSTRAINT `id_key` UNIQUE (`id`)", move},
		},
		{
			name:   "a foreign key the plan removes goes ahead of the move",
			change: removedForeignKey,
			want:   []string{"ALTER TABLE `child` DROP FOREIGN KEY `child_fk`", move},
		},
		{
			name: "a foreign key the plan changes leaves ahead of the move and comes back after it",
			change: func(diff *difftypes.SchemaDiff) {
				removedForeignKey(diff)
				diff.ConstraintsAdded = append(diff.ConstraintsAdded, difftypes.ConstraintAdditionInfo{
					Name: "child_fk", TableName: "child", Type: "FOREIGN KEY", Columns: []string{"pid"},
					ForeignTable: "parent", ForeignColumns: []string{"code"},
				})
			},
			want: []string{
				"ALTER TABLE `child` DROP FOREIGN KEY `child_fk`",
				move,
				"ALTER TABLE `child` ADD CONSTRAINT `child_fk` FOREIGN KEY (`pid`) REFERENCES `parent`(`code`)",
			},
		},
		{
			name: "a foreign key over a table whose key stays keeps its place",
			change: func(diff *difftypes.SchemaDiff) {
				diff.ConstraintsRemoved = append(diff.ConstraintsRemoved,
					difftypes.ConstraintRemovalInfo{Name: "other_fk", TableName: "child", Type: "FOREIGN KEY"})
				diff.ForeignKeysRemovedWithTables = append(diff.ForeignKeysRemovedWithTables, difftypes.ForeignKeyRemovalInfo{
					Name: "other_fk", TableName: "child", Columns: []string{"oid"}, ForeignTable: "other", ForeignColumns: []string{"id"},
				})
			},
			want: []string{move, "ALTER TABLE `child` DROP FOREIGN KEY `other_fk`"},
		},
	}
	for _, dialect := range []string{platform.MySQL, platform.MariaDB} {
		for _, test := range tests {
			t.Run(dialect+" "+test.name, func(t *testing.T) {
				c := qt.New(t)
				c.Assert(keyMovePlan(c, dialect, test.change), qt.DeepEquals, test.want)
			})
		}
	}
}

// TestMovedKeyOrder_AKeyThePlanOnlyRemoves drops a foreign key ahead of the key it
// stands over, where the sorted removals name another table first.
func TestMovedKeyOrder_AKeyThePlanOnlyRemoves(t *testing.T) {
	for _, dialect := range []string{platform.MySQL, platform.MariaDB} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			got := keyMovePlan(c, dialect, func(diff *difftypes.SchemaDiff) {
				diff.ConstraintsAdded = nil
				diff.ConstraintsRemoved = append(diff.ConstraintsRemoved,
					difftypes.ConstraintRemovalInfo{Name: "child_fk", TableName: "zchild", Type: "FOREIGN KEY"})
				diff.ForeignKeysRemovedWithTables = append(diff.ForeignKeysRemovedWithTables, difftypes.ForeignKeyRemovalInfo{
					Name: "child_fk", TableName: "zchild", Columns: []string{"pid"}, ForeignTable: "parent", ForeignColumns: []string{"id"},
				})
			})
			c.Assert(got[0], qt.Equals, "ALTER TABLE `zchild` DROP FOREIGN KEY `child_fk`")
		})
	}
}

// TestMovedKeyOrder_OtherDialectsOfThePlannerKeepTheirs is the control: SQL Server
// shares this planner, drops a key in a statement of its own, and has no Error 1553
// to order around, so its additions and its removals keep their order.
func TestMovedKeyOrder_OtherDialectsOfThePlannerKeepTheirs(t *testing.T) {
	c := qt.New(t)
	got := keyMovePlan(c, platform.SQLServer, func(diff *difftypes.SchemaDiff) {
		removedForeignKey(diff)
		diff.ConstraintsAdded = append(diff.ConstraintsAdded, difftypes.ConstraintAdditionInfo{
			Name: "id_key", TableName: "parent", Type: "UNIQUE", Columns: []string{"id"},
		})
	})
	c.Assert(got[0], qt.Contains, "[parent]")
	c.Assert(got[len(got)-1], qt.Contains, "[child_fk]")
	c.Assert(got[len(got)-2], qt.Contains, "UNIQUE")
}
