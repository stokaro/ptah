package mysql_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff/difftypes"
)

// uniqueKeyRebuildDiff is a database `UNIQUE KEY uq_users_email (email)` against
// a desired state that names the same object as a plain index: one object,
// replaced, which the comparator states as an index addition plus the removal of
// the index it replaces, with the removal marked as a UNIQUE constraint's.
func uniqueKeyRebuildDiff() *difftypes.SchemaDiff {
	return &difftypes.SchemaDiff{
		IndexesAdded:   difftypes.IndexAdditionsFor(uniqueKeyRebuildSchema(), difftypes.IndexRef{Name: "uq_users_email", TableName: "users"}),
		IndexesRemoved: []difftypes.IndexRef{{Name: "uq_users_email", TableName: "users"}},
		ConstraintBackedIndexRemovals: []difftypes.IndexRef{
			{Name: "uq_users_email", TableName: "users"},
		},
	}
}

func uniqueKeyRebuildSchema() *schemamodel.Database {
	return &schemamodel.Database{
		Tables:  []schemamodel.Table{{Name: "users", StructName: "User"}},
		Fields:  []schemamodel.Field{{Name: "email", StructName: "User", Type: "VARCHAR(255)"}},
		Indexes: []schemamodel.Index{{Name: "uq_users_email", StructName: "User", Fields: []string{"email"}}},
	}
}

// TestPlanner_MySQLFamilyDropsAConstraintBackedKeyAsAnIndex pins the spelling
// these engines accept, now that the comparator marks a constraint-backed
// removal on every engine rather than only where the spelling changes.
//
// A unique key and its constraint are one catalog row here and
// `ALTER TABLE ... DROP INDEX` removes it, which is what the pinned community
// binary v1.3.0 plans on MySQL 9.7.1. Reading the mark as "spell this as a
// constraint drop" -- the PostgreSQL-family answer -- would emit
// ALTER TABLE ... DROP CONSTRAINT here and change a plan that was already right.
// The key is rebuilt under its own name, so the drop and the add are one
// statement.
func TestPlanner_MySQLFamilyDropsAConstraintBackedKeyAsAnIndex(t *testing.T) {
	for _, test := range mysqlFamilyPlannerCases() {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			nodes, err := test.planner.GenerateMigrationAST(withDeclaredTables(uniqueKeyRebuildDiff(), uniqueKeyRebuildSchema()))

			c.Assert(err, qt.IsNil)
			c.Assert(nodes, qt.HasLen, 1)
			table, replacement := replacedIndex(c, nodes[0])
			c.Assert(table, qt.Equals, "users")
			c.Assert(replacement.Name, qt.Equals, "uq_users_email")
		})
	}
}

// TestPlanner_MySQLFamilyMarksTheUniquenessLossOnTheDrop covers what the mark is
// for on these engines: the statement is the same either way, so the risk
// classifier cannot read the spelling. Without the flag on the node, replacing a
// unique key with a plain index passes `--check-destructive`.
func TestPlanner_MySQLFamilyMarksTheUniquenessLossOnTheDrop(t *testing.T) {
	for _, test := range mysqlFamilyPlannerCases() {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			nodes, err := test.planner.GenerateMigrationAST(withDeclaredTables(uniqueKeyRebuildDiff(), uniqueKeyRebuildSchema()))

			c.Assert(err, qt.IsNil)
			c.Assert(nodes, qt.HasLen, 1)
			alter, isAlter := nodes[0].(*ast.AlterTableNode)
			c.Assert(isAlter, qt.IsTrue, qt.Commentf("node: %T", nodes[0]))
			replace, isReplace := alter.Operations[0].(*ast.ReplaceIndexOperation)
			c.Assert(isReplace, qt.IsTrue, qt.Commentf("operation: %T", alter.Operations[0]))
			c.Assert(replace.DropsUniqueConstraint, qt.IsTrue)
		})
	}
}

// TestPlanner_MySQLFamilyLeavesAPlainIndexDropUnmarked is the control: an index
// no constraint enforces carries no mark, so its removal keeps the warning
// severity a query-plan change deserves.
func TestPlanner_MySQLFamilyLeavesAPlainIndexDropUnmarked(t *testing.T) {
	for _, test := range mysqlFamilyPlannerCases() {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := &difftypes.SchemaDiff{
				IndexesRemoved: []difftypes.IndexRef{{Name: "idx_users_email", TableName: "users"}},
			}

			nodes, err := test.planner.GenerateMigrationAST(diff)

			c.Assert(err, qt.IsNil)
			c.Assert(nodes, qt.HasLen, 1)
			drop, isDropIndex := nodes[0].(*ast.DropIndexNode)
			c.Assert(isDropIndex, qt.IsTrue, qt.Commentf("only node: %T", nodes[0]))
			c.Assert(drop.EnforcesUniqueConstraint, qt.IsFalse)
		})
	}
}
