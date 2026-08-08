package postgres_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/internal/planner/dialects/postgres"
	"go.5x5.cz/ptah/migration/schemadiff/types"
)

// uniqueKeyRebuildDiff is a database `CONSTRAINT uq_users_email UNIQUE (email)`
// against a desired state that names the same object as a plain index: one
// object, replaced, which the comparator states as an index addition plus the
// removal of the index it replaces.
func uniqueKeyRebuildDiff(constraintBacked []types.IndexRef) *types.SchemaDiff {
	return &types.SchemaDiff{
		IndexesAdded:                  []types.IndexRef{{Name: "uq_users_email", TableName: "users"}},
		IndexesRemoved:                []types.IndexRef{{Name: "uq_users_email", TableName: "users"}},
		ConstraintBackedIndexRemovals: constraintBacked,
	}
}

func uniqueKeyRebuildSchema() *goschema.Database {
	return &goschema.Database{
		Tables:  []goschema.Table{{Name: "users", StructName: "User"}},
		Fields:  []goschema.Field{{Name: "email", StructName: "User", Type: "TEXT"}},
		Indexes: []goschema.Index{{Name: "uq_users_email", StructName: "User", Fields: []string{"email"}}},
	}
}

// TestPlanner_ConstraintBackedIndexRebuildDropsTheConstraint pins the spelling
// PostgreSQL accepts for the drop half of a rebuild whose object a UNIQUE
// constraint owns. Measured on PostgreSQL 17.10, `DROP INDEX "uq_users_email"`
// against a table with `CONSTRAINT uq_users_email UNIQUE (email)` answers
// `cannot drop index uq_users_email because constraint uq_users_email on table
// users requires it (SQLSTATE 2BP01)`, and the pinned community binary v1.3.0
// plans `ALTER TABLE "users" DROP CONSTRAINT "uq_users_email"` followed by the
// CREATE INDEX for the same change.
func TestPlanner_ConstraintBackedIndexRebuildDropsTheConstraint(t *testing.T) {
	c := qt.New(t)

	nodes, err := postgres.New().GenerateMigrationASTChecked(
		uniqueKeyRebuildDiff([]types.IndexRef{{Name: "uq_users_email", TableName: "users"}}),
		uniqueKeyRebuildSchema(),
	)

	c.Assert(err, qt.IsNil)
	c.Assert(nodes, qt.HasLen, 2)
	drop, isAlterTable := nodes[0].(*ast.AlterTableNode)
	c.Assert(isAlterTable, qt.IsTrue, qt.Commentf("first node: %T", nodes[0]))
	c.Assert(drop.Name, qt.Equals, "users")
	c.Assert(drop.Operations, qt.HasLen, 1)
	dropConstraint, isDropConstraint := drop.Operations[0].(*ast.DropConstraintOperation)
	c.Assert(isDropConstraint, qt.IsTrue, qt.Commentf("first operation: %T", drop.Operations[0]))
	c.Assert(dropConstraint.ConstraintName, qt.Equals, "uq_users_email")
	c.Assert(dropConstraint.IfExists, qt.IsTrue)
	create, isIndex := nodes[1].(*ast.IndexNode)
	c.Assert(isIndex, qt.IsTrue, qt.Commentf("second node: %T", nodes[1]))
	c.Assert(create.Name, qt.Equals, "uq_users_email")
	c.Assert(create.Table, qt.Equals, "users")
}

// TestPlanner_UnmarkedIndexRebuildStillDropsTheIndex is the control: the
// constraint spelling is reserved for the removals the comparator marked. An
// ordinary index rebuild -- the same diff with nothing marked -- keeps
// DROP INDEX, which is the only statement that removes an index PostgreSQL
// does not enforce a constraint with.
func TestPlanner_UnmarkedIndexRebuildStillDropsTheIndex(t *testing.T) {
	c := qt.New(t)

	nodes, err := postgres.New().GenerateMigrationASTChecked(
		uniqueKeyRebuildDiff(nil),
		uniqueKeyRebuildSchema(),
	)

	c.Assert(err, qt.IsNil)
	c.Assert(nodes, qt.HasLen, 2)
	drop, isDropIndex := nodes[0].(*ast.DropIndexNode)
	c.Assert(isDropIndex, qt.IsTrue, qt.Commentf("first node: %T", nodes[0]))
	c.Assert(drop.Name, qt.Equals, "uq_users_email")
	c.Assert(drop.IfExists, qt.IsTrue)
}

// TestPlanner_ConstraintBackedStandaloneRemovalDropsTheConstraint covers the
// removal that has no addition beside it. The object is the same catalog row
// either way, so the statement that removes it is the same one.
func TestPlanner_ConstraintBackedStandaloneRemovalDropsTheConstraint(t *testing.T) {
	c := qt.New(t)
	diff := &types.SchemaDiff{
		IndexesRemoved:                []types.IndexRef{{Name: "uq_users_email", TableName: "users"}},
		ConstraintBackedIndexRemovals: []types.IndexRef{{Name: "uq_users_email", TableName: "users"}},
	}

	nodes, err := postgres.New().GenerateMigrationASTChecked(diff, &goschema.Database{})

	c.Assert(err, qt.IsNil)
	c.Assert(nodes, qt.HasLen, 1)
	drop, isAlterTable := nodes[0].(*ast.AlterTableNode)
	c.Assert(isAlterTable, qt.IsTrue, qt.Commentf("only node: %T", nodes[0]))
	c.Assert(drop.Name, qt.Equals, "users")
	c.Assert(drop.Operations, qt.HasLen, 1)
	dropConstraint, isDropConstraint := drop.Operations[0].(*ast.DropConstraintOperation)
	c.Assert(isDropConstraint, qt.IsTrue, qt.Commentf("only operation: %T", drop.Operations[0]))
	c.Assert(dropConstraint.ConstraintName, qt.Equals, "uq_users_email")
}
