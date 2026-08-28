package postgres_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/renderer"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/internal/planner/dialects/postgres"
	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
)

// TestPlannerOrdersABareNameForeignKeyAfterTheOtherKinds pins that a constraint
// addition carrying nothing but a NAME is still classified by kind.
//
// The planner adds constraints in two passes, everything else and then the
// foreign keys, because a FOREIGN KEY may reference columns a UNIQUE constraint
// in the same plan is what makes referenceable. Which pass a name belongs to is
// decided by resolving it against the declaration.
//
// A bare name is a real shape rather than a hand-built curiosity.
// `constraintscope.coverBareAdditions` gives every name in `ConstraintsAdded`
// that no record answers a record of its own -- `{Name: name}`, with no Type --
// and the reversal produces exactly that when it has no database schema to
// resolve against. So a classification that read the record instead of the
// declaration would answer "not a foreign key" for every one of them.
//
// Measured: with the resolution replaced by a read of
// `ConstraintsAddedWithTables[...].Type`, this test's foreign key moves ahead of
// the check constraint and the whole unit suite still passes. That is what this
// test exists to stop, and the names are chosen so alphabetical order is the
// WRONG answer -- `aaa_` for the foreign key, `zzz_` for the check -- because a
// classification that collapses to "emit in name order" would otherwise agree
// with the correct one by accident.
func TestPlannerOrdersABareNameForeignKeyAfterTheOtherKinds(t *testing.T) {
	c := qt.New(t)

	desired := bareConstraintKindSchema()
	diff := &difftypes.SchemaDiff{
		ConstraintsAdded: []string{"aaa_fk_child_parent", "zzz_ck_child_amount"},
	}

	nodes, err := (&postgres.Planner{}).GenerateMigrationAST(diff, desired)
	c.Assert(err, qt.IsNil)
	sql, err := renderer.RenderSQL("postgres", nodes...)
	c.Assert(err, qt.IsNil)

	check := strings.Index(sql, "zzz_ck_child_amount")
	foreignKey := strings.Index(sql, "aaa_fk_child_parent")
	c.Assert(check, qt.Not(qt.Equals), -1, qt.Commentf("plan:\n%s", sql))
	c.Assert(foreignKey, qt.Not(qt.Equals), -1, qt.Commentf("plan:\n%s", sql))
	c.Assert(check < foreignKey, qt.IsTrue,
		qt.Commentf("a bare-name FOREIGN KEY must be added after the other kinds; plan:\n%s", sql))
}

// bareConstraintKindSchema declares one child table with a foreign key and a
// check, both as table-level constraints so that a diff naming them carries no
// kind of its own.
func bareConstraintKindSchema() *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "Parent", Name: "wf2315_parents"},
			{StructName: "Child", Name: "wf2315_children"},
		},
		Fields: []schemamodel.Field{
			{StructName: "Parent", Name: "id", Type: "BIGINT", Primary: true},
			{StructName: "Child", Name: "id", Type: "BIGINT", Primary: true},
			{StructName: "Child", Name: "parent_id", Type: "BIGINT"},
			{StructName: "Child", Name: "amount", Type: "BIGINT"},
		},
		Constraints: []schemamodel.Constraint{
			{
				StructName: "Child", Table: "wf2315_children",
				Name: "aaa_fk_child_parent", Type: "FOREIGN KEY",
				Columns: []string{"parent_id"}, ForeignTable: "wf2315_parents",
				ForeignColumn: "id",
			},
			{
				StructName: "Child", Table: "wf2315_children",
				Name: "zzz_ck_child_amount", Type: "CHECK",
				CheckExpression: "amount > 0",
			},
		},
	}
}
