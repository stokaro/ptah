package mysql_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/renderer"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/internal/planner/dialects/mysql"
	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
)

// TestPlannerOrdersABareNameForeignKeyAfterTheOtherKinds is the MySQL half of
// the property its PostgreSQL namesake states, and it is written out rather than
// shared because each dialect resolves the name with its own predicate.
//
// The two are not the same code: MySQL's foreign-key name derivation takes the
// table name as declared where PostgreSQL takes it unqualified. A control that
// exercised one and assumed the other would leave whichever it skipped free to
// stop classifying at all.
func TestPlannerOrdersABareNameForeignKeyAfterTheOtherKinds(t *testing.T) {
	c := qt.New(t)

	desired := bareConstraintKindSchema()
	diff := &difftypes.SchemaDiff{
		ConstraintsAdded: []string{"aaa_fk_child_parent", "zzz_ck_child_amount"},
	}

	nodes, err := mysql.New().GenerateMigrationAST(withDeclaredObjects(diff, desired))
	c.Assert(err, qt.IsNil)
	sql, err := renderer.RenderSQL("mysql", nodes...)
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
// kind of its own. The names are chosen so alphabetical order is the wrong
// answer.
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
