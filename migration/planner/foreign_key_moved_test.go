package planner_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
)

// heldMovedForeignKey is a read of child, whose foreign key child_fk stands on the index
// the server built for it over pid, with the column qid beside it.
func heldMovedForeignKey() *catalog.Database {
	held := heldBackedForeignKey()
	held.Tables[1].Columns = append(held.Tables[1].Columns, catalog.Column{Name: "qid", DataType: "bigint", ColumnType: "bigint", IsNullable: "YES"})
	return held
}

// declaredMovedForeignKey declares the foreign key name over qid.
func declaredMovedForeignKey(name string) *schemamodel.Database {
	return declaredWithoutForeignKey(func(declared *schemamodel.Database) {
		declared.Fields = append(declared.Fields, schemamodel.Field{StructName: "Child", Name: "qid", Type: "bigint", Nullable: true})
		declared.Constraints = append(declared.Constraints, schemamodel.Constraint{
			StructName: "Child", Name: name, Type: "FOREIGN KEY", Table: "child", Columns: []string{"qid"},
			ForeignTable: "parent", ForeignColumn: "id",
		})
	})
}

// TestForeignKeyMoved_HappyPath pins the order for a foreign key moved to another column
// under its own name. The server builds the key an index of its name, which the old
// index still holds until it leaves, so the old index goes between the two.
func TestForeignKeyMoved_HappyPath(t *testing.T) {
	want := []string{
		"ALTER TABLE `child` DROP FOREIGN KEY `child_fk`",
		"DROP INDEX `child_fk` ON `child`",
		"ALTER TABLE `child` ADD CONSTRAINT `child_fk` FOREIGN KEY (`qid`) REFERENCES `parent`(`id`)",
	}
	for _, dialect := range []string{"mysql", "mariadb"} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)

			got := plannedOverHeld(c, dialect, declaredMovedForeignKey("child_fk"), heldMovedForeignKey())

			c.Assert(got, qt.DeepEquals, want)
		})
	}
}

// TestForeignKeyMoved_FailurePath pins the plan that keeps its order: the new key has
// another name, so the old index leaves with the removals.
func TestForeignKeyMoved_FailurePath(t *testing.T) {
	c := qt.New(t)

	got := plannedOverHeld(c, "mysql", declaredMovedForeignKey("other_fk"), heldMovedForeignKey())

	c.Assert(got, qt.DeepEquals, []string{
		"ALTER TABLE `child` ADD CONSTRAINT `other_fk` FOREIGN KEY (`qid`) REFERENCES `parent`(`id`)",
		"ALTER TABLE `child` DROP FOREIGN KEY `child_fk`",
		"DROP INDEX `child_fk` ON `child`",
	})
}

// TestForeignKeyMoved_ACheckLeavesTheIndexToTheRemovals pins an index under the name of
// a CHECK constraint the plan adds: the server builds no index for it, so the index
// leaves with the removals, and it does leave.
func TestForeignKeyMoved_ACheckLeavesTheIndexToTheRemovals(t *testing.T) {
	c := qt.New(t)
	held := heldMovedForeignKey()
	held.Indexes = append(held.Indexes, catalog.Index{Name: "qid_rule", TableName: "child", Columns: []string{"qid"}})
	declared := declaredWithoutForeignKey(func(declared *schemamodel.Database) {
		declared.Fields = append(declared.Fields, schemamodel.Field{StructName: "Child", Name: "qid", Type: "bigint", Nullable: true})
		declared.Constraints = append(declared.Constraints,
			schemamodel.Constraint{
				StructName: "Child", Name: "child_fk", Type: "FOREIGN KEY", Table: "child", Columns: []string{"pid"},
				ForeignTable: "parent", ForeignColumn: "id",
			},
			schemamodel.Constraint{StructName: "Child", Name: "qid_rule", Type: "CHECK", Table: "child", CheckExpression: "qid > 0"},
		)
	})

	got := plannedOverHeld(c, "mysql", declared, held)

	c.Assert(got, qt.DeepEquals, []string{
		"ALTER TABLE `child` ADD CONSTRAINT `qid_rule` CHECK (qid > 0)",
		"DROP INDEX `qid_rule` ON `child`",
	})
}
