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

// TestForeignKeyMoved_SQLServerBuildsNoIndex pins a foreign key moved under its own name
// on SQL Server, which builds a key no index: nothing takes the old index's name, so the
// index leaves with the removals.
func TestForeignKeyMoved_SQLServerBuildsNoIndex(t *testing.T) {
	c := qt.New(t)

	got := plannedOverHeld(c, "sqlserver", declaredMovedForeignKey("child_fk"), heldMovedForeignKey())

	c.Assert(got, qt.DeepEquals, []string{
		"ALTER TABLE [child] DROP CONSTRAINT [child_fk]",
		"ALTER TABLE [child] ADD CONSTRAINT [child_fk] FOREIGN KEY ([qid]) REFERENCES [parent] ([id])",
		"DROP INDEX [child_fk] ON [child]",
	})
}

// TestForeignKeyMoved_AKeptKeyStandsOnTheOldIndex pins the order for a key moved under its
// name over another key the plan keeps. The old index cannot leave alone, and it holds the
// name the moved key's new index takes, so the statement that gives the kept key an index
// of its own comes ahead of the addition.
func TestForeignKeyMoved_AKeptKeyStandsOnTheOldIndex(t *testing.T) {
	c := qt.New(t)
	held := heldMovedForeignKey()
	held.Constraints = append(held.Constraints, catalog.Constraint{
		Name: "d_fk", TableName: "child", Type: "FOREIGN KEY", ColumnName: "pid", ColumnNames: []string{"pid"},
		ForeignTable: new("parent"), ForeignColumn: new("code"),
	})
	held.Tables[0].Columns = append(held.Tables[0].Columns, catalog.Column{Name: "code", DataType: "bigint", ColumnType: "bigint", IsNullable: "NO"})
	declared := declaredMovedForeignKey("child_fk")
	declared.Fields = append(declared.Fields, schemamodel.Field{StructName: "Parent", Name: "code", Type: "bigint"})
	declared.Constraints = append(declared.Constraints, schemamodel.Constraint{
		StructName: "Child", Name: "d_fk", Type: "FOREIGN KEY", Table: "child", Columns: []string{"pid"},
		ForeignTable: "parent", ForeignColumn: "code",
	})

	got := plannedOverHeld(c, "mysql", declared, held)

	c.Assert(got, qt.DeepEquals, []string{
		"ALTER TABLE `child` DROP FOREIGN KEY `child_fk`",
		"ALTER TABLE `child` DROP INDEX `child_fk`, ADD INDEX `d_fk` (`pid`)",
		"ALTER TABLE `child` ADD CONSTRAINT `child_fk` FOREIGN KEY (`qid`) REFERENCES `parent`(`id`)",
	})
}

// TestForeignKeyMoved_TheRollbackRebuildsTheIndexWithTheKeyGone pins the rollback of a key
// moved over a key the plan keeps. The index under the key's name comes back in one
// statement, and the key stands on that name until then, so it goes first.
func TestForeignKeyMoved_TheRollbackRebuildsTheIndexWithTheKeyGone(t *testing.T) {
	c := qt.New(t)
	const parent = `CREATE TABLE parent (id bigint PRIMARY KEY, code bigint NOT NULL, CONSTRAINT parent_code UNIQUE (code));`

	got := rollbackOfPlan(c, "mysql",
		parent+`CREATE TABLE child (id bigint PRIMARY KEY, pid bigint, qid bigint,
			CONSTRAINT d_fk FOREIGN KEY (pid) REFERENCES parent (code), CONSTRAINT c_fk FOREIGN KEY (pid) REFERENCES parent (id));
			CREATE INDEX c_fk ON child (pid);`,
		parent+`CREATE TABLE child (id bigint PRIMARY KEY, pid bigint, qid bigint,
			CONSTRAINT d_fk FOREIGN KEY (pid) REFERENCES parent (code), CONSTRAINT c_fk FOREIGN KEY (qid) REFERENCES parent (id));`,
	)

	c.Assert(got, qt.DeepEquals, []string{
		"ALTER TABLE `child` DROP FOREIGN KEY `c_fk`",
		"ALTER TABLE `child` DROP INDEX `c_fk`, ADD INDEX `c_fk` (`pid`)",
		"ALTER TABLE `child` ADD CONSTRAINT `c_fk` FOREIGN KEY (`pid`) REFERENCES `parent`(`id`)",
		"DROP INDEX `d_fk` ON `child`",
	})
}
