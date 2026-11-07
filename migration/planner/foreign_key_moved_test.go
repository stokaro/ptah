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

// heldKeptKeyOnTwoIndexes is a read of child, whose foreign key k_fk stands on the indexes
// x_fk and y_fk over pid, with the keys given beside it.
func heldKeptKeyOnTwoIndexes(keys ...catalog.Constraint) *catalog.Database {
	held := heldMovedForeignKey()
	held.Tables[0].Columns = append(held.Tables[0].Columns,
		catalog.Column{Name: "code", DataType: "bigint", ColumnType: "bigint", IsNullable: "NO"},
		catalog.Column{Name: "alt", DataType: "bigint", ColumnType: "bigint", IsNullable: "NO"},
	)
	held.Tables[1].Columns = append(held.Tables[1].Columns, catalog.Column{Name: "rid", DataType: "bigint", ColumnType: "bigint", IsNullable: "YES"})
	held.Indexes = []catalog.Index{
		{Name: "x_fk", TableName: "child", Columns: []string{"pid"}},
		{Name: "y_fk", TableName: "child", Columns: []string{"pid"}},
	}
	held.Constraints = append([]catalog.Constraint{{
		Name: "k_fk", TableName: "child", Type: "FOREIGN KEY", ColumnName: "pid", ColumnNames: []string{"pid"},
		ForeignTable: new("parent"), ForeignColumn: new("alt"),
	}}, keys...)
	return held
}

// declaredKeptKeyBesideTwoKeys declares k_fk over pid, x_fk over qid and y_fk over rid.
func declaredKeptKeyBesideTwoKeys() *schemamodel.Database {
	return declaredWithoutForeignKey(func(declared *schemamodel.Database) {
		declared.Fields = append(declared.Fields,
			schemamodel.Field{StructName: "Parent", Name: "code", Type: "bigint"},
			schemamodel.Field{StructName: "Parent", Name: "alt", Type: "bigint"},
			schemamodel.Field{StructName: "Child", Name: "qid", Type: "bigint", Nullable: true},
			schemamodel.Field{StructName: "Child", Name: "rid", Type: "bigint", Nullable: true},
		)
		declared.Constraints = append(declared.Constraints,
			schemamodel.Constraint{
				StructName: "Child", Name: "k_fk", Type: "FOREIGN KEY", Table: "child", Columns: []string{"pid"},
				ForeignTable: "parent", ForeignColumn: "alt",
			},
			schemamodel.Constraint{
				StructName: "Child", Name: "x_fk", Type: "FOREIGN KEY", Table: "child", Columns: []string{"qid"},
				ForeignTable: "parent", ForeignColumn: "id",
			},
			schemamodel.Constraint{
				StructName: "Child", Name: "y_fk", Type: "FOREIGN KEY", Table: "child", Columns: []string{"rid"},
				ForeignTable: "parent", ForeignColumn: "code",
			},
		)
	})
}

// TestForeignKeyMoved_TwoKeysTakeTheNamesOfTheIndexesAKeptKeyStandsOn pins one statement
// for a kept key whose indexes all leave, each under the name of a foreign key the plan
// moves or adds. The statement drops both indexes, and the plan wrote it at each key's
// addition, so the second answered `Error 1091: Can't DROP` with the first key in place.
func TestForeignKeyMoved_TwoKeysTakeTheNamesOfTheIndexesAKeptKeyStandsOn(t *testing.T) {
	const (
		takeover = "ALTER TABLE `child` DROP INDEX `x_fk`, DROP INDEX `y_fk`, ADD INDEX `k_fk` (`pid`)"
		addX     = "ALTER TABLE `child` ADD CONSTRAINT `x_fk` FOREIGN KEY (`qid`) REFERENCES `parent`(`id`)"
		addY     = "ALTER TABLE `child` ADD CONSTRAINT `y_fk` FOREIGN KEY (`rid`) REFERENCES `parent`(`code`)"
	)
	tests := []struct {
		name string
		keys []catalog.Constraint
		want []string
	}{
		{
			name: "two keys move under their names",
			keys: []catalog.Constraint{
				{
					Name: "x_fk", TableName: "child", Type: "FOREIGN KEY", ColumnName: "pid", ColumnNames: []string{"pid"},
					ForeignTable: new("parent"), ForeignColumn: new("id"),
				},
				{
					Name: "y_fk", TableName: "child", Type: "FOREIGN KEY", ColumnName: "pid", ColumnNames: []string{"pid"},
					ForeignTable: new("parent"), ForeignColumn: new("code"),
				},
			},
			want: []string{
				"ALTER TABLE `child` DROP FOREIGN KEY `x_fk`",
				"ALTER TABLE `child` DROP FOREIGN KEY `y_fk`",
				takeover, addX, addY,
			},
		},
		{
			name: "two new keys take the names",
			want: []string{takeover, addX, addY},
		},
	}
	for _, dialect := range []string{"mysql", "mariadb"} {
		for _, test := range tests {
			t.Run(dialect+" "+test.name, func(t *testing.T) {
				c := qt.New(t)

				got := plannedOverHeld(c, dialect, declaredKeptKeyBesideTwoKeys(), heldKeptKeyOnTwoIndexes(test.keys...))

				c.Assert(got, qt.DeepEquals, test.want)
			})
		}
	}
}

// childKey is the foreign key name of child over column, referencing parent's referenced.
func childKey(name, column, referenced string) catalog.Constraint {
	return catalog.Constraint{
		Name: name, TableName: "child", Type: "FOREIGN KEY", ColumnName: column, ColumnNames: []string{column},
		ForeignTable: new("parent"), ForeignColumn: new(referenced),
	}
}

// declaredChildKey declares the foreign key name of child over column, referencing
// parent's referenced.
func declaredChildKey(name, column, referenced string) schemamodel.Constraint {
	return schemamodel.Constraint{
		StructName: "Child", Name: name, Type: "FOREIGN KEY", Table: "child", Columns: []string{column},
		ForeignTable: "parent", ForeignColumn: referenced,
	}
}

// TestForeignKeyMoved_AnotherChangedKeyStandsOnTheOldIndex pins the order for a key moved
// under its name whose old index another key the plan moves or removes stands on. The old
// index leaves at the moved key's addition, ahead of the plan's constraint removals, and
// the server answered its drop with Error 1553 while the other key stood, on MySQL 8.4 and
// 26.10 and MariaDB 12.3. Every key the plan moves or removes on the table leaves first.
func TestForeignKeyMoved_AnotherChangedKeyStandsOnTheOldIndex(t *testing.T) {
	tests := []struct {
		name     string
		index    string
		keys     []catalog.Constraint
		declared []schemamodel.Constraint
		want     []string
	}{
		{
			name:     "both keys move under their names",
			index:    "c_fk",
			keys:     []catalog.Constraint{childKey("d_fk", "pid", "code"), childKey("c_fk", "pid", "id")},
			declared: []schemamodel.Constraint{declaredChildKey("c_fk", "qid", "id"), declaredChildKey("d_fk", "rid", "code")},
			want: []string{
				"ALTER TABLE `child` DROP FOREIGN KEY `c_fk`",
				"ALTER TABLE `child` DROP FOREIGN KEY `d_fk`",
				"DROP INDEX `c_fk` ON `child`",
				"ALTER TABLE `child` ADD CONSTRAINT `c_fk` FOREIGN KEY (`qid`) REFERENCES `parent`(`id`)",
				"ALTER TABLE `child` ADD CONSTRAINT `d_fk` FOREIGN KEY (`rid`) REFERENCES `parent`(`code`)",
			},
		},
		{
			name:     "a removed key stands on it too",
			index:    "d_fk",
			keys:     []catalog.Constraint{childKey("c_fk", "pid", "id"), childKey("d_fk", "pid", "code")},
			declared: []schemamodel.Constraint{declaredChildKey("d_fk", "qid", "code")},
			want: []string{
				"ALTER TABLE `child` DROP FOREIGN KEY `c_fk`",
				"ALTER TABLE `child` DROP FOREIGN KEY `d_fk`",
				"DROP INDEX `d_fk` ON `child`",
				"ALTER TABLE `child` ADD CONSTRAINT `d_fk` FOREIGN KEY (`qid`) REFERENCES `parent`(`code`)",
			},
		},
		{
			name:     "two keys move and a third leaves",
			index:    "c_fk",
			keys:     []catalog.Constraint{childKey("d_fk", "pid", "code"), childKey("e_fk", "pid", "alt"), childKey("c_fk", "pid", "id")},
			declared: []schemamodel.Constraint{declaredChildKey("c_fk", "qid", "id"), declaredChildKey("d_fk", "rid", "code")},
			want: []string{
				"ALTER TABLE `child` DROP FOREIGN KEY `c_fk`",
				"ALTER TABLE `child` DROP FOREIGN KEY `d_fk`",
				"ALTER TABLE `child` DROP FOREIGN KEY `e_fk`",
				"DROP INDEX `c_fk` ON `child`",
				"ALTER TABLE `child` ADD CONSTRAINT `c_fk` FOREIGN KEY (`qid`) REFERENCES `parent`(`id`)",
				"ALTER TABLE `child` ADD CONSTRAINT `d_fk` FOREIGN KEY (`rid`) REFERENCES `parent`(`code`)",
			},
		},
	}
	for _, dialect := range []string{"mysql", "mariadb"} {
		for _, test := range tests {
			t.Run(dialect+" "+test.name, func(t *testing.T) {
				c := qt.New(t)
				held := heldKeptKeyOnTwoIndexes()
				held.Indexes = []catalog.Index{{Name: test.index, TableName: "child", Columns: []string{"pid"}}}
				held.Constraints = test.keys
				declared := declaredKeptKeyBesideTwoKeys()
				declared.Constraints = test.declared

				got := plannedOverHeld(c, dialect, declared, held)

				c.Assert(got, qt.DeepEquals, test.want)
			})
		}
	}
}

// TestForeignKeyMoved_TheRollbackOfTwoKeysMovedOntoOneColumn pins the rollback of two keys
// moved onto one column under their names. The server builds the second key an index in
// place of the one it built for the first, so the migration leaves one index under the
// second key's name, measured on MySQL 8.4 and 26.10 and MariaDB 12.3. The rollback added
// the old index under that name while the new one held it, and the server answered Error
// 1061. It rebuilds that index in one statement, with both keys gone.
func TestForeignKeyMoved_TheRollbackOfTwoKeysMovedOntoOneColumn(t *testing.T) {
	c := qt.New(t)
	const parent = `CREATE TABLE parent (id bigint PRIMARY KEY, code bigint NOT NULL, CONSTRAINT parent_code UNIQUE (code));`

	got := rollbackOfPlan(c, "mysql",
		parent+`CREATE TABLE child (id bigint PRIMARY KEY, pid bigint, qid bigint,
			CONSTRAINT c_fk FOREIGN KEY (pid) REFERENCES parent (id), CONSTRAINT d_fk FOREIGN KEY (pid) REFERENCES parent (code));
			CREATE INDEX d_fk ON child (pid);`,
		parent+`CREATE TABLE child (id bigint PRIMARY KEY, pid bigint, qid bigint,
			CONSTRAINT c_fk FOREIGN KEY (qid) REFERENCES parent (id), CONSTRAINT d_fk FOREIGN KEY (qid) REFERENCES parent (code));`,
	)

	c.Assert(got, qt.DeepEquals, []string{
		"ALTER TABLE `child` DROP FOREIGN KEY `c_fk`",
		"ALTER TABLE `child` DROP FOREIGN KEY `d_fk`",
		"ALTER TABLE `child` DROP INDEX `d_fk`, ADD INDEX `d_fk` (`pid`)",
		"ALTER TABLE `child` ADD CONSTRAINT `c_fk` FOREIGN KEY (`pid`) REFERENCES `parent`(`id`)",
		"ALTER TABLE `child` ADD CONSTRAINT `d_fk` FOREIGN KEY (`pid`) REFERENCES `parent`(`code`)",
	})
}
