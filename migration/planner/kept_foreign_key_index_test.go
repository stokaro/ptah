package planner_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
)

// heldKeptForeignKey is a read of child, whose foreign key a_fk stands on the indexes given.
func heldKeptForeignKey(indexes ...catalog.Index) *catalog.Database {
	held := heldBackedForeignKey()
	held.Constraints[0].Name = "a_fk"
	held.Indexes = indexes
	return held
}

// declaredKeptForeignKey declares parent and child with the foreign key a_fk.
func declaredKeptForeignKey() *schemamodel.Database {
	return declaredWithoutForeignKey(func(declared *schemamodel.Database) {
		declared.Constraints = append(declared.Constraints, schemamodel.Constraint{
			StructName: "Child", Name: "a_fk", Type: "FOREIGN KEY", Table: "child", Columns: []string{"pid"},
			ForeignTable: "parent", ForeignColumn: "id",
		})
	})
}

// TestKeptForeignKeyIndex_HappyPath pins the statement for an index the plan removes from
// under a foreign key it keeps. DROP INDEX alone answers Error 1553, and an index added
// ahead of it takes the key over, which can make the server drop an index it built by
// itself, so the removal then answers Error 1091. One statement does both.
func TestKeptForeignKeyIndex_HappyPath(t *testing.T) {
	cover := catalog.Index{Name: "cover", TableName: "child", Columns: []string{"pid"}}
	tests := []struct {
		name    string
		indexes []catalog.Index
		want    []string
	}{
		{
			name:    "one index",
			indexes: []catalog.Index{cover},
			want:    []string{"ALTER TABLE `child` DROP INDEX `cover`, ADD INDEX `a_fk` (`pid`)"},
		},
		{
			name:    "two indexes",
			indexes: []catalog.Index{cover, {Name: "other", TableName: "child", Columns: []string{"pid"}}},
			want:    []string{"ALTER TABLE `child` DROP INDEX `cover`, DROP INDEX `other`, ADD INDEX `a_fk` (`pid`)"},
		},
		{
			name:    "a wider index",
			indexes: []catalog.Index{{Name: "wide", TableName: "child", Columns: []string{"pid", "id"}}},
			want:    []string{"ALTER TABLE `child` DROP INDEX `wide`, ADD INDEX `a_fk` (`pid`)"},
		},
	}
	for _, dialect := range []string{"mysql", "mariadb"} {
		for _, test := range tests {
			t.Run(dialect+" "+test.name, func(t *testing.T) {
				c := qt.New(t)

				got := plannedOverHeld(c, dialect, declaredKeptForeignKey(), heldKeptForeignKey(test.indexes...))

				c.Assert(got, qt.DeepEquals, test.want)
			})
		}
	}
}

// TestKeptForeignKeyIndex_FailurePath pins the plans that add no index: the key keeps one
// to stand on, or stands on none the plan removes.
func TestKeptForeignKeyIndex_FailurePath(t *testing.T) {
	declaredIndex := func() *schemamodel.Database {
		declared := declaredKeptForeignKey()
		declared.Indexes = append(declared.Indexes, schemamodel.Index{StructName: "Child", Name: "kept", Fields: []string{"pid"}})
		return declared
	}
	tests := []struct {
		name     string
		declared *schemamodel.Database
		indexes  []catalog.Index
		want     []string
	}{
		{
			name:     "another index stays under the key",
			declared: declaredIndex(),
			indexes: []catalog.Index{
				{Name: "kept", TableName: "child", Columns: []string{"pid"}},
				{Name: "cover", TableName: "child", Columns: []string{"pid"}},
			},
			want: []string{"DROP INDEX `cover` ON `child`"},
		},
		{
			name:     "the removed index is on another column",
			declared: declaredKeptForeignKey(),
			indexes: []catalog.Index{
				{Name: "a_fk", TableName: "child", Columns: []string{"pid"}},
				{Name: "other", TableName: "child", Columns: []string{"id"}},
			},
			want: []string{"DROP INDEX `other` ON `child`"},
		},
		{
			name:     "the server builds its own for the key",
			declared: declaredKeptForeignKey(),
			indexes:  []catalog.Index{{Name: "a_fk", TableName: "child", Columns: []string{"pid"}}},
			want:     make([]string, 0),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := plannedOverHeld(c, "mysql", test.declared, heldKeptForeignKey(test.indexes...))

			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

// TestKeptForeignKeyIndex_AnIndexThePlanAddsTakesItOver pins the statement for an index
// the plan adds over a foreign key it keeps, where the key stands on the index the server
// built for it. The server drops that index by itself once the new one is there, and the
// rollback's DROP INDEX of the new index then answered Error 1553 on MySQL 8.4 and 26.10
// and MariaDB 12.3; where the author had named an index that way it stayed, and the next
// comparison dropped it. The statement that adds the new index drops the old one.
func TestKeptForeignKeyIndex_AnIndexThePlanAddsTakesItOver(t *testing.T) {
	unnamed := heldKeptForeignKey(catalog.Index{Name: "pid", TableName: "child", Columns: []string{"pid"}})
	unnamed.Constraints[0].Name = "child_ibfk_1"
	shared := heldKeptForeignKey(catalog.Index{Name: "a_fk", TableName: "child", Columns: []string{"pid"}})
	shared.Constraints = append(shared.Constraints, catalog.Constraint{
		Name: "b_fk", TableName: "child", Type: "FOREIGN KEY", ColumnName: "pid", ColumnNames: []string{"pid"},
		ForeignTable: new("parent"), ForeignColumn: new("id"),
	})
	withIndex := func(declared *schemamodel.Database, name string, fields ...string) *schemamodel.Database {
		declared.Indexes = append(declared.Indexes, schemamodel.Index{StructName: "Child", Name: name, Fields: fields})
		return declared
	}
	wide := heldKeptForeignKey(catalog.Index{Name: "a_fk", TableName: "child", Columns: []string{"pid"}})
	wide.Tables[1].Columns = append(wide.Tables[1].Columns, catalog.Column{Name: "qid", DataType: "bigint", ColumnType: "bigint", IsNullable: "YES"})
	wideDeclared := declaredKeptForeignKey()
	wideDeclared.Fields = append(wideDeclared.Fields, schemamodel.Field{StructName: "Child", Name: "qid", Type: "bigint", Nullable: true})
	unnamedDeclared := declaredKeptForeignKey()
	unnamedDeclared.Constraints[0].Name = "child_ibfk_1"
	tests := []struct {
		name     string
		held     *catalog.Database
		declared *schemamodel.Database
		want     []string
	}{
		{
			name:     "an index over the key's column",
			held:     heldKeptForeignKey(catalog.Index{Name: "a_fk", TableName: "child", Columns: []string{"pid"}}),
			declared: withIndex(declaredKeptForeignKey(), "idx_pid", "pid"),
			want:     []string{"ALTER TABLE `child` DROP INDEX `a_fk`, ADD INDEX `idx_pid` (`pid`)"},
		},
		{
			name:     "a wider index",
			held:     wide,
			declared: withIndex(wideDeclared, "w", "pid", "qid"),
			want:     []string{"ALTER TABLE `child` DROP INDEX `a_fk`, ADD INDEX `w` (`pid`, `qid`)"},
		},
		{
			name:     "an unnamed key, whose index the server named after the column",
			held:     unnamed,
			declared: withIndex(unnamedDeclared, "idx_pid", "pid"),
			want:     []string{"ALTER TABLE `child` DROP INDEX `pid`, ADD INDEX `idx_pid` (`pid`)"},
		},
		{
			name:     "a removed key shares the index",
			held:     shared,
			declared: withIndex(declaredKeptForeignKey(), "idx_pid", "pid"),
			want: []string{
				"ALTER TABLE `child` DROP FOREIGN KEY `b_fk`",
				"ALTER TABLE `child` DROP INDEX `a_fk`, ADD INDEX `idx_pid` (`pid`)",
			},
		},
	}
	for _, dialect := range []string{"mysql", "mariadb"} {
		for _, test := range tests {
			t.Run(dialect+" "+test.name, func(t *testing.T) {
				c := qt.New(t)

				got := plannedOverHeld(c, dialect, test.declared, test.held)

				c.Assert(got, qt.DeepEquals, test.want)
			})
		}
	}
}

// TestKeptForeignKeyIndex_ADescendingIndexTakesItOverOnMariaDBAlone pins a descending
// index over the kept key's column. MySQL stands no key on it and keeps its own index
// beside it, while MariaDB stands the key on it, measured on MySQL 8.4 and MariaDB 12.3.
func TestKeptForeignKeyIndex_ADescendingIndexTakesItOverOnMariaDBAlone(t *testing.T) {
	tests := []struct {
		dialect string
		want    []string
	}{
		{dialect: "mysql", want: []string{"CREATE INDEX `idx_pid` ON `child` (`pid` DESC)"}},
		{dialect: "mariadb", want: []string{"ALTER TABLE `child` DROP INDEX `a_fk`, ADD INDEX `idx_pid` (`pid` DESC)"}},
	}
	for _, test := range tests {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)
			declared := declaredKeptForeignKey()
			declared.Indexes = []schemamodel.Index{{
				StructName: "Child", Name: "idx_pid", Fields: []string{"pid"}, Parts: []schemamodel.IndexPart{{Name: "pid", Desc: true}},
			}}

			got := plannedOverHeld(c, test.dialect, declared, heldKeptForeignKey(catalog.Index{Name: "a_fk", TableName: "child", Columns: []string{"pid"}}))

			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

// TestKeptForeignKeyIndex_TheRollbackPutsTheIndexBackFirst pins the rollback of an index
// added over a foreign key the plan keeps. It puts the key's own index back ahead of the
// drop of the added one, which the key stands on until then.
func TestKeptForeignKeyIndex_TheRollbackPutsTheIndexBackFirst(t *testing.T) {
	const (
		parent = `CREATE TABLE parent (id bigint PRIMARY KEY, code bigint NOT NULL, CONSTRAINT parent_code UNIQUE (code));`
		child  = `CREATE TABLE child (id bigint PRIMARY KEY, pid bigint, CONSTRAINT c_fk FOREIGN KEY (pid) REFERENCES parent (id)`
	)
	tests := []struct {
		name, held, desired string
		want                []string
	}{
		{
			name:    "the key keeps its column",
			held:    parent + child + `); CREATE INDEX c_fk ON child (pid);`,
			desired: parent + child + `); CREATE INDEX idx_pid ON child (pid);`,
			want:    []string{"CREATE INDEX `c_fk` ON `child` (`pid`)", "DROP INDEX `idx_pid` ON `child`"},
		},
		{
			name:    "a removed key shared the index",
			held:    parent + child + `, CONSTRAINT d_fk FOREIGN KEY (pid) REFERENCES parent (code)); CREATE INDEX d_fk ON child (pid);`,
			desired: parent + `CREATE TABLE child (id bigint PRIMARY KEY, pid bigint, CONSTRAINT d_fk FOREIGN KEY (pid) REFERENCES parent (code)); CREATE INDEX idx_pid ON child (pid);`,
			want: []string{
				"CREATE INDEX `d_fk` ON `child` (`pid`)",
				"ALTER TABLE `child` ADD CONSTRAINT `c_fk` FOREIGN KEY (`pid`) REFERENCES `parent`(`id`)",
				"DROP INDEX `idx_pid` ON `child`",
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := rollbackOfPlan(c, "mysql", test.held, test.desired)

			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}
