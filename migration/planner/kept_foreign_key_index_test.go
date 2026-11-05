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
