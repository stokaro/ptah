package schemadiff_test

import (
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff/difftypes"
)

// optionedIndexTable declares `t (id, a)` with index k_a over a, carrying
// comment and hidden from the optimizer or not.
func optionedIndexTable(comment string, invisible bool) *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "T", Name: "t", PrimaryKey: []string{"id"}}},
		Fields: []schemamodel.Field{
			{StructName: "T", Name: "id", Type: "int", Primary: true},
			{StructName: "T", Name: "a", Type: "int", Nullable: true},
		},
		Indexes: []schemamodel.Index{{
			StructName: "T", Name: "k_a", TableName: "t", Fields: []string{"a"}, Comment: comment, Invisible: invisible,
		}},
	}
}

// liveOptionedIndexTable is the same table as a MySQL catalog reports it.
func liveOptionedIndexTable(comment string, invisible bool) *catalog.Database {
	return &catalog.Database{
		Tables: []catalog.Table{{Name: "t", Columns: []catalog.Column{
			{Name: "id", DataType: "int", IsNullable: "NO", IsPrimaryKey: true},
			{Name: "a", DataType: "int", IsNullable: "YES"},
		}}},
		Indexes: []catalog.Index{{
			Name: "k_a", TableName: "t", Columns: []string{"a"}, Method: "BTREE", Comment: comment, Invisible: invisible,
		}},
	}
}

// TestCompare_IndexOptions_Synced pairs an index with the one the server built
// from it (stokaro/ptah#3853).
func TestCompare_IndexOptions_Synced(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		desired *schemamodel.Database
		live    *catalog.Database
	}{
		{name: "a comment", dialect: platform.MySQL, desired: optionedIndexTable("x", false), live: liveOptionedIndexTable("x", false)},
		{name: "an invisible index", dialect: platform.MariaDB, desired: optionedIndexTable("", true), live: liveOptionedIndexTable("", true)},
		{
			// PostgreSQL keeps an index comment as an object comment, which
			// this comparison does not read.
			name: "a PostgreSQL comment", dialect: platform.Postgres,
			desired: optionedIndexTable("x", false), live: liveOptionedIndexTable("y", false),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff := compareForDialect(test.dialect, test.desired, test.live)

			c.Assert(diff.HasChanges(), qt.IsFalse, qt.Commentf("%+v", diff))
		})
	}
}

// TestCompare_IndexOptions_ChangedComment rebuilds an index whose comment
// changed: on the MySQL family no statement changes it in place.
func TestCompare_IndexOptions_ChangedComment(t *testing.T) {
	c := qt.New(t)

	diff := compareForDialect(platform.MySQL, optionedIndexTable("new", true), liveOptionedIndexTable("old", false))

	c.Assert(diff.IndexesRemoved, qt.DeepEquals, []difftypes.IndexRef{{Name: "k_a", TableName: "t"}})
	c.Assert(diff.IndexesAdded, qt.HasLen, 1)
	c.Assert(diff.IndexesAdded[0].Index.Comment, qt.Equals, "new")
	c.Assert(diff.IndexesAdded[0].Index.Invisible, qt.IsTrue)
	c.Assert(diff.IndexVisibilityChanged, qt.HasLen, 0)
}

// TestCompare_IndexOptions_ChangedVisibility changes an index's visibility in
// place, in both directions.
func TestCompare_IndexOptions_ChangedVisibility(t *testing.T) {
	for _, invisible := range []bool{true, false} {
		t.Run(fmt.Sprintf("invisible=%t", invisible), func(t *testing.T) {
			c := qt.New(t)

			diff := compareForDialect(platform.MySQL, optionedIndexTable("", invisible), liveOptionedIndexTable("", !invisible))

			c.Assert(diff.IndexesAdded, qt.HasLen, 0)
			c.Assert(diff.IndexesRemoved, qt.HasLen, 0)
			c.Assert(diff.IndexVisibilityChanged, qt.DeepEquals, []difftypes.IndexVisibilityChange{
				{TableName: "t", Name: "k_a", Invisible: invisible},
			})
		})
	}
}
