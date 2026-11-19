package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/difftypes"
	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff/internal/compare"
)

// kidsParentKey is the foreign key of kids over parent_id, named by its author.
var kidsParentKey = catalog.Constraint{
	Name: "kids_parent_fk", TableName: "kids", Type: "FOREIGN KEY",
	ColumnName: "parent_id", ColumnNames: []string{"parent_id"},
}

// kidsIndex is an index of kids over columns.
func kidsIndex(name string, columns ...string) catalog.Index {
	return catalog.Index{Name: name, TableName: "kids", Columns: columns}
}

// declaredIndexLeaving is a plan that removes kids_parent_idx, the one index
// in scope under the key.
func declaredIndexLeaving() *difftypes.SchemaDiff {
	return &difftypes.SchemaDiff{IndexesRemoved: []difftypes.IndexRef{{Name: "kids_parent_idx", TableName: "kids"}}}
}

// The plan removes the declared index a foreign key stands on. The key gets an
// index of its own under its name, in the statement that drops the declared
// one, whether the key is in scope or an exclude selector keeps it out. Only
// the key out of scope gets a note, since the declaration does not show it.
func TestForeignKeyIndexesStaying_GivesAKeyAnIndexInAndOutOfScope(t *testing.T) {
	const note = "Add index kids_parent_fk for foreign key kids_parent_fk, which an exclude selector keeps out of scope: " +
		"the server refuses to drop kids_parent_idx while the key has no other index"
	tests := []struct {
		name     string
		database catalog.Database
		note     string
	}{
		{
			name:     "a key in scope",
			database: catalog.Database{Indexes: []catalog.Index{kidsIndex("kids_parent_idx", "parent_id")}, Constraints: []catalog.Constraint{kidsParentKey}},
		},
		{
			name: "a key out of scope",
			database: catalog.Database{Indexes: []catalog.Index{kidsIndex("kids_parent_idx", "parent_id")},
				OutOfScope: catalog.TableParts{Constraints: []catalog.Constraint{kidsParentKey}}},
			note: note,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := declaredIndexLeaving()

			compare.ForeignKeyIndexesStaying(&test.database, diff, platform.MySQL, identifier.ForDialect(platform.MySQL))

			c.Assert(diff.IndexesAdded, qt.DeepEquals, difftypes.IndexChanges{{
				TableName: "kids",
				Index:     schemamodel.Index{Name: "kids_parent_fk", Fields: []string{"parent_id"}},
				Replaces:  []difftypes.IndexRef{{Name: "kids_parent_idx", TableName: "kids"}},
				Note:      test.note,
			}})
		})
	}
}

// An index an exclude selector keeps out of scope stays, so a foreign key that
// stands on it needs no other, in scope or out of it. A name such an index
// holds is taken too: the server's name for the key's index goes to the next
// free one.
func TestForeignKeyIndexesStaying_ReadsTheIndexesOutOfScope(t *testing.T) {
	tests := []struct {
		name     string
		database catalog.Database
		want     difftypes.IndexChanges
	}{
		{
			name: "an index out of scope under a key in scope",
			database: catalog.Database{Indexes: []catalog.Index{kidsIndex("kids_parent_idx", "parent_id")},
				Constraints: []catalog.Constraint{kidsParentKey},
				OutOfScope:  catalog.TableParts{Indexes: []catalog.Index{kidsIndex("kids_parent_wide", "parent_id", "id")}}},
		},
		{
			name: "an index out of scope under a key out of scope",
			database: catalog.Database{Indexes: []catalog.Index{kidsIndex("kids_parent_idx", "parent_id")},
				OutOfScope: catalog.TableParts{
					Indexes:     []catalog.Index{kidsIndex("kids_parent_wide", "parent_id", "id")},
					Constraints: []catalog.Constraint{kidsParentKey},
				}},
		},
		{
			name: "the column's name held out of scope, for a key the server named",
			database: catalog.Database{Indexes: []catalog.Index{kidsIndex("kids_parent_idx", "parent_id")},
				Constraints: []catalog.Constraint{{Name: "kids_ibfk_1", TableName: "kids", Type: "FOREIGN KEY", ColumnNames: []string{"parent_id"}}},
				OutOfScope:  catalog.TableParts{Indexes: []catalog.Index{kidsIndex("parent_id", "id")}}},
			want: difftypes.IndexChanges{{
				TableName: "kids",
				Index:     schemamodel.Index{Name: "parent_id_2", Fields: []string{"parent_id"}},
				Replaces:  []difftypes.IndexRef{{Name: "kids_parent_idx", TableName: "kids"}},
			}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := declaredIndexLeaving()

			compare.ForeignKeyIndexesStaying(&test.database, diff, platform.MySQL, identifier.ForDialect(platform.MySQL))

			c.Assert(diff.IndexesAdded, qt.DeepEquals, test.want)
		})
	}
}
