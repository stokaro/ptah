package spannercomparison_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/spannercomparison"
)

// TestIndexes_OptionsEqual reads each declared key's NULLS ordering against
// the one Spanner reports. Spanner reports an ordering only where it
// contradicts the direction's default, and refuses an access method, so a
// declared method is not an option compared here.
func TestIndexes_OptionsEqual(t *testing.T) {
	tests := []struct {
		name     string
		declared schemamodel.Index
		reported catalog.Index
		want     bool
	}{
		{
			name:     "fields against reported columns",
			declared: schemamodel.Index{Fields: []string{"a", "b"}},
			reported: catalog.Index{Columns: []string{"a", "b"}},
			want:     true,
		},
		{
			name: "an explicit spelling of the default ordering",
			declared: schemamodel.Index{Parts: []schemamodel.IndexPart{
				{Name: "a", NullsOrder: "LAST"}, {Name: "b", Desc: true, NullsOrder: "FIRST"},
			}},
			reported: catalog.Index{Parts: []catalog.IndexPart{{Name: "a"}, {Name: "b", Desc: true}}},
			want:     true,
		},
		{
			name:     "an ordering against the default on both sides",
			declared: schemamodel.Index{Parts: []schemamodel.IndexPart{{Name: "b", Desc: true, NullsOrder: "LAST"}}},
			reported: catalog.Index{Parts: []catalog.IndexPart{{Name: "b", Desc: true, NullsOrder: catalog.NullsOrderLast}}},
			want:     true,
		},
		{
			name:     "a declared ordering the server does not hold",
			declared: schemamodel.Index{Parts: []schemamodel.IndexPart{{Name: "b", NullsOrder: "FIRST"}}},
			reported: catalog.Index{Parts: []catalog.IndexPart{{Name: "b"}}},
			want:     false,
		},
		{
			name:     "a held ordering the declaration leaves out",
			declared: schemamodel.Index{Parts: []schemamodel.IndexPart{{Name: "a"}, {Name: "b"}}},
			reported: catalog.Index{Parts: []catalog.IndexPart{{Name: "a"}, {Name: "b", NullsOrder: catalog.NullsOrderFirst}}},
			want:     false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := spannercomparison.Indexes{}.OptionsEqual(test.declared, test.reported)

			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// TestIndexes_RenameCarries keeps every facet through a rename, because
// ALTER INDEX ... RENAME TO changes the name alone.
func TestIndexes_RenameCarries(t *testing.T) {
	c := qt.New(t)

	c.Assert(spannercomparison.Indexes{}.RenameCarries(schemaext.Kind("any.index.facet")), qt.IsTrue)
}

// TestRules_RegistersIndexesAndForeignKeyIndexes is the shape the built-in
// runtime installs: index rules and the foreign key backing indexes, and
// nothing else, so the other comparisons stay shared.
func TestRules_RegistersIndexesAndForeignKeyIndexes(t *testing.T) {
	c := qt.New(t)

	rules := spannercomparison.Rules()

	c.Assert(rules.Indexes, qt.Equals, spannercomparison.Indexes{})
	c.Assert(rules.ForeignKeyIndexes, qt.Equals, spannercomparison.ForeignKeyIndexes{})
	c.Assert(rules.IndexDefinitions, qt.IsNil)
	c.Assert(rules.IndexPredicates, qt.IsNil)
	c.Assert(rules.GeneratedExpressions, qt.IsNil)
	c.Assert(rules.Columns, qt.IsNil)
	c.Assert(rules.Views, qt.IsNil)
	c.Assert(rules.Routines, qt.IsNil)
}

// indexNames lists the names of indexes in order, nil for none.
func indexNames(indexes []catalog.Index) []string {
	var names []string
	for _, index := range indexes {
		names = append(names, index.Name)
	}
	return names
}

// TestForeignKeyIndexes_BackingIndexes finds the index Spanner built for a
// foreign key by two signals together: a name of Spanner's own shape,
// `IDX_<table>_<columns>_<16 hex digits>`, and the key's columns in its order.
// A user's own index over the same columns stays the user's.
func TestForeignKeyIndexes_BackingIndexes(t *testing.T) {
	key := catalog.Constraint{Name: "children_parent_fk", Type: "FOREIGN KEY", TableName: "children", ColumnName: "parent_id"}
	tests := []struct {
		name    string
		indexes []catalog.Index
		want    []string
	}{
		{
			name:    "the index Spanner built",
			indexes: []catalog.Index{{Name: "IDX_children_parent_id_FBF4366D73F2084A", TableName: "children", Columns: []string{"parent_id"}}},
			want:    []string{"IDX_children_parent_id_FBF4366D73F2084A"},
		},
		{
			name:    "a user's index over the same column",
			indexes: []catalog.Index{{Name: "idx_children_parent", TableName: "children", Columns: []string{"parent_id"}}},
			want:    nil,
		},
		{
			name:    "a name of Spanner's shape over another column",
			indexes: []catalog.Index{{Name: "IDX_children_other_FBF4366D73F2084A", TableName: "children", Columns: []string{"other"}}},
			want:    nil,
		},
		{
			name:    "a suffix that is not hexadecimal",
			indexes: []catalog.Index{{Name: "IDX_children_parent_id_FBF4366D73F2084Z", TableName: "children", Columns: []string{"parent_id"}}},
			want:    nil,
		},
		{
			name:    "a primary key is its own key's",
			indexes: []catalog.Index{{Name: "IDX_children_parent_id_FBF4366D73F2084A", TableName: "children", Columns: []string{"parent_id"}, IsPrimary: true}},
			want:    nil,
		},
		{
			name:    "an index of another table",
			indexes: []catalog.Index{{Name: "IDX_children_parent_id_FBF4366D73F2084A", TableName: "others", Columns: []string{"parent_id"}}},
			want:    nil,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			backing := spannercomparison.ForeignKeyIndexes{}.BackingIndexes(&catalog.Database{Indexes: test.indexes}, key, identifier.ForDialect("spanner"))
			c.Assert(indexNames(backing), qt.DeepEquals, test.want)
		})
	}
}
