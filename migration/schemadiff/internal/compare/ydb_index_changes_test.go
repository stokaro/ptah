package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff/difftypes"
	"ptah.run/migration/schemadiff/internal/compare"
)

// compareYDBIndexes compares one table's indexes on YDB with caps.
func compareYDBIndexes(desired []schemamodel.Index, database []catalog.Index, caps capability.Capabilities) *difftypes.SchemaDiff {
	diff := &difftypes.SchemaDiff{}
	compare.IndexesWithSemantics(
		&schemamodel.Database{Indexes: desired},
		&catalog.Database{Indexes: database},
		diff, platform.YDB, identifier.ForDialect(platform.YDB), nil, caps,
	)
	return diff
}

// namedIndex renames one of the indexes ydbDeclaredIndex and ydbIndex build.
func namedIndex(index schemamodel.Index, name string) schemamodel.Index {
	index.Name = name
	return index
}

func namedCatalogIndex(index catalog.Index, name string) catalog.Index {
	index.Name = name
	return index
}

// TestIndexes_YDBPairsARenamedIndex reads an index the database drops and one
// the declaration adds, on the same table with the same definition, as one
// index renamed. IndexRenames, which the feature comparison reads to bind a
// renamed index's settings to its new name, returns the same renames.
func TestIndexes_YDBPairsARenamedIndex(t *testing.T) {
	c := qt.New(t)
	desired := []schemamodel.Index{
		namedIndex(ydbDeclaredIndex("async", false, []string{"a"}, []string{"c"}), "by_a"),
		namedIndex(ydbDeclaredIndex("", true, []string{"b"}, nil), "uq_b"),
	}
	database := []catalog.Index{
		namedCatalogIndex(ydbIndex("GLOBAL ASYNC", false, []string{"a"}, []string{"c"}), "old_a"),
		namedCatalogIndex(ydbIndex("GLOBAL SYNC", true, []string{"b"}, nil), "old_uq_b"),
	}

	diff := compareYDBIndexes(desired, database, capability.YDB262())

	want := []difftypes.IndexRename{
		{TableName: "t", From: "old_a", To: "by_a"},
		{TableName: "t", From: "old_uq_b", To: "uq_b"},
	}
	c.Assert(diff.IndexesRenamed, qt.DeepEquals, want)
	c.Assert(diff.IndexAdditions(), qt.HasLen, 0)
	c.Assert(diff.IndexRemovals(), qt.HasLen, 0)
	c.Assert(compare.IndexRenames(&schemamodel.Database{Indexes: desired}, &catalog.Database{Indexes: database},
		platform.YDB, identifier.ForDialect(platform.YDB), nil, capability.YDB262()), qt.DeepEquals, want)
}

// TestIndexes_YDBPairsEqualRenamesInNameOrder pairs two equal indexes renamed
// at once the same way on every run: each addition, in name order, takes the
// first removal in name order.
func TestIndexes_YDBPairsEqualRenamesInNameOrder(t *testing.T) {
	c := qt.New(t)
	index := ydbDeclaredIndex("", false, []string{"a"}, nil)
	held := ydbIndex("GLOBAL SYNC", false, []string{"a"}, nil)

	diff := compareYDBIndexes(
		[]schemamodel.Index{namedIndex(index, "new_2"), namedIndex(index, "new_1")},
		[]catalog.Index{namedCatalogIndex(held, "old_2"), namedCatalogIndex(held, "old_1")},
		capability.YDB262(),
	)

	c.Assert(diff.IndexesRenamed, qt.DeepEquals, []difftypes.IndexRename{
		{TableName: "t", From: "old_1", To: "new_1"},
		{TableName: "t", From: "old_2", To: "new_2"},
	})
}

// TestIndexes_RenameNotPaired leaves a removal and an addition as a drop and a
// create where they are not one index renamed, or where the target plans no
// rename.
func TestIndexes_RenameNotPaired(t *testing.T) {
	sync := ydbDeclaredIndex("", false, []string{"a"}, nil)
	held := namedCatalogIndex(ydbIndex("GLOBAL SYNC", false, []string{"a"}, nil), "old")
	otherTable := namedCatalogIndex(ydbIndex("GLOBAL SYNC", false, []string{"a"}, nil), "old")
	otherTable.TableName = "u"

	tests := []struct {
		name     string
		desired  schemamodel.Index
		database catalog.Index
		caps     capability.Capabilities
	}{
		{name: "a target without the key", desired: namedIndex(sync, "new"), database: held,
			caps: capability.YDB262().With(capability.IndexRename, false)},
		{name: "another kind", desired: namedIndex(ydbDeclaredIndex("async", false, []string{"a"}, nil), "new"), database: held,
			caps: capability.YDB262()},
		{name: "another column", desired: namedIndex(ydbDeclaredIndex("", false, []string{"b"}, nil), "new"), database: held,
			caps: capability.YDB262()},
		{name: "another table", desired: namedIndex(sync, "new"), database: otherTable, caps: capability.YDB262()},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff := compareYDBIndexes([]schemamodel.Index{test.desired}, []catalog.Index{test.database}, test.caps)

			c.Assert(diff.IndexesRenamed, qt.HasLen, 0)
			c.Assert(diff.IndexAdditions(), qt.DeepEquals, []difftypes.IndexRef{{Name: "new", TableName: "t"}})
			c.Assert(diff.IndexRemovals(), qt.DeepEquals, []difftypes.IndexRef{{Name: "old", TableName: test.database.TableName}})
		})
	}
}

// TestIndexes_PostgresRecordsNoYDBIndexChange is the control on another
// dialect: its preset plans no rename, so a renamed index is a drop and a
// create.
func TestIndexes_PostgresRecordsNoYDBIndexChange(t *testing.T) {
	c := qt.New(t)
	desired := schemamodel.Index{Name: "new", TableName: "t", Fields: []string{"a"}}
	diff := &difftypes.SchemaDiff{}

	compare.IndexesWithDialect(
		&schemamodel.Database{Indexes: []schemamodel.Index{desired, {Name: "kept", TableName: "t", Fields: []string{"b"}}}},
		&catalog.Database{Indexes: []catalog.Index{
			{Name: "old", TableName: "t", Columns: []string{"a"}},
			{Name: "kept", TableName: "t", Columns: []string{"b"}},
		}},
		diff, platform.Postgres,
	)

	c.Assert(diff.IndexesRenamed, qt.HasLen, 0)
	c.Assert(diff.IndexAdditions(), qt.DeepEquals, []difftypes.IndexRef{{Name: "new", TableName: "t"}})
	c.Assert(diff.IndexRemovals(), qt.DeepEquals, []difftypes.IndexRef{{Name: "old", TableName: "t"}})
}
