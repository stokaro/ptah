package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/ast"
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

// TestIndexes_YDBPartitioningChangesInPlace records a change of partitioning
// on an index both sides hold with the same definition as a change made in
// place, carrying both settings, and plans no rebuild for it.
func TestIndexes_YDBPartitioningChangesInPlace(t *testing.T) {
	tests := []struct {
		name     string
		desired  *ast.IndexPartitioningSpec
		database *ast.IndexPartitioningSpec
	}{
		{name: "a setting declared", desired: &ast.IndexPartitioningSpec{MinPartitions: 3}, database: nil},
		{name: "a setting moved", desired: &ast.IndexPartitioningSpec{MinPartitions: 4}, database: &ast.IndexPartitioningSpec{MinPartitions: 3}},
		{name: "a setting no longer declared", desired: nil, database: &ast.IndexPartitioningSpec{ReadReplicas: "PER_AZ:1"}},
		{name: "a maximum lowered", desired: &ast.IndexPartitioningSpec{MaxPartitions: 4}, database: &ast.IndexPartitioningSpec{MaxPartitions: 9}},
		{name: "a declaration YDB refuses", desired: &ast.IndexPartitioningSpec{BySize: new(false), PartitionSizeMB: 100}, database: nil},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired := ydbDeclaredIndex("", false, []string{"a"}, nil)
			desired.Partitioning = test.desired
			database := ydbIndex("GLOBAL SYNC", false, []string{"a"}, nil)
			database.Partitioning = test.database

			diff := compareYDBIndexes([]schemamodel.Index{desired}, []catalog.Index{database}, capability.YDB262())

			c.Assert(diff.IndexPartitioningChanged, qt.DeepEquals, []difftypes.IndexPartitioningChange{
				{TableName: "t", Name: "i", Partitioning: test.desired, Previous: test.database},
			})
			c.Assert(diff.IndexAdditions(), qt.HasLen, 0)
			c.Assert(diff.IndexRemovals(), qt.HasLen, 0)
		})
	}
}

// TestIndexes_YDBPartitioningUnchanged is the control: settings that resolve
// alike are one index, whichever side names them.
func TestIndexes_YDBPartitioningUnchanged(t *testing.T) {
	tests := []struct {
		name     string
		desired  *ast.IndexPartitioningSpec
		database *ast.IndexPartitioningSpec
	}{
		{name: "neither side tuned", desired: nil, database: nil},
		{name: "the defaults declared", desired: &ast.IndexPartitioningSpec{BySize: new(true), PartitionSizeMB: 2048, MinPartitions: 1}, database: nil},
		{name: "read replicas of zero", desired: &ast.IndexPartitioningSpec{ReadReplicas: "PER_AZ:0"}, database: nil},
		{name: "the same settings", desired: &ast.IndexPartitioningSpec{ByLoad: new(true), MaxPartitions: 9},
			database: &ast.IndexPartitioningSpec{ByLoad: new(true), MaxPartitions: 9}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired := ydbDeclaredIndex("", false, []string{"a"}, nil)
			desired.Partitioning = test.desired
			database := ydbIndex("GLOBAL SYNC", false, []string{"a"}, nil)
			database.Partitioning = test.database

			diff := compareYDBIndexes([]schemamodel.Index{desired}, []catalog.Index{database}, capability.YDB262())

			c.Assert(diff.HasChanges(), qt.IsFalse)
		})
	}
}

// TestIndexes_YDBMaximumRemovedRebuilds plans the one change of partitioning
// YDB cannot make in place, removing a maximum, as a rebuild: the addition
// carries the declared settings, which the renderer sets on the new index.
func TestIndexes_YDBMaximumRemovedRebuilds(t *testing.T) {
	c := qt.New(t)
	desired := ydbDeclaredIndex("", false, []string{"a"}, nil)
	desired.Partitioning = &ast.IndexPartitioningSpec{MinPartitions: 2}
	database := ydbIndex("GLOBAL SYNC", false, []string{"a"}, nil)
	database.Partitioning = &ast.IndexPartitioningSpec{MinPartitions: 2, MaxPartitions: 9}

	diff := compareYDBIndexes([]schemamodel.Index{desired}, []catalog.Index{database}, capability.YDB262())

	c.Assert(diff.IndexAdditions(), qt.DeepEquals, []difftypes.IndexRef{{Name: "i", TableName: "t"}})
	c.Assert(diff.IndexesAdded[0].Index.Partitioning, qt.DeepEquals, desired.Partitioning)
	c.Assert(diff.IndexRemovals(), qt.DeepEquals, []difftypes.IndexRef{{Name: "i", TableName: "t"}})
	c.Assert(diff.IndexPartitioningChanged, qt.HasLen, 0)
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
// index renamed, and carries a change of partitioning under the new name.
func TestIndexes_YDBPairsARenamedIndex(t *testing.T) {
	c := qt.New(t)
	plain := ydbDeclaredIndex("async", false, []string{"a"}, []string{"c"})
	tuned := namedIndex(plain, "by_a")
	tuned.Partitioning = &ast.IndexPartitioningSpec{MinPartitions: 4}
	held := namedCatalogIndex(ydbIndex("GLOBAL ASYNC", false, []string{"a"}, []string{"c"}), "old_a")
	held.Partitioning = &ast.IndexPartitioningSpec{MinPartitions: 3}

	diff := compareYDBIndexes(
		[]schemamodel.Index{tuned, namedIndex(ydbDeclaredIndex("", true, []string{"b"}, nil), "uq_b")},
		[]catalog.Index{held, namedCatalogIndex(ydbIndex("GLOBAL SYNC", true, []string{"b"}, nil), "old_uq_b")},
		capability.YDB262(),
	)

	c.Assert(diff.IndexesRenamed, qt.DeepEquals, []difftypes.IndexRename{
		{TableName: "t", From: "old_a", To: "by_a"},
		{TableName: "t", From: "old_uq_b", To: "uq_b"},
	})
	c.Assert(diff.IndexPartitioningChanged, qt.DeepEquals, []difftypes.IndexPartitioningChange{
		{TableName: "t", Name: "by_a", Partitioning: tuned.Partitioning, Previous: held.Partitioning},
	})
	c.Assert(diff.IndexAdditions(), qt.HasLen, 0)
	c.Assert(diff.IndexRemovals(), qt.HasLen, 0)
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
	capped := namedCatalogIndex(ydbIndex("GLOBAL SYNC", false, []string{"a"}, nil), "old")
	capped.Partitioning = &ast.IndexPartitioningSpec{MaxPartitions: 9}
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
		{name: "a maximum only a rebuild removes", desired: namedIndex(sync, "new"), database: capped, caps: capability.YDB262()},
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
// dialect: its catalog reports no partitioning and its preset plans no rename,
// so a renamed index is a drop and a create and a declared partitioning is not
// compared.
func TestIndexes_PostgresRecordsNoYDBIndexChange(t *testing.T) {
	c := qt.New(t)
	desired := schemamodel.Index{Name: "new", TableName: "t", Fields: []string{"a"},
		Partitioning: &ast.IndexPartitioningSpec{MinPartitions: 3}}
	diff := &difftypes.SchemaDiff{}

	compare.IndexesWithDialect(
		&schemamodel.Database{Indexes: []schemamodel.Index{desired, {Name: "kept", TableName: "t", Fields: []string{"b"},
			Partitioning: &ast.IndexPartitioningSpec{MinPartitions: 3}}}},
		&catalog.Database{Indexes: []catalog.Index{
			{Name: "old", TableName: "t", Columns: []string{"a"}},
			{Name: "kept", TableName: "t", Columns: []string{"b"}},
		}},
		diff, platform.Postgres,
	)

	c.Assert(diff.IndexesRenamed, qt.HasLen, 0)
	c.Assert(diff.IndexPartitioningChanged, qt.HasLen, 0)
	c.Assert(diff.IndexAdditions(), qt.DeepEquals, []difftypes.IndexRef{{Name: "new", TableName: "t"}})
	c.Assert(diff.IndexRemovals(), qt.DeepEquals, []difftypes.IndexRef{{Name: "old", TableName: "t"}})
}
