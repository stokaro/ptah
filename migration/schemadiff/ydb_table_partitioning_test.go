package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/migration/schemadiff"
	"ptah.run/migration/schemadiff/difftypes"
)

// partitionedDeclaration declares one YDB table with settings.
func partitionedDeclaration(partitioning *ast.YDBTablePartitioningSpec) *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Item", Name: "items", YDBPartitioning: partitioning}},
		Fields: []schemamodel.Field{{StructName: "Item", Name: "id", Type: "BIGINT UNSIGNED", Primary: true}},
	}
}

// partitionedCatalog is the table as the YDB reader reports it, with settings.
func partitionedCatalog(partitioning *ast.YDBTablePartitioningSpec) *catalog.Database {
	return &catalog.Database{
		Tables: []catalog.Table{{Name: "items", Type: "TABLE", YDBPartitioning: partitioning, Columns: []catalog.Column{
			{Name: "id", DataType: "Uint64", ColumnType: "Uint64", IsNullable: "NO", IsPrimaryKey: true, OrdinalPosition: 1},
		}}},
		Constraints: []catalog.Constraint{{Name: "items_pkey", TableName: "items", Type: "PRIMARY KEY",
			ColumnName: "id", ColumnNames: []string{"id"}}},
	}
}

// TestCompare_YDBTablePartitioning_NothingToPlan reads a declaration and a
// database holding the same settings as the same table: a setting declared at
// the value the table holds, a setting left out, which keeps what the table
// holds, and a starting layout through the minimum it gives a new table,
// which is the one record of it YDB keeps.
func TestCompare_YDBTablePartitioning_NothingToPlan(t *testing.T) {
	tests := []struct {
		name     string
		desired  *ast.YDBTablePartitioningSpec
		database *ast.YDBTablePartitioningSpec
	}{
		{name: "the defaults declared", desired: &ast.YDBTablePartitioningSpec{BySize: new(true), MinPartitions: 1,
			KeyBloomFilter: new(false), ReadReplicas: "PER_AZ:0"}, database: nil},
		{name: "nothing declared over tuned settings", desired: nil,
			database: &ast.YDBTablePartitioningSpec{MinPartitions: 4, ByLoad: new(true), MaxPartitions: 9, KeyBloomFilter: new(true)}},
		{name: "one setting declared over the rest held", desired: &ast.YDBTablePartitioningSpec{MinPartitions: 4},
			database: &ast.YDBTablePartitioningSpec{MinPartitions: 4, ByLoad: new(true), MaxPartitions: 9}},
		{name: "uniform partitions read back as their minimum", desired: &ast.YDBTablePartitioningSpec{UniformPartitions: 4},
			database: &ast.YDBTablePartitioningSpec{MinPartitions: 4}},
		{name: "split points read back as their minimum", desired: &ast.YDBTablePartitioningSpec{PartitionAtKeys: [][]string{{"10"}, {"20"}}},
			database: &ast.YDBTablePartitioningSpec{MinPartitions: 3}},
		{name: "every setting", desired: &ast.YDBTablePartitioningSpec{BySize: new(false), ByLoad: new(true), MinPartitions: 2,
			MaxPartitions: 8, ReadReplicas: "any_az:1", KeyBloomFilter: new(true)},
			database: &ast.YDBTablePartitioningSpec{BySize: new(false), ByLoad: new(true), MinPartitions: 2, MaxPartitions: 8,
				ReadReplicas: "ANY_AZ:1", KeyBloomFilter: new(true)}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := must.Must(schemadiff.CompareWithDialect(t.Context(), partitionedDeclaration(test.desired), partitionedCatalog(test.database), platform.YDB, must.Must(builtin.New())))
			c.Assert(diff.TablesModified, qt.HasLen, 0)
			c.Assert(diff.HasChanges(), qt.IsFalse)
		})
	}
}

// TestCompare_YDBTablePartitioning_Change reports a table whose only
// difference is its settings, with both sides, which the planner writes the
// statement from: a table declared back to a default included, and a
// declaration on a target whose catalog has no settings, which that target's
// planner refuses.
func TestCompare_YDBTablePartitioning_Change(t *testing.T) {
	tests := []struct {
		name     string
		dialect  string
		desired  *ast.YDBTablePartitioningSpec
		database *ast.YDBTablePartitioningSpec
	}{
		{name: "a new minimum", dialect: platform.YDB, desired: &ast.YDBTablePartitioningSpec{MinPartitions: 4}, database: nil},
		{name: "declared back to a default", dialect: platform.YDB, desired: &ast.YDBTablePartitioningSpec{KeyBloomFilter: new(false)},
			database: &ast.YDBTablePartitioningSpec{KeyBloomFilter: new(true)}},
		{name: "a layout on a table holding another minimum", dialect: platform.YDB,
			desired: &ast.YDBTablePartitioningSpec{UniformPartitions: 4}, database: &ast.YDBTablePartitioningSpec{MinPartitions: 2}},
		{name: "settings declared for another engine", dialect: platform.Postgres,
			desired: &ast.YDBTablePartitioningSpec{KeyBloomFilter: new(true)}, database: nil},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := must.Must(schemadiff.CompareWithDialect(t.Context(), partitionedDeclaration(test.desired), partitionedCatalog(test.database), test.dialect, must.Must(builtin.New())))
			c.Assert(diff.TablesModified, qt.HasLen, 1)
			c.Assert(diff.TablesModified[0].YDBPartitioningChange, qt.DeepEquals,
				&difftypes.YDBTablePartitioningChange{Desired: test.desired, Current: test.database})
		})
	}
}

// TestCompare_YDBTablePartitioning_LeftOutKeepsTheDatabase plans nothing for a
// table whose description is silent about its settings, whatever the format:
// HCL and DBML cannot spell them, and a Go or YAML schema that names none
// leaves each to what the table holds. A table that declares a setting the
// table does not hold changes it.
func TestCompare_YDBTablePartitioning_LeftOutKeepsTheDatabase(t *testing.T) {
	held := &ast.YDBTablePartitioningSpec{MinPartitions: 4, KeyBloomFilter: new(true)}
	tests := []struct {
		name    string
		desired *schemamodel.Database
		want    *difftypes.YDBTablePartitioningChange
	}{
		{name: "a description silent about them", desired: partitionedDeclaration(nil), want: nil},
		{name: "a table that declares its own", desired: partitionedDeclaration(&ast.YDBTablePartitioningSpec{MinPartitions: 2}),
			want: &difftypes.YDBTablePartitioningChange{Desired: &ast.YDBTablePartitioningSpec{MinPartitions: 2}, Current: held}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := must.Must(schemadiff.CompareWithDialect(t.Context(), test.desired, partitionedCatalog(held), platform.YDB, must.Must(builtin.New())))
			var got *difftypes.YDBTablePartitioningChange
			for _, table := range diff.TablesModified {
				got = table.YDBPartitioningChange
			}
			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

// TestCompare_YDBTablePartitioning_SameDocument compares a document against
// itself through the catalog built from it, which keeps its starting layout:
// nothing to plan.
func TestCompare_YDBTablePartitioning_SameDocument(t *testing.T) {
	c := qt.New(t)
	document := func() *schemamodel.Database {
		return partitionedDeclaration(&ast.YDBTablePartitioningSpec{UniformPartitions: 4, ReadReplicas: "PER_AZ:1"})
	}
	diff := must.Must(schemadiff.CompareSchemas(t.Context(), document(), document(), platform.YDB, must.Must(builtin.New())))
	c.Assert(diff.HasChanges(), qt.IsFalse)
}

// TestCompare_YDBTablePartitioning_CarriesWhatTheTableHolds carries a tuned
// table's settings on the diff even where nothing differs, so a plan that
// rebuilds the table for another change can write them on the new one. It is
// not a change.
func TestCompare_YDBTablePartitioning_CarriesWhatTheTableHolds(t *testing.T) {
	c := qt.New(t)
	held := &ast.YDBTablePartitioningSpec{MinPartitions: 4, KeyBloomFilter: new(true)}

	diff := must.Must(schemadiff.CompareWithDialect(t.Context(), partitionedDeclaration(nil), partitionedCatalog(held), platform.YDB, must.Must(builtin.New())))

	c.Assert(diff.CurrentYDBSettings, qt.DeepEquals, []difftypes.YDBHeldSettings{{TableName: "items", Partitioning: held}})
	c.Assert(diff.HasChanges(), qt.IsFalse)
}
