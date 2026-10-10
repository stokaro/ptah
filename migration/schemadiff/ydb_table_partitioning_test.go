package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/engine/builtin"
	"ptah.run/migration/schemadiff"
)

var knownSettings = schemaext.Knowledge{State: schemaext.Complete}

// partitionedDeclaration declares one YDB table with settings, as the YDB
// owner's facet, with complete knowledge of them, as a Go annotation, YAML or
// YQL source records it.
func partitionedDeclaration(partitioning *ydbschema.TablePartitioning, fields ...schemamodel.Field) *schemamodel.Database {
	db := &schemamodel.Database{
		Tables:          []schemamodel.Table{{StructName: "Item", Name: "items"}},
		Fields:          append([]schemamodel.Field{{StructName: "Item", Name: "id", Type: "BIGINT UNSIGNED", Primary: true}}, fields...),
		FeatureCoverage: must.Must(ydbschema.TablePartitioningCoverage(schemaext.Desired, knownSettings, nil)),
	}
	if partitioning != nil {
		db.Tables[0].Facets = must.Must(schemaext.NewFacets(&ydbschema.DesiredTablePartitioning{TablePartitioning: *partitioning}))
	}
	return db
}

// partitionedCatalog is the table as the YDB reader reports it, its settings
// read back as the owner's facet and recorded as known.
func partitionedCatalog(partitioning *ydbschema.TablePartitioning) *catalog.Database {
	db := &catalog.Database{
		Tables: []catalog.Table{{Name: "items", Type: "TABLE", Columns: []catalog.Column{
			{Name: "id", DataType: "Uint64", ColumnType: "Uint64", IsNullable: "NO", IsPrimaryKey: true, OrdinalPosition: 1},
		}}},
		Constraints: []catalog.Constraint{{Name: "items_pkey", TableName: "items", Type: "PRIMARY KEY",
			ColumnName: "id", ColumnNames: []string{"id"}}},
		FeatureCoverage: must.Must(ydbschema.TablePartitioningCoverage(schemaext.Observed, knownSettings, nil)),
	}
	if partitioning != nil {
		db.Tables[0].Facets = must.Must(must.Must(schemaext.NewFacets(&ydbschema.ObservedTablePartitioning{TablePartitioning: *partitioning})).
			WithTargetScope(ydbschema.TablePartitioningKind, platform.YDB))
	}
	return db
}

// settingsChange is the change of the table's settings the comparison reports,
// or nil.
func settingsChange(tables []schemaext.ChangeRecord) *ydbdiff.TablePartitioning {
	for _, record := range tables {
		if change, ok := record.Value.(*ydbdiff.TablePartitioning); ok {
			return change
		}
	}
	return nil
}

// TestCompare_YDBTablePartitioning_NothingToPlan reads a declaration and a
// database holding the same settings as the same table: a setting declared at
// the value the table holds, a setting left out, which keeps what the table
// holds, and a starting layout through the minimum it gives a new table,
// which is the one record of it YDB keeps.
func TestCompare_YDBTablePartitioning_NothingToPlan(t *testing.T) {
	tests := []struct {
		name     string
		desired  *ydbschema.TablePartitioning
		database *ydbschema.TablePartitioning
	}{
		{name: "the defaults declared", desired: &ydbschema.TablePartitioning{BySize: new(true), MinPartitions: 1,
			KeyBloomFilter: new(false), ReadReplicas: "PER_AZ:0"}, database: nil},
		{name: "nothing declared over tuned settings", desired: nil,
			database: &ydbschema.TablePartitioning{MinPartitions: 4, ByLoad: new(true), MaxPartitions: 9, KeyBloomFilter: new(true)}},
		{name: "one setting declared over the rest held", desired: &ydbschema.TablePartitioning{MinPartitions: 4},
			database: &ydbschema.TablePartitioning{MinPartitions: 4, ByLoad: new(true), MaxPartitions: 9}},
		{name: "uniform partitions read back as their minimum", desired: &ydbschema.TablePartitioning{UniformPartitions: 4},
			database: &ydbschema.TablePartitioning{MinPartitions: 4}},
		{name: "split points read back as their minimum", desired: &ydbschema.TablePartitioning{PartitionAtKeys: [][]string{{"10"}, {"20"}}},
			database: &ydbschema.TablePartitioning{MinPartitions: 3}},
		{name: "every setting", desired: &ydbschema.TablePartitioning{BySize: new(false), ByLoad: new(true), MinPartitions: 2,
			MaxPartitions: 8, ReadReplicas: "ANY_AZ:1", KeyBloomFilter: new(true)},
			database: &ydbschema.TablePartitioning{BySize: new(false), ByLoad: new(true), MinPartitions: 2, MaxPartitions: 8,
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
// statement from: a table declared back to a default included.
func TestCompare_YDBTablePartitioning_Change(t *testing.T) {
	tests := []struct {
		name     string
		desired  *ydbschema.TablePartitioning
		database *ydbschema.TablePartitioning
	}{
		{name: "a new minimum", desired: &ydbschema.TablePartitioning{MinPartitions: 4}, database: nil},
		{name: "declared back to a default", desired: &ydbschema.TablePartitioning{KeyBloomFilter: new(false)},
			database: &ydbschema.TablePartitioning{KeyBloomFilter: new(true)}},
		{name: "a layout on a table holding another minimum",
			desired: &ydbschema.TablePartitioning{UniformPartitions: 4}, database: &ydbschema.TablePartitioning{MinPartitions: 2}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := must.Must(schemadiff.CompareWithDialect(t.Context(), partitionedDeclaration(test.desired), partitionedCatalog(test.database), platform.YDB, must.Must(builtin.New())))
			c.Assert(diff.TablesModified, qt.HasLen, 1)
			change := settingsChange(diff.TablesModified[0].FeatureChanges)
			c.Assert(change.After.TablePartitioning, qt.DeepEquals, *test.desired)
			c.Assert(change.Before, qt.DeepEquals, observedSettings(test.database))
		})
	}
}

// observedSettings is the reader's value for settings, or nil for YDB's
// defaults.
func observedSettings(settings *ydbschema.TablePartitioning) *ydbschema.ObservedTablePartitioning {
	if settings == nil {
		return nil
	}
	return &ydbschema.ObservedTablePartitioning{TablePartitioning: *settings}
}

// TestCompare_YDBTablePartitioning_AnotherEngineRefusesTheSettings refuses
// settings declared for a target no YDB owner serves, rather than dropping
// them.
func TestCompare_YDBTablePartitioning_AnotherEngineRefusesTheSettings(t *testing.T) {
	c := qt.New(t)
	declaration := partitionedDeclaration(&ydbschema.TablePartitioning{KeyBloomFilter: new(true)})
	declaration.FeatureCoverage = schemaext.Coverage{}

	diff, err := schemadiff.CompareWithDialect(t.Context(), declaration, partitionedCatalog(nil), platform.Postgres, must.Must(builtin.New()))

	c.Assert(err, qt.ErrorMatches, `.*no facet comparison for "postgres"/"ptah.run/ydb/table-partitioning"`)
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(diff, qt.IsNil)
}

// TestCompare_YDBTablePartitioning_LeftOutKeepsTheDatabase plans nothing for a
// table whose description is silent about its settings, whatever the format:
// HCL and DBML cannot spell them, and a Go or YAML schema that names none
// leaves each to what the table holds. A table that declares a setting the
// table does not hold changes it.
func TestCompare_YDBTablePartitioning_LeftOutKeepsTheDatabase(t *testing.T) {
	held := &ydbschema.TablePartitioning{MinPartitions: 4, KeyBloomFilter: new(true)}
	silent := partitionedDeclaration(nil)
	silent.FeatureCoverage = schemaext.Coverage{}
	tests := []struct {
		name      string
		desired   *schemamodel.Database
		wantAfter *ydbschema.DesiredTablePartitioning
	}{
		{name: "a description that cannot spell them", desired: silent},
		{name: "a description silent about them", desired: partitionedDeclaration(nil)},
		{name: "a table that declares its own", desired: partitionedDeclaration(&ydbschema.TablePartitioning{MinPartitions: 2}),
			wantAfter: &ydbschema.DesiredTablePartitioning{TablePartitioning: ydbschema.TablePartitioning{MinPartitions: 2}}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := must.Must(schemadiff.CompareWithDialect(t.Context(), test.desired, partitionedCatalog(held), platform.YDB, must.Must(builtin.New())))
			var changes []schemaext.ChangeRecord
			for _, table := range diff.TablesModified {
				changes = append(changes, table.FeatureChanges...)
			}
			c.Assert(settingsChangeAfter(settingsChange(changes)), qt.DeepEquals, test.wantAfter)
		})
	}
}

// settingsChangeAfter is the declaration a change carries, or nil for none.
func settingsChangeAfter(change *ydbdiff.TablePartitioning) *ydbschema.DesiredTablePartitioning {
	if change == nil {
		return nil
	}
	return change.After
}

// TestCompare_YDBTablePartitioning_SameDocument compares a document against
// itself through the catalog built from it, which keeps its starting layout:
// nothing to plan.
func TestCompare_YDBTablePartitioning_SameDocument(t *testing.T) {
	c := qt.New(t)
	document := func() *schemamodel.Database {
		return partitionedDeclaration(&ydbschema.TablePartitioning{UniformPartitions: 4, ReadReplicas: "PER_AZ:1"})
	}
	diff := must.Must(schemadiff.CompareSchemas(t.Context(), document(), document(), platform.YDB, must.Must(builtin.New())))
	c.Assert(diff.HasChanges(), qt.IsFalse)
}

// TestCompare_YDBTablePartitioning_CarriesWhatTheTableHolds carries a tuned
// table's settings on the observation of a table changed for another reason,
// so a plan that rebuilds the table for that change can write them on the new
// one. They are not a change.
func TestCompare_YDBTablePartitioning_CarriesWhatTheTableHolds(t *testing.T) {
	c := qt.New(t)
	held := &ydbschema.TablePartitioning{MinPartitions: 4, KeyBloomFilter: new(true)}
	declaration := partitionedDeclaration(nil, schemamodel.Field{StructName: "Item", Name: "note", Type: "TEXT", Nullable: true})

	diff := must.Must(schemadiff.CompareWithDialect(t.Context(), declaration, partitionedCatalog(held), platform.YDB, must.Must(builtin.New())))

	c.Assert(diff.TablesModified, qt.HasLen, 1)
	c.Assert(settingsChange(diff.TablesModified[0].FeatureChanges), qt.IsNil)
	observed, found, err := schemaext.FacetAs[*ydbschema.ObservedTablePartitioning](diff.TablesModified[0].Current.Table.Facets, ydbschema.TablePartitioningKind)
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	c.Assert(observed.TablePartitioning, qt.DeepEquals, *held)
}
