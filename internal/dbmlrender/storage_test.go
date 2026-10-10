package dbmlrender_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbcoordination"
	"ptah.run/dialect/ydb/ydbexternal"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/dialect/ydb/ydbsecret"
	"ptah.run/dialect/ydb/ydbstreaming"
	"ptah.run/dialect/ydb/ydbtopic"
	"ptah.run/dialect/ydb/ydbworkload"
	"ptah.run/internal/dbmlrender"
)

func TestRender_ReportsYDBObjectsWithoutDBMLBlocks(t *testing.T) {
	c := qt.New(t)
	db := &schemamodel.Database{
		AsyncReplications: []schemamodel.AsyncReplication{{Name: "mirror"}},
		FeatureObjects: must.Must(schemaext.NewObjects(ydbworkload.DesiredPoolObject("batch", "", ydbworkload.PoolSpec{}), ydbworkload.DesiredClassifierObject("route", "", ydbworkload.ClassifierSpec{ResourcePool: "default"}), ydbcoordination.DesiredObject("", "locks", "", ydbcoordination.Spec{}), ydbstreaming.DesiredObject("", "stream", "", ydbstreaming.Spec{Text: "SELECT 1;"}, false), ydbsecret.DesiredObject("", "credentials", "", "PTAH_SECRET_CREDENTIALS"), ydbtopic.DesiredObject("", "events", "", ydbtopic.Spec{}), ydbtopic.DesiredObject("", "audit", "", ydbtopic.Spec{}),
			ydbexternal.DesiredSourceObject("", "bucket", "", ydbexternal.DataSource{SourceType: "ObjectStorage", AuthMethod: "NONE"}),
			ydbexternal.DesiredTableObject("", "files", "", ydbexternal.Table{DataSource: "bucket", Location: "f/",
				Columns: []ydbexternal.Column{{Name: "id", Type: "Int64"}}}))),

		Transfers: []schemamodel.Transfer{{Name: "copy"}},
	}

	result, err := renderDBML(c, db, dbmlrender.Options{})

	c.Assert(err, qt.IsNil)
	c.Assert(result.DBML, qt.Equals, "")
	c.Assert(result.Omitted, qt.DeepEquals, []string{
		"async replications (1)",
		"coordination nodes (1)",
		"external data sources (1)",
		"external tables (1)",
		"resource pool classifiers (1)",
		"resource pools (1)",
		"secrets (1)",
		"streaming queries (1)",
		"topics (2)",
		"transfers (1)",
	})
}

func storageSchema(c *qt.C) *schemamodel.Database {
	c.Helper()
	objects, err := schemaext.NewObjects(
		ydbschema.DesiredObject("", "events", ydbschema.ChangefeedSpec{Name: "updates", Mode: "UPDATES", Format: "JSON"}),
		ydbschema.DesiredObject("", "events", ydbschema.ChangefeedSpec{Name: "audit", Mode: "UPDATES", Format: "JSON"}),
	)
	c.Assert(err, qt.IsNil)
	return &schemamodel.Database{
		FeatureObjects: objects,
		Tables: []schemamodel.Table{
			{StructName: "Plain", Name: "plain"},
			{
				StructName: "Events", Name: "events",
				Facets:          must.Must(ydbTTL(c).With(&ydbschema.DesiredColumnFamilies{Families: []ydbschema.ColumnFamily{{Name: "cold", Columns: []string{"id"}}}})),
				YDBPartitioning: &ast.YDBTablePartitioningSpec{MinPartitions: 4},
			},
			{StructName: "Archive", Name: "archive", YDBColumnTable: &ast.YDBColumnTableSpec{}},
		},
		Fields: []schemamodel.Field{
			{StructName: "Plain", Name: "id", Type: "Int64", Primary: true, AutoInc: true},
			{
				StructName: "Events", Name: "id", Type: "Int64", Primary: true, AutoInc: true,
				IdentityGeneration: "BY_DEFAULT", IdentityStart: "100", IdentityIncrement: "5",
			},
		},
		Indexes: []schemamodel.Index{
			{
				StructName: "Events", Name: "search", Fields: []string{"id"},
				IncludeColumns: []string{"created_at"}, StorageParams: map[string]string{"tokenizer": "standard"},
				Partitioning: &ast.IndexPartitioningSpec{MinPartitions: 2},
			},
			{StructName: "Events", Name: "vector", Fields: []string{"id"}, Vector: &ast.VectorIndexSpec{Dimension: 3}},
		},
	}
}

func TestRender_ReportsStorageSettingsItLeavesOut(t *testing.T) {
	c := qt.New(t)

	result, err := renderDBML(c, storageSchema(c), dbmlrender.Options{Target: "ydb"})

	c.Assert(err, qt.IsNil)
	c.Assert(result.DBML, qt.Contains, `"id" Int64 [pk, increment, not null]`)
	c.Assert(result.Omitted, qt.DeepEquals, []string{
		"TTL settings (1)",
		"changefeeds (2)",
		"column families (1)",
		"column-oriented storage and settings (1)",
		"covering columns on indexes (1)",
		"identity generation modes (1)",
		"identity sequence settings (1)",
		"index partitioning and read replicas (1)",
		"index storage and analyzer settings (1)",
		"table partitioning, read replicas and key bloom filters (1)",
		"vector index settings (1)",
	})
}

func TestRender_StorageWarningsRespectTableSelection(t *testing.T) {
	tests := []struct {
		name string
		opts dbmlrender.Options
	}{
		{name: "include", opts: dbmlrender.Options{Target: "ydb", IncludeTables: []string{"plain"}}},
		{name: "exclude", opts: dbmlrender.Options{Target: "ydb", ExcludeTables: []string{"events", "archive"}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			result, err := renderDBML(c, storageSchema(c), test.opts)
			c.Assert(err, qt.IsNil)
			c.Assert(result.Omitted, qt.HasLen, 0)
			c.Assert(result.DBML, qt.Contains, `Table "plain"`)
			c.Assert(result.DBML, qt.Not(qt.Contains), `Table "events"`)
		})
	}
}

func TestRender_EmptyOptionalStorageSettingsDoNotWarn(t *testing.T) {
	c := qt.New(t)
	db := &schemamodel.Database{
		Tables: []schemamodel.Table{{
			StructName: "T", Name: "t",
			YDBPartitioning: &ast.YDBTablePartitioningSpec{},
		}},
		Indexes: []schemamodel.Index{{StructName: "T", Name: "idx", Partitioning: &ast.IndexPartitioningSpec{}}},
	}

	result, err := renderDBML(c, db, dbmlrender.Options{})

	c.Assert(err, qt.IsNil)
	c.Assert(result.Omitted, qt.HasLen, 0)
}

// ydbTTL is a YDB TTL on created_at, bound to the YDB target.
func ydbTTL(c *qt.C) schemaext.Facets {
	c.Helper()
	facets, err := schemaext.NewFacets(&ydbschema.DesiredTTL{Policy: ydbschema.TTL{Column: "created_at", Interval: "P1D"}})
	c.Assert(err, qt.IsNil)
	facets, err = facets.WithTargetScope(ydbschema.TTLKind, "ydb")
	c.Assert(err, qt.IsNil)
	return facets
}
