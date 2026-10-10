package schemacensus

import (
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/clickhouse/chschema"
)

func tableClickHouseSettingsFixture() schemamodel.Database {
	settings := &chschema.DesiredTable{
		Engine:      chschema.Setting{State: chschema.Explicit, Value: "ReplacingMergeTree(version)"},
		OrderBy:     chschema.Setting{State: chschema.Explicit, Value: "id, ts"},
		PrimaryKey:  chschema.Setting{State: chschema.Explicit, Value: "id"},
		PartitionBy: chschema.Setting{State: chschema.Explicit, Value: "toYYYYMM(ts)"},
		SampleBy:    chschema.Setting{State: chschema.Explicit, Value: "id"},
		TTL:         chschema.Setting{State: chschema.Explicit, Value: "ts + INTERVAL 7 DAY"},
		Settings:    chschema.Setting{State: chschema.Explicit, Value: "index_granularity = 4096"},
	}
	return oneTable("Events", schemamodel.Table{Name: "events", Facets: must.Must(must.Must(schemaext.NewFacets(settings)).WithTargetScope(chschema.TableKind, "clickhouse"))},
		schemamodel.Field{StructName: "Events", Name: "ts", Type: "DateTime"},
		schemamodel.Field{StructName: "Events", Name: "version", Type: "UInt64"},
	)
}

// indexClickHouseSettingsFixture declares a skipping index's settings as the
// owner's typed facet, the shape source decoding produces, so the census
// measures each setting of the model.
func indexClickHouseSettingsFixture() schemamodel.Database {
	db := indexedTable()
	settings := &chschema.DesiredIndex{
		IndexType:   chschema.Setting{State: chschema.Explicit, Value: "bloom_filter(0.01)"},
		Granularity: chschema.GranularitySetting{State: chschema.Explicit, Value: 4},
	}
	db.Indexes = []schemamodel.Index{{
		StructName: "T", Name: "idx_t_s", TableName: "t", Fields: []string{"s"},
		Facets: must.Must(must.Must(schemaext.NewFacets(settings)).WithTargetScope(chschema.IndexKind, "clickhouse")),
	}}
	return db
}

// matViewRefreshFixture declares a materialized view's refresh schedule as the
// owner's typed facet, every clause set, so the census measures each one.
func matViewRefreshFixture() schemamodel.Database {
	db := oneTable("T", schemamodel.Table{Name: "t"})
	schedule := &chschema.DesiredRefresh{Schedule: chschema.Schedule{
		Mode: chschema.RefreshEvery, Interval: "1 HOUR", Offset: "5 MINUTE",
		Randomize: "1 MINUTE", Append: true, DependsOn: []string{"other"},
	}}
	db.MaterializedViews = []schemamodel.MaterializedView{{
		StructName: "MV", Name: "daily", Body: "SELECT id FROM t",
		Facets: must.Must(must.Must(schemaext.NewFacets(schedule)).WithTargetScope(chschema.RefreshKind, "clickhouse")),
	}}
	return db
}
