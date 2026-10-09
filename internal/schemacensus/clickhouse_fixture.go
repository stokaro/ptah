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
