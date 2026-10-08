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
