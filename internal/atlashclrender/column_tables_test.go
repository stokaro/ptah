package atlashclrender_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/platform"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/internal/atlashclrender"
	"ptah.run/internal/builtintest"
)

func TestRender_ColumnStorageIsALoss(t *testing.T) {
	c := qt.New(t)
	db := &schemamodel.Database{Tables: []schemamodel.Table{{StructName: "T", Name: "events", Facets: must.Must(schemaext.NewFacets(&ydbschema.DesiredColumnStore{ColumnStore: ydbschema.ColumnStore{HashColumns: []string{"id"}, Partitions: 1}}))}}, Fields: []schemamodel.Field{{StructName: "T", Name: "id", Type: "Uint64", Primary: true}}}
	result, err := atlashclrender.RenderInspectedForAtlasCLI(db, "", atlashclrender.Options{HCL: builtintest.Runtime().HCL(), Dialect: platform.YDB})
	c.Assert(err, qt.IsNil)
	c.Assert(result.Diagnostics, qt.DeepEquals, []atlashclrender.Diagnostic{{Severity: atlashclrender.SeverityWarning, Path: "table.events", Message: "YDB column storage, hash partitioning and tiered TTL are not represented in HCL"}})
}

// TestRender_ReportsEachTablesYDBSettingsAtTheTable reports the YDB table
// settings HCL leaves out where it writes each table, in the order it writes
// the tables, and for each table its column families, its column storage and
// its partitioning, in that order.
func TestRender_ReportsEachTablesYDBSettingsAtTheTable(t *testing.T) {
	c := qt.New(t)
	families := &ydbschema.DesiredColumnFamilies{Families: []ydbschema.ColumnFamily{{Name: "cold", Data: "hdd"}}}
	partitioning := &ydbschema.DesiredTablePartitioning{TablePartitioning: ydbschema.TablePartitioning{MinPartitions: 3}}
	db := &schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "Z", Name: "z", Facets: must.Must(schemaext.NewFacets(families, partitioning))},
			{StructName: "A", Name: "a", Facets: must.Must(schemaext.NewFacets(partitioning, &ydbschema.DesiredColumnStore{}))},
		},
		Fields: []schemamodel.Field{{StructName: "Z", Name: "id", Type: "Int64"}, {StructName: "A", Name: "id", Type: "Int64"}},
	}

	result, err := atlashclrender.Render(db, atlashclrender.Options{HCL: builtintest.Runtime().HCL(), Dialect: "ydb"})

	c.Assert(err, qt.IsNil)
	settings := "YDB table settings (AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 3) are not represented in HCL"
	c.Assert(result.Diagnostics, qt.DeepEquals, []atlashclrender.Diagnostic{
		{Severity: atlashclrender.SeverityWarning, Path: "table.a", Message: "YDB column storage, hash partitioning and tiered TTL are not represented in HCL"},
		{Severity: atlashclrender.SeverityWarning, Path: "table.a", Message: settings},
		{Severity: atlashclrender.SeverityWarning, Path: "table.z", Message: "column family cold is not represented in HCL"},
		{Severity: atlashclrender.SeverityWarning, Path: "table.z", Message: settings},
	})
}
