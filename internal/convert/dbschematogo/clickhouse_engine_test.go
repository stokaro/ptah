package dbschematogo_test

import (
	"reflect"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/convert/dbschematogo"
	"ptah.run/internal/schemaexportloss"
	"ptah.run/internal/schemastats"
)

func engineTable(table catalog.Table) *catalog.Database {
	table.Name = "events"
	table.Type = "TABLE"
	table.Columns = []catalog.Column{{Name: "id", DataType: "UInt64", IsNullable: "NO"}}
	return &catalog.Database{Tables: []catalog.Table{table}}
}

// Every observed property is explicit after conversion, including empty keys.
// Reflection makes a new property participate without maintaining a second list.
func TestConvert_CarriesEveryClickHouseEngineClause(t *testing.T) {
	for _, observed := range []*chschema.ObservedTable{
		{Engine: "ReplacingMergeTree(ver)", OrderBy: "day, id", PrimaryKey: "day", PartitionBy: "toYYYYMM(day)", SampleBy: "id", TTL: "day + toIntervalDay(90)", Settings: "index_granularity = 4096"},
		{Engine: "MergeTree", OrderBy: "day, id", PrimaryKey: "day, id"},
		{Engine: "MergeTree", OrderBy: "id"},
		{Engine: "Memory"},
		markedClickHouseProperties(),
	} {
		t.Run(observed.Engine+observed.PrimaryKey, func(t *testing.T) {
			c := qt.New(t)
			facets := must.Must(schemaext.NewFacets(observed))
			facets = must.Must(facets.WithTargetScope(chschema.TableKind, "clickhouse"))
			source := engineTable(catalog.Table{Facets: facets})
			database, err := dbschematogo.ConvertDBSchemaToGoSchema(t.Context(), source, "clickhouse", must.Must(builtin.New()))
			c.Assert(err, qt.IsNil)
			c.Assert(database.Tables, qt.HasLen, 1)
			value, found, err := schemaext.FacetAs[*chschema.DesiredTable](database.Tables[0].Facets, chschema.TableKind)
			c.Assert(err, qt.IsNil)
			c.Assert(found, qt.IsTrue)
			back, err := value.Observed()
			c.Assert(err, qt.IsNil)
			c.Assert(back, qt.DeepEquals, observed)
			c.Assert(database.Tables[0].Facets.TargetScope(chschema.TableKind), qt.DeepEquals, []string{"clickhouse"})
			c.Assert(database.Tables[0].Overrides, qt.HasLen, 0)
			original, _, err := schemaext.FacetAs[*chschema.ObservedTable](source.Tables[0].Facets, chschema.TableKind)
			c.Assert(err, qt.IsNil)
			c.Assert(original, qt.DeepEquals, observed)
		})
	}
}

func markedClickHouseProperties() *chschema.ObservedTable {
	table := new(chschema.ObservedTable)
	value := reflect.ValueOf(table).Elem()
	for i := range value.NumField() {
		value.Field(i).SetString("marker_" + value.Type().Field(i).Name)
	}
	return table
}

func TestConvert_LeavesATableWithNoEngineFactsAlone(t *testing.T) {
	c := qt.New(t)
	database, err := dbschematogo.ConvertDBSchemaToGoSchema(t.Context(), engineTable(catalog.Table{}), "clickhouse", must.Must(builtin.New()))
	c.Assert(err, qt.IsNil)
	c.Assert(database.Tables, qt.HasLen, 1)
	c.Assert(database.Tables[0].Overrides, qt.HasLen, 0)
	c.Assert(database.Tables[0].Facets.IsZero(), qt.IsTrue)
}

func TestConvertedClickHouseSettingsReachRenderingAndReports(t *testing.T) {
	c := qt.New(t)
	observed := &chschema.ObservedTable{Engine: "MergeTree", OrderBy: "id"}
	runtime := must.Must(builtin.New())
	source := engineTable(catalog.Table{Facets: must.Must(schemaext.NewFacets(observed))})
	database, err := dbschematogo.ConvertDBSchemaToGoSchema(t.Context(), source, "clickhouse", runtime)
	c.Assert(err, qt.IsNil)
	statements, err := builtin.GetOrderedCreateStatements(database, "clickhouse")
	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.HasLen, 1)
	c.Assert(statements[0], qt.Contains, "PRIMARY KEY (tuple())")
	c.Assert(statements[0], qt.Contains, "ORDER BY (id)")
	stats, err := schemastats.Collect(t.Context(), database, "clickhouse", runtime)
	c.Assert(err, qt.IsNil)
	c.Assert(stats.Metrics, qt.Contains, schemastats.Metric{Name: "clickhouse_table_settings", Help: "Tables with captured ClickHouse storage settings", Value: 1})
	values := must.Must(database.Tables[0].Facets.Values())
	labels, err := schemaexportloss.FeatureLabels(t.Context(), "clickhouse", values, runtime)
	c.Assert(err, qt.IsNil)
	c.Assert(labels, qt.DeepEquals, []string{"ClickHouse table settings"})
}

// An inspected skipping index converts to an explicit declaration that renders
// the same type and granularity, and reports count and label it like a table's
// storage settings.
func TestConvertedClickHouseIndexSettingsReachRenderingAndReports(t *testing.T) {
	c := qt.New(t)
	observed := &chschema.ObservedIndex{IndexType: "set(100)", Granularity: 4}
	runtime := must.Must(builtin.New())
	source := engineTable(catalog.Table{Facets: must.Must(schemaext.NewFacets(&chschema.ObservedTable{Engine: "MergeTree", OrderBy: "id"}))})
	source.Indexes = []catalog.Index{{Name: "idx_id", TableName: "events", Columns: []string{"id"}, Facets: must.Must(schemaext.NewFacets(observed))}}
	database, err := dbschematogo.ConvertDBSchemaToGoSchema(t.Context(), source, "clickhouse", runtime)
	c.Assert(err, qt.IsNil)
	c.Assert(database.Indexes, qt.HasLen, 1)
	statements, err := builtin.GetOrderedCreateStatements(database, "clickhouse")
	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.Contains, "ALTER TABLE `events` ADD INDEX `idx_id` id TYPE set(100) GRANULARITY 4;\n")
	stats, err := schemastats.Collect(t.Context(), database, "clickhouse", runtime)
	c.Assert(err, qt.IsNil)
	c.Assert(stats.Metrics, qt.Contains, schemastats.Metric{Name: "clickhouse_index_settings", Help: "Indexes with captured ClickHouse data-skipping settings", Value: 1})
	values := must.Must(database.Indexes[0].Facets.Values())
	labels, err := schemaexportloss.FeatureLabels(t.Context(), "clickhouse", values, runtime)
	c.Assert(err, qt.IsNil)
	c.Assert(labels, qt.DeepEquals, []string{"ClickHouse index settings"})
}
