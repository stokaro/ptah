package dbschematogo_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/convert/dbschematogo"
)

// engineTable is one ClickHouse table as the reader reports it.
func engineTable(table types.DBTable) *types.DBSchema {
	table.Name = "events"
	table.Type = "TABLE"
	table.Columns = []types.DBColumn{{Name: "id", DataType: "UInt64", IsNullable: "NO"}}
	return &types.DBSchema{Tables: []types.DBTable{table}}
}

// clickHouseOverrides returns the converted table's ClickHouse override group.
func clickHouseOverrides(c *qt.C, database *goschema.Database) map[string]string {
	c.Helper()
	c.Assert(database.Tables, qt.HasLen, 1)
	return database.Tables[0].Overrides["clickhouse"]
}

// TestConvert_CarriesEveryClickHouseEngineClause pins the six facts.
//
// The conversion carried only the sorting key, so every other clause fell to the
// renderer's defaults: a ReplacingMergeTree came back a MergeTree -- losing the
// deduplicating merge the table exists for -- and the partition key, the
// sampling key, the TTL and the settings came back absent. The TTL is the one
// that changes what the data does rather than how fast it is read: a table
// replayed without it keeps rows it was configured to delete
// (stokaro/ptah#2198).
func TestConvert_CarriesEveryClickHouseEngineClause(t *testing.T) {
	c := qt.New(t)

	database := dbschematogo.ConvertDBSchemaToGoSchema(engineTable(types.DBTable{
		ClickHouseEngine:       "ReplacingMergeTree(ver)",
		ClickHouseOrderBy:      "day, id",
		ClickHousePartitionKey: "toYYYYMM(day)",
		ClickHouseSamplingKey:  "id",
		ClickHouseTTL:          "day + toIntervalDay(90)",
		ClickHouseSettings:     "index_granularity = 4096",
	}))

	c.Assert(clickHouseOverrides(c, database), qt.DeepEquals, map[string]string{
		"engine":       "ReplacingMergeTree(ver)",
		"order_by":     "day, id",
		"partition_by": "toYYYYMM(day)",
		"sample_by":    "id",
		"ttl":          "day + toIntervalDay(90)",
		"settings":     "index_granularity = 4096",
	})
}

// TestConvert_CarriesTheOrderByEvenWhenItMatchesThePrimaryKey pins the order.
//
// The renderer can derive the ORDER BY columns from the primary key, which is
// why the older field omitted that case -- but it derives the COLUMNS, not their
// ORDER, and a table sorted `(day, id)` came back sorted `(id, day)`. That is a
// different physical layout, not a spelling difference.
func TestConvert_CarriesTheOrderByEvenWhenItMatchesThePrimaryKey(t *testing.T) {
	c := qt.New(t)

	database := dbschematogo.ConvertDBSchemaToGoSchema(engineTable(types.DBTable{
		ClickHouseEngine:  "MergeTree",
		ClickHouseOrderBy: "day, id",
		// Empty on purpose: this is what the reader reports when the sorting key
		// is nothing beyond the primary key, which is the case that lost the
		// order.
		ClickHouseSortingKey: "",
	}))

	c.Assert(clickHouseOverrides(c, database)["order_by"], qt.Equals, "day, id")
}

// TestConvert_LeavesATableWithNoEngineFactsAlone is the control.
//
// A reader that reports none of these -- every non-ClickHouse reader -- must not
// produce an override group, or a MySQL or PostgreSQL table would carry a
// ClickHouse engine section.
func TestConvert_LeavesATableWithNoEngineFactsAlone(t *testing.T) {
	c := qt.New(t)

	database := dbschematogo.ConvertDBSchemaToGoSchema(engineTable(types.DBTable{}))

	c.Assert(database.Tables, qt.HasLen, 1)
	c.Assert(database.Tables[0].Overrides, qt.HasLen, 0)
}
