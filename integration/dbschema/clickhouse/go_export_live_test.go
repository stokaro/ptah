//go:build integration

package clickhouse_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/goschema"
	"ptah.run/dbschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/convert/dbschematogo"
	"ptah.run/internal/convert/goschematogo"
)

// A successful Go export must recreate the storage clauses the server actually
// reports. Comparing catalogs catches an exporter that writes valid Go while
// dropping its platform properties.
func TestGoExportPreservesClickHouseStorageLive(t *testing.T) {
	c := qt.New(t)
	conn := openLiveClickHouseRBACTarget(c)
	name := uniqueClickHouseRBACName("go_export")
	copyName := name + "_copy"
	c.Cleanup(func() {
		cleanupClickHouseRBACFixture(c, conn, "DROP TABLE IF EXISTS "+copyName+" SYNC")
		cleanupClickHouseRBACFixture(c, conn, "DROP TABLE IF EXISTS "+name+" SYNC")
	})
	c.Assert(conn.Writer().ExecuteSQL(c.Context(), "CREATE TABLE "+name+" (tenant UInt64, id UInt64, created_at DateTime, version UInt64) "+
		"ENGINE = ReplacingMergeTree(version) PARTITION BY toYYYYMM(created_at) PRIMARY KEY tenant ORDER BY (tenant, id) "+
		"SAMPLE BY tenant TTL created_at + toIntervalDay(30) SETTINGS index_granularity = 4096"), qt.IsNil)
	before := readStorageClauses(c, conn, name)
	live := readLive(c, conn)
	index := slices.IndexFunc(live.Tables, func(table catalog.Table) bool { return table.Name == name })
	c.Assert(index, qt.Not(qt.Equals), -1)
	runtime := must.Must(builtin.New())
	model, err := dbschematogo.ConvertDBSchemaToGoSchema(c.Context(), &catalog.Database{Tables: []catalog.Table{live.Tables[index]}}, "clickhouse", runtime)
	c.Assert(err, qt.IsNil)
	files, err := goschematogo.Render(c.Context(), model, goschematogo.Options{SingleFile: true, Dialect: "clickhouse", Runtime: runtime})
	c.Assert(err, qt.IsNil)
	c.Assert(files, qt.HasLen, 1)
	reparsed, err := goschema.ParseSource("schema.go", files[0].Data)
	c.Assert(err, qt.IsNil)
	reparsed.Tables[0].Name = copyName
	statements, err := builtin.GetOrderedCreateStatements(&reparsed, "clickhouse")
	c.Assert(err, qt.IsNil)
	applyStatements(c, conn, statements)
	c.Assert(readStorageClauses(c, conn, copyName), qt.DeepEquals, before)
}

func readStorageClauses(c *qt.C, conn *dbschema.DatabaseConnection, name string) [5]string {
	c.Helper()
	var clauses [5]string
	c.Assert(conn.QueryRowContext(c.Context(), "SELECT engine_full, partition_key, sampling_key, sorting_key, primary_key FROM system.tables WHERE database = currentDatabase() AND name = ?", name).
		Scan(&clauses[0], &clauses[1], &clauses[2], &clauses[3], &clauses[4]), qt.IsNil)
	return clauses
}
