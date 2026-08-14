//go:build integration

package clickhouse_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	clickhousedb "go.5x5.cz/ptah/internal/dbschema/clickhouse"
	"go.5x5.cz/ptah/internal/sqlident"
)

// TestInnerTableSubtractionKeepsADeclaredDotNameLive pins which spelling of a
// materialized view's storage the read subtracts.
//
// The storage name follows the database engine, and the view's own row says
// which: an Atomic database gives the view a UUID and names the storage
// ".inner_id.<that uuid>", an Ordinary database leaves the UUID at all zeros and
// names it ".inner.<view name>". Measured on 26.7.3.19 and 24.10.4.191 alike.
//
// Subtracting both spellings unconditionally removed a real table. A leading dot
// is legal in a quoted ClickHouse name, so a user table can be called
// ".inner.user_counts" in an Atomic database where the view "user_counts" stores
// its rows somewhere else entirely, and that table vanished from the table read,
// from the index read, and from what DropAllTables destroys.
//
// The realm database this test opens uses the server default, which is Atomic on
// both pinned lines -- so the decoy here is a real table, not storage, and the
// read has to say so.
func TestInnerTableSubtractionKeepsADeclaredDotNameLive(t *testing.T) {
	c := qt.New(t)
	db, database := openLiveClickHouseRealmDatabase(t, "PTAH_CLICKHOUSE_REALM_TEST_URL")
	sourceTable := sqlident.Qualified(platform.ClickHouse, database, "users")
	storedView := sqlident.Qualified(platform.ClickHouse, database, "user_counts")
	decoyTable := sqlident.Qualified(platform.ClickHouse, database, ".inner.user_counts")
	executeClickHouseViewPlan(t, db, []string{
		"CREATE TABLE " + sourceTable + " (id UInt64) ENGINE = MergeTree ORDER BY id",
		"CREATE MATERIALIZED VIEW " + storedView +
			" ENGINE = MergeTree ORDER BY tuple() AS SELECT count() AS c FROM " + sourceTable,
		"CREATE TABLE " + decoyTable +
			" (x UInt64, INDEX x_minmax x TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY x",
	})

	readback, err := clickhousedb.NewClickHouseReader(db, database).ReadSchema()
	c.Assert(err, qt.IsNil)
	c.Assert(clickHouseTableNames(readback), qt.DeepEquals, []string{".inner.user_counts", "users"})
	c.Assert(readback.MatViews, qt.HasLen, 1)
	c.Assert(readback.MatViews[0].Name, qt.Equals, "user_counts")
	// The index read subtracts the same set, so the decoy's index has to survive
	// with it.
	c.Assert(readback.Indexes, qt.HasLen, 1)
	c.Assert(readback.Indexes[0].TableName, qt.Equals, ".inner.user_counts")

	// The view's actual storage is still subtracted: exactly the two tables
	// above are reported, and system.tables holds one more row than that.
	var storedTables uint64
	err = db.QueryRowContext(
		t.Context(),
		"SELECT count() FROM system.tables WHERE database = ? AND engine LIKE '%MergeTree'",
		database,
	).Scan(&storedTables)
	c.Assert(err, qt.IsNil)
	c.Assert(storedTables, qt.Equals, uint64(3))

	// A reset that leaves the decoy behind is not a reset: the next replay of
	// the same DDL fails on a table that already exists.
	err = clickhousedb.NewClickHouseWriter(db, database).DropAllTables(t.Context())
	c.Assert(err, qt.IsNil)
	var remaining uint64
	err = db.QueryRowContext(
		t.Context(),
		"SELECT count() FROM system.tables WHERE database = ? AND is_temporary = 0",
		database,
	).Scan(&remaining)
	c.Assert(err, qt.IsNil)
	c.Assert(remaining, qt.Equals, uint64(0))
}

// clickHouseTableNames returns the names of the tables a read reported, in the
// order the reader produced them.
func clickHouseTableNames(schema *dbschematypes.DBSchema) []string {
	names := make([]string, 0, len(schema.Tables))
	for _, table := range schema.Tables {
		names = append(names, table.Name)
	}
	return names
}
