//go:build integration

package clickhouse_test

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"testing"
	"time"

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

// TestDropAllTablesRemovesATargetNamedLikeInnerStorageLive pins that the reset
// does not decide what to destroy by deriving storage names.
//
// A materialized view created with `TO <target>` owns no storage of its own, and
// in an Ordinary database ".inner.<view name>" is nevertheless exactly what a
// storage-owning view of that name would be called -- the target may even be
// that table. Measured on 26.7.3.19, both statements are accepted. So a reset
// that subtracted a derived ".inner.mv" left a real table standing, and the
// replay every caller of DropAllTables performs afterwards then failed on a
// table that already existed.
//
// The reset now takes its table inventory after the views are dropped, where no
// derivation is needed: what is still present is a table.
//
// This is the one shape the Atomic test above cannot reach, because an Atomic
// database names storage after the view's own UUID and no target collides with
// that. The database is therefore created as Ordinary, which needs the
// deprecated-engine setting; a server that refuses it skips rather than passes.
func TestDropAllTablesRemovesATargetNamedLikeInnerStorageLive(t *testing.T) {
	c := qt.New(t)
	db, database := openLiveClickHouseOrdinaryDatabase(t, "PTAH_CLICKHOUSE_REALM_TEST_URL")
	sourceTable := sqlident.Qualified(platform.ClickHouse, database, "users")
	targetTable := sqlident.Qualified(platform.ClickHouse, database, ".inner.mv")
	routingView := sqlident.Qualified(platform.ClickHouse, database, "mv")
	migration := []string{
		"CREATE TABLE " + sourceTable + " (id UInt64) ENGINE = MergeTree ORDER BY id",
		"CREATE TABLE " + targetTable + " (c UInt64) ENGINE = MergeTree ORDER BY tuple()",
		"CREATE MATERIALIZED VIEW " + routingView + " TO " + targetTable +
			" AS SELECT count() AS c FROM " + sourceTable,
	}
	executeClickHouseViewPlan(t, db, migration)

	err := clickhousedb.NewClickHouseWriter(db, database).DropAllTables(t.Context())
	c.Assert(err, qt.IsNil)

	var remaining uint64
	err = db.QueryRowContext(
		t.Context(),
		"SELECT count() FROM system.tables WHERE database = ? AND is_temporary = 0",
		database,
	).Scan(&remaining)
	c.Assert(err, qt.IsNil)
	c.Assert(remaining, qt.Equals, uint64(0))

	// The reset is only a reset if the same migration runs again on top of it.
	executeClickHouseViewPlan(t, db, migration)
}

// openLiveClickHouseOrdinaryDatabase opens a throwaway database whose engine is
// Ordinary, the engine under which ClickHouse names materialized-view storage
// after the view rather than after a UUID. Ordinary is deprecated, so both the
// creating connection and the working one carry
// allow_deprecated_database_ordinary; a server that refuses the engine skips the
// test rather than passing it.
func openLiveClickHouseOrdinaryDatabase(t *testing.T, environmentVariable string) (*sql.DB, string) {
	t.Helper()
	c := qt.New(t)

	adminURL, configured := os.LookupEnv(environmentVariable)
	if !configured {
		t.Skip(environmentVariable + " is not configured")
	}

	parsedAdmin, err := url.Parse(adminURL)
	c.Assert(err, qt.IsNil)
	adminQuery := parsedAdmin.Query()
	adminQuery.Set("allow_deprecated_database_ordinary", "1")
	parsedAdmin.RawQuery = adminQuery.Encode()

	admin, err := sql.Open("clickhouse", parsedAdmin.String())
	c.Assert(err, qt.IsNil)
	c.Assert(admin.PingContext(t.Context()), qt.IsNil)

	database := fmt.Sprintf("ptah_ordinary_%d", time.Now().UnixNano())
	quotedDatabase := sqlident.Quote(platform.ClickHouse, database)
	_, err = admin.ExecContext(t.Context(), "CREATE DATABASE "+quotedDatabase+" ENGINE = Ordinary")
	if err != nil {
		c.Check(admin.Close(), qt.IsNil)
		t.Skip("server refuses the deprecated Ordinary database engine: " + err.Error())
	}

	working := *parsedAdmin
	working.Path = "/" + database
	working.RawPath = ""
	db, err := sql.Open("clickhouse", working.String())
	c.Assert(err, qt.IsNil)
	c.Assert(db.PingContext(t.Context()), qt.IsNil)

	t.Cleanup(func() {
		c.Check(db.Close(), qt.IsNil)
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_, cleanupErr := admin.ExecContext(
			cleanupCtx,
			"DROP DATABASE IF EXISTS "+quotedDatabase+" SYNC",
		)
		c.Check(cleanupErr, qt.IsNil)
		c.Check(admin.Close(), qt.IsNil)
	})
	return db, database
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
