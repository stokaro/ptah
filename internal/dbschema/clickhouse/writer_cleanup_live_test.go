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

	"github.com/stokaro/ptah/core/platform"
	"github.com/stokaro/ptah/internal/dbschema/clickhouse"
	"github.com/stokaro/ptah/internal/sqlident"
)

func TestWriterDropDatabaseRealm_Live(t *testing.T) {
	c := qt.New(t)
	db, database := openLiveClickHouseRealmDatabase(t)
	writer := clickhouse.NewClickHouseWriter(db, database)

	err := writer.DropDatabaseRealm(t.Context())
	c.Assert(err, qt.IsNil)

	table := sqlident.Qualified(platform.ClickHouse, database, "events'raw")
	view := sqlident.Qualified(platform.ClickHouse, database, "events_view")
	materializedView := sqlident.Qualified(platform.ClickHouse, database, "events_mv")
	statements := []string{
		"CREATE TABLE " + table + " (id UInt64) ENGINE = MergeTree ORDER BY id",
		"CREATE VIEW " + view + " AS SELECT id FROM " + table,
		"CREATE MATERIALIZED VIEW " + materializedView +
			" ENGINE = Memory AS SELECT id FROM " + table,
	}
	for _, statement := range statements {
		_, err = db.ExecContext(t.Context(), statement)
		c.Assert(err, qt.IsNil, qt.Commentf("execute ClickHouse fixture statement: %s", statement))
	}

	err = writer.DropDatabaseRealm(t.Context())
	c.Assert(err, qt.IsNil)
	err = writer.DropDatabaseRealm(t.Context())
	c.Assert(err, qt.IsNil)

	var objectCount uint64
	err = db.QueryRowContext(
		t.Context(),
		"SELECT count() FROM system.tables WHERE database = ? AND is_temporary = 0",
		database,
	).Scan(&objectCount)
	c.Assert(err, qt.IsNil)
	c.Assert(objectCount, qt.Equals, uint64(0))
}

func openLiveClickHouseRealmDatabase(t *testing.T) (*sql.DB, string) {
	t.Helper()
	adminURL, configured := os.LookupEnv("PTAH_CLICKHOUSE_REALM_TEST_URL")
	if !configured {
		t.Skip("PTAH_CLICKHOUSE_REALM_TEST_URL is not configured")
	}

	admin, err := sql.Open("clickhouse", adminURL)
	qt.Assert(t, err, qt.IsNil)
	qt.Assert(t, admin.PingContext(t.Context()), qt.IsNil)

	database := fmt.Sprintf("ptah_realm_%d", time.Now().UnixNano())
	quotedDatabase := sqlident.Quote(platform.ClickHouse, database)
	_, err = admin.ExecContext(t.Context(), "CREATE DATABASE "+quotedDatabase)
	qt.Assert(t, err, qt.IsNil)

	parsedURL, err := url.Parse(adminURL)
	qt.Assert(t, err, qt.IsNil)
	parsedURL.Path = "/" + database
	parsedURL.RawPath = ""
	db, err := sql.Open("clickhouse", parsedURL.String())
	qt.Assert(t, err, qt.IsNil)
	qt.Assert(t, db.PingContext(t.Context()), qt.IsNil)

	t.Cleanup(func() {
		qt.Check(t, db.Close(), qt.IsNil)
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_, cleanupErr := admin.ExecContext(
			cleanupCtx,
			"DROP DATABASE IF EXISTS "+quotedDatabase+" SYNC",
		)
		qt.Check(t, cleanupErr, qt.IsNil)
		qt.Check(t, admin.Close(), qt.IsNil)
	})
	return db, database
}
