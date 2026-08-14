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
	"go.5x5.cz/ptah/internal/dbschema/clickhouse"
	"go.5x5.cz/ptah/internal/sqlident"
)

func TestWriterDropDatabaseRealm_Live(t *testing.T) {
	c := qt.New(t)
	db, database := openLiveClickHouseRealmDatabase(t, "PTAH_CLICKHOUSE_REALM_TEST_URL")
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

func TestWriterDropDatabaseRealm_RejectsExternalDependencyLive(t *testing.T) {
	db, database := openLiveClickHouseRealmDatabase(t, "PTAH_CLICKHOUSE_REALM_TEST_URL")
	writer := clickhouse.NewClickHouseWriter(db, database)
	tests := []struct {
		name            string
		engine          string
		objectName      string
		createStatement func(string, string, string) string
	}{
		{
			name:       "buffer table",
			engine:     "Buffer",
			objectName: "events_buffer",
			createStatement: func(externalObject, _, targetDatabase string) string {
				return "CREATE TABLE " + externalObject +
					" (id UInt64) ENGINE = Buffer('" + targetDatabase +
					"', 'events', 1, 1, 10, 1, 10, 1, 1000000)"
			},
		},
		{
			name:       "merge table",
			engine:     "Merge",
			objectName: "events_merge",
			createStatement: func(externalObject, _, targetDatabase string) string {
				return "CREATE TABLE " + externalObject +
					" (id UInt64) ENGINE = Merge('" + targetDatabase + "', '^events$')"
			},
		},
		{
			name:       "view",
			engine:     "View",
			objectName: "events_view",
			createStatement: func(externalObject, targetTable, _ string) string {
				return "CREATE VIEW " + externalObject + " AS SELECT id FROM " + targetTable
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t2 *testing.T) {
			c := qt.New(t2)
			externalDatabase := fmt.Sprintf(
				"ptah_realm_external_%s_%d",
				test.name,
				time.Now().UnixNano(),
			)
			quotedExternalDatabase := sqlident.Quote(platform.ClickHouse, externalDatabase)
			_, err := db.ExecContext(t.Context(), "CREATE DATABASE "+quotedExternalDatabase)
			c.Assert(err, qt.IsNil)
			table := sqlident.Qualified(platform.ClickHouse, database, "events")
			c.Cleanup(func() {
				cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				_, cleanupErr := db.ExecContext(
					cleanupCtx,
					"DROP DATABASE IF EXISTS "+quotedExternalDatabase+" SYNC",
				)
				c.Check(cleanupErr, qt.IsNil)
				_, cleanupErr = db.ExecContext(
					cleanupCtx,
					"DROP TABLE IF EXISTS "+table+" SYNC",
				)
				c.Check(cleanupErr, qt.IsNil)
			})

			externalObject := sqlident.Qualified(
				platform.ClickHouse,
				externalDatabase,
				test.objectName,
			)
			_, err = db.ExecContext(
				t.Context(),
				"CREATE TABLE "+table+" (id UInt64) ENGINE = MergeTree ORDER BY id",
			)
			c.Assert(err, qt.IsNil)
			_, err = db.ExecContext(
				t.Context(),
				test.createStatement(externalObject, table, database),
			)
			c.Assert(err, qt.IsNil)

			err = writer.DropDatabaseRealm(t.Context())

			c.Assert(err, qt.IsNotNil)
			c.Assert(err.Error(), qt.Contains, "external object "+externalObject)
			c.Assert(err.Error(), qt.Contains, `engine "`+test.engine+`"`)
			var objectCount uint64
			err = db.QueryRowContext(
				t.Context(),
				"SELECT count() FROM system.tables WHERE database = ? AND is_temporary = 0",
				database,
			).Scan(&objectCount)
			c.Assert(err, qt.IsNil)
			c.Assert(objectCount, qt.Equals, uint64(1))
		})
	}
}

func TestWriterDropDatabaseRealm_RejectsLegacyServerLive(t *testing.T) {
	c := qt.New(t)
	db, database := openLiveClickHouseRealmDatabase(
		t,
		"PTAH_CLICKHOUSE_LEGACY_REALM_TEST_URL",
	)
	writer := clickhouse.NewClickHouseWriter(db, database)
	table := sqlident.Qualified(platform.ClickHouse, database, "events")
	_, err := db.ExecContext(
		t.Context(),
		"CREATE TABLE "+table+" (id UInt64) ENGINE = MergeTree ORDER BY id",
	)
	c.Assert(err, qt.IsNil)

	err = writer.DropDatabaseRealm(t.Context())

	c.Assert(err, qt.IsNotNil)
	c.Assert(
		err.Error(),
		qt.Contains,
		"ClickHouse 24.11 or newer is required to prove complete catalog visibility with CHECK GRANT",
	)
	var objectCount uint64
	err = db.QueryRowContext(
		t.Context(),
		"SELECT count() FROM system.tables WHERE database = ? AND is_temporary = 0",
		database,
	).Scan(&objectCount)
	c.Assert(err, qt.IsNil)
	c.Assert(objectCount, qt.Equals, uint64(1))
}

func openLiveClickHouseRealmDatabase(t *testing.T, environmentVariable string) (*sql.DB, string) {
	c := qt.New(t)
	t.Helper()
	adminURL, configured := os.LookupEnv(environmentVariable)
	if !configured {
		t.Skip(environmentVariable + " is not configured")
	}

	admin, err := sql.Open("clickhouse", adminURL)
	c.Assert(err, qt.IsNil)
	c.Assert(admin.PingContext(t.Context()), qt.IsNil)

	database := fmt.Sprintf("ptah_realm_%d", time.Now().UnixNano())
	quotedDatabase := sqlident.Quote(platform.ClickHouse, database)
	_, err = admin.ExecContext(t.Context(), "CREATE DATABASE "+quotedDatabase)
	c.Assert(err, qt.IsNil)

	parsedURL, err := url.Parse(adminURL)
	c.Assert(err, qt.IsNil)
	parsedURL.Path = "/" + database
	parsedURL.RawPath = ""
	db, err := sql.Open("clickhouse", parsedURL.String())
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
