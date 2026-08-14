//go:build integration

package migrationreplay_test

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"testing"
	"testing/fstest"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/migrationreplay"
	"go.5x5.cz/ptah/internal/sqlident"
	"go.5x5.cz/ptah/migration/migrator"
)

func TestWithReplayedSnapshot_ClickHouseLive(t *testing.T) {
	c := qt.New(t)
	admin, realm, database := openClickHouseReplayRealm(t)
	snapshot := fstest.MapFS{
		"1_realm.sql": {
			Data: []byte(`
CREATE TABLE events (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE VIEW event_ids AS SELECT id FROM events;
CREATE MATERIALIZED VIEW event_rollup ENGINE = MergeTree ORDER BY id AS SELECT id FROM events;
`),
		},
	}

	err := migrationreplay.WithReplayedSnapshot(
		t.Context(),
		realm,
		snapshot,
		migrator.MigrationDirFormatAtlas,
		func(conn *dbschema.DatabaseConnection) error {
			var objectCount uint64
			queryErr := conn.QueryRowContext(
				t.Context(),
				`SELECT count()
				 FROM system.tables
				 WHERE database = ?
				   AND name IN ('events', 'event_ids', 'event_rollup')`,
				database,
			).Scan(&objectCount)
			c.Assert(queryErr, qt.IsNil)
			c.Assert(objectCount, qt.Equals, uint64(3))
			return nil
		},
	)

	c.Assert(err, qt.IsNil)
	var objectCount uint64
	err = realm.QueryRowContext(
		t.Context(),
		"SELECT count() FROM system.tables WHERE database = ? AND is_temporary = 0",
		database,
	).Scan(&objectCount)
	c.Assert(err, qt.IsNil)
	c.Assert(objectCount, qt.Equals, uint64(0))

	escapedDatabase := fmt.Sprintf("ptah_replay_escape_%d", time.Now().UnixNano())
	unsafeSnapshot := fstest.MapFS{
		"2_unsafe.sql": {
			Data: []byte(
				"CREATE TABLE local_first (id UInt64) ENGINE = MergeTree ORDER BY id;\n" +
					"CREATE DATABASE " + sqlident.Quote(platform.ClickHouse, escapedDatabase) + ";\n",
			),
		},
	}

	err = migrationreplay.WithReplayedSnapshot(
		t.Context(),
		realm,
		unsafeSnapshot,
		migrator.MigrationDirFormatAtlas,
		func(*dbschema.DatabaseConnection) error {
			return nil
		},
	)

	c.Assert(err, qt.IsNotNil)
	c.Assert(
		err.Error(),
		qt.Contains,
		"clickhouse migration replay rejects CREATE DATABASE because its effects cannot be confined to the disposable database realm",
	)
	err = realm.QueryRowContext(
		t.Context(),
		"SELECT count() FROM system.tables WHERE database = ? AND is_temporary = 0",
		database,
	).Scan(&objectCount)
	c.Assert(err, qt.IsNil)
	c.Assert(objectCount, qt.Equals, uint64(0))
	var escapedCount uint64
	err = admin.QueryRowContext(
		t.Context(),
		"SELECT count() FROM system.databases WHERE name = ?",
		escapedDatabase,
	).Scan(&escapedCount)
	c.Assert(err, qt.IsNil)
	c.Assert(escapedCount, qt.Equals, uint64(0))
}

func TestWithReplayedSnapshot_SQLServerLive(t *testing.T) {
	c := qt.New(t)
	admin, realm := openSQLServerReplayRealm(t)
	snapshot := fstest.MapFS{
		"1_realm.sql": {
			Data: []byte(`
CREATE SCHEMA app;
CREATE TABLE app.events (id BIGINT PRIMARY KEY);
CREATE VIEW app.event_ids AS SELECT id FROM app.events;
CREATE SEQUENCE app.event_sequence AS BIGINT START WITH 1;
`),
		},
	}

	err := migrationreplay.WithReplayedSnapshot(
		t.Context(),
		realm,
		snapshot,
		migrator.MigrationDirFormatAtlas,
		func(conn *dbschema.DatabaseConnection) error {
			var objectCount int
			queryErr := conn.QueryRowContext(t.Context(), `
SELECT COUNT(*)
FROM sys.objects
WHERE is_ms_shipped = 0
  AND type IN ('U', 'V', 'SO')
`).Scan(&objectCount)
			c.Assert(queryErr, qt.IsNil)
			c.Assert(objectCount, qt.Equals, 3)
			return nil
		},
	)

	c.Assert(err, qt.IsNil)
	var objectCount int
	err = realm.QueryRowContext(t.Context(), `
SELECT COUNT(*)
FROM sys.objects
WHERE is_ms_shipped = 0
  AND type IN ('U', 'V', 'SO')
`).Scan(&objectCount)
	c.Assert(err, qt.IsNil)
	c.Assert(objectCount, qt.Equals, 0)

	escapedDatabase := fmt.Sprintf("ptah_replay_escape_%d", time.Now().UnixNano())
	unsafeSnapshot := fstest.MapFS{
		"2_unsafe.sql": {
			Data: []byte(
				"CREATE TABLE dbo.local_first (id BIGINT PRIMARY KEY);\n" +
					"CREATE DATABASE " + sqlident.Quote(platform.SQLServer, escapedDatabase) + ";\n",
			),
		},
	}

	err = migrationreplay.WithReplayedSnapshot(
		t.Context(),
		realm,
		unsafeSnapshot,
		migrator.MigrationDirFormatAtlas,
		func(*dbschema.DatabaseConnection) error {
			return nil
		},
	)

	c.Assert(err, qt.IsNotNil)
	c.Assert(
		err.Error(),
		qt.Contains,
		"sqlserver migration replay rejects CREATE DATABASE because its effects cannot be confined to the disposable database realm",
	)
	err = realm.QueryRowContext(t.Context(), `
SELECT COUNT(*)
FROM sys.objects
WHERE is_ms_shipped = 0
  AND type IN ('U', 'V', 'SO')
`).Scan(&objectCount)
	c.Assert(err, qt.IsNil)
	c.Assert(objectCount, qt.Equals, 0)
	var escapedCount int
	err = admin.QueryRowContext(
		t.Context(),
		"SELECT COUNT(*) FROM sys.databases WHERE name = @p1",
		escapedDatabase,
	).Scan(&escapedCount)
	c.Assert(err, qt.IsNil)
	c.Assert(escapedCount, qt.Equals, 0)
}

func openClickHouseReplayRealm(
	t *testing.T,
) (adminConnection, realmConnection *dbschema.DatabaseConnection, databaseName string) {
	c := qt.New(t)
	t.Helper()
	adminURL, configured := os.LookupEnv("PTAH_CLICKHOUSE_REALM_TEST_URL")
	if !configured {
		t.Skip("PTAH_CLICKHOUSE_REALM_TEST_URL is not configured")
	}
	admin, err := dbschema.ConnectToDatabase(t.Context(), adminURL)
	c.Assert(err, qt.IsNil)
	database := fmt.Sprintf("ptah_replay_%d", time.Now().UnixNano())
	quotedDatabase := sqlident.Quote(platform.ClickHouse, database)
	_, err = admin.ExecContext(t.Context(), "CREATE DATABASE "+quotedDatabase)
	c.Assert(err, qt.IsNil)

	parsedURL, err := url.Parse(adminURL)
	c.Assert(err, qt.IsNil)
	parsedURL.Path = "/" + database
	parsedURL.RawPath = ""
	realm, err := dbschema.ConnectToDatabase(t.Context(), parsedURL.String())
	c.Assert(err, qt.IsNil)

	t.Cleanup(func() {
		c.Check(realm.Close(), qt.IsNil)
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_, cleanupErr := admin.ExecContext(
			cleanupCtx,
			"DROP DATABASE IF EXISTS "+quotedDatabase+" SYNC",
		)
		c.Check(cleanupErr, qt.IsNil)
		c.Check(admin.Close(), qt.IsNil)
	})
	return admin, realm, database
}

func openSQLServerReplayRealm(
	t *testing.T,
) (adminConnection, realmConnection *dbschema.DatabaseConnection) {
	c := qt.New(t)
	t.Helper()
	adminURL, configured := os.LookupEnv("PTAH_SQLSERVER_REALM_TEST_URL")
	if !configured {
		t.Skip("PTAH_SQLSERVER_REALM_TEST_URL is not configured")
	}
	admin, err := dbschema.ConnectToDatabase(t.Context(), adminURL)
	c.Assert(err, qt.IsNil)
	database := fmt.Sprintf("ptah_replay_%d", time.Now().UnixNano())
	quotedDatabase := sqlident.Quote(platform.SQLServer, database)
	_, err = admin.ExecContext(t.Context(), "CREATE DATABASE "+quotedDatabase)
	c.Assert(err, qt.IsNil)

	parsedURL, err := url.Parse(adminURL)
	c.Assert(err, qt.IsNil)
	query := parsedURL.Query()
	query.Set("database", database)
	parsedURL.RawQuery = query.Encode()
	realm, err := dbschema.ConnectToDatabase(t.Context(), parsedURL.String())
	c.Assert(err, qt.IsNil)

	t.Cleanup(func() {
		c.Check(realm.Close(), qt.IsNil)
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_, cleanupErr := admin.ExecContext(
			cleanupCtx,
			"ALTER DATABASE "+quotedDatabase+" SET SINGLE_USER WITH ROLLBACK IMMEDIATE; "+
				"DROP DATABASE "+quotedDatabase,
		)
		c.Check(cleanupErr, qt.IsNil)
		c.Check(admin.Close(), qt.IsNil)
	})
	return admin, realm
}
