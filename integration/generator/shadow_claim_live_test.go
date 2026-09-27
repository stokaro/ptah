//go:build integration

package generator_test

import (
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
	"ptah.run/migration/shadow"
)

// shadowClaimPostgresHistory creates a schema of its own besides a table in
// public. A reset of public alone leaves the schema behind, and the next claim
// of a URL that pins no search_path judges the whole database and refuses it.
var shadowClaimPostgresHistory = fstest.MapFS{
	"0000000001_audit.up.sql":   {Data: []byte("CREATE SCHEMA audit;\nCREATE TABLE audit.log (id integer PRIMARY KEY);\n")},
	"0000000001_audit.down.sql": {Data: []byte("DROP TABLE audit.log;\nDROP SCHEMA audit;\n")},
	"0000000002_users.up.sql":   {Data: []byte("CREATE TABLE users (id integer PRIMARY KEY);\n")},
	"0000000002_users.down.sql": {Data: []byte("DROP TABLE users;\n")},
}

// shadowClaimPostgres is a scratch PostgreSQL database with an open connection
// to it, dropped when the test ends.
func shadowClaimPostgres(c *qt.C, prefix string) (string, *dbschema.DatabaseConnection) {
	c.Helper()
	baseURL := dbtarget.URL(c, dbtarget.PostgreSQL)
	admin, err := dbschema.ConnectToDatabase(c.Context(), baseURL)
	c.Assert(err, qt.IsNil)
	databaseURL, database := createGeneratorTestPostgres(c, admin, baseURL, prefix)
	c.Cleanup(func() {
		dropGeneratorTestPostgres(c, admin, database)
		dbschema.CloseAndWarn(admin)
	})
	conn, err := dbschema.ConnectToDatabase(c.Context(), databaseURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	return databaseURL, conn
}

// postgresUserObjects names the schemas other than public and the tables
// anywhere in a PostgreSQL database, in name order.
func postgresUserObjects(c *qt.C, conn *dbschema.DatabaseConnection) string {
	c.Helper()
	var objects string
	c.Assert(conn.QueryRowContext(c.Context(), `
		SELECT coalesce(string_agg(name, ',' ORDER BY name), '')
		FROM (
			SELECT 'schema ' || nspname AS name
			FROM pg_namespace
			WHERE nspname NOT LIKE 'pg\_%' AND nspname NOT IN ('information_schema', 'public')
			UNION ALL
			SELECT 'table ' || schemaname || '.' || tablename
			FROM pg_tables
			WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
		) objects`).Scan(&objects), qt.IsNil)
	return objects
}

// TestVerifyRollback_LivePostgresHandsTheShadowBackEmpty verifies a rollback
// twice against one PostgreSQL shadow database whose URL pins no schema. The
// replay creates a schema of its own; the hand-back removes it with the rest,
// so the second verification claims the database the first one used.
func TestVerifyRollback_LivePostgresHandsTheShadowBackEmpty(t *testing.T) {
	c := qt.New(t)
	shadowURL, shadowConn := shadowClaimPostgres(c, "ptah_shadow_claim")
	_, target := shadowClaimPostgres(c, "ptah_shadow_claim_target")
	verify := func() error {
		return shadow.VerifyRollback(c.Context(), shadow.RollbackVerifyOptions{
			TargetConnection:  target,
			ShadowDatabaseURL: shadowURL,
			FS:                shadowClaimPostgresHistory,
			CurrentVersion:    2,
			TargetVersion:     1,
		})
	}

	first := verify()
	second := verify()

	c.Assert(first, qt.IsNil)
	c.Assert(second, qt.IsNil)
	c.Assert(postgresUserObjects(c, shadowConn), qt.Equals, "")
}

// TestVerifyRollback_LivePostgresRefusesAShadowHoldingATable is the refusal on
// a server: the whole database is judged, so the schema is named, and the row
// is still there afterwards.
func TestVerifyRollback_LivePostgresRefusesAShadowHoldingATable(t *testing.T) {
	c := qt.New(t)
	shadowURL, shadowConn := shadowClaimPostgres(c, "ptah_shadow_claim")
	_, target := shadowClaimPostgres(c, "ptah_shadow_claim_target")
	for _, statement := range []string{"CREATE TABLE keep_me (id integer PRIMARY KEY)", "INSERT INTO keep_me VALUES (1)"} {
		_, err := shadowConn.ExecContext(c.Context(), statement)
		c.Assert(err, qt.IsNil)
	}

	err := shadow.VerifyRollback(c.Context(), shadow.RollbackVerifyOptions{
		TargetConnection:  target,
		ShadowDatabaseURL: shadowURL,
		FS:                shadowClaimPostgresHistory,
		CurrentVersion:    2,
		TargetVersion:     1,
	})

	c.Assert(err, qt.ErrorMatches, `rollback verification failed: connected database is not clean: `+
		`found table "keep_me" in schema "public"; Ptah resets this database before and after the replay, so point the URL at an empty database`)
	var rows int
	c.Assert(shadowConn.QueryRowContext(c.Context(), "SELECT count(*) FROM keep_me").Scan(&rows), qt.IsNil)
	c.Assert(rows, qt.Equals, 1)
}
