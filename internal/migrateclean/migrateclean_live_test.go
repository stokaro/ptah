//go:build integration

package migrateclean_test

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/jackc/pgx/v5"
	_ "github.com/jackc/pgx/v5/stdlib"

	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/migrateclean"
)

// These cases pin the half of the gate a SQLite fixture cannot express: which
// schema the catalog probe looks in. SQLite has one namespace, so the unit
// tests can pin the predicate and the wording but not the scope.
//
// Each expected string was produced by the pinned community binary v1.3.0
// against PostgreSQL 17 on 2026-08-07; the state is named on each row.

const migratecleanRevisionTable = "atlas_schema_revisions"

// TestInspectLive_CleanSchemas walks the states the binary applies against, so
// the probe cannot start refusing objects that are none of its business.
func TestInspectLive_CleanSchemas(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()

	tests := []struct {
		name  string
		setup []string
	}{
		{name: "an empty schema"},
		{
			name:  "a table in another schema is out of scope",
			setup: []string{"CREATE SCHEMA extra", "CREATE TABLE extra.legacy_stuff (id integer PRIMARY KEY)"},
		},
		{
			name:  "an empty schema beside the connected one",
			setup: []string{"CREATE SCHEMA extra"},
		},
		{
			name:  "a view is not a table",
			setup: []string{"CREATE VIEW v AS SELECT 1 AS one"},
		},
		{
			name:  "a sequence is not a table",
			setup: []string{"CREATE SEQUENCE s1"},
		},
		{
			name:  "the run's own revisions table alone",
			setup: []string{"CREATE TABLE atlas_schema_revisions (version varchar PRIMARY KEY)"},
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			conn := newMigratecleanLiveConnection(c, ctx)
			applyMigratecleanFixture(c, ctx, conn, test.setup)

			scope, err := migrateclean.Inspect(ctx, conn, migratecleanRevisionTable, "")

			c.Assert(err, qt.IsNil)
			c.Assert(scope.Schema, qt.Equals, "public")
			c.Assert(scope.Refusal(), qt.IsNil)
		})
	}
}

// TestInspectLive_UncleanSchemas walks the states the binary refuses, and
// checks the refusal names the table the binary names.
func TestInspectLive_UncleanSchemas(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()

	tests := []struct {
		name string
		// setup puts the throwaway database into the state under test.
		setup []string
		// revisionsSchema is the run's --revisions-schema.
		revisionsSchema string
		wantErr         string
	}{
		{
			name: "one unrelated table beside the revisions table",
			setup: []string{
				"CREATE TABLE legacy_stuff (id integer PRIMARY KEY)",
				"CREATE TABLE atlas_schema_revisions (version varchar PRIMARY KEY)",
			},
			wantErr: `sql/migrate: connected database is not clean: found table "atlas_schema_revisions" in schema "public". baseline version or allow-dirty is required`,
		},
		{
			// The refusal names the alphabetically first table, so a table
			// sorting before the revisions table displaces it.
			name: "the reported table is the alphabetically first one",
			setup: []string{
				"CREATE TABLE aaa_legacy (id integer PRIMARY KEY)",
				"CREATE TABLE atlas_schema_revisions (version varchar PRIMARY KEY)",
			},
			wantErr: `sql/migrate: connected database is not clean: found table "aaa_legacy" in schema "public". baseline version or allow-dirty is required`,
		},
		{
			// The discriminating fixture. `public` holds exactly one table and
			// the binary still refuses, which is what rules out "more than one
			// table" as the predicate.
			name: "revisions kept in another schema still refuses on one table",
			setup: []string{
				"CREATE SCHEMA revs",
				"CREATE TABLE revs.atlas_schema_revisions (version varchar PRIMARY KEY)",
				"CREATE TABLE legacy_stuff (id integer PRIMARY KEY)",
			},
			revisionsSchema: "revs",
			wantErr:         `sql/migrate: connected database is not clean: found table "legacy_stuff" in schema "public". baseline version or allow-dirty is required`,
		},
		{
			// A partitioned table is a table. Its partitions are too, and both
			// relkinds are in the probe.
			name: "a partitioned table counts",
			setup: []string{
				"CREATE TABLE parted (id integer NOT NULL, part integer NOT NULL) PARTITION BY RANGE (part)",
			},
			wantErr: `sql/migrate: connected database is not clean: found table "atlas_schema_revisions" in schema "public". baseline version or allow-dirty is required`,
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			conn := newMigratecleanLiveConnection(c, ctx)
			applyMigratecleanFixture(c, ctx, conn, test.setup)

			scope, err := migrateclean.Inspect(
				ctx, conn, migratecleanRevisionTable, test.revisionsSchema,
			)

			c.Assert(err, qt.IsNil)
			c.Assert(scope.Schema, qt.Equals, "public")
			refusal := scope.Refusal()
			c.Assert(refusal, qt.IsNotNil)
			c.Assert(refusal.Error(), qt.Equals, test.wantErr)
		})
	}
}

func applyMigratecleanFixture(
	c *qt.C,
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	statements []string,
) {
	c.Helper()
	for _, statement := range statements {
		_, err := conn.ExecContext(ctx, statement)
		c.Assert(err, qt.IsNil, qt.Commentf("statement: %s", statement))
	}
}

// newMigratecleanLiveConnection provisions a throwaway database, so a case that
// creates schemas and tables cannot disturb any other live test sharing the
// server.
func newMigratecleanLiveConnection(c *qt.C, ctx context.Context) *dbschema.DatabaseConnection {
	c.Helper()
	adminURL := requireMigratecleanLiveURL(c)
	admin, err := sql.Open("pgx", adminURL)
	c.Assert(err, qt.IsNil)
	c.Assert(admin.PingContext(ctx), qt.IsNil)

	name := fmt.Sprintf("ptah_migrateclean_%d", time.Now().UnixNano())
	nameIdent := pgx.Identifier{name}.Sanitize()
	_, err = admin.ExecContext(ctx, "CREATE DATABASE "+nameIdent)
	c.Assert(err, qt.IsNil)

	parsed, err := url.Parse(adminURL)
	c.Assert(err, qt.IsNil)
	parsed.Path = "/" + name
	parsed.RawPath = ""

	conn, err := dbschema.ConnectToDatabase(ctx, parsed.String())
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		dbschema.CloseAndWarn(conn)
		_, dropErr := admin.ExecContext(
			context.WithoutCancel(ctx),
			"DROP DATABASE IF EXISTS "+nameIdent+" WITH (FORCE)",
		)
		c.Check(dropErr, qt.IsNil)
		c.Check(admin.Close(), qt.IsNil)
	})
	return conn
}

func requireMigratecleanLiveURL(c *qt.C) string {
	c.Helper()
	for _, name := range []string{"POSTGRES_TEST_DSN", "POSTGRES_URL", "TEST_DATABASE_URL"} {
		raw := os.Getenv(name)
		if raw == "" {
			continue
		}
		parsed, err := url.Parse(raw)
		c.Assert(err, qt.IsNil)
		parsed.Scheme = "postgres"
		return parsed.String()
	}
	c.Skip("POSTGRES_TEST_DSN, POSTGRES_URL, or TEST_DATABASE_URL is not set")
	return ""
}
