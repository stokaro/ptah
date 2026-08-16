//go:build integration

package migrateclean_test

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/jackc/pgx/v5"
	_ "github.com/jackc/pgx/v5/stdlib"

	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/dbtarget"
	"go.5x5.cz/ptah/internal/migrateclean"
)

// These cases pin the half of the gate a SQLite fixture cannot express: which
// catalog the connection's scope selects, and which schema the probe looks in.
// SQLite has one namespace, so the unit tests can pin the predicate and the
// wording but not the scope.
//
// The two scopes are reached by two URL spellings and the tables below are
// deliberately kept apart, because a suite that only ever pins a schema cannot
// fail on the realm scope — which is exactly how stokaro/ptah#1257 stayed
// invisible under stokaro/ptah#1252's PostgreSQL table.
//
// Each expected string was produced by the pinned community binary v1.3.0
// against PostgreSQL 17 on 2026-08-07; the state is named on each row.

const migratecleanRevisionTable = "atlas_schema_revisions"

// migratecleanRevisionColumns is the revision table as the pinned binary
// creates it. A fixture that writes a thinner table is not the state under
// test: the revision reader fails on it before the gate is reached.
const migratecleanRevisionColumns = `(
	version character varying NOT NULL PRIMARY KEY,
	description character varying NOT NULL,
	type bigint NOT NULL DEFAULT 2,
	applied bigint NOT NULL DEFAULT 0,
	total bigint NOT NULL DEFAULT 0,
	executed_at timestamp with time zone NOT NULL,
	execution_time bigint NOT NULL,
	error text,
	error_stmt text,
	hash character varying NOT NULL,
	partial_hashes jsonb,
	operator_version character varying NOT NULL
)`

// TestInspectLive_SchemaScopeClean walks the states the binary applies against
// when the URL pins `?search_path=public`, so the probe cannot start refusing
// objects that are none of its business.
//
// The second and third rows are the control for stokaro/ptah#1257: the same two
// databases are refusals through a URL that pins nothing, and the fix must not
// move them here.
func TestInspectLive_SchemaScopeClean(t *testing.T) {
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
			setup: []string{"CREATE TABLE atlas_schema_revisions " + migratecleanRevisionColumns},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			conn := newMigratecleanLiveConnection(c, ctx, "search_path=public", test.setup)

			scope, err := migrateclean.Inspect(ctx, conn)

			c.Assert(err, qt.IsNil)
			c.Assert(scope.Realm, qt.IsFalse)
			c.Assert(scope.Schema, qt.Equals, "public")
			c.Assert(scope.ForRevisions("", migratecleanRevisionTable).Refusal(), qt.IsNil)
		})
	}
}

// TestInspectLive_SchemaScopeUnclean walks the states the binary refuses in
// schema scope, and checks the refusal names the table the binary names.
func TestInspectLive_SchemaScopeUnclean(t *testing.T) {
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
				"CREATE TABLE atlas_schema_revisions " + migratecleanRevisionColumns,
			},
			wantErr: `sql/migrate: connected database is not clean: found table "atlas_schema_revisions" in schema "public". baseline version or allow-dirty is required`,
		},
		{
			// The refusal names the alphabetically first table, so a table
			// sorting before the revisions table displaces it.
			name: "the reported table is the alphabetically first one",
			setup: []string{
				"CREATE TABLE aaa_legacy (id integer PRIMARY KEY)",
				"CREATE TABLE atlas_schema_revisions " + migratecleanRevisionColumns,
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
				"CREATE TABLE revs.atlas_schema_revisions " + migratecleanRevisionColumns,
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
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			conn := newMigratecleanLiveConnection(c, ctx, "search_path=public", test.setup)

			scope, err := migrateclean.Inspect(ctx, conn)

			c.Assert(err, qt.IsNil)
			c.Assert(scope.Realm, qt.IsFalse)
			c.Assert(scope.Schema, qt.Equals, "public")
			refusal := scope.ForRevisions(test.revisionsSchema, migratecleanRevisionTable).Refusal()
			c.Assert(refusal, qt.IsNotNil)
			c.Assert(refusal.Error(), qt.Equals, test.wantErr)
		})
	}
}

// TestInspectLive_RealmScopeClean walks the states the binary applies against
// through a PLAIN URL — the spelling the compatibility documentation uses, with
// no search_path on it at all.
func TestInspectLive_RealmScopeClean(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name            string
		setup           []string
		revisionsSchema string
	}{
		{name: "an empty database"},
		{
			name:  "a view in public",
			setup: []string{"CREATE VIEW v AS SELECT 1 AS one"},
		},
		{
			name:  "a sequence in public",
			setup: []string{"CREATE SEQUENCE s1"},
		},
		{
			name:  "a materialized view in public",
			setup: []string{"CREATE MATERIALIZED VIEW mv AS SELECT 1 AS one"},
		},
		{
			name:  "an enum type in public",
			setup: []string{"CREATE TYPE mood AS ENUM ('sad', 'ok')"},
		},
		{
			// The state a first realm-scope run leaves behind.
			name: "the bookkeeping schema holding only the revision table",
			setup: []string{
				"CREATE SCHEMA atlas_schema_revisions",
				"CREATE TABLE atlas_schema_revisions.atlas_schema_revisions " + migratecleanRevisionColumns,
			},
		},
		{
			name:  "an empty bookkeeping schema",
			setup: []string{"CREATE SCHEMA atlas_schema_revisions"},
		},
		{
			name: "the bookkeeping schema named by --revisions-schema",
			setup: []string{
				"CREATE SCHEMA revs",
				"CREATE TABLE revs.atlas_schema_revisions " + migratecleanRevisionColumns,
			},
			revisionsSchema: "revs",
		},
		{
			// --revisions-schema public turns the one schema realm scope
			// tolerates into the bookkeeping schema.
			name:            "the revision table in public with --revisions-schema public",
			setup:           []string{"CREATE TABLE atlas_schema_revisions " + migratecleanRevisionColumns},
			revisionsSchema: "public",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			conn := newMigratecleanLiveConnection(c, ctx, "", test.setup)

			scope, err := migrateclean.Inspect(ctx, conn)

			c.Assert(err, qt.IsNil)
			c.Assert(scope.Realm, qt.IsTrue)
			c.Assert(
				scope.ForRevisions(test.revisionsSchema, migratecleanRevisionTable).Refusal(),
				qt.IsNil,
			)
		})
	}
}

// TestInspectLive_RealmScopeUnclean is the table stokaro/ptah#1257 exists for:
// every row is a database the pinned binary refuses through the plain URL and
// the gate applied to before this change.
func TestInspectLive_RealmScopeUnclean(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name            string
		setup           []string
		revisionsSchema string
		wantErr         string
	}{
		{
			// An empty schema is enough. The binary is not looking for tables
			// in this mode — it is looking for schemas.
			name:    "an empty extra schema",
			setup:   []string{"CREATE SCHEMA extra"},
			wantErr: `sql/migrate: connected database is not clean: found schema "extra". baseline version or allow-dirty is required`,
		},
		{
			name: "a table living only in another schema",
			setup: []string{
				"CREATE SCHEMA extra",
				"CREATE TABLE extra.legacy_stuff (id integer PRIMARY KEY)",
			},
			wantErr: `sql/migrate: connected database is not clean: found schema "extra". baseline version or allow-dirty is required`,
		},
		{
			name:    "a table in public",
			setup:   []string{"CREATE TABLE legacy_stuff (id integer PRIMARY KEY)"},
			wantErr: `sql/migrate: connected database is not clean: found schema "public". baseline version or allow-dirty is required`,
		},
		{
			name: "a partitioned table in public",
			setup: []string{
				"CREATE TABLE parted (id integer NOT NULL, part integer NOT NULL) PARTITION BY RANGE (part)",
			},
			wantErr: `sql/migrate: connected database is not clean: found schema "public". baseline version or allow-dirty is required`,
		},
		{
			// Creation order is not the operand: z_extra exists first and
			// a_extra is still the schema reported.
			name:    "the reported schema is the first by name",
			setup:   []string{"CREATE SCHEMA z_extra", "CREATE SCHEMA a_extra"},
			wantErr: `sql/migrate: connected database is not clean: found schema "a_extra". baseline version or allow-dirty is required`,
		},
		{
			// This row and the next one keep the walk in name order: the two
			// offenders sort either side of `public`.
			name: "an offender sorting before a dirty public",
			setup: []string{
				"CREATE TABLE legacy_stuff (id integer PRIMARY KEY)",
				"CREATE SCHEMA extra",
			},
			wantErr: `sql/migrate: connected database is not clean: found schema "extra". baseline version or allow-dirty is required`,
		},
		{
			name: "an offender sorting after a dirty public",
			setup: []string{
				"CREATE TABLE legacy_stuff (id integer PRIMARY KEY)",
				"CREATE SCHEMA zextra",
			},
			wantErr: `sql/migrate: connected database is not clean: found schema "public". baseline version or allow-dirty is required`,
		},
		{
			// Byte order rather than this database's collation, which sorts
			// "app" first.
			name:    "uppercase schema names sort by byte",
			setup:   []string{`CREATE SCHEMA "Zed"`, "CREATE SCHEMA app"},
			wantErr: `sql/migrate: connected database is not clean: found schema "Zed". baseline version or allow-dirty is required`,
		},
		{
			// The count includes the revision table the binary creates before
			// it looks, so one unrelated table reads as two.
			name: "an unrelated table in the bookkeeping schema",
			setup: []string{
				"CREATE SCHEMA atlas_schema_revisions",
				"CREATE TABLE atlas_schema_revisions.other (id integer PRIMARY KEY)",
			},
			wantErr: `sql/migrate: connected database is not clean: found 2 tables in schema "atlas_schema_revisions". baseline version or allow-dirty is required`,
		},
		{
			name: "the revision table in public beside another table",
			setup: []string{
				"CREATE TABLE atlas_schema_revisions " + migratecleanRevisionColumns,
				"CREATE TABLE legacy_stuff (id integer PRIMARY KEY)",
			},
			revisionsSchema: "public",
			wantErr:         `sql/migrate: connected database is not clean: found 2 tables in schema "public". baseline version or allow-dirty is required`,
		},
		{
			// The exemption follows --revisions-schema, so the default
			// bookkeeping schema is an ordinary offender once the run moved.
			name: "the default bookkeeping schema while the run keeps revisions elsewhere",
			setup: []string{
				"CREATE SCHEMA atlas_schema_revisions",
				"CREATE TABLE atlas_schema_revisions.atlas_schema_revisions " + migratecleanRevisionColumns,
			},
			revisionsSchema: "revs",
			wantErr:         `sql/migrate: connected database is not clean: found schema "atlas_schema_revisions". baseline version or allow-dirty is required`,
		},
		{
			// `public` is tolerated by NAME, not because the session lands
			// there: the database default moves the session into `zapp` and the
			// binary refuses on `zapp` while still saying nothing about the
			// empty `public`.
			name: "the schema the session lands in is not exempt",
			setup: []string{
				"CREATE SCHEMA zapp",
				`DO $do$ BEGIN EXECUTE format('ALTER DATABASE %I SET search_path TO zapp', current_database()); END $do$`,
			},
			wantErr: `sql/migrate: connected database is not clean: found schema "zapp". baseline version or allow-dirty is required`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			conn := newMigratecleanLiveConnection(c, ctx, "", test.setup)

			scope, err := migrateclean.Inspect(ctx, conn)

			c.Assert(err, qt.IsNil)
			c.Assert(scope.Realm, qt.IsTrue)
			refusal := scope.ForRevisions(test.revisionsSchema, migratecleanRevisionTable).Refusal()
			c.Assert(refusal, qt.IsNotNil)
			c.Assert(refusal.Error(), qt.Equals, test.wantErr)
		})
	}
}

// TestInspectLive_ScopeSelection pins what selects the scope, which is the URL
// and not the session. A `search_path` set through libpq's `options` moves
// `current_schema()` without moving the binary out of realm scope.
func TestInspectLive_ScopeSelection(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name      string
		query     string
		wantRealm bool
	}{
		{name: "no query at all", wantRealm: true},
		{name: "search_path names one schema", query: "search_path=public"},
		{
			// Not a search-path list to the binary but one schema NAME, which
			// it then fails to find. Selecting nothing here keeps every schema
			// under review, which is the direction that refuses rather than
			// applies.
			name:      "search_path carries a comma",
			query:     "search_path=public,extra",
			wantRealm: true,
		},
		{
			name:      "search_path arrives through libpq options",
			query:     "options=-c%20search_path%3Dextra",
			wantRealm: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			conn := newMigratecleanLiveConnection(c, ctx, test.query, []string{"CREATE SCHEMA extra"})

			scope, err := migrateclean.Inspect(ctx, conn)

			c.Assert(err, qt.IsNil)
			c.Assert(scope.Realm, qt.Equals, test.wantRealm)
		})
	}
}

// newMigratecleanLiveConnection provisions a throwaway database in the state
// the case is about, so a case that creates schemas and tables cannot disturb
// any other live test sharing the server.
//
// query is appended to the URL and is what selects the scope. Passing "" is the
// plain URL the compatibility documentation uses, and it is a fixture in its own
// right: a suite that always pins a schema cannot fail on realm scope.
//
// setup runs through a separate plain connection that is closed again before
// the connection under test is opened. That is the real order — the database is
// already in that state when a run reaches it — and one case depends on it: a
// URL carrying `options=-c search_path=extra` cannot be opened at all until
// `extra` exists.
func newMigratecleanLiveConnection(
	c *qt.C,
	ctx context.Context,
	query string,
	setup []string,
) *dbschema.DatabaseConnection {
	c.Helper()
	adminURL := requireMigratecleanLiveURL(c)
	admin, err := sql.Open("pgx", adminURL)
	c.Assert(err, qt.IsNil)
	c.Assert(admin.PingContext(ctx), qt.IsNil)

	name := fmt.Sprintf("ptah_migrateclean_%d", time.Now().UnixNano())
	nameIdent := pgx.Identifier{name}.Sanitize()
	_, err = admin.ExecContext(ctx, "CREATE DATABASE "+nameIdent)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		_, dropErr := admin.ExecContext(
			context.WithoutCancel(ctx),
			"DROP DATABASE IF EXISTS "+nameIdent+" WITH (FORCE)",
		)
		c.Check(dropErr, qt.IsNil)
		c.Check(admin.Close(), qt.IsNil)
	})

	parsed, err := url.Parse(adminURL)
	c.Assert(err, qt.IsNil)
	parsed.Path = "/" + name
	parsed.RawPath = ""
	plainURL := parsed.String()
	applyMigratecleanFixture(c, ctx, plainURL, setup)

	// RawQuery is assigned rather than built through url.Values so a fixture
	// can spell an already-encoded parameter such as `options=-c%20…` exactly
	// as an operator would write it on a command line.
	parsed.RawQuery = migratecleanQuery(parsed.RawQuery, query)
	conn, err := dbschema.ConnectToDatabase(ctx, parsed.String())
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	return conn
}

// applyMigratecleanFixture puts the throwaway database into the state under
// test through a connection of its own.
func applyMigratecleanFixture(c *qt.C, ctx context.Context, dbURL string, statements []string) {
	c.Helper()
	if len(statements) == 0 {
		return
	}
	seed, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(seed.Close(), qt.IsNil) }()
	for _, statement := range statements {
		_, execErr := seed.ExecContext(ctx, statement)
		c.Assert(execErr, qt.IsNil, qt.Commentf("statement: %s", statement))
	}
}

// migratecleanQuery joins the admin URL's own parameters, such as sslmode, with
// the fixture's.
func migratecleanQuery(base, extra string) string {
	switch {
	case base == "":
		return extra
	case extra == "":
		return base
	default:
		return base + "&" + extra
	}
}

func requireMigratecleanLiveURL(c *qt.C) string {
	c.Helper()
	// dbtarget answers with the address as configured, and this helper has
	// always handed its callers the postgres:// spelling. The fold stays here
	// rather than moving into the registry, where it would rewrite the address
	// every PostgreSQL consumer receives on the strength of what three
	// integration tests happen to want.
	parsed, err := url.Parse(dbtarget.URL(c, dbtarget.PostgreSQL))
	c.Assert(err, qt.IsNil)
	parsed.Scheme = "postgres"
	return parsed.String()
}
