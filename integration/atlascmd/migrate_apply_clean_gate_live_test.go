//go:build integration

package atlas_test

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/jackc/pgx/v5/stdlib"

	"go.5x5.cz/ptah/cmd/atlas"
	"go.5x5.cz/ptah/internal/migratesum"
	"go.5x5.cz/ptah/migration/migrator"
)

// stokaro/ptah#1257: the adoption gate stokaro/ptah#1252 shipped resolved its
// scope through current_schema(), so it could only ever evaluate one schema. A
// PLAIN PostgreSQL URL — no search_path, the spelling the compatibility
// documentation itself uses — puts the pinned community binary v1.3.0 at realm
// scope, where the operand is schemas and an EMPTY extra schema is enough to
// refuse.
//
// These cases drive the whole command rather than the predicate, because the
// half that broke is an ordering question the predicate cannot see: the catalog
// has to be read before this implementation creates its own revision table.
// Reading it afterwards refuses `found schema "public"` against a database whose
// `public` holds nothing but the table the run itself just created.
//
// Every expected string was produced by that binary against PostgreSQL 17 on
// 2026-08-07, one throwaway database per cell.

// livePostgresURLForCleanGate gates these cases on the same environment
// variables as the other PostgreSQL live tests in this package.
func livePostgresURLForCleanGate(t *testing.T) string {
	t.Helper()
	dbURL := os.Getenv("POSTGRES_TEST_DSN")
	if dbURL == "" {
		dbURL = os.Getenv("TEST_DATABASE_URL")
	}
	if dbURL == "" {
		t.Skip("POSTGRES_TEST_DSN or TEST_DATABASE_URL not set")
	}
	if !strings.HasPrefix(dbURL, "postgres://") && !strings.HasPrefix(dbURL, "postgresql://") {
		t.Skip("PostgreSQL URL required for the clean gate live tests")
	}
	return dbURL
}

// newCleanGateDatabase creates a throwaway database in the state the case is
// about and returns its plain URL, so no two cases can see each other's schemas.
func newCleanGateDatabase(c *qt.C, adminURL string, setup []string) string {
	c.Helper()
	ctx := context.Background()
	admin, err := sql.Open("pgx", adminURL)
	c.Assert(err, qt.IsNil)
	c.Assert(admin.PingContext(ctx), qt.IsNil)

	name := fmt.Sprintf("ptah_clean_gate_%d", time.Now().UnixNano())
	_, err = admin.ExecContext(ctx, `CREATE DATABASE "`+name+`"`)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		_, dropErr := admin.ExecContext(
			context.WithoutCancel(ctx), `DROP DATABASE IF EXISTS "`+name+`" WITH (FORCE)`,
		)
		c.Check(dropErr, qt.IsNil)
		c.Check(admin.Close(), qt.IsNil)
	})

	parsed, err := url.Parse(adminURL)
	c.Assert(err, qt.IsNil)
	parsed.Path = "/" + name
	parsed.RawPath = ""
	dbURL := parsed.String()

	seed, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(seed.Close(), qt.IsNil) }()
	for _, statement := range setup {
		_, execErr := seed.ExecContext(ctx, statement)
		c.Assert(execErr, qt.IsNil, qt.Commentf("statement: %s", statement))
	}
	return dbURL
}

// writeCleanGatePostgresFixture writes a hashed one-migration Atlas directory
// whose SQL PostgreSQL accepts.
func writeCleanGatePostgresFixture(c *qt.C) string {
	c.Helper()
	dir := filepath.Join(c.TempDir(), "migrations")
	c.Assert(os.MkdirAll(dir, 0o755), qt.IsNil)
	body := "CREATE TABLE cg_users (id integer NOT NULL, PRIMARY KEY (id));\n"
	c.Assert(os.WriteFile(filepath.Join(dir, "20240101000000_first.sql"), []byte(body), 0o600), qt.IsNil)
	_, err := migratesum.WriteWithFormat(dir, migrator.MigrationDirFormatAtlas)
	c.Assert(err, qt.IsNil)
	return dir
}

func TestMigrateApplyCleanGateRealmScopeLivePostgres(t *testing.T) {
	adminURL := livePostgresURLForCleanGate(t)

	tests := []struct {
		name string
		// query is appended to the throwaway database's URL. Empty is the
		// plain URL, which is what selects realm scope.
		query string
		setup []string
		// assert names the outcome so the table carries no branch.
		assert func(c *qt.C, err error, out string)
	}{
		{
			// The regression this file exists for. Nothing but an empty
			// `public`: the binary applies, and so must this, which it cannot
			// do if the gate reads the catalog after its own revision table
			// lands in `public`.
			name: "an empty database applies",
			assert: func(c *qt.C, err error, out string) {
				c.Assert(err, qt.IsNil)
				c.Assert(out, qt.Contains, "20240101000000")
			},
		},
		{
			name:  "an empty extra schema refuses",
			setup: []string{"CREATE SCHEMA extra"},
			assert: func(c *qt.C, err error, _ string) {
				c.Assert(err, qt.IsNotNil)
				c.Assert(err.Error(), qt.Equals,
					`sql/migrate: connected database is not clean: found schema "extra". `+
						`baseline version or allow-dirty is required`)
			},
		},
		{
			name: "a table living only in another schema refuses",
			setup: []string{
				"CREATE SCHEMA extra",
				"CREATE TABLE extra.legacy_stuff (id integer PRIMARY KEY)",
			},
			assert: func(c *qt.C, err error, _ string) {
				c.Assert(err, qt.IsNotNil)
				c.Assert(err.Error(), qt.Equals,
					`sql/migrate: connected database is not clean: found schema "extra". `+
						`baseline version or allow-dirty is required`)
			},
		},
		{
			name:  "a table in public refuses by schema, not by table",
			setup: []string{"CREATE TABLE legacy_stuff (id integer PRIMARY KEY)"},
			assert: func(c *qt.C, err error, _ string) {
				c.Assert(err, qt.IsNotNil)
				c.Assert(err.Error(), qt.Equals,
					`sql/migrate: connected database is not clean: found schema "public". `+
						`baseline version or allow-dirty is required`)
			},
		},
		{
			// A dry run refuses too, which is only possible because the gate
			// runs before execution rather than inside it.
			name:  "a dry run refuses",
			query: "",
			setup: []string{"CREATE SCHEMA extra"},
			assert: func(c *qt.C, err error, _ string) {
				c.Assert(err, qt.IsNotNil)
				c.Assert(err.Error(), qt.Contains, `found schema "extra"`)
			},
		},
		{
			// The control that keeps stokaro/ptah#1252 where it was: the same
			// database through a URL that pins a schema stays in schema scope
			// and applies.
			name:  "an empty extra schema applies when the URL pins a schema",
			query: "search_path=public",
			setup: []string{"CREATE SCHEMA extra"},
			assert: func(c *qt.C, err error, out string) {
				c.Assert(err, qt.IsNil)
				c.Assert(out, qt.Contains, "20240101000000")
			},
		},
		{
			name:  "a table in another schema applies when the URL pins a schema",
			query: "search_path=public",
			setup: []string{
				"CREATE SCHEMA extra",
				"CREATE TABLE extra.legacy_stuff (id integer PRIMARY KEY)",
			},
			assert: func(c *qt.C, err error, out string) {
				c.Assert(err, qt.IsNil)
				c.Assert(out, qt.Contains, "20240101000000")
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := writeCleanGatePostgresFixture(c)
			dbURL := newCleanGateDatabase(c, adminURL, test.setup)
			args := []string{"migrate", "apply", "--dir", "file://" + dir, "--url", withCleanGateQuery(dbURL, test.query)}
			cmd := atlas.NewCompatCommand("atlas")
			var out bytes.Buffer
			cmd.SetOut(&out)
			cmd.SetErr(&out)
			cmd.SetArgs(args)

			err := cmd.Execute()

			test.assert(c, err, out.String())
		})
	}
}

// TestMigrateApplyCleanGateRealmScopeOptOutsLivePostgres pins the two documented
// opt-ins at realm scope. Measured: either one makes the binary apply against
// the same database it otherwise refuses.
func TestMigrateApplyCleanGateRealmScopeOptOutsLivePostgres(t *testing.T) {
	adminURL := livePostgresURLForCleanGate(t)

	tests := []struct {
		name  string
		flags []string
	}{
		{name: "allow-dirty", flags: []string{"--allow-dirty"}},
		{name: "baseline", flags: []string{"--baseline", "20240101000000"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := writeCleanGatePostgresFixture(c)
			dbURL := newCleanGateDatabase(c, adminURL, []string{"CREATE SCHEMA extra"})
			cmd := atlas.NewCompatCommand("atlas")
			var out bytes.Buffer
			cmd.SetOut(&out)
			cmd.SetErr(&out)
			cmd.SetArgs(append([]string{
				"migrate", "apply", "--dir", "file://" + dir, "--url", dbURL,
			}, test.flags...))

			err := cmd.Execute()

			c.Assert(err, qt.IsNil)
		})
	}
}

// withCleanGateQuery appends a fixture's query to a database URL that already
// carries the server's own parameters, such as sslmode.
func withCleanGateQuery(dbURL, query string) string {
	if query == "" {
		return dbURL
	}
	if strings.Contains(dbURL, "?") {
		return dbURL + "&" + query
	}
	return dbURL + "?" + query
}
