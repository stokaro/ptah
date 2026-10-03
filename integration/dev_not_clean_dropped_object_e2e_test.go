//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
	"ptah.run/internal/devclean"
)

// A dev database is refused when it holds anything the reset would drop, not
// only a table. Measured against the pinned community binary v1.3.0 on
// PostgreSQL 18.6 on 2026-09-27, with each object below alone in the dev
// database: with `search_path=public` the binary accepts the database and
// leaves the object where it is, and with no search_path it accepts it and
// drops every object but the enum. Ptah's reset drops all of them in both
// scopes, and a replay drops a large object too. Refusing is stricter than the
// binary on purpose: nothing the operator left there is dropped in silence,
// and no dev read has to filter kept objects out (stokaro/ptah#3808). Each test
// reads the object back after the refusal.

// droppedObjectKinds are objects a reset of the dev database drops, each with
// the statement that creates it, the query that counts it, and the words the
// refusal names it with. The large object's oid is the server's to choose.
var droppedObjectKinds = []struct {
	name   string
	ddl    string
	count  string
	object string
}{
	{name: "view", ddl: "CREATE VIEW keep_v AS SELECT 1 AS x",
		count: "SELECT count(*) FROM pg_views WHERE viewname = 'keep_v'", object: `view "keep_v"`},
	{name: "materialized view", ddl: "CREATE MATERIALIZED VIEW keep_m AS SELECT 1 AS x",
		count: "SELECT count(*) FROM pg_matviews WHERE matviewname = 'keep_m'", object: `materialized view "keep_m"`},
	{name: "function", ddl: "CREATE FUNCTION keep_f() RETURNS int LANGUAGE sql AS 'SELECT 1'",
		count: "SELECT count(*) FROM pg_proc WHERE proname = 'keep_f'", object: `function "keep_f"`},
	{name: "procedure", ddl: "CREATE PROCEDURE keep_p() LANGUAGE sql AS 'SELECT 1'",
		count: "SELECT count(*) FROM pg_proc WHERE proname = 'keep_p'", object: `procedure "keep_p"`},
	{name: "aggregate", ddl: "CREATE AGGREGATE keep_a(int) (SFUNC = int4pl, STYPE = int)",
		count: "SELECT count(*) FROM pg_proc WHERE proname = 'keep_a'", object: `aggregate "keep_a"`},
	{name: "sequence", ddl: "CREATE SEQUENCE keep_s",
		count: "SELECT count(*) FROM pg_class WHERE relname = 'keep_s'", object: `sequence "keep_s"`},
	{name: "enum", ddl: "CREATE TYPE keep_e AS ENUM ('a')",
		count: "SELECT count(*) FROM pg_type WHERE typname = 'keep_e'", object: `type "keep_e"`},
	{name: "domain", ddl: "CREATE DOMAIN keep_d AS int",
		count: "SELECT count(*) FROM pg_type WHERE typname = 'keep_d'", object: `type "keep_d"`},
	{name: "composite type", ddl: "CREATE TYPE keep_c AS (x int)",
		count: "SELECT count(*) FROM pg_type WHERE typname = 'keep_c'", object: `type "keep_c"`},
	{name: "collation", ddl: "CREATE COLLATION keep_coll (provider = icu, locale = 'und')",
		count: "SELECT count(*) FROM pg_collation WHERE collname = 'keep_coll'", object: `collation "keep_coll"`},
}

// droppedObjectScopes are the two scopes a PostgreSQL dev URL selects, with
// the words a refusal places an object in.
var droppedObjectScopes = []struct {
	name       string
	searchPath string
	where      string
}{
	{name: "pinned to public", searchPath: "public", where: `in connected schema`},
	{name: "realm", searchPath: "", where: `in schema "public"`},
}

// droppedObjectDev is an empty PostgreSQL scratch database, reached through a
// URL that pins searchPath when it is set, with a connection to it.
func droppedObjectDev(c *qt.C, searchPath string) (string, *dbschema.DatabaseConnection) {
	c.Helper()
	devURL := postgresScratchDevURL(c, "ptah_dropped_dev")
	if searchPath != "" {
		parsed, err := url.Parse(devURL)
		c.Assert(err, qt.IsNil)
		query := parsed.Query()
		query.Set("search_path", searchPath)
		parsed.RawQuery = query.Encode()
		devURL = parsed.String()
	}
	conn, err := dbschema.ConnectToDatabase(c.Context(), devURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	return devURL, conn
}

// countOf runs a count query on conn.
func countOf(c *qt.C, conn *dbschema.DatabaseConnection, query string) int {
	c.Helper()
	var count int
	c.Assert(conn.QueryRowContext(c.Context(), query).Scan(&count), qt.IsNil)
	return count
}

// TestDevDatabaseClaimRefusesAnObjectTheResetDropsLive claims a dev database
// holding one object the reset would drop, in each scope. The claim refuses it
// and names the object, and the object is still there.
func TestDevDatabaseClaimRefusesAnObjectTheResetDropsLive(t *testing.T) {
	for _, scope := range droppedObjectScopes {
		for _, kind := range droppedObjectKinds {
			t.Run(scope.name+"/"+kind.name, func(t *testing.T) {
				c := qt.New(t)
				_, dev := droppedObjectDev(c, scope.searchPath)
				_, err := dev.ExecContext(c.Context(), kind.ddl)
				c.Assert(err, qt.IsNil)

				_, err = devclean.Claim(c.Context(), dev)

				c.Assert(err, qt.ErrorMatches, `connected database is not clean: found `+kind.object+` `+scope.where+
					`; Ptah resets a dev database before and after it uses one, so point --dev-url at an empty database`)
				c.Assert(countOf(c, dev, kind.count), qt.Equals, 1)
			})
		}
	}
}

// TestDevDatabaseClaimRefusesALargeObjectLive is the object that belongs to no
// schema. A replay resets the whole database whatever the URL pins, and that
// reset removes every large object, so the claim refuses one in both scopes.
func TestDevDatabaseClaimRefusesALargeObjectLive(t *testing.T) {
	for _, scope := range droppedObjectScopes {
		t.Run(scope.name, func(t *testing.T) {
			c := qt.New(t)
			_, dev := droppedObjectDev(c, scope.searchPath)
			_, err := dev.ExecContext(c.Context(), "SELECT lo_create(0)")
			c.Assert(err, qt.IsNil)

			_, err = devclean.Claim(c.Context(), dev)

			c.Assert(err, qt.ErrorMatches, `connected database is not clean: found large object "\d+"; Ptah resets a dev database .*`)
			c.Assert(countOf(c, dev, "SELECT count(*) FROM pg_largeobject_metadata"), qt.Equals, 1)
		})
	}
}

// TestDevDatabaseClaimRefusesADefaultPrivilegeLive is a setting rather than
// an object. With no search_path the reset drops public with every default set
// in it, as the pinned community binary does, so a dev database carrying one
// is refused and names it by owner, object type and grantee. A URL pinning
// public keeps it; see [TestDevDatabaseClaimTakesWhatTheResetKeepsLive]. The
// role is created before the database, so it is dropped after the database
// that refers to it.
func TestDevDatabaseClaimRefusesADefaultPrivilegeLive(t *testing.T) {
	c := qt.New(t)
	role := fmt.Sprintf("ptah_dp_%d", time.Now().UnixNano()%1_000_000_000_000)
	admin, err := dbschema.ConnectToDatabase(c.Context(), dbtarget.URL(c, dbtarget.PostgreSQL))
	c.Assert(err, qt.IsNil)
	_, err = admin.ExecContext(c.Context(), "CREATE ROLE "+role)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		_, dropErr := admin.ExecContext(context.Background(), "DROP ROLE IF EXISTS "+role)
		c.Check(dropErr, qt.IsNil)
		dbschema.CloseAndWarn(admin)
	})
	_, dev := droppedObjectDev(c, "")
	_, err = dev.ExecContext(c.Context(), "ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO "+role)
	c.Assert(err, qt.IsNil)

	_, err = devclean.Claim(c.Context(), dev)

	c.Assert(err, qt.ErrorMatches, `connected database is not clean: found default privilege "[^"]+/r/`+role+
		`" in schema "public"; Ptah resets a dev database .*`)
	c.Assert(countOf(c, dev, "SELECT count(*) FROM pg_default_acl"), qt.Equals, 1)
}

// TestDevDatabaseClaimTakesWhatTheResetKeepsLive is the control: what a reset
// keeps does not refuse the database. An extension the database holds is kept
// with the types and functions it owns, and with search_path=public another
// schema is kept whole, a view in it included. A default privilege set in
// public is kept with search_path=public, and a global one in either scope
// (stokaro/ptah#4034).
func TestDevDatabaseClaimTakesWhatTheResetKeepsLive(t *testing.T) {
	tests := []struct {
		name       string
		searchPath string
		ddl        string
	}{
		{name: "an extension at realm scope", ddl: "CREATE EXTENSION citext SCHEMA public"},
		{name: "an extension pinned to public", searchPath: "public", ddl: "CREATE EXTENSION citext SCHEMA public"},
		{name: "a view in another schema", searchPath: "public", ddl: "CREATE SCHEMA other; CREATE VIEW other.keep_v AS SELECT 1 AS x"},
		{name: "a default privilege pinned to public", searchPath: "public",
			ddl: "ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO PUBLIC"},
		{name: "a global default privilege at realm scope",
			ddl: "ALTER DEFAULT PRIVILEGES REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC"},
		{name: "a global default privilege pinned to public", searchPath: "public",
			ddl: "ALTER DEFAULT PRIVILEGES REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			_, dev := droppedObjectDev(c, test.searchPath)
			_, err := dev.ExecContext(c.Context(), test.ddl)
			c.Assert(err, qt.IsNil)

			_, err = devclean.Claim(c.Context(), dev)

			c.Assert(err, qt.IsNil)
		})
	}
}

// TestCompatVerbsRefuseAViewTheResetDropsE2E runs every Atlas-compatible verb
// that resets a dev database, with a view alone in it, in each scope. Each
// refuses in the words its snapshot error uses, and the view is still there.
func TestCompatVerbsRefuseAViewTheResetDropsE2E(t *testing.T) {
	for _, scope := range droppedObjectScopes {
		for _, verb := range devBaselineVerbs {
			t.Run(scope.name+"/"+verb.name, func(t *testing.T) {
				c := qt.New(t)
				devURL, dev := droppedObjectDev(c, scope.searchPath)
				_, err := dev.ExecContext(c.Context(), droppedObjectKinds[0].ddl)
				c.Assert(err, qt.IsNil)
				target := newDevIdentityTarget(c, dbtarget.PostgreSQL, " WITH (FORCE)")
				schemaFile, dir := writeDevIdentitySources(c)
				out := filepath.Join(c.TempDir(), "out")
				c.Assert(os.MkdirAll(out, 0o755), qt.IsNil)

				output, err := runCompatVerb(devNotCleanArgs(verb.args, devURL, target.url, schemaFile, dir, out)...)

				c.Assert(err, qt.ErrorMatches,
					`(?s).*sql/migrate: connected database is not clean: found view "keep_v" `+scope.where,
					qt.Commentf("%s", output))
				c.Assert(countOf(c, dev, droppedObjectKinds[0].count), qt.Equals, 1)
				c.Assert(target.keptRows(c), qt.Equals, 1)
			})
		}
	}
}

// TestNativeVerbsRefuseAViewTheResetDropsE2E is the same refusal on the native
// verbs that reset a dev database, in Ptah's own words.
func TestNativeVerbsRefuseAViewTheResetDropsE2E(t *testing.T) {
	verbs := []struct {
		name string
		args []string
	}{
		{name: "migrations validate", args: []string{"migrations", "validate", "--dir", "{rawdir}", "--dev-url", "{dev}"}},
		{name: "migrations lint", args: []string{"migrations", "lint", "--dir", "{rawdir}", "--dev-url", "{dev}"}},
		{
			name: "schema apply",
			args: []string{"schema", "apply", "--db-url", "{target}", "--schema-file", "{rawschema}", "--dev-url", "{dev}", "--auto-approve"},
		},
	}

	for _, scope := range droppedObjectScopes {
		for _, verb := range verbs {
			t.Run(scope.name+"/"+verb.name, func(t *testing.T) {
				c := qt.New(t)
				devURL, dev := droppedObjectDev(c, scope.searchPath)
				_, err := dev.ExecContext(c.Context(), droppedObjectKinds[0].ddl)
				c.Assert(err, qt.IsNil)
				target := newDevIdentityTarget(c, dbtarget.PostgreSQL, " WITH (FORCE)")
				schemaFile, dir := writeDevIdentitySources(c)

				output, err := runPtahNativeWithError(devNotCleanArgs(verb.args, devURL, target.url, schemaFile, dir, "")...)

				c.Assert(err, qt.ErrorMatches, `(?s).*connected database is not clean: found view "keep_v" `+scope.where+
					`; Ptah resets a dev database before and after it uses one, so point --dev-url at an empty database.*`,
					qt.Commentf("%s", output))
				c.Assert(countOf(c, dev, droppedObjectKinds[0].count), qt.Equals, 1)
			})
		}
	}
}
