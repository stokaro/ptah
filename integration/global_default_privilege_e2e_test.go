//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/jackc/pgx/v5/stdlib" // registers the pgx driver for database/sql

	"ptah.run/internal/cli/atlas"
	"ptah.run/internal/dbtarget"
)

// globalDefaultEngines are the servers the PostgreSQL-family reader serves and
// CI starts. CockroachDB reads its global defaults through SHOW DEFAULT
// PRIVILEGES and the other two through pg_default_acl and acldefault, so each
// is its own row.
var globalDefaultEngines = []struct {
	name   string
	engine dbtarget.Engine
}{
	{name: "PostgreSQL", engine: dbtarget.PostgreSQL},
	{name: "CockroachDB", engine: dbtarget.CockroachDB},
	{name: "YugabyteDB", engine: dbtarget.YugabyteDB},
}

// TestGlobalDefaultPrivilegeE2E_ConvergesFromTheBuiltInDefault applies global
// default privileges, ALTER DEFAULT PRIVILEGES without IN SCHEMA, to a
// database that holds none, and then takes them back (stokaro/ptah#3772).
//
// A database with no row holds the built-in default, so the revoke of
// PUBLIC's EXECUTE has to be planned against nothing, and the revoke of the
// owner's own ALL too. Each apply is followed by a comparison that has to plan
// nothing, which only a read that subtracts the built-in default can answer.
// The server is the oracle for what each apply left: the same statements run
// by hand in a reference database, and a database nobody touched for the
// built-in default.
func TestGlobalDefaultPrivilegeE2E_ConvergesFromTheBuiltInDefault(t *testing.T) {
	for _, test := range globalDefaultEngines {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(c.Context(), 5*time.Minute)
			defer cancel()
			fixture := newGlobalDefaultFixture(c, ctx, test.engine, "target", "reference", "pristine")
			target := fixture.urls["target"]
			fixture.exec(c, ctx, fixture.urls["reference"], fixture.declared())

			declared := fixture.schemaFile(c, "declared.sql", fixture.declared())
			fixture.apply(c, ctx, declared, target)
			fixture.converged(c, ctx, declared, target)
			c.Assert(fixture.globalDefaults(c, ctx, target), qt.DeepEquals,
				fixture.globalDefaults(c, ctx, fixture.urls["reference"]))
			c.Assert(fixture.globalDefaults(c, ctx, target), qt.Not(qt.DeepEquals),
				fixture.globalDefaults(c, ctx, fixture.urls["pristine"]))

			read, readErr, err := runPtahSplitStreams(ctx, []string{"db", "read", "--db-url", target})
			c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", readErr))
			c.Assert(defaultPrivilegeStatements(read), qt.DeepEquals, fixture.rendered())
			c.Assert(readErr, qt.Not(qt.Contains), "default privilege")

			restoring := fixture.schemaFile(c, "restoring.sql", fixture.restoring())
			fixture.apply(c, ctx, restoring, target)
			fixture.converged(c, ctx, restoring, target)
			c.Assert(fixture.globalDefaults(c, ctx, target), qt.DeepEquals,
				fixture.globalDefaults(c, ctx, fixture.urls["pristine"]))
		})
	}
}

// TestGlobalDefaultPrivilegeE2E_ReplayResetsTheDevDatabase replays a migration
// that sets global default privileges into a dev database. The realm cleanup
// after the replay returns each one to the built-in default, so the dev
// database ends as the server made it, which is what lets the replay accept
// the statements. The same run also carries a schema-scoped default FOR ROLE,
// which the replay read as ALTER ROLE and refused.
func TestGlobalDefaultPrivilegeE2E_ReplayResetsTheDevDatabase(t *testing.T) {
	for _, test := range globalDefaultEngines {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(c.Context(), 5*time.Minute)
			defer cancel()
			fixture := newGlobalDefaultFixture(c, ctx, test.engine, "dev", "pristine")
			migrations := fixture.migrationDir(c, ctx, append([]string{
				`CREATE TABLE "replayed" ("id" bigint PRIMARY KEY);`,
				"ALTER DEFAULT PRIVILEGES FOR ROLE " + quoteE2EIdent(fixture.owner) +
					` IN SCHEMA "public" GRANT SELECT ON TABLES TO ` + quoteE2EIdent(fixture.reader) + ";",
			}, fixture.declared()...))

			stdout, stderr, err := runCompat(ctx, "migrate", "validate",
				"--dir", "file://"+filepath.ToSlash(migrations), "--dev-url", fixture.urls["dev"])

			c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
			c.Assert(fixture.globalDefaults(c, ctx, fixture.urls["dev"]), qt.DeepEquals,
				fixture.globalDefaults(c, ctx, fixture.urls["pristine"]))
			c.Assert(fixture.defaultACLRows(c, ctx, fixture.urls["dev"]), qt.Equals, 0)
		})
	}
}

// TestGlobalDefaultPrivilegeE2E_ADevDatabaseHoldingOneIsRefused is the claim
// on a dev database: the reset would return a global default somebody set to
// the built-in one, so a dev database holding one is refused and left as it
// was, as a table is. The pinned community binary v1.3.0 accepts it and keeps
// it, measured on PostgreSQL 18.6; Ptah is stricter because its reset does not
// keep it.
func TestGlobalDefaultPrivilegeE2E_ADevDatabaseHoldingOneIsRefused(t *testing.T) {
	for _, test := range globalDefaultEngines {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(c.Context(), 5*time.Minute)
			defer cancel()
			fixture := newGlobalDefaultFixture(c, ctx, test.engine, "dev")
			revoke := "ALTER DEFAULT PRIVILEGES FOR ROLE " + quoteE2EIdent(fixture.owner) +
				" REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC;"
			fixture.exec(c, ctx, fixture.urls["dev"], []string{revoke})
			held := fixture.globalDefaults(c, ctx, fixture.urls["dev"])
			migrations := fixture.migrationDir(c, ctx, []string{`CREATE TABLE "replayed" ("id" bigint PRIMARY KEY);`})

			_, stderr, err := runCompat(ctx, "migrate", "validate",
				"--dir", "file://"+filepath.ToSlash(migrations), "--dev-url", fixture.urls["dev"])

			c.Assert(err, qt.IsNotNil)
			c.Assert(stderr, qt.Contains, `connected database is not clean: found default privilege "`+
				fixture.owner+`/f/PUBLIC"`)
			c.Assert(fixture.globalDefaults(c, ctx, fixture.urls["dev"]), qt.DeepEquals, held)
		})
	}
}

// globalDefaultFixture is one or more empty databases on one server, and the
// two roles the defaults name.
type globalDefaultFixture struct {
	engine dbtarget.Engine
	urls   map[string]string
	owner  string
	reader string
}

// newGlobalDefaultFixture creates the roles and one database per name. A role
// belongs to the server, so its cleanup is registered first and runs after the
// databases are gone.
func newGlobalDefaultFixture(c *qt.C, ctx context.Context, engine dbtarget.Engine, names ...string) globalDefaultFixture {
	c.Helper()
	adminURL := dbtarget.URL(c, engine)
	admin, err := sql.Open("pgx", postgresFamilyDriverURL(c, adminURL))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(admin.Close(), qt.IsNil) })
	c.Assert(admin.PingContext(ctx), qt.IsNil)

	stamp := time.Now().UnixNano()
	fixture := globalDefaultFixture{
		engine: engine,
		urls:   make(map[string]string, len(names)),
		owner:  fmt.Sprintf("ptah_gd_owner_%d", stamp),
		reader: fmt.Sprintf("ptah_gd_reader_%d", stamp),
	}
	for _, role := range []string{fixture.owner, fixture.reader} {
		_, err := admin.ExecContext(ctx, "CREATE ROLE "+quoteE2EIdent(role)+" NOLOGIN")
		c.Assert(err, qt.IsNil)
		c.Cleanup(func() {
			_, dropErr := admin.ExecContext(context.Background(), "DROP ROLE IF EXISTS "+quoteE2EIdent(role))
			c.Check(dropErr, qt.IsNil, qt.Commentf("drop role %s", role))
		})
	}
	for _, name := range names {
		database := fmt.Sprintf("ptah_gd_%s_%d", name, stamp)
		createE2EDatabase(c, ctx, admin, database)
		c.Cleanup(func() { dropPostgresFamilyE2EDatabase(c, admin, database) })
		fixture.urls[name] = replaceDatabaseName(c, adminURL, database)
	}
	return fixture
}

// declared is the global defaults the tests set: a revoke of a built-in
// privilege from PUBLIC, a grant to another role, a grant on SCHEMAS, a class
// only the global form has, and a revoke of the owner's own ALL.
func (f globalDefaultFixture) declared() []string {
	prefix := "ALTER DEFAULT PRIVILEGES FOR ROLE " + quoteE2EIdent(f.owner)
	reader := quoteE2EIdent(f.reader)
	return []string{
		prefix + " REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC;",
		prefix + " GRANT SELECT ON TABLES TO " + reader + ";",
		prefix + " GRANT USAGE ON SCHEMAS TO " + reader + ";",
		prefix + " REVOKE ALL ON SEQUENCES FROM " + quoteE2EIdent(f.owner) + ";",
	}
}

// rendered is what a read renders for [globalDefaultFixture.declared], sorted
// as [defaultPrivilegeStatements] sorts it.
func (f globalDefaultFixture) rendered() []string {
	prefix := "ALTER DEFAULT PRIVILEGES FOR ROLE " + quoteE2EIdent(f.owner)
	reader := quoteE2EIdent(f.reader)
	return []string{
		prefix + " GRANT SELECT ON TABLES TO " + reader + ";",
		prefix + " GRANT USAGE ON SCHEMAS TO " + reader + ";",
		prefix + " REVOKE ALL ON SEQUENCES FROM " + quoteE2EIdent(f.owner) + ";",
		prefix + " REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC;",
	}
}

// restoring declares the built-in default back: the built-in privileges
// granted again and the grants to the other role revoked. Nothing declares
// the owner as a role, so each is said.
func (f globalDefaultFixture) restoring() []string {
	prefix := "ALTER DEFAULT PRIVILEGES FOR ROLE " + quoteE2EIdent(f.owner)
	reader := quoteE2EIdent(f.reader)
	return []string{
		prefix + " GRANT EXECUTE ON FUNCTIONS TO PUBLIC;",
		prefix + " REVOKE SELECT ON TABLES FROM " + reader + ";",
		prefix + " REVOKE USAGE ON SCHEMAS FROM " + reader + ";",
		prefix + " GRANT ALL ON SEQUENCES TO " + quoteE2EIdent(f.owner) + ";",
	}
}

// schemaFile writes statements to a schema file and returns its path.
func (globalDefaultFixture) schemaFile(c *qt.C, name string, statements []string) string {
	c.Helper()
	path := filepath.Join(c.TempDir(), name)
	c.Assert(os.WriteFile(path, []byte(strings.Join(statements, "\n")+"\n"), 0o600), qt.IsNil)
	return path
}

// apply runs `ptah schema apply` with schemaFile against dbURL.
func (globalDefaultFixture) apply(c *qt.C, ctx context.Context, schemaFile, dbURL string) {
	c.Helper()
	stdout, stderr, err := runPtahSplitStreams(ctx, []string{
		"schema", "apply", "--schema-file", schemaFile, "--db-url", dbURL, "--auto-approve",
	})
	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
}

// converged asserts that a comparison of schemaFile with dbURL plans nothing
// and withholds nothing.
func (globalDefaultFixture) converged(c *qt.C, ctx context.Context, schemaFile, dbURL string) {
	c.Helper()
	stdout, stderr, err := runPtahSplitStreams(ctx, []string{
		"schema", "compare", "--schema-file", schemaFile, "--db-url", dbURL, "--exit-code",
	})
	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	c.Assert(stdout, qt.Contains, "No schema differences detected.")
}

// exec runs statements on dbURL as the connecting role.
func (globalDefaultFixture) exec(c *qt.C, ctx context.Context, dbURL string, statements []string) {
	c.Helper()
	db, err := sql.Open("pgx", postgresFamilyDriverURL(c, dbURL))
	c.Assert(err, qt.IsNil)
	defer db.Close()
	for _, statement := range statements {
		_, err := db.ExecContext(ctx, statement)
		c.Assert(err, qt.IsNil, qt.Commentf("exec: %s", statement))
	}
}

// migrationDir writes statements as one migration and hashes the directory.
func (globalDefaultFixture) migrationDir(c *qt.C, ctx context.Context, statements []string) string {
	c.Helper()
	dir := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "20260927000001_defaults.sql"),
		[]byte(strings.Join(statements, "\n")+"\n"), 0o600), qt.IsNil)
	stdout, stderr, err := runCompat(ctx, "migrate", "hash", "--dir", "file://"+filepath.ToSlash(dir))
	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	return dir
}

// globalDefaults is what the owner's new objects receive in every schema of
// one database, for the reason [defaultPrivilegeFixture.globalDefaults] gives.
func (f globalDefaultFixture) globalDefaults(c *qt.C, ctx context.Context, dbURL string) []string {
	c.Helper()
	return defaultPrivilegeFixture{engine: f.engine, grantor: f.owner}.globalDefaults(c, ctx, dbURL)
}

// defaultACLRows counts every pg_default_acl row of one database.
func (globalDefaultFixture) defaultACLRows(c *qt.C, ctx context.Context, dbURL string) int {
	c.Helper()
	db, err := sql.Open("pgx", postgresFamilyDriverURL(c, dbURL))
	c.Assert(err, qt.IsNil)
	defer db.Close()
	var count int
	c.Assert(db.QueryRowContext(ctx, "SELECT count(*) FROM pg_default_acl").Scan(&count), qt.IsNil)
	return count
}

// runCompat runs the compatibility surface in process.
func runCompat(ctx context.Context, args ...string) (stdout, stderr string, err error) {
	cmd := atlas.NewCompatCommand("atlas")
	var out, errOut bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	cmd.SetArgs(args)
	err = cmd.ExecuteContext(ctx)
	return out.String(), errOut.String(), err
}
