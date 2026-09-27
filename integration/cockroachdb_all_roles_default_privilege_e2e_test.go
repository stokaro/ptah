//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/jackc/pgx/v5/stdlib" // registers the pgx driver for database/sql

	"ptah.run/internal/dbtarget"
)

// TestCockroachDBAllRolesDefaultPrivilegeE2E_HappyPath reads, applies and cleans
// a CockroachDB database holding default privileges set FOR ALL ROLES.
//
// CockroachDB records FOR ALL ROLES as role 0, and pg_get_userbyid answers
// `unknown (OID=0)` for it. A read that takes that as a grantor describes
// `ALTER DEFAULT PRIVILEGES FOR ROLE "unknown (OID=0)" ...`, and applying the
// description elsewhere fails on a role nobody has; `ptah db drop-all` builds
// the same name into its revoke and stops after the tables are gone
// (stokaro/ptah#3770). Measured on CockroachDB v26.2.7 and v26.3.1; on v25.4.16
// aclexplode answers no rows, so no default privilege is described or revoked
// there at all (stokaro/ptah#3802).
//
// No declaration can name FOR ALL ROLES, so the read leaves those rows out and
// the note names them. The FOR ROLE default beside them is the control: it is
// still described, and still arrives in the fresh database.
func TestCockroachDBAllRolesDefaultPrivilegeE2E_HappyPath(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(c.Context(), 5*time.Minute)
	defer cancel()
	fixture := newAllRolesFixture(c, ctx)

	read, readErr, err := runPtahSplitStreams(ctx, []string{
		"db", "read", "--db-url", fixture.sourceURL, "--schemas", "public,app",
	})
	c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", readErr))
	c.Assert(defaultPrivilegeStatements(read), qt.DeepEquals, []string{
		"ALTER DEFAULT PRIVILEGES FOR ROLE " + quoteE2EIdent(fixture.owner) +
			` IN SCHEMA "public" GRANT INSERT ON TABLES TO ` + quoteE2EIdent(fixture.reader) + ";",
	})
	c.Assert(readErr, qt.Contains, "note: 3 default privileges are not described, because no schema"+
		" source can declare one set without IN SCHEMA or FOR ALL ROLES; a description applied to"+
		" another database does not carry them: SEQUENCES in app for all roles, TABLES in public"+
		" for all roles, TYPES in every schema for all roles.\n")

	rendered := filepath.Join(c.TempDir(), "rendered.sql")
	c.Assert(os.WriteFile(rendered, []byte(read), 0o600), qt.IsNil)
	applied, appliedErr, err := runPtahSplitStreams(ctx, []string{
		"schema", "apply", "--schema-file", rendered, "--db-url", fixture.targetURL,
		"--schemas", "public,app", "--auto-approve",
	})
	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied, appliedErr))
	c.Assert(allRolesDefaultACL(c, ctx, fixture.targetURL), qt.DeepEquals, []string{
		fixture.owner + " public r",
	})

	dropped, droppedErr, err := runPtahSplitStreams(ctx, []string{
		"db", "drop-all", "--db-url", fixture.sourceURL, "--auto-approve",
	})
	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", dropped, droppedErr))
	// drop-all cleans the connection's schema, so the default in app stays and
	// both of public's go, the FOR ALL ROLES one included.
	c.Assert(allRolesDefaultACL(c, ctx, fixture.sourceURL), qt.DeepEquals, []string{"role 0 app S"})
}

// allRolesFixture is a source database holding FOR ALL ROLES defaults and one
// FOR ROLE default, an empty target database, and the two roles they name.
type allRolesFixture struct {
	sourceURL string
	targetURL string
	owner     string
	reader    string
}

// newAllRolesFixture creates both databases and the roles on the CockroachDB
// server, and seeds the source. The roles are registered for cleanup first so
// they are dropped after both databases.
func newAllRolesFixture(c *qt.C, ctx context.Context) allRolesFixture {
	c.Helper()
	adminURL := dbtarget.URL(c, dbtarget.CockroachDB)
	admin, err := sql.Open("pgx", postgresFamilyDriverURL(c, adminURL))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(admin.Close(), qt.IsNil) })

	stamp := time.Now().UnixNano()
	fixture := allRolesFixture{
		owner:  fmt.Sprintf("ptah_ar_owner_%d", stamp),
		reader: fmt.Sprintf("ptah_ar_reader_%d", stamp),
	}
	for _, role := range []string{fixture.owner, fixture.reader} {
		_, err := admin.ExecContext(ctx, "CREATE ROLE "+quoteE2EIdent(role))
		c.Assert(err, qt.IsNil)
		c.Cleanup(func() {
			_, dropErr := admin.ExecContext(context.Background(), "DROP ROLE IF EXISTS "+quoteE2EIdent(role))
			c.Check(dropErr, qt.IsNil, qt.Commentf("drop role %s", role))
		})
	}
	source := fmt.Sprintf("ptah_ar_source_%d", stamp)
	target := fmt.Sprintf("ptah_ar_target_%d", stamp)
	for _, name := range []string{source, target} {
		createE2EDatabase(c, ctx, admin, name)
		c.Cleanup(func() { dropPostgresFamilyE2EDatabase(c, admin, name) })
	}
	fixture.sourceURL = replaceDatabaseName(c, adminURL, source)
	fixture.targetURL = replaceDatabaseName(c, adminURL, target)

	db, err := sql.Open("pgx", postgresFamilyDriverURL(c, fixture.sourceURL))
	c.Assert(err, qt.IsNil)
	defer db.Close()
	reader := quoteE2EIdent(fixture.reader)
	for _, statement := range []string{
		"CREATE SCHEMA app",
		"CREATE TABLE t (id INT8 PRIMARY KEY)",
		"ALTER DEFAULT PRIVILEGES FOR ALL ROLES IN SCHEMA public GRANT SELECT ON TABLES TO " + reader,
		"ALTER DEFAULT PRIVILEGES FOR ALL ROLES IN SCHEMA app GRANT USAGE ON SEQUENCES TO " + reader,
		"ALTER DEFAULT PRIVILEGES FOR ALL ROLES GRANT USAGE ON TYPES TO " + reader,
		"ALTER DEFAULT PRIVILEGES FOR ROLE " + quoteE2EIdent(fixture.owner) +
			" IN SCHEMA public GRANT INSERT ON TABLES TO " + reader,
	} {
		_, err := db.ExecContext(ctx, statement)
		c.Assert(err, qt.IsNil, qt.Commentf("seed: %s", statement))
	}
	return fixture
}

// allRolesDefaultACL lists a database's schema-scoped pg_default_acl rows as
// grantor, schema and object class, with FOR ALL ROLES spelled as the catalog
// records it, role 0, so a row the read left out still shows here.
func allRolesDefaultACL(c *qt.C, ctx context.Context, dbURL string) []string {
	c.Helper()
	db, err := sql.Open("pgx", postgresFamilyDriverURL(c, dbURL))
	c.Assert(err, qt.IsNil)
	defer db.Close()
	rows, err := db.QueryContext(ctx, `
		SELECT CASE WHEN d.defaclrole = 0 THEN 'role 0' ELSE pg_get_userbyid(d.defaclrole) END
			|| ' ' || n.nspname || ' ' || d.defaclobjtype::text
		FROM pg_default_acl d
		JOIN pg_namespace n ON n.oid = d.defaclnamespace
		ORDER BY 1`)
	c.Assert(err, qt.IsNil)
	defer rows.Close()
	var entries []string
	for rows.Next() {
		var entry string
		c.Assert(rows.Scan(&entry), qt.IsNil)
		entries = append(entries, entry)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return entries
}
