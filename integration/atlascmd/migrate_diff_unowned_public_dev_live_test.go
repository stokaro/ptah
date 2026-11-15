//go:build integration

package atlas_test

import (
	"context"
	"database/sql"
	"net/url"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
)

// publicSchemaState is what a dev database's claim records of schema public:
// its owner, its access list with the entries sorted, and its comment.
type publicSchemaState struct {
	owner   string
	acl     string
	comment string
}

// migrate diff runs on a dev database that an ordinary role owns and connects
// to with a realm URL, while another role owns schema public in it. That is
// every new database on PostgreSQL 13 and 14: the bootstrap superuser owns
// public, also in a database created with OWNER, and PUBLIC holds CREATE on
// it. PostgreSQL 15 gives public to pg_database_owner instead, so the test
// sets the older shape up as the admin on any server.
//
// The realm cleanup cannot drop a schema the connecting role does not own.
// Dropped and created again, public refused every cleanup of the run with
// `must be owner of schema public` (SQLSTATE 42501), and no diff was written.
// Emptied in place, it keeps the owner, privileges and comment the claim
// recorded, and the second diff finds the dev database clean.
func TestMigrateDiffOnADevDatabaseWhosePublicAnotherRoleOwnsPostgresLive(t *testing.T) {
	c := qt.New(t)
	adminURL := dbtarget.URL(c, dbtarget.PostgreSQL)
	suffix := uniqueScopeSuffix()
	owner := "ptah_unowned_public_" + suffix
	databaseName := "ptah_unowned_public_" + suffix
	const connectionPassword = "connection_password_fixture"

	admin, err := dbschema.ConnectToDatabase(c.Context(), adminURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		_, _ = admin.ExecContext(context.Background(), `DROP DATABASE IF EXISTS "`+databaseName+`" WITH (FORCE)`)
		_, _ = admin.ExecContext(context.Background(), `DROP ROLE IF EXISTS "`+owner+`"`)
		dbschema.CloseAndWarn(admin)
	})
	_, err = admin.ExecContext(c.Context(), `CREATE ROLE "`+owner+`" LOGIN PASSWORD '`+
		connectionPassword+`' NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS`)
	c.Assert(err, qt.IsNil)
	_, err = admin.ExecContext(c.Context(), `CREATE DATABASE "`+databaseName+`" OWNER "`+owner+`"`)
	c.Assert(err, qt.IsNil)

	parsed, err := url.Parse(adminURL)
	c.Assert(err, qt.IsNil)
	parsed.Path = "/" + databaseName
	parsed.RawPath = ""
	devAdmin, err := sql.Open("pgx", parsed.String())
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(devAdmin.Close(), qt.IsNil) })
	_, err = devAdmin.ExecContext(c.Context(), `
		ALTER SCHEMA public OWNER TO CURRENT_USER;
		GRANT CREATE ON SCHEMA public TO PUBLIC;
	`)
	c.Assert(err, qt.IsNil)
	claimed := readPublicSchemaState(c, devAdmin)
	c.Assert(claimed.owner, qt.Not(qt.Equals), owner)

	parsed.User = url.UserPassword(owner, connectionPassword)
	devURL := parsed.String()
	root := c.TempDir()
	dir := filepath.Join(root, "migrations")
	c.Assert(os.Mkdir(dir, 0o700), qt.IsNil)
	desired := filepath.Join(root, "schema.sql")
	c.Assert(os.WriteFile(desired, []byte("CREATE TABLE public.notes (id bigint PRIMARY KEY);\n"), 0o600), qt.IsNil)
	args := []string{
		"--dir", "file://" + filepath.ToSlash(dir),
		"--to", "file://" + filepath.ToSlash(desired),
		"--dev-url", devURL,
	}

	out, err := runCompat(c, append([]string{"migrate", "diff", "initial"}, args...)...)

	c.Assert(err, qt.IsNil, qt.Commentf("output:\n%s", out))
	written, err := filepath.Glob(filepath.Join(dir, "*_initial.sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(written, qt.HasLen, 1)
	migration, err := os.ReadFile(written[0])
	c.Assert(err, qt.IsNil)
	c.Assert(string(migration), qt.Contains, `CREATE TABLE "public"."notes"`)

	out, err = runCompat(c, append([]string{"migrate", "diff", "again"}, args...)...)

	c.Assert(err, qt.IsNil, qt.Commentf("output:\n%s", out))
	c.Assert(out, qt.Contains, "The migration directory is synced with the desired state, no changes to be made")
	c.Assert(readPublicSchemaState(c, devAdmin), qt.Equals, claimed)
	var schemas []string
	rows, err := devAdmin.QueryContext(c.Context(), `
		SELECT n.nspname
		FROM pg_namespace n
		WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
		  AND n.nspname NOT LIKE 'pg\_toast%'
		  AND n.nspname NOT LIKE 'pg\_temp\_%'
		ORDER BY n.nspname
	`)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(rows.Close(), qt.IsNil) }()
	for rows.Next() {
		var schema string
		c.Assert(rows.Scan(&schema), qt.IsNil)
		schemas = append(schemas, schema)
	}
	c.Assert(rows.Err(), qt.IsNil)
	c.Assert(schemas, qt.DeepEquals, []string{"public"})
	var relations int
	c.Assert(devAdmin.QueryRowContext(c.Context(), `
		SELECT count(*)
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = 'public'
	`).Scan(&relations), qt.IsNil)
	c.Assert(relations, qt.Equals, 0)
}

// readPublicSchemaState reads schema public of the database db is connected
// to.
func readPublicSchemaState(c *qt.C, db *sql.DB) publicSchemaState {
	c.Helper()
	var state publicSchemaState
	c.Assert(db.QueryRowContext(c.Context(), `
		SELECT
			pg_get_userbyid(n.nspowner),
			array_to_string(ARRAY(SELECT item::text FROM unnest(n.nspacl) item ORDER BY 1), ','),
			COALESCE(obj_description(n.oid, 'pg_namespace'), '')
		FROM pg_namespace n
		WHERE n.nspname = 'public'
	`).Scan(&state.owner, &state.acl, &state.comment), qt.IsNil)
	return state
}
