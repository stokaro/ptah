//go:build integration

package atlas_test

import (
	"context"
	"net/url"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
)

// migrate diff runs on a dev database that an ordinary role owns and connects
// to. The claim of the dev database lists the database-scoped objects in it,
// user mappings among them; read from the catalog pg_user_mapping, which only a
// superuser may read, the listing refuses every such run with SQLSTATE 42501
// before anything is compared. The dev URL is a realm URL, as a user's sibling
// dev database usually is.
func TestMigrateDiffOnAnOrdinaryRoleDevDatabasePostgresLive(t *testing.T) {
	c := qt.New(t)
	adminURL := dbtarget.URL(c, dbtarget.PostgreSQL)
	suffix := uniqueScopeSuffix()
	owner := "ptah_ordinary_dev_" + suffix
	databaseName := "ptah_ordinary_dev_" + suffix
	connectionPassword := "connection_password_fixture"

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
}
