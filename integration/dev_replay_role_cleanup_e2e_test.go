//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
	"ptah.run/internal/devdocker"
	"ptah.run/internal/envbool/envbooltest"
)

// A replay on a server the operator declared disposable runs CREATE ROLE, and
// a role is the server's rather than the dev database's. The reset after a
// replay drops the roles the server did not hold when the run claimed its dev
// database, and keeps every role it did hold. Without the drop, the server,
// which outlives the run, holds the role for the next replay of the same
// directory: measured on PostgreSQL 18.6, the second `migrate diff` fails with
// `role "app_reader" already exists (SQLSTATE 42710)`.

// roleReplayFixture is a hashed directory whose first migration creates a
// role and grants it privileges, a schema file the directory builds, and a
// scratch dev database on the PostgreSQL test server.
type roleReplayFixture struct {
	dir, schema, devURL string
	admin               *dbschema.DatabaseConnection
}

// newRoleReplayFixture writes the directory for role, whose first migration
// begins with create: the CREATE ROLE, or nothing for a role the caller
// creates before the run. The role is dropped after the scratch database,
// whatever the run left.
func newRoleReplayFixture(c *qt.C, role, create string) roleReplayFixture {
	c.Helper()
	admin, err := dbschema.ConnectToDatabase(c.Context(), dbtarget.URL(c, dbtarget.PostgreSQL))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		_, dropErr := admin.ExecContext(context.Background(), `DROP ROLE IF EXISTS "`+role+`"`)
		c.Check(dropErr, qt.IsNil)
		dbschema.CloseAndWarn(admin)
	})
	root := c.TempDir()
	dir := filepath.Join(root, "migrations")
	c.Assert(os.MkdirAll(dir, 0o755), qt.IsNil)
	first := create + fmt.Sprintf("CREATE TABLE t1 (id int);\nGRANT SELECT ON t1 TO %[1]s;\nGRANT USAGE ON SCHEMA public TO %[1]s;\n", role)
	c.Assert(os.WriteFile(filepath.Join(dir, "20260101000000_init.sql"), []byte(first), 0o600), qt.IsNil)
	out, err := runCompatVerb("migrate", "hash", "--dir", "file://"+dir)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	schema := filepath.Join(root, "schema.sql")
	c.Assert(os.WriteFile(schema, []byte("CREATE TABLE t1 (id int);\n"), 0o600), qt.IsNil)
	devURL, _ := scratchReplayDatabase(c)
	envbooltest.Set(devdocker.DisposableServerEnvVar, "1")(c)
	return roleReplayFixture{dir: dir, schema: schema, devURL: devURL, admin: admin}
}

// diff adds table to the schema file and runs `migrate diff` named after it.
func (f roleReplayFixture) diff(c *qt.C, table string) {
	c.Helper()
	schema, err := os.OpenFile(f.schema, os.O_APPEND|os.O_WRONLY, 0)
	c.Assert(err, qt.IsNil)
	_, err = fmt.Fprintf(schema, "CREATE TABLE %s (id int);\n", table)
	c.Assert(err, qt.IsNil)
	c.Assert(schema.Close(), qt.IsNil)

	out, err := runCompatVerb("migrate", "diff", "--dir", "file://"+f.dir, "--to", "file://"+f.schema, "--dev-url", f.devURL, table)

	c.Assert(err, qt.IsNil, qt.Commentf("migrate diff %s:\n%s", table, out))
}

// roles counts the roles on the server named role.
func (f roleReplayFixture) roles(c *qt.C, role string) int {
	c.Helper()
	var count int
	c.Assert(f.admin.QueryRowContext(c.Context(), "SELECT count(*) FROM pg_roles WHERE rolname = $1", role).Scan(&count), qt.IsNil)
	return count
}

// TestDevReplayRoleCleanupE2E_RepeatedDiffsOnADeclaredServer runs `migrate
// diff` three times over a directory that creates a role, each time with one
// more table, on one dev database of a server declared disposable. Each run
// writes its migration, and none leaves the role on the server.
func TestDevReplayRoleCleanupE2E_RepeatedDiffsOnADeclaredServer(t *testing.T) {
	c := qt.New(t)
	role := fmt.Sprintf("ptah_replay_role_%d", time.Now().UnixNano())
	fx := newRoleReplayFixture(c, role, "CREATE ROLE "+role+";\n")

	for _, table := range []string{"t2", "t3", "t4"} {
		fx.diff(c, table)

		c.Assert(fx.roles(c, role), qt.Equals, 0, qt.Commentf("after the diff that adds %s", table))
	}

	entries, err := os.ReadDir(fx.dir)
	c.Assert(err, qt.IsNil)
	c.Assert(entries, qt.HasLen, 5, qt.Commentf("the first migration, three diffs and atlas.sum"))
}

// TestDevReplayRoleCleanupE2E_KeepsARoleTheServerHeld is the control: a role
// the server held when the run claimed its dev database stays, though the
// replay granted it privileges, since the run did not create it.
func TestDevReplayRoleCleanupE2E_KeepsARoleTheServerHeld(t *testing.T) {
	c := qt.New(t)
	role := fmt.Sprintf("ptah_replay_kept_%d", time.Now().UnixNano())
	fx := newRoleReplayFixture(c, role, "")
	_, err := fx.admin.ExecContext(c.Context(), `CREATE ROLE "`+role+`" NOLOGIN`)
	c.Assert(err, qt.IsNil)

	fx.diff(c, "t2")

	c.Assert(fx.roles(c, role), qt.Equals, 1)
}
