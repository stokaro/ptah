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
)

// A migration directory that creates its application role in a DO block,
// grants it a schema privilege and defines a function is ordinary, and the
// pinned community binary replays it on any dev database. Ptah replays it on a
// docker:// server it provisioned, whose whole realm is the run's own, and
// still refuses it on a server the operator named, where the role would
// outlive the run: measured, the community binary leaves the role behind on
// such a server.
//
// Only a run that provisions a server can tell the two apart. The guard reads
// what the provisioner recorded about the server, not the operator's spelling
// of the URL, and every consumer has resolved the docker URL to an ordinary one
// before its replay starts.

// devReplayDockerURL names a database no fallback produces, so a run that
// passes can only have replayed on the provisioned server.
const devReplayDockerURL = "docker://postgres/16-alpine/ptahreplay"

// writeServerWideReplayDir writes a hashed Atlas directory whose first
// migration creates role in a DO block. The role name is the caller's, so a
// run against a shared server cannot collide with another test's.
func writeServerWideReplayDir(c *qt.C, role string) (dir, schema string) {
	c.Helper()
	root := c.TempDir()
	dir = filepath.Join(root, "migrations")
	c.Assert(os.MkdirAll(dir, 0o755), qt.IsNil)
	files := map[string]string{
		"20240101000000_roles.sql": fmt.Sprintf(`DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '%[1]s') THEN
    CREATE ROLE %[1]s NOLOGIN NOSUPERUSER NOBYPASSRLS;
  END IF;
END
$$;
GRANT USAGE ON SCHEMA public TO %[1]s;
`, role),
		"20240101000001_orders.sql": fmt.Sprintf(`CREATE TABLE orders (
    id    bigint PRIMARY KEY,
    total integer NOT NULL
);
CREATE FUNCTION order_count() RETURNS bigint LANGUAGE sql STABLE AS $$ SELECT count(*) FROM orders $$;
GRANT SELECT ON orders TO %[1]s;
`, role),
	}
	for name, body := range files {
		c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600), qt.IsNil)
	}
	_, err := runCompatVerb("migrate", "hash", "--dir", "file://"+dir)
	c.Assert(err, qt.IsNil)

	schema = filepath.Join(root, "schema.sql")
	c.Assert(os.WriteFile(schema, []byte(`CREATE TABLE orders (
    id    bigint PRIMARY KEY,
    total integer NOT NULL
);
CREATE FUNCTION order_count() RETURNS bigint LANGUAGE sql STABLE AS $$ SELECT count(*) FROM orders $$;
`), 0o600), qt.IsNil)
	return dir, schema
}

func TestDevDockerReplayRunsServerWideStatements(t *testing.T) {
	c := qt.New(t)
	dir, schema := writeServerWideReplayDir(c, "ptah_replay_app")

	out, err := runCompatVerb("migrate", "validate", "--dir", "file://"+dir, "--dev-url", devReplayDockerURL)
	c.Assert(err, qt.IsNil, qt.Commentf("migrate validate output:\n%s", out))

	out, err = runCompatVerb(
		"migrate", "diff",
		"--dir", "file://"+dir,
		"--to", "file://"+schema,
		"--dev-url", devReplayDockerURL,
	)
	c.Assert(err, qt.IsNil, qt.Commentf("migrate diff output:\n%s", out))
	// The desired file declares what the directory builds, so a diff that
	// replayed the whole directory writes no migration.
	entries, err := os.ReadDir(dir)
	c.Assert(err, qt.IsNil)
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	c.Assert(names, qt.DeepEquals, []string{
		"20240101000000_roles.sql",
		"20240101000001_orders.sql",
		"atlas.sum",
	})

	native := runPtahNative(c,
		"migrations", "validate",
		"--dir", dir,
		"--dir-format", "atlas",
		"--dev-url", devReplayDockerURL,
	)
	c.Assert(native, qt.Contains, "migration SQL validated on dev database")
}

// TestDevReplayOnANamedServerRefusesServerWideStatements is the control: the
// same directory on a server the operator named is refused at its first
// migration, and the role it would have created is not on the server.
func TestDevReplayOnANamedServerRefusesServerWideStatements(t *testing.T) {
	c := qt.New(t)
	role := fmt.Sprintf("ptah_replay_named_%d", time.Now().UnixNano())
	dir, _ := writeServerWideReplayDir(c, role)
	devURL, admin := scratchReplayDatabase(c)

	_, err := runCompatVerb("migrate", "validate", "--dir", "file://"+dir, "--dev-url", devURL)

	c.Assert(err, qt.ErrorMatches, `(?s).*replay migration 20240101000000 on dev database: .*`+
		`postgres migration replay rejects DO sublanguage because its effects cannot be confined to the disposable database realm.*`)
	var roles int
	c.Assert(admin.QueryRowContext(c.Context(),
		"SELECT count(*) FROM pg_roles WHERE rolname = $1", role,
	).Scan(&roles), qt.IsNil)
	c.Assert(roles, qt.Equals, 0)
}

// scratchReplayDatabase creates a database of its own on the PostgreSQL test
// server and returns its URL with an admin connection to the server. A replay
// empties the dev database it is given, so it must not be handed the shared
// one.
func scratchReplayDatabase(c *qt.C) (string, *dbschema.DatabaseConnection) {
	c.Helper()
	adminURL := dbtarget.URL(c, dbtarget.PostgreSQL)
	admin, err := dbschema.ConnectToDatabase(c.Context(), adminURL)
	c.Assert(err, qt.IsNil)
	name := fmt.Sprintf("ptah_replay_%d", time.Now().UnixNano())
	_, err = admin.ExecContext(c.Context(), `CREATE DATABASE "`+name+`"`)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		_, dropErr := admin.ExecContext(context.Background(), `DROP DATABASE IF EXISTS "`+name+`" WITH (FORCE)`)
		c.Check(dropErr, qt.IsNil)
		dbschema.CloseAndWarn(admin)
	})
	parsed, err := url.Parse(adminURL)
	c.Assert(err, qt.IsNil)
	parsed.Path = "/" + name
	parsed.RawPath = ""
	return parsed.String(), admin
}
