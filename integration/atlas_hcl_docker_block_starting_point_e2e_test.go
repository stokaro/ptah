//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/devdocker"
)

// A dev database an atlas.hcl docker block provisions starts from the state
// the block's image and baseline leave, in realm scope too (stokaro/ptah#4056).
// The baseline here stands in for an image such as Supabase's: an auth schema
// with a users table, a role, and grants to it. Atlas's community binary
// refuses such a dev database as not clean; with the block, it is the
// environment the run works in.
//
// The migration works inside the starting point: it adds a trigger and a
// column to auth.users and revokes the starting point's grant, after a probe
// that fails unless the grant is there. The claim accepts the dev database
// rather than refusing it, the replay's resets return it to its starting point
// and check that they did, and `migrate diff` and `schema diff` compare what
// the directory built without the starting point the schema file does not
// declare. The writer's live tests pin what a reset puts back.

const startingPointBlockBaseline = `CREATE ROLE ptah_start_reader;
CREATE SCHEMA auth;
CREATE TABLE auth.users (id bigint PRIMARY KEY, email text);
GRANT USAGE ON SCHEMA auth TO ptah_start_reader;
GRANT SELECT ON auth.users TO ptah_start_reader;
`

// startingPointBlockChanges are what the migration and the schema file share.
const startingPointBlockChanges = `CREATE TABLE public.profiles (id bigint PRIMARY KEY);
CREATE FUNCTION public.on_signup() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  INSERT INTO public.profiles (id) VALUES (NEW.id);
  RETURN NEW;
END
$$;
CREATE TRIGGER on_signup AFTER INSERT ON auth.users FOR EACH ROW EXECUTE FUNCTION public.on_signup();
REVOKE SELECT ON auth.users FROM ptah_start_reader;
`

// startingPointBlockMigration reads the starting point's grant back before it
// revokes it, into a column whose CHECK refuses false.
const startingPointBlockMigration = `CREATE TABLE public.granted (ok boolean NOT NULL, CONSTRAINT granted_ok CHECK (ok));
INSERT INTO public.granted (ok) VALUES (has_table_privilege('ptah_start_reader', 'auth.users', 'SELECT'));
ALTER TABLE auth.users ADD COLUMN nickname text;
` + startingPointBlockChanges

// startingPointBlockSchema is the state the migration leaves.
const startingPointBlockSchema = `CREATE TABLE public.granted (ok boolean NOT NULL, CONSTRAINT granted_ok CHECK (ok));
` + startingPointBlockChanges

func TestCompatDockerBlockStartingPointInRealmScopeE2E(t *testing.T) {
	c := qt.New(t)
	c.Assert(devdocker.DockerCLI{}.Available(t.Context()), qt.IsNil)
	dir := c.TempDir()
	migrations := filepath.Join(dir, "migrations")
	c.Assert(os.MkdirAll(migrations, 0o755), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(migrations, "20240101000000_init.sql"),
		[]byte(startingPointBlockMigration), 0o600), qt.IsNil)
	_, err := runCompatVerb("migrate", "hash", "--dir", "file://"+filepath.ToSlash(migrations))
	c.Assert(err, qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "schema.sql"), []byte(startingPointBlockSchema), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "atlas.hcl"), []byte(`docker "postgres" "dev" {
  image    = "postgres:16-alpine"
  baseline = <<-SQL
`+startingPointBlockBaseline+`  SQL
}

env "dev" {
  dev = docker.postgres.dev.url
  migration {
    dir = "file://migrations"
  }
  schema {
    src = "file://schema.sql"
  }
}
`), 0o600), qt.IsNil)
	t.Chdir(dir)

	diff, diffErr := runCompatVerb("migrate", "diff", "--env", "dev", "next")
	schemaDiff, schemaDiffErr := runCompatVerb("schema", "diff", "--env", "dev",
		"--from", "file://migrations", "--to", "file://schema.sql")

	c.Assert(diffErr, qt.IsNil, qt.Commentf("%s", diff))
	c.Assert(diff, qt.Contains, "The migration directory is synced with the desired state, no changes to be made")
	c.Assert(schemaDiffErr, qt.IsNil, qt.Commentf("%s", schemaDiff))
	c.Assert(schemaDiff, qt.Contains, "Schemas are synced, no changes to be made")
	entries, err := os.ReadDir(migrations)
	c.Assert(err, qt.IsNil)
	c.Assert(entries, qt.HasLen, 2)
}
