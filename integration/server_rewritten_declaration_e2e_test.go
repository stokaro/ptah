//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
)

// The server stores a rewrite of each declaration below rather than the text
// it was given: a CHECK and a policy clause come back from their parse tree
// with parentheses and casts, an index predicate the same, a column default
// with its casts spelled out, and an unnamed CHECK, UNIQUE or foreign key under
// the name the server chose. A schema file identical to the migration that
// built the database compares unequal to it unless the comparison asks the
// server and names each unnamed constraint as the server does; without that,
// `migrate diff` and `schema apply` drop and recreate every one of them, or add
// a second foreign key beside the first (stokaro/ptah#3643,
// stokaro/ptah#3729).
var serverRewrittenDeclarations = []struct {
	name string
	sql  string
}{
	{
		name: "a CHECK over two comparisons",
		sql: `CREATE TABLE notes (id bigint PRIMARY KEY, owner text NOT NULL, n integer NOT NULL,
  CONSTRAINT notes_n_check CHECK (n > 0 AND owner <> ''));`,
	},
	{
		name: "column-level CHECKs, one named and one not",
		sql: `CREATE TABLE notes (id bigint PRIMARY KEY,
  kind text NOT NULL CONSTRAINT notes_kind_check CHECK (kind IN ('plugin')),
  n integer CHECK (n BETWEEN 1 AND 10));`,
	},
	{
		name: "a policy naming current_user",
		sql: `CREATE TABLE notes (id bigint PRIMARY KEY, owner text NOT NULL);
ALTER TABLE notes ENABLE ROW LEVEL SECURITY;
CREATE POLICY notes_owner ON notes USING (owner = current_user);`,
	},
	{
		name: "a policy with a cast and a setting",
		sql: `CREATE TABLE notes (id bigint PRIMARY KEY, owner text NOT NULL);
ALTER TABLE notes ENABLE ROW LEVEL SECURITY;
CREATE POLICY notes_agent ON notes
  USING (current_setting('app.agent', true) = 'on')
  WITH CHECK (id = nullif(current_setting('app.id', true), '')::bigint);`,
	},
	{
		// Each expression names its own table. The server resolves the name
		// against the table the object is on and stores it unqualified, so
		// the probe has to create its table under the same name
		// (stokaro/ptah#3654).
		name: "a CHECK, an index predicate and a policy naming their own table",
		sql: `CREATE TABLE clients (id bigint PRIMARY KEY, parent_id bigint, n integer NOT NULL,
  CONSTRAINT clients_n_check CHECK (clients.n > 0));
CREATE INDEX clients_parent_idx ON clients (parent_id) WHERE clients.parent_id IS NOT NULL;
ALTER TABLE clients ENABLE ROW LEVEL SECURITY;
CREATE POLICY clients_child_read ON clients
  USING (EXISTS (SELECT 1 FROM clients c2 WHERE c2.parent_id = clients.id));`,
	},
	{
		// The probe table takes the real table's name, and a subquery naming
		// the real table through its schema must still read the real table:
		// the server prints it unqualified, and so must the probe.
		name: "a policy reading its own table through the schema",
		sql: `CREATE TABLE clients (id bigint PRIMARY KEY, parent_id bigint);
ALTER TABLE clients ENABLE ROW LEVEL SECURITY;
CREATE POLICY clients_parent_read ON clients
  USING (EXISTS (SELECT 1 FROM public.clients c2 WHERE c2.parent_id = clients.id));`,
	},
	{
		name: "a partial index predicate",
		sql: `CREATE TABLE sites (id bigint PRIMARY KEY, owner text NOT NULL, is_default boolean NOT NULL DEFAULT false, site_id bigint);
CREATE UNIQUE INDEX sites_default_idx ON sites (owner) WHERE is_default = true AND site_id IS NOT NULL;`,
	},
	{
		name: "column defaults the server casts",
		sql: `CREATE TABLE members (id bigint PRIMARY KEY,
  roles text[] NOT NULL DEFAULT ARRAY['administrator'],
  expires_at timestamptz NOT NULL DEFAULT now() + interval '24 hours',
  secret bytea NOT NULL DEFAULT ''::bytea);`,
	},
	{
		// PostgreSQL 18.6 names these a_plan_check, b_plan_check, c_check,
		// c_lo_check, d_lo_check, d_check, d_hi_check, e_check, h2_a_check and
		// h2_a_check1.
		name: "unnamed CHECKs, on a column and on the table",
		sql: `CREATE TABLE a (plan text, CHECK (plan IN ('x','y')));
CREATE TABLE b (plan text CHECK (plan IN ('x','y')));
CREATE TABLE c (lo int, hi int, CHECK (lo < hi));
ALTER TABLE c ADD CHECK (lo > 0);
CREATE TABLE d (lo int CHECK (lo > 0), hi int, CHECK (lo < hi), CHECK (hi > 0));
CREATE TABLE e (a int CHECK (a IS NULL OR b IS NOT NULL), b int);
CREATE TABLE h2 (a int CHECK (a > 0) CHECK (a < 10));`,
	},
	{
		name: "an unnamed inline foreign key and table-level UNIQUE",
		sql: `CREATE TABLE tenants (id bigint PRIMARY KEY);
CREATE TABLE keys (id bigint PRIMARY KEY, tenant_id bigint NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  code text, scope text, UNIQUE (code, scope));`,
	},
}

// serverRewrittenControls change one declaration of each family for real, so
// a comparison that stopped seeing differences at all would still pass the
// rows above and fail here.
var serverRewrittenControls = []struct {
	name       string
	migration  string
	schema     string
	wantInPlan string
}{
	{
		name:       "a CHECK bound moves",
		migration:  `CREATE TABLE notes (id bigint PRIMARY KEY, n integer NOT NULL, CONSTRAINT notes_n_check CHECK (n > 0));`,
		schema:     `CREATE TABLE notes (id bigint PRIMARY KEY, n integer NOT NULL, CONSTRAINT notes_n_check CHECK (n > 1));`,
		wantInPlan: "n > 1",
	},
	{
		name:       "an unnamed CHECK bound moves",
		migration:  `CREATE TABLE c (lo int, hi int, CHECK (lo < hi));`,
		schema:     `CREATE TABLE c (lo int, hi int, CHECK (lo <= hi));`,
		wantInPlan: "lo <= hi",
	},
	{
		name: "a policy names another setting",
		migration: `CREATE TABLE notes (id bigint PRIMARY KEY);
ALTER TABLE notes ENABLE ROW LEVEL SECURITY;
CREATE POLICY notes_agent ON notes USING (current_setting('app.agent', true) = 'on');`,
		schema: `CREATE TABLE notes (id bigint PRIMARY KEY);
ALTER TABLE notes ENABLE ROW LEVEL SECURITY;
CREATE POLICY notes_agent ON notes USING (current_setting('app.robot', true) = 'on');`,
		wantInPlan: "app.robot",
	},
	{
		name: "an index predicate loses a term",
		migration: `CREATE TABLE sites (id bigint PRIMARY KEY, owner text NOT NULL, is_default boolean NOT NULL, site_id bigint);
CREATE UNIQUE INDEX sites_default_idx ON sites (owner) WHERE is_default = true AND site_id IS NOT NULL;`,
		schema: `CREATE TABLE sites (id bigint PRIMARY KEY, owner text NOT NULL, is_default boolean NOT NULL, site_id bigint);
CREATE UNIQUE INDEX sites_default_idx ON sites (owner) WHERE is_default = true;`,
		wantInPlan: "sites_default_idx",
	},
	{
		name:       "a default interval grows",
		migration:  `CREATE TABLE members (id bigint PRIMARY KEY, expires_at timestamptz DEFAULT now() + interval '24 hours');`,
		schema:     `CREATE TABLE members (id bigint PRIMARY KEY, expires_at timestamptz DEFAULT now() + interval '48 hours');`,
		wantInPlan: "48",
	},
}

// writeRewrittenMigrationProject writes a hashed Atlas directory holding one
// migration and a schema file beside it, and returns both paths.
func writeRewrittenMigrationProject(c *qt.C, migration, schema string) (dir, schemaPath string) {
	c.Helper()
	root := c.TempDir()
	dir = filepath.Join(root, "migrations")
	c.Assert(os.MkdirAll(dir, 0o755), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "20260101000000_init.sql"), []byte(migration+"\n"), 0o600), qt.IsNil)
	schemaPath = filepath.Join(root, "schema.sql")
	c.Assert(os.WriteFile(schemaPath, []byte(schema+"\n"), 0o600), qt.IsNil)
	out, err := runCompatVerb("migrate", "hash", "--dir", "file://"+dir)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	return dir, schemaPath
}

// databaseBuiltFrom returns a scratch database the SQL was run on.
func databaseBuiltFrom(c *qt.C, sql string) string {
	c.Helper()
	target, _ := scratchReplayDatabase(c)
	conn, err := dbschema.ConnectToDatabase(c.Context(), target)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	_, err = conn.ExecContext(c.Context(), sql)
	c.Assert(err, qt.IsNil)
	return target
}

// TestMigrateDiffFindsAServerRewrittenDeclarationSyncedE2E replays a directory
// whose one migration is byte-identical to the schema file, and diffs: the
// directory is synced.
func TestMigrateDiffFindsAServerRewrittenDeclarationSyncedE2E(t *testing.T) {
	for _, test := range serverRewrittenDeclarations {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir, schema := writeRewrittenMigrationProject(c, test.sql, test.sql)
			dev, _ := scratchReplayDatabase(c)

			out, err := runCompatVerb("migrate", "diff", "--dir", "file://"+dir,
				"--to", "file://"+schema, "--dev-url", dev, "--dry-run")

			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			c.Assert(out, qt.Contains, "The migration directory is synced with the desired state")
		})
	}
}

// TestSchemaApplyFindsAServerRewrittenDeclarationSyncedE2E plans the native
// apply of a schema file against the database the same SQL built: nothing is
// planned.
func TestSchemaApplyFindsAServerRewrittenDeclarationSyncedE2E(t *testing.T) {
	for _, test := range serverRewrittenDeclarations {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			_, schema := writeRewrittenMigrationProject(c, test.sql, test.sql)
			target := databaseBuiltFrom(c, test.sql)

			out := runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--dry-run")

			c.Assert(out, qt.Contains, "Schema is synced")
		})
	}
}

// TestMigrationsGenerateReplayFindsAServerRewrittenDeclarationSyncedE2E is the
// native counterpart of the migrate diff rows: the directory is replayed on the
// dev database and compared with the schema file, and no migration is written.
func TestMigrationsGenerateReplayFindsAServerRewrittenDeclarationSyncedE2E(t *testing.T) {
	for _, test := range serverRewrittenDeclarations {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir, schema := writeRewrittenMigrationProject(c, test.sql, test.sql)
			dev, _ := scratchReplayDatabase(c)

			out := runPtahNative(c, "migrations", "generate", "--schema-file", schema,
				"--migrations-dir", dir, "--replay", "--dev-url", dev, "--dir-format", "atlas")

			c.Assert(out, qt.Contains, "no migration files generated")
			entries, err := os.ReadDir(dir)
			c.Assert(err, qt.IsNil)
			c.Assert(entries, qt.HasLen, 2)
		})
	}
}

// TestMigrateDiffPlansAServerRewrittenDeclarationThatChangedE2E is the control
// for the migrate diff rows: a declaration that really changed is planned.
func TestMigrateDiffPlansAServerRewrittenDeclarationThatChangedE2E(t *testing.T) {
	for _, test := range serverRewrittenControls {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir, schema := writeRewrittenMigrationProject(c, test.migration, test.schema)
			dev, _ := scratchReplayDatabase(c)

			out, err := runCompatVerb("migrate", "diff", "--dir", "file://"+dir,
				"--to", "file://"+schema, "--dev-url", dev, "--dry-run")

			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			c.Assert(out, qt.Contains, test.wantInPlan)
		})
	}
}

// TestSchemaApplyPlansAServerRewrittenDeclarationThatChangedE2E is the control
// for the schema apply rows.
func TestSchemaApplyPlansAServerRewrittenDeclarationThatChangedE2E(t *testing.T) {
	for _, test := range serverRewrittenControls {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			_, schema := writeRewrittenMigrationProject(c, test.migration, test.schema)
			target := databaseBuiltFrom(c, test.migration)

			out := runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--dry-run")

			c.Assert(out, qt.Contains, test.wantInPlan)
		})
	}
}
