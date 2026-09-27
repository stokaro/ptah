//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
)

// uniqueKeysQuery lists every UNIQUE constraint of the public schema as
// `<table>.<name>: <definition>`, sorted.
const uniqueKeysQuery = `SELECT conrelid::regclass::text || '.' || conname || ': ' || pg_get_constraintdef(oid)
FROM pg_constraint WHERE contype = 'u' AND connamespace = 'public'::regnamespace ORDER BY 1`

// uniqueKeysOf reads the UNIQUE constraints of the database at databaseURL; see
// [uniqueKeysQuery].
func uniqueKeysOf(c *qt.C, databaseURL string) []string {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), databaseURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	rows, err := conn.QueryContext(c.Context(), uniqueKeysQuery)
	c.Assert(err, qt.IsNil)
	defer rows.Close()
	var keys []string
	for rows.Next() {
		var key string
		c.Assert(rows.Scan(&key), qt.IsNil)
		keys = append(keys, key)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return keys
}

// columnKeyBesideAnIndex is a table whose column x has no key yet, beside a
// unique index over y that holds c_x_key, the name the server tries first for
// x's own UNIQUE.
const columnKeyBesideAnIndex = `CREATE TABLE c (id int PRIMARY KEY, y int);
CREATE UNIQUE INDEX c_x_key ON c (y);
ALTER TABLE c ADD COLUMN x int;`

// columnKeyBesideAnIndexDeclared is the same table with x UNIQUE. PostgreSQL
// 18.6 names x's key c_x_key1, because the index holds c_x_key.
const columnKeyBesideAnIndexDeclared = `CREATE TABLE c (id int PRIMARY KEY, y int);
CREATE UNIQUE INDEX c_x_key ON c (y);
ALTER TABLE c ADD COLUMN x int UNIQUE;`

// TestSchemaApplyGivesAColumnTheKeyNameAnIndexLeavesE2E applies a YAML schema
// that makes x UNIQUE to a database where an index the plan keeps holds the
// name the server tries first for x's key. The plan adds the key under the
// next name; written under the first, the statement is refused with `relation
// "c_x_key" already exists` (stokaro/ptah#3859). The keys read back are
// compared with a database the equivalent SQL built.
func TestSchemaApplyGivesAColumnTheKeyNameAnIndexLeavesE2E(t *testing.T) {
	c := qt.New(t)
	want := uniqueKeysOf(c, databaseBuiltFrom(c, columnKeyBesideAnIndexDeclared))
	target := databaseBuiltFrom(c, columnKeyBesideAnIndex)
	schema := filepath.Join(c.TempDir(), "schema.yaml")
	c.Assert(os.WriteFile(schema, []byte(`tables:
  c:
    columns:
      id:
        type: INTEGER
        primary: true
      y:
        type: INTEGER
      x:
        type: INTEGER
        unique: true
    indexes:
      c_x_key:
        fields: [y]
        unique: true
`), 0o600), qt.IsNil)

	runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--auto-approve")

	c.Assert(uniqueKeysOf(c, target), qt.DeepEquals, want)
	out := runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--dry-run")
	c.Assert(out, qt.Contains, "Schema is synced")
}

// TestMigrateDiffGivesAColumnTheKeyNameAnIndexLeavesE2E writes the migration
// that makes x UNIQUE beside the index, as Atlas CE v1.3.0 writes it, and
// diffs the directory again: the directory replays on the dev database with
// the new migration in it, and is synced with the schema file
// (stokaro/ptah#3859).
func TestMigrateDiffGivesAColumnTheKeyNameAnIndexLeavesE2E(t *testing.T) {
	c := qt.New(t)
	dir, schema := writeRewrittenMigrationProject(c, columnKeyBesideAnIndex, columnKeyBesideAnIndexDeclared)
	dev, _ := scratchReplayDatabase(c)

	out, err := runCompatVerb("migrate", "diff", "add", "--dir", "file://"+dir, "--to", "file://"+schema, "--dev-url", dev)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	written, err := filepath.Glob(filepath.Join(dir, "*_add.sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(written, qt.HasLen, 1)
	migration, err := os.ReadFile(written[0])
	c.Assert(err, qt.IsNil)
	c.Assert(string(migration), qt.Contains, `ADD CONSTRAINT "c_x_key1" UNIQUE ("x")`)

	again, err := runCompatVerb("migrate", "diff", "--dir", "file://"+dir, "--to", "file://"+schema, "--dev-url", dev, "--dry-run")

	c.Assert(err, qt.IsNil, qt.Commentf("%s", again))
	c.Assert(again, qt.Contains, "The migration directory is synced with the desired state")
}
