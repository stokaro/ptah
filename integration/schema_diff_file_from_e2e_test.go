//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

// `schema diff` with a schema file on --from compares against a --to the
// server wrote: a database the same SQL built, or a migration directory holding
// it. The file is created on the dev database and read back, so both sides hold
// the server's spelling of each declaration in [serverRewrittenDeclarations]
// and of each key column's NOT NULL. Compared as written, the defaults, CHECKs
// and policies were planned again and every key column was given SET NOT NULL
// (stokaro/ptah#3658). Atlas CE reports each of these diffs synced.

// TestSchemaDiffFromAFileToItsDatabaseIsSyncedE2E diffs the schema file against
// the database its SQL built, with a dev database.
func TestSchemaDiffFromAFileToItsDatabaseIsSyncedE2E(t *testing.T) {
	for _, test := range serverRewrittenDeclarations {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			_, schema := writeRewrittenMigrationProject(c, test.sql, test.sql)
			target := databaseBuiltFrom(c, test.sql)
			dev, _ := scratchReplayDatabase(c)

			out := runPtahNative(c, "schema", "diff", "--from", "file://"+schema, "--to", target, "--dev-url", dev)

			c.Assert(out, qt.Contains, "Schemas are synced")
		})
	}
}

// TestSchemaDiffFromAFileToItsDirectoryIsSyncedE2E diffs the schema file
// against a migration directory whose one migration is the same SQL.
func TestSchemaDiffFromAFileToItsDirectoryIsSyncedE2E(t *testing.T) {
	for _, test := range serverRewrittenDeclarations {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir, schema := writeRewrittenMigrationProject(c, test.sql, test.sql)
			dev, _ := scratchReplayDatabase(c)

			out, err := runCompatVerb("schema", "diff", "--from", "file://"+schema,
				"--to", "file://"+dir, "--dev-url", dev)

			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			c.Assert(out, qt.Contains, "Schemas are synced")
		})
	}
}

// TestSchemaDiffFromAFileToItselfIsSyncedE2E diffs the schema file against
// itself. Two documents are compared as written, and a key column is NOT NULL
// on both.
func TestSchemaDiffFromAFileToItselfIsSyncedE2E(t *testing.T) {
	for _, test := range serverRewrittenDeclarations {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			_, schema := writeRewrittenMigrationProject(c, test.sql, test.sql)
			dev, _ := scratchReplayDatabase(c)

			out, err := runCompatVerb("schema", "diff", "--from", "file://"+schema,
				"--to", "file://"+schema, "--dev-url", dev)

			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			c.Assert(out, qt.Contains, "Schemas are synced")
		})
	}
}

// TestSchemaDiffFromAFileToAChangedDatabasePlansTheChangeE2E is the control:
// a declaration the database holds differently is planned.
func TestSchemaDiffFromAFileToAChangedDatabasePlansTheChangeE2E(t *testing.T) {
	for _, test := range serverRewrittenControls {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			_, schema := writeRewrittenMigrationProject(c, test.migration, test.migration)
			target := databaseBuiltFrom(c, test.schema)
			dev, _ := scratchReplayDatabase(c)

			out := runPtahNative(c, "schema", "diff", "--from", "file://"+schema, "--to", target, "--dev-url", dev)

			c.Assert(out, qt.Contains, test.wantInPlan)
		})
	}
}

// keyOnlySchema declares key columns and nothing the server rewrites.
const keyOnlySchema = `CREATE TABLE tenants (id bigint PRIMARY KEY);
CREATE TABLE memberships (tenant_id bigint, user_id bigint, PRIMARY KEY (tenant_id, user_id));`

// TestSchemaDiffFromAFileToItsDatabaseWithoutADevDatabaseKeepsKeysE2E diffs
// without a dev database, where the file is compared as written: its key
// columns are still NOT NULL, as the database holds them, so no key column is
// given SET NOT NULL. Other differences between a file and a server's catalog
// -- a grant the server makes by default -- remain on this path and are not
// asserted here.
func TestSchemaDiffFromAFileToItsDatabaseWithoutADevDatabaseKeepsKeysE2E(t *testing.T) {
	c := qt.New(t)
	_, schema := writeRewrittenMigrationProject(c, keyOnlySchema, keyOnlySchema)
	target := databaseBuiltFrom(c, keyOnlySchema)

	out := runPtahNative(c, "schema", "diff", "--from", "file://"+schema, "--to", target)

	c.Assert(out, qt.Not(qt.Contains), "SET NOT NULL")
	c.Assert(out, qt.Not(qt.Contains), "nullable: true -> false")
}

// writeFileIn writes body to name inside dir and returns the path.
func writeFileIn(c *qt.C, dir, name, body string) string {
	c.Helper()
	path := filepath.Join(dir, name)
	c.Assert(os.WriteFile(path, []byte(body), 0o600), qt.IsNil)
	return path
}

// notDescribedExtensions is a --from document that records it describes no
// extensions, and a table.
const notDescribedExtensions = `// ptah:not-described extension
table "notes" {
  schema = schema.public
  column "id" {
    type = bigint
  }
  primary_key {
    columns = [column.id]
  }
}
schema "public" {}
`

// TestSchemaDiffFromAFileKeepsItsCoverageRecordE2E: a --from document that
// records `ptah:not-described extension` makes no claim about extensions, and
// the materialized read-back keeps that record. An extension the --to database
// holds is reported as undecided rather than planned.
func TestSchemaDiffFromAFileKeepsItsCoverageRecordE2E(t *testing.T) {
	c := qt.New(t)
	root := c.TempDir()
	from := writeFileIn(c, root, "from.hcl", notDescribedExtensions)
	target := databaseBuiltFrom(c, "CREATE EXTENSION IF NOT EXISTS citext; CREATE TABLE notes (id bigint PRIMARY KEY);")
	dev, _ := scratchReplayDatabase(c)

	out := runPtahNative(c, "schema", "diff", "--from", "file://"+from, "--to", target, "--dev-url", dev)

	c.Assert(out, qt.Contains, "Schemas are synced")
	c.Assert(out, qt.Contains, `extension "citext" is declared by --to but no change was planned for it`)
}
