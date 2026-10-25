package migratetests_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

// These tests pin stokaro/ptah#4953 on `ptah-compat migrate lint`. TX201
// reports a BEGIN/COMMIT block that a migrator wrapping each file cannot nest.
// golang-migrate opens no transaction around a file, so in its layout the
// block is the file's only transaction and is not reported, whichever spelling
// selects the layout. The same block in Atlas's layout, which Atlas wraps per
// file, is still reported.
//
// The fixtures have OpenMeter's shape: fourteen-digit versions, and a
// hand-written migration that wraps its statements in BEGIN/COMMIT. The runs
// are static (PTAH_ATLAS_LINT_WITHOUT_DEV_URL), because the subject is the
// verdict over the text.

// transactionBlock is a migration that opens and commits its own transaction.
const transactionBlock = "BEGIN;\nCREATE TABLE t1 (id int);\nINSERT INTO t1 VALUES (1);\nCOMMIT;\n"

// transactionMix is a migration whose second statement PostgreSQL refuses
// inside any transaction block, the implicit one included.
const transactionMix = "CREATE TABLE t1 (id int);\nCREATE INDEX CONCURRENTLY t1_id_idx ON t1 (id);\n"

// writeTransactionProject writes files under <root>/migrations, hashes the
// directory as layout through the shipped `migrate hash`, writes project as
// <root>/atlas.hcl, and returns root.
func writeTransactionProject(c *qt.C, layout, project string, files map[string]string) string {
	c.Helper()
	root := c.TempDir()
	dir := filepath.Join(root, "migrations")
	for name, body := range files {
		writeAtlasLintFile(c, dir, name, body)
	}
	_, stderr, err := runCompatExit("migrate", "hash", "--dir", "file://"+dir+"?format="+layout)
	c.Assert(err, qt.IsNil, qt.Commentf("hash stderr: %s", stderr))
	c.Assert(os.WriteFile(filepath.Join(root, "atlas.hcl"), []byte(project), 0o600), qt.IsNil)
	return root
}

// golangMigrateFiles is a golang-migrate directory whose latest migration
// holds body.
func golangMigrateFiles(body string) map[string]string {
	return map[string]string{
		"20240826120919_init.up.sql":       "CREATE TABLE base (id int);\n",
		"20240826120919_init.down.sql":     "DROP TABLE base;\n",
		"20240827000000_backfill.up.sql":   body,
		"20240827000000_backfill.down.sql": "BEGIN;\nDROP TABLE t1;\nCOMMIT;\n",
	}
}

// golangMigrateProject is an atlas.hcl that selects the layout by its own
// key, the way OpenMeter's does.
const golangMigrateProject = `env "ci" {
  migration {
    dir    = "file://migrations"
    format = golang-migrate
  }
}
`

// runTransactionLint lints the latest migration of the project at root
// without a dev database.
func runTransactionLint(c *qt.C, root string, args ...string) (stdout, stderr string, err error) {
	c.Helper()
	c.Setenv("PTAH_ATLAS_LINT_WITHOUT_DEV_URL", "1")
	c.Chdir(root)
	return runCompatExit(append(append([]string{"migrate", "lint"}, args...), "--latest", "1")...)
}

func TestCompatMigrateLint_GolangMigrateTransactionBlockIsNotReported(t *testing.T) {
	for _, test := range []struct {
		name string
		args []string
	}{
		{"the ?format= query", []string{"--dir", "file://migrations?format=golang-migrate"}},
		{"the --dir-format flag", []string{"--dir", "file://migrations", "--dir-format", "golang-migrate"}},
		{"migration.format in atlas.hcl", []string{"--env", "ci"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			root := writeTransactionProject(c, "golang-migrate", golangMigrateProject, golangMigrateFiles(transactionBlock))

			stdout, stderr, err := runTransactionLint(c, root, test.args...)

			c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
			c.Assert(stdout, qt.Contains, "-- analyzing version 20240827000000")
			c.Assert(stdout, qt.Not(qt.Contains), "TX201")
		})
	}
}

// The control that the layout is not read as a file outside a transaction:
// golang-migrate sends the file as one query by default, and PostgreSQL's
// implicit transaction refuses the concurrent index beside another statement,
// so TX101 still holds.
func TestCompatMigrateLint_GolangMigrateTransactionMixIsReported(t *testing.T) {
	c := qt.New(t)
	root := writeTransactionProject(c, "golang-migrate", golangMigrateProject, golangMigrateFiles(transactionMix))

	stdout, stderr, err := runTransactionLint(c, root, "--env", "ci")

	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	c.Assert(stdout, qt.Contains, "[TX101]")
}

func TestCompatMigrateLint_AtlasTransactionBlockIsReported(t *testing.T) {
	c := qt.New(t)
	root := writeTransactionProject(c, "atlas", "", map[string]string{
		"20240826120919_init.sql":     "CREATE TABLE base (id int);\n",
		"20240827000000_backfill.sql": transactionBlock,
	})

	stdout, stderr, err := runTransactionLint(c, root, "--dir", "file://migrations")

	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	c.Assert(stdout, qt.Contains, "-- analyzing version 20240827000000")
	c.Assert(stdout, qt.Contains, "[TX201]")
}
