//go:build integration

package atlas_test

import (
	"bytes"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/cli/atlas"
	"ptah.run/internal/dbtarget"
)

// The commands that replay a golang-migrate directory on a dev database read a
// file the way `migrate apply` runs it (stokaro/ptah#4962). A file that holds
// one CREATE INDEX CONCURRENTLY runs without a transaction there, so lint does
// not report PG103 and validate and diff replay it. A file that opens a
// transaction inside its own one is refused for the same reason by apply and
// by the replay lint and validate run before they judge anything.

// runCompat runs the compat command with args and returns its output with
// whitespace folded, and its error.
func runCompat(c *qt.C, args ...string) (string, error) {
	c.Helper()
	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs(args)
	err := cmd.Execute()
	return strings.Join(strings.Fields(out.String()), " "), err
}

// loneConcurrentIndexDirectory is a golang-migrate directory whose latest
// migration holds one concurrent build.
var loneConcurrentIndexDirectory = withGolangMigrateBase(map[string]string{
	"20240829000000_idx.up.sql":   "CREATE INDEX CONCURRENTLY base_id_idx ON base (id);\n",
	"20240829000000_idx.down.sql": "DROP INDEX CONCURRENTLY base_id_idx;\n",
})

func TestGolangMigrateLoneConcurrentIndexDevReplayLive(t *testing.T) {
	adminURL := dbtarget.URL(t, dbtarget.PostgreSQL)

	t.Run("lint", func(t *testing.T) {
		c := qt.New(t)
		dir := writeHashedLayoutDirectory(c, "golang-migrate", loneConcurrentIndexDirectory)

		out, err := runCompat(c, "migrate", "lint", "--dir", "file://"+dir+"?format=golang-migrate",
			"--dev-url", newCleanGateDatabase(c, adminURL, nil), "--latest", "1")

		c.Assert(err, qt.IsNil, qt.Commentf("output:\n%s", out))
		c.Assert(out, qt.Contains, "-- analyzing version 20240829000000 -- no diagnostics found")
		c.Assert(out, qt.Not(qt.Contains), "PG103")
	})

	t.Run("validate", func(t *testing.T) {
		c := qt.New(t)
		dir := writeHashedLayoutDirectory(c, "golang-migrate", loneConcurrentIndexDirectory)

		out, err := runCompat(c, "migrate", "validate", "--dir", "file://"+dir+"?format=golang-migrate",
			"--dev-url", newCleanGateDatabase(c, adminURL, nil))

		c.Assert(err, qt.IsNil, qt.Commentf("output:\n%s", out))
	})

	t.Run("diff", func(t *testing.T) {
		c := qt.New(t)
		dir := writeHashedLayoutDirectory(c, "golang-migrate", loneConcurrentIndexDirectory)
		schema := filepath.Join(c.TempDir(), "schema.sql")
		c.Assert(os.WriteFile(schema, []byte("CREATE TABLE base (id int);\nCREATE INDEX base_id_idx ON base (id);\n"), 0o600), qt.IsNil)

		out, err := runCompat(c, "migrate", "diff", "--dir", "file://"+dir+"?format=golang-migrate",
			"--to", "file://"+schema, "--dev-url", newCleanGateDatabase(c, adminURL, nil))

		c.Assert(err, qt.IsNil, qt.Commentf("output:\n%s", out))
		c.Assert(out, qt.Contains, "The migration directory is synced with the desired state, no changes to be made")
	})
}

func TestGolangMigrateNestedTransactionDevReplayLive(t *testing.T) {
	adminURL := dbtarget.URL(t, dbtarget.PostgreSQL)
	files := withGolangMigrateBase(map[string]string{
		"20240827000000_nested.up.sql":   "BEGIN;\nCREATE TABLE t1 (id int);\nBEGIN;\nINSERT INTO t1 VALUES (1);\nCOMMIT;\n",
		"20240827000000_nested.down.sql": "DROP TABLE t1;\n",
	})

	const refusal = `(?s).*at statement 3: a transaction is already open since statement 1.*`
	for _, command := range [][]string{
		{"migrate", "lint", "--latest", "1"},
		{"migrate", "validate"},
	} {
		t.Run(command[1], func(t *testing.T) {
			c := qt.New(t)
			dir := writeHashedLayoutDirectory(c, "golang-migrate", files)
			args := slices.Concat(command, []string{"--dir", "file://" + dir + "?format=golang-migrate",
				"--dev-url", newCleanGateDatabase(c, adminURL, nil)})

			out, err := runCompat(c, args...)

			c.Assert(err, qt.ErrorMatches, refusal, qt.Commentf("output:\n%s", out))
		})
	}

	t.Run("apply", func(t *testing.T) {
		c := qt.New(t)
		db, out, err := applyGolangMigrateDirectory(c, adminURL, files)

		c.Assert(err, qt.ErrorMatches, refusal, qt.Commentf("output:\n%s", out))
		c.Assert(tableExistsIn(c, db, "t1"), qt.IsFalse)
	})
}
