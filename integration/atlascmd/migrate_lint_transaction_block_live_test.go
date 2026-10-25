//go:build integration

package atlas_test

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/cli/atlas"
	"ptah.run/internal/dbtarget"
)

// OpenMeter's CI runs `migrate lint --latest N` with a PostgreSQL dev database
// over a golang-migrate directory whose hand-written migrations wrap their
// statements in BEGIN/COMMIT (stokaro/ptah#4953). golang-migrate opens no
// transaction around a file, so the block is the file's only transaction and
// TX201 does not apply there. These tests run that shape against a live
// server: the dev database replays the block, and the verdict follows the
// layout. The same block in Atlas's layout, which Atlas wraps per file, keeps
// TX201.

// transactionBlockLive is a migration that opens and commits its own
// transaction.
const transactionBlockLive = "BEGIN;\nCREATE TABLE t1 (id int);\nINSERT INTO t1 VALUES (1);\nCOMMIT;\n"

// writeHashedLayoutDirectory writes files and hashes the directory as layout
// with the shipped `migrate hash`, so the covered set is the layout's own.
func writeHashedLayoutDirectory(c *qt.C, layout string, files map[string]string) string {
	c.Helper()
	dir := filepath.Join(c.TempDir(), "migrations")
	c.Assert(os.MkdirAll(dir, 0o755), qt.IsNil)
	for name, body := range files {
		c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600), qt.IsNil)
	}
	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"migrate", "hash", "--dir", "file://" + dir + "?format=" + layout})
	c.Assert(cmd.Execute(), qt.IsNil, qt.Commentf("output:\n%s", out.String()))
	return dir
}

// runLayoutLint lints the latest migration of the directory dirURL names
// against devURL.
func runLayoutLint(c *qt.C, dirURL, devURL string) (string, error) {
	c.Helper()
	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"migrate", "lint", "--dir", dirURL, "--dev-url", devURL, "--latest", "1"})
	err := cmd.Execute()
	return strings.Join(strings.Fields(out.String()), " "), err
}

func TestMigrateLintGolangMigrateTransactionBlockLive(t *testing.T) {
	c := qt.New(t)
	devURL := newCleanGateDatabase(c, dbtarget.URL(c, dbtarget.PostgreSQL), nil)
	dir := writeHashedLayoutDirectory(c, "golang-migrate", map[string]string{
		"20240826120919_init.up.sql":       "CREATE TABLE base (id int);\n",
		"20240826120919_init.down.sql":     "DROP TABLE base;\n",
		"20240827000000_backfill.up.sql":   transactionBlockLive,
		"20240827000000_backfill.down.sql": "BEGIN;\nDROP TABLE t1;\nCOMMIT;\n",
	})

	out, err := runLayoutLint(c, "file://"+dir+"?format=golang-migrate", devURL)

	c.Assert(err, qt.IsNil, qt.Commentf("output:\n%s", out))
	c.Assert(out, qt.Contains, "-- analyzing version 20240827000000 -- no diagnostics found")
	c.Assert(out, qt.Contains, "-- 1 version ok")
	c.Assert(out, qt.Not(qt.Contains), "TX201")
}

func TestMigrateLintAtlasTransactionBlockLive(t *testing.T) {
	c := qt.New(t)
	devURL := newCleanGateDatabase(c, dbtarget.URL(c, dbtarget.PostgreSQL), nil)
	dir := writeHashedLayoutDirectory(c, "atlas", map[string]string{
		"20240826120919_init.sql":     "CREATE TABLE base (id int);\n",
		"20240827000000_backfill.sql": transactionBlockLive,
	})

	out, err := runLayoutLint(c, "file://"+dir, devURL)

	c.Assert(err, qt.IsNil, qt.Commentf("output:\n%s", out))
	c.Assert(out, qt.Contains, "[TX201]")
	c.Assert(out, qt.Contains, "-- 1 version with warnings")
}
