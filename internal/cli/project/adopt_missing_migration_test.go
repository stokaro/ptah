package project_test

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/cli/migrateup"
)

// The checksum dimension answers "may native Ptah take over this history". A
// revision the directory holds no file for is a refusal there for a reason of
// its own: there is nothing to compare the recorded checksum with, so the
// finding cannot be worded as a checksum that disagrees (stokaro/ptah#3441).

func TestProjectAdoptPreflight_FailurePath(t *testing.T) {
	t.Run("an applied migration is not in the directory", func(t *testing.T) {
		c := qt.New(t)
		dir, dbPath := appliedSQLiteHistory(c)
		c.Assert(os.Remove(filepath.Join(dir, "0000000002_orders.up.sql")), qt.IsNil)
		c.Assert(os.Remove(filepath.Join(dir, "0000000002_orders.down.sql")), qt.IsNil)
		path := ptahProjectFile(c, sqliteProjectDocument(dir, dbPath))

		stdout, _, err := runInspect(c, "adopt", "--check", "--preflight", "--config", path, "--env", "local")

		c.Assert(err, qt.IsNotNil)
		c.Assert(stdout, qt.Contains, "revision 2 names a migration this directory does not hold")
		c.Assert(stdout, qt.Not(qt.Contains), "no recorded checksum contradicts")
	})
}

func TestProjectAdoptPreflight_HappyPath(t *testing.T) {
	t.Run("every applied migration is in the directory", func(t *testing.T) {
		c := qt.New(t)
		dir, dbPath := appliedSQLiteHistory(c)
		path := ptahProjectFile(c, sqliteProjectDocument(dir, dbPath))

		stdout, _, err := runInspect(c, "adopt", "--check", "--preflight", "--config", path, "--env", "local")

		c.Assert(err, qt.IsNil, qt.Commentf("%s", stdout))
		c.Assert(stdout, qt.Contains, "no recorded checksum contradicts")
	})
}

// sqliteProjectDocument renders the env both tests read, as ptah.yaml rather
// than atlas.hcl: the history below is written by native `migrations up`, and
// naming the revision format is what points the preflight at the table those
// migrations actually wrote to. atlas.hcl has no such attribute, which is
// correct -- it is Atlas's language, and Atlas has one layout.
func sqliteProjectDocument(dir, dbPath string) string {
	return fmt.Sprintf(`env:
  local:
    url: "sqlite://%s"
    migration:
      dir: "%s"
      revision_format: "ptah"
`, filepath.ToSlash(dbPath), filepath.ToSlash(dir))
}

// ptahProjectFile writes a ptah.yaml beside nothing else, so the run reads the
// document under test and not a file the working directory happens to hold.
func ptahProjectFile(c *qt.C, document string) string {
	c.Helper()
	path := filepath.Join(c.TempDir(), "ptah.yaml")
	c.Assert(os.WriteFile(path, []byte(document), 0o600), qt.IsNil)
	return path
}

func appliedSQLiteHistory(c *qt.C) (dir, dbPath string) {
	c.Helper()
	dir = c.TempDir()
	files := map[string]string{
		"0000000001_users.up.sql":    "CREATE TABLE users (id INTEGER PRIMARY KEY);\n",
		"0000000001_users.down.sql":  "DROP TABLE users;\n",
		"0000000002_orders.up.sql":   "CREATE TABLE orders (id INTEGER PRIMARY KEY);\n",
		"0000000002_orders.down.sql": "DROP TABLE orders;\n",
	}
	for name, content := range files {
		c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600), qt.IsNil)
	}
	dbPath = filepath.Join(c.TempDir(), "app.db")
	cmd := migrateup.NewMigrateUpCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"--db-url", "sqlite://" + filepath.ToSlash(dbPath), "--migrations-dir", dir})
	c.Assert(cmd.Execute(), qt.IsNil, qt.Commentf("%s", out.String()))
	return dir, dbPath
}
