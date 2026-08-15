package atlas_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/atlas"
)

func writeSchemaSourceDir(tb testing.TB, files map[string]string) string {
	c := qt.New(tb)
	c.Helper()
	dir := c.TempDir()
	for name, contents := range files {
		c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(contents), 0o600), qt.IsNil)
	}
	return dir
}

// TestSchemaDiffAcceptsDirectorySource is the regression test for
// stokaro/ptah#940 item B on the compat surface: `--to file://<dir>` failed with
// `load --to schema: schema file is a directory` while the pinned community
// binary v1.3.0 emitted a CREATE TABLE for every file in the directory.
func TestSchemaDiffAcceptsDirectorySource(t *testing.T) {
	c := qt.New(t)
	dir := writeSchemaSourceDir(c.TB, map[string]string{
		"1_users.sql": "CREATE TABLE dir_users (id INTEGER PRIMARY KEY);\n",
		"2_posts.sql": "CREATE TABLE dir_posts (id INTEGER PRIMARY KEY);\n",
	})
	emptyHCL := filepath.Join(c.TempDir(), "empty.hcl")
	c.Assert(os.WriteFile(emptyHCL, []byte("schema \"main\" {}\n"), 0o600), qt.IsNil)

	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"schema", "diff",
		"--from", "file://" + emptyHCL,
		"--to", "file://" + dir,
		"--dev-url", "sqlite://" + filepath.Join(c.TempDir(), "dev.db"),
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out.String()))
	c.Assert(out.String(), qt.Contains, "dir_users")
	c.Assert(out.String(), qt.Contains, "dir_posts")
}

// TestSchemaApplyAcceptsDirectorySource covers the same source on the verb that
// writes: the directory is planned and really applied, so the fix is not just a
// rendering change.
func TestSchemaApplyAcceptsDirectorySource(t *testing.T) {
	c := qt.New(t)
	workdir := c.TempDir()
	dir := writeSchemaSourceDir(c.TB, map[string]string{
		"1_users.sql": "CREATE TABLE applied_dir_users (id INTEGER PRIMARY KEY);\n",
		"2_posts.sql": "CREATE TABLE applied_dir_posts (id INTEGER PRIMARY KEY);\n",
	})
	dbPath := filepath.Join(workdir, "target.db")

	out, err := runSchemaApply(c.TB,
		"--url", "sqlite://"+dbPath,
		"--to", "file://"+dir,
		"--dev-url", "sqlite://"+filepath.Join(workdir, "dev.db"),
		"--auto-approve",
	)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(sqliteTableCount(c.TB, dbPath, "applied_dir_users"), qt.Equals, 1)
	c.Assert(sqliteTableCount(c.TB, dbPath, "applied_dir_posts"), qt.Equals, 1)
}
