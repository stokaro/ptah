package atlas_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/cli/atlas"
)

// TestAtlasMigrateDiff_ReadsTheDirectoryFormatFromItsEnv names a
// golang-migrate directory only through the env's migration block, as
// OpenMeter's atlas.hcl does. Atlas CE v1.0.0 hashes and diffs it in that
// layout; ptah-compat read it as an Atlas directory and refused each
// `.down.sql` as added to the checksum.
func TestAtlasMigrateDiff_ReadsTheDirectoryFormatFromItsEnv(t *testing.T) {
	tests := []struct {
		name   string
		format string
	}{
		{name: "a literal format", format: `"golang-migrate"`},
		{name: "a format read from locals", format: `"${local.migrations_format}"`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := c.TempDir()
			migrations := filepath.Join(dir, "migrations")
			c.Assert(os.MkdirAll(migrations, 0o755), qt.IsNil)
			c.Assert(os.WriteFile(filepath.Join(migrations, "1_init.up.sql"), []byte("CREATE TABLE a (id integer PRIMARY KEY);\n"), 0o600), qt.IsNil)
			c.Assert(os.WriteFile(filepath.Join(migrations, "1_init.down.sql"), []byte("DROP TABLE a;\n"), 0o600), qt.IsNil)
			c.Assert(os.WriteFile(filepath.Join(dir, "schema.sql"), []byte("CREATE TABLE a (id integer PRIMARY KEY);\n"), 0o600), qt.IsNil)
			c.Assert(os.WriteFile(filepath.Join(dir, "atlas.hcl"), []byte(`locals {
  migrations_format = "golang-migrate"
}

env "local" {
  src = "file://schema.sql"
  dev = "sqlite://dev?mode=memory"
  migration {
    dir    = "file://migrations"
    format = `+test.format+`
  }
}
`), 0o600), qt.IsNil)
			t.Chdir(dir)

			hash := atlas.NewCompatCommand("atlas")
			hash.SetOut(&bytes.Buffer{})
			hash.SetErr(&bytes.Buffer{})
			hash.SetArgs([]string{"migrate", "hash", "--env", "local"})
			c.Assert(hash.Execute(), qt.IsNil)

			diff := atlas.NewCompatCommand("atlas")
			var out bytes.Buffer
			diff.SetOut(&out)
			diff.SetErr(&out)
			diff.SetArgs([]string{"migrate", "diff", "--env", "local", "probe"})

			c.Assert(diff.Execute(), qt.IsNil, qt.Commentf("%s", out.String()))
			c.Assert(out.String(), qt.Contains, "The migration directory is synced with the desired state")
		})
	}
}
