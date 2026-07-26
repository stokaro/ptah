package atlas_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/atlascompat"
	"github.com/stokaro/ptah/cmd/atlas"
	"github.com/stokaro/ptah/migration/migrator"
)

func TestMigrateNewWithAtlasProjectEnumIdentifiers(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	t.Chdir(root)
	c.Assert(os.Mkdir("migrations", 0o755), qt.IsNil)
	c.Assert(os.WriteFile("atlas.hcl", []byte(`env "local" {
  migration {
    dir        = "file://migrations"
    format     = atlas
    exec_order = linear
    tx_mode    = file
  }
}
`), 0o600), qt.IsNil)

	cmd := atlas.NewAtlasCommand()
	var output bytes.Buffer
	cmd.SetOut(&output)
	cmd.SetErr(&output)
	cmd.SetArgs([]string{"migrate", "new", "manual_hotfix", "--env", "local"})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(output.String(), qt.Contains, "Generated empty migration file:")
	migrations, globErr := filepath.Glob(filepath.Join(root, "migrations", "*_manual_hotfix.sql"))
	c.Assert(globErr, qt.IsNil)
	c.Assert(migrations, qt.HasLen, 1)
	migrationSQL, readErr := os.ReadFile(migrations[0])
	c.Assert(readErr, qt.IsNil)
	c.Assert(string(migrationSQL), qt.Equals, "")
	migrationDir := filepath.Join(root, "migrations")
	gotSum, readErr := os.ReadFile(filepath.Join(migrationDir, atlascompat.AtlasSumFileName))
	c.Assert(readErr, qt.IsNil)
	wantSum, sumErr := atlascompat.ComputeSum(os.DirFS(migrationDir), migrator.MigrationDirFormatAtlas)
	c.Assert(sumErr, qt.IsNil)
	c.Assert(string(gotSum), qt.Equals, string(wantSum.Bytes()))
}
