package generate_test

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/generate"
)

// writeExternalSchemaAtlasConfig writes an atlas.hcl declaring a
// data.external_schema source backed by this test binary into dir.
func writeExternalSchemaAtlasConfig(t *testing.T, dir string) {
	t.Helper()
	c := qt.New(t)
	config := fmt.Sprintf(`data "external_schema" "app" {
  program = [%s, "-test.run=TestGenerateHelperProcess"]
  format  = "yaml"
  env     = ["GO_WANT_GENERATE_HELPER_PROCESS=1"]
}

env "verified" {
  src = data.external_schema.app.url
}
`, strconv.Quote(os.Args[0]))
	// The config path is rooted in t.TempDir and not influenced by production input.
	c.Assert(os.WriteFile(filepath.Join(dir, "atlas.hcl"), []byte(config), 0o600), qt.IsNil) //nolint:gosec // controlled test-only path
}

func TestGenerateCommand_UsesExternalSchemaFromAtlasConfigEnv(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	writeExternalSchemaAtlasConfig(t, dir)
	t.Chdir(dir)

	cmd := generate.NewGenerateCommand()
	cmd.SetArgs([]string{
		"--env", "verified",
		"--allow-external-schema",
		"--dialect", "postgres",
	})

	stdout, stderr, err := executeGenerate(c.TB, cmd)

	c.Assert(err, qt.IsNil)
	c.Assert(stdout, qt.Contains, `CREATE TABLE "configured_widgets"`)
	c.Assert(stdout, qt.Not(qt.Contains), "Found 1 tables")
	c.Assert(stderr, qt.Contains, "Found 1 tables")
}

func TestGenerateCommand_RejectsImplicitExternalSchemaFromAtlasConfig(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	writeExternalSchemaAtlasConfig(t, dir)
	t.Chdir(dir)

	cmd := generate.NewGenerateCommand()
	cmd.SetArgs([]string{"--env", "verified", "--dialect", "postgres"})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, "atlas.hcl data.external_schema is disabled by default; pass --allow-external-schema to execute it")
}
