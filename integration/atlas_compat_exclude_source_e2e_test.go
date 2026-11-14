//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
)

// excludeSourceProject is an atlas.hcl whose env lists an exclude selector
// that names no object of the schema it applies.
const excludeSourceProject = `env "local" {
  url     = "sqlite://db.sqlite"
  dev     = "sqlite://file?mode=memory"
  src     = "file://schema.sql"
  exclude = ["nosuch_object"]
}
`

// TestAtlasCompatExcludeSourceE2E runs the shipped ptah-compat over a project
// whose atlas.hcl env lists an exclude selector that matches nothing. The
// pinned community binary v1.3.0 exits 0 and prints nothing about it. Ptah
// keeps its warning on inspect and its refusal on apply, and names the file
// the selector came from, since the command line carries no --exclude.
func TestAtlasCompatExcludeSourceE2E(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "atlas.hcl"), []byte(excludeSourceProject), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "schema.sql"), []byte("CREATE TABLE keep (id INTEGER PRIMARY KEY);\n"), 0o600), qt.IsNil)

	inspect := clirun.Run(c, clirun.Compat, clirun.Options{Dir: dir}, "schema", "inspect", "--env", "local")
	apply := clirun.Run(c, clirun.Compat, clirun.Options{Dir: dir}, "schema", "apply", "--env", "local", "--dry-run")
	flag := clirun.Run(c, clirun.Compat, clirun.Options{Dir: dir}, "schema", "inspect", "--env", "local", "--exclude", "other_object")

	c.Assert(inspect.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", inspect.Stderr))
	c.Assert(inspect.Stderr, qt.Equals, `Warning: the exclude selection from atlas.hcl matched no objects: "nosuch_object".`+"\n")
	c.Assert(apply.ExitCode, qt.Equals, 1)
	c.Assert(apply.Stdout, qt.Equals, "")
	c.Assert(apply.Stderr, qt.Equals, `Error: the exclude selection from atlas.hcl matched no objects: "nosuch_object"; `+
		"schema apply refuses a selection that protects nothing, set PTAH_ATLAS_ALLOW_UNMATCHED_EXCLUDE=1 to keep the permissive behavior\n")
	c.Assert(flag.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", flag.Stderr))
	c.Assert(flag.Stderr, qt.Equals, `Warning: the --exclude selection matched no objects: "other_object".`+"\n")
}
