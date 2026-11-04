package root_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

// TestSchemaRenderRefusesAMisspelledDirective renders an entity package whose
// field carries `//ptah:schema:column`. The command parses with the runtime's
// owners, which are every owner it reads, so the misspelling is refused with
// its position instead of rendering a table without the column.
func TestSchemaRenderRefusesAMisspelledDirective(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "go.mod"), []byte("module entities\n\ngo 1.24\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "user.go"), []byte("package entities\n\n"+
		"//ptah:schema:table name=\"users\"\ntype User struct {\n"+
		"\t//ptah:schema:column name=\"id\" type=\"INTEGER\" primary=\"true\"\n\tID int\n}\n"), 0o600), qt.IsNil)

	stdout, stderr, code := runRoot(c, "schema", "render", "--root-dir", dir, "--dialect", "postgres")

	c.Assert(code, qt.Not(qt.Equals), 0, qt.Commentf("stdout:\n%s", stdout))
	c.Assert(stderr, qt.Contains, `unknown annotation directive "//ptah:schema:column" on User.ID at `)
	c.Assert(stderr, qt.Contains, "user.go:5: neither Ptah nor a selected provider declares it")
	c.Assert(stdout, qt.Not(qt.Contains), `CREATE TABLE "users"`)
}
