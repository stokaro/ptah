package schema_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

const messyHCL = "schema   \"main\" {\n}\n"

func TestSchemaFmtRewritesFileAndPrintsIt(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "schema.hcl")
	c.Assert(os.WriteFile(path, []byte(messyHCL), 0o600), qt.IsNil)

	out, err := runSchema("", "fmt", path)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, path)
	formatted, err := os.ReadFile(path)
	c.Assert(err, qt.IsNil)
	c.Assert(string(formatted), qt.Equals, "schema \"main\" {\n}\n")
}

func TestSchemaFmtCheckReportsWithoutRewriting(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "schema.hcl")
	c.Assert(os.WriteFile(path, []byte(messyHCL), 0o600), qt.IsNil)

	out, err := runSchema("", "fmt", "--check", path)

	c.Assert(err, qt.ErrorMatches, `1 file\(s\) are not canonically formatted; .*`, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, path)
	content, err := os.ReadFile(path)
	c.Assert(err, qt.IsNil)
	c.Assert(string(content), qt.Equals, messyHCL)
}

func TestSchemaFmtCheckPassesOnCanonicalFiles(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "schema.hcl")
	c.Assert(os.WriteFile(path, []byte("schema \"main\" {\n}\n"), 0o600), qt.IsNil)

	out, err := runSchema("", "fmt", "--check", dir)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Equals, "")
}
