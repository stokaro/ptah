package generate

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

func TestLoadSchemaFile_YAML(t *testing.T) {
	c := qt.New(t)

	path := filepath.Join(t.TempDir(), "schema.yaml")
	c.Assert(
		os.WriteFile(path, []byte(`
tables:
  users:
    columns:
      id: { type: SERIAL, primary: true }
`), 0o600),
		qt.IsNil,
	)

	db, err := loadSchema("", path)
	c.Assert(err, qt.IsNil)
	c.Assert(db.Tables, qt.HasLen, 1)
	c.Assert(db.Tables[0].Name, qt.Equals, "users")
}

func TestLoadSchemaFile_RejectsUnsupportedExtension(t *testing.T) {
	c := qt.New(t)

	path := filepath.Join(t.TempDir(), "schema.hcl")
	c.Assert(os.WriteFile(path, []byte(`schema "users" {}`), 0o600), qt.IsNil)

	_, err := loadSchema("", path)
	c.Assert(err, qt.ErrorMatches, `unsupported schema file extension ".hcl": only .yaml and .yml are supported`)
}
