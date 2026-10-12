package schemaload_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/builtintest"
	"ptah.run/internal/schemaload"
)

// TestLoad_ASchemaFileDropsATableBesideAnExtension pins that a schema file is
// read with its owners' relation discovery: a DROP TABLE reads beside an
// extension, a standalone object attached to no table, which stays.
func TestLoad_ASchemaFileDropsATableBesideAnExtension(t *testing.T) {
	c := qt.New(t)
	file := filepath.Join(t.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(file, []byte("CREATE EXTENSION citext;\nCREATE TABLE a (id int);\nCREATE TABLE b (id int);\nDROP TABLE a;\n"), 0o600), qt.IsNil)

	result, err := schemaload.Load(schemaload.Options{Owners: builtintest.Runtime(), SchemaFiles: []string{file}, Dialect: "postgres"})

	c.Assert(err, qt.IsNil)
	c.Assert(result.Tables, qt.HasLen, 1)
	c.Assert(result.FeatureObjects.Len(), qt.Equals, 1)
}
