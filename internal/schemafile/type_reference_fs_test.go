package schemafile_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/builtintest"
	"ptah.run/internal/schemafile"
)

// TestLoadPath_ResolvesATypeDeclaredInAnotherFile reads a column whose type
// names a domain another file of the schema directory declares. Each file is
// read on its own, so the reference resolves by the block that declares types
// and not by a lookup among the declared ones.
func TestLoadPath_ResolvesATypeDeclaredInAnotherFile(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "a_types.hcl"),
		[]byte("schema \"public\" {}\n\ndomain \"pos2\" {\n  schema = schema.public\n  type   = sql(\"int4\")\n}\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "b_tables.hcl"),
		[]byte("table \"t\" {\n  schema = schema.public\n  column \"c\" {\n    type = domain.pos2\n  }\n}\n"), 0o600), qt.IsNil)

	db, err := schemafile.LoadPath(t.Context(), dir, schemafile.Options{YAML: builtintest.Runtime().YAML(), SQL: builtintest.Runtime().SQL(),
		HCL: builtintest.Runtime().HCL(), Dialect: "postgres"})

	c.Assert(err, qt.IsNil)
	c.Assert(fieldByName(db, "c").Type, qt.Equals, "pos2")
}
