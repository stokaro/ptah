package schemaload_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/schemaload"
)

// TestLoad_SchemaDirectory is the regression test for stokaro/ptah#940 item B on
// the NATIVE surface. `ptah schema render --schema-file ./dir` used to fail with
// `unsupported schema file extension ""` — a message about a file, for something
// that is not one — because the extension switch ran before the loader had a say.
func TestLoad_SchemaDirectory(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "1_users.sql"),
		[]byte("CREATE TABLE native_dir_users (id INTEGER PRIMARY KEY);\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "2_posts.sql"),
		[]byte("CREATE TABLE native_dir_posts (id INTEGER PRIMARY KEY);\n"), 0o600), qt.IsNil)

	result, err := schemaload.Load(schemaload.Options{SchemaFiles: []string{dir}, Dialect: "sqlite"})

	c.Assert(err, qt.IsNil)
	c.Assert(result.Tables, qt.HasLen, 2)
}

// TestLoad_EmptySchemaDirectoryRefuses keeps the diagnostic about the directory
// rather than about a missing extension.
func TestLoad_EmptySchemaDirectoryRefuses(t *testing.T) {
	c := qt.New(t)

	_, err := schemaload.Load(schemaload.Options{SchemaFiles: []string{t.TempDir()}, Dialect: "sqlite"})

	c.Assert(err, qt.ErrorMatches, `error parsing schema file: ".*" contains neither SQL nor HCL files`)
	c.Assert(err.Error(), qt.Not(qt.Contains), "unsupported schema file extension")
}
