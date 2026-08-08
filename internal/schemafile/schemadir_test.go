package schemafile_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/internal/schemafile"
)

// writeSchemaDir materializes a directory fixture from a name -> contents map.
// A name carrying a path separator creates the parent directory too, which is
// how the no-recursion row gets its subdirectory.
func writeSchemaDir(c *qt.C, files map[string]string) string {
	c.Helper()
	dir := c.TempDir()
	for name, contents := range files {
		path := filepath.Join(dir, name)
		c.Assert(os.MkdirAll(filepath.Dir(path), 0o750), qt.IsNil)
		c.Assert(os.WriteFile(path, []byte(contents), 0o600), qt.IsNil)
	}
	return dir
}

func tableNames(db *goschema.Database) []string {
	names := make([]string, 0, len(db.Tables))
	for _, table := range db.Tables {
		names = append(names, table.Name)
	}
	return names
}

// TestLoadPathReadsSchemaDirectory is the regression test for stokaro/ptah#940
// item B. A file:// directory of schema files was classified as a local file and
// then refused with `schema file is a directory`, so a multi-file schema had to
// be passed one flag at a time.
//
// Every row is one behavior measured on the pinned Atlas community binary v1.3.0
// against the same fixture, so the table is a comparison and not a restatement
// of this implementation.
func TestLoadPathReadsSchemaDirectory(t *testing.T) {
	tests := []struct {
		name   string
		files  map[string]string
		assert func(c *qt.C, db *goschema.Database, err error)
	}{
		{
			name: "a directory of SQL files loads every file",
			files: map[string]string{
				"1_users.sql": "CREATE TABLE users (id INTEGER PRIMARY KEY);\n",
				"2_posts.sql": "CREATE TABLE posts (id INTEGER PRIMARY KEY);\n",
			},
			assert: func(c *qt.C, db *goschema.Database, err error) {
				c.Assert(err, qt.IsNil)
				c.Assert(tableNames(db), qt.Contains, "users")
				c.Assert(tableNames(db), qt.Contains, "posts")
			},
		},
		{
			name: "a directory of HCL files loads every file",
			files: map[string]string{
				"a.hcl": "schema \"main\" {}\ntable \"users\" {\n  schema = schema.main\n  column \"id\" {\n    type = int\n  }\n}\n",
				"b.hcl": "table \"posts\" {\n  schema = schema.main\n  column \"id\" {\n    type = int\n  }\n}\n",
			},
			assert: func(c *qt.C, db *goschema.Database, err error) {
				c.Assert(err, qt.IsNil)
				c.Assert(tableNames(db), qt.Contains, "users")
				c.Assert(tableNames(db), qt.Contains, "posts")
			},
		},
		{
			name: "a directory holding both formats is ambiguous",
			files: map[string]string{
				"a.sql": "CREATE TABLE users (id INTEGER PRIMARY KEY);\n",
				"b.hcl": "schema \"main\" {}\n",
			},
			assert: func(c *qt.C, db *goschema.Database, err error) {
				c.Assert(err, qt.ErrorMatches, `ambiguous schema: both SQL and HCL files found: "a.sql", "b.hcl"`)
			},
		},
		{
			name: "a file with another extension is ignored",
			files: map[string]string{
				"1_users.sql": "CREATE TABLE users (id INTEGER PRIMARY KEY);\n",
				"notes.txt":   "not a schema\n",
			},
			assert: func(c *qt.C, db *goschema.Database, err error) {
				c.Assert(err, qt.IsNil)
				c.Assert(tableNames(db), qt.DeepEquals, []string{"users"})
			},
		},
		{
			name: "a subdirectory refuses instead of being descended into",
			files: map[string]string{
				"1_users.sql": "CREATE TABLE users (id INTEGER PRIMARY KEY);\n",
				"sub/x.sql":   "CREATE TABLE nope (id INTEGER PRIMARY KEY);\n",
			},
			assert: func(c *qt.C, db *goschema.Database, err error) {
				c.Assert(err, qt.ErrorMatches, `read [^/]+/sub: is a directory`)
			},
		},
		{
			name:  "an empty directory refuses",
			files: map[string]string{},
			assert: func(c *qt.C, db *goschema.Database, err error) {
				c.Assert(err, qt.ErrorMatches, `".*" contains neither SQL nor HCL files`)
			},
		},
		{
			name: "a migration directory stays a migration directory",
			files: map[string]string{
				"atlas.sum":               "h1:whatever\n",
				"20260101000000_init.sql": "CREATE TABLE users (id INTEGER PRIMARY KEY);\n",
			},
			assert: func(c *qt.C, db *goschema.Database, err error) {
				c.Assert(err, qt.ErrorMatches,
					`".*" is a migration directory \(it contains atlas\.sum\), not a schema directory`)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := writeSchemaDir(c, test.files)

			db, err := schemafile.LoadPath(dir, schemafile.Options{Dialect: "sqlite"})

			test.assert(c, db, err)
		})
	}
}

// TestLoadAllMixesDirectoriesAndFiles keeps the directory source composable with
// the per-file spelling it replaces: --to may still be repeated.
func TestLoadAllMixesDirectoriesAndFiles(t *testing.T) {
	c := qt.New(t)
	dir := writeSchemaDir(c, map[string]string{
		"1_users.sql": "CREATE TABLE users (id INTEGER PRIMARY KEY);\n",
	})
	extra := filepath.Join(c.TempDir(), "extra.sql")
	c.Assert(os.WriteFile(extra, []byte("CREATE TABLE extra (id INTEGER PRIMARY KEY);\n"), 0o600), qt.IsNil)

	db, err := schemafile.LoadAll([]string{"file://" + dir, "file://" + extra}, schemafile.Options{Dialect: "sqlite"})

	c.Assert(err, qt.IsNil)
	c.Assert(tableNames(db), qt.Contains, "users")
	c.Assert(tableNames(db), qt.Contains, "extra")
}
