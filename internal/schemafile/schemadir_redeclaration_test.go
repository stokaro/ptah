package schemafile_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/internal/schemafile"
)

// TestLoadPathRefusesADirectoryThatRedeclaresAnObject is the regression test for
// the compatibility defect the first cut of the directory source shipped: the
// files were MERGED as declarations instead of read as an ordered script, so a
// directory whose files both declare `users` produced a desired state that
// appears in neither file and loaded without complaint. Through `ptah-compat`
// that was exit 0 -- and on `schema apply`, an exit 0 that really wrote the
// merged table -- where the pinned Atlas community binary v1.3.0 exits 1 with
//
//	read state from "2_b.sql": executing statement: "CREATE TABLE users (...)":
//	table users already exists
//
// because it executes the files in filename order against the dev database.
//
// Every admitting row is a control the refusing rows need, and each is a
// separate measurement on that binary rather than a restatement of this
// implementation: a guarded redeclaration, a same-named table in two schemas,
// an index that belongs to a table an earlier file created, and an HCL
// directory whose files each open with the schema block are all exit 0 there.
// Without them, "refuse a directory whose files mention the same word twice"
// would pass this table while breaking every one of those layouts.
func TestLoadPathRefusesADirectoryThatRedeclaresAnObject(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		files   map[string]string
		assert  func(c *qt.C, db *goschema.Database, err error)
	}{
		{
			name:    "a later file that declares the same table refuses",
			dialect: "sqlite",
			files: map[string]string{
				"1_a.sql": "CREATE TABLE users (id INTEGER PRIMARY KEY);\n",
				"2_b.sql": "CREATE TABLE users (id INTEGER PRIMARY KEY, extra TEXT);\n",
			},
			assert: func(c *qt.C, db *goschema.Database, err error) {
				c.Assert(err, qt.ErrorMatches, `read state from "2_b.sql": table "users" already exists`)
				c.Assert(db, qt.IsNil)
			},
		},
		{
			name:    "a later file that declares the same index refuses",
			dialect: "sqlite",
			files: map[string]string{
				"1_a.sql": "CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT);\n" +
					"CREATE INDEX idx_users_email ON users (email);\n",
				"2_b.sql": "CREATE INDEX idx_users_email ON users (email);\n",
			},
			assert: func(c *qt.C, db *goschema.Database, err error) {
				c.Assert(err, qt.ErrorMatches, `read state from "2_b.sql": index "users.idx_users_email" already exists`)
			},
		},
		{
			name:    "a guarded redeclaration is admitted",
			dialect: "sqlite",
			files: map[string]string{
				"1_a.sql": "CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY);\n",
				"2_b.sql": "CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, extra TEXT);\n",
			},
			assert: func(c *qt.C, db *goschema.Database, err error) {
				c.Assert(err, qt.IsNil)
				c.Assert(tableNames(db), qt.Contains, "users")
			},
		},
		{
			name:    "the same table name in two schemas is not a redeclaration",
			dialect: "postgres",
			files: map[string]string{
				"1_a.sql": "CREATE TABLE app.users (id INTEGER PRIMARY KEY);\n",
				"2_b.sql": "CREATE TABLE other.users (id INTEGER PRIMARY KEY);\n",
			},
			assert: func(c *qt.C, db *goschema.Database, err error) {
				c.Assert(err, qt.IsNil)
				c.Assert(db.Tables, qt.HasLen, 2)
			},
		},
		{
			name:    "an index on a table an earlier file created is not a redeclaration",
			dialect: "sqlite",
			files: map[string]string{
				"1_a.sql": "CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT);\n",
				"2_b.sql": "CREATE INDEX idx_users_email ON users (email);\n",
			},
			assert: func(c *qt.C, db *goschema.Database, err error) {
				c.Assert(err, qt.IsNil)
				c.Assert(db.Indexes, qt.HasLen, 1)
			},
		},
		{
			name:    "an ALTER of a table an earlier file created is not a redeclaration",
			dialect: "sqlite",
			files: map[string]string{
				"1_a.sql": "CREATE TABLE users (id INTEGER PRIMARY KEY);\n",
				"2_b.sql": "ALTER TABLE users ADD COLUMN extra TEXT;\n",
			},
			assert: func(c *qt.C, db *goschema.Database, err error) {
				c.Assert(err, qt.IsNil)
				c.Assert(tableNames(db), qt.DeepEquals, []string{"users"})
			},
		},
		{
			name:    "a later HCL file that declares the same table refuses",
			dialect: "sqlite",
			files: map[string]string{
				"a.hcl": "schema \"main\" {}\ntable \"users\" {\n  schema = schema.main\n  column \"id\" {\n    type = int\n  }\n}\n",
				"b.hcl": "table \"users\" {\n  schema = schema.main\n  column \"id\" {\n    type = int\n  }\n}\n",
			},
			assert: func(c *qt.C, db *goschema.Database, err error) {
				c.Assert(err, qt.ErrorMatches, `read state from "b.hcl": table "main.users" already exists`)
			},
		},
		{
			name:    "HCL files that each repeat the schema block are admitted",
			dialect: "sqlite",
			files: map[string]string{
				"a.hcl": "schema \"main\" {}\ntable \"users\" {\n  schema = schema.main\n  column \"id\" {\n    type = int\n  }\n}\n",
				"b.hcl": "schema \"main\" {}\ntable \"posts\" {\n  schema = schema.main\n  column \"id\" {\n    type = int\n  }\n}\n",
			},
			assert: func(c *qt.C, db *goschema.Database, err error) {
				c.Assert(err, qt.IsNil)
				c.Assert(tableNames(db), qt.Contains, "users")
				c.Assert(tableNames(db), qt.Contains, "posts")
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := writeSchemaDir(c.TB, test.files)

			db, err := schemafile.LoadPath(dir, schemafile.Options{Dialect: test.dialect})

			test.assert(c, db, err)
		})
	}
}
