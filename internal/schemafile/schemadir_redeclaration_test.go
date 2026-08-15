package schemafile_test

import (
	"slices"
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
// The layouts this refusal must NOT reach are the control set, and they live in
// TestLoadPathAdmitsADirectoryThatBuildsOnEarlierFiles. Without them, "refuse a
// directory whose files mention the same word twice" would pass this test while
// breaking every one of them.
func TestLoadPathRefusesADirectoryThatRedeclaresAnObject(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		files   map[string]string
		wantErr string
	}{
		{
			name:    "a later file that declares the same table refuses",
			dialect: "sqlite",
			files: map[string]string{
				"1_a.sql": "CREATE TABLE users (id INTEGER PRIMARY KEY);\n",
				"2_b.sql": "CREATE TABLE users (id INTEGER PRIMARY KEY, extra TEXT);\n",
			},
			wantErr: `read state from "2_b.sql": table "users" already exists`,
		},
		{
			name:    "a later file that declares the same index refuses",
			dialect: "sqlite",
			files: map[string]string{
				"1_a.sql": "CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT);\n" +
					"CREATE INDEX idx_users_email ON users (email);\n",
				"2_b.sql": "CREATE INDEX idx_users_email ON users (email);\n",
			},
			wantErr: `read state from "2_b.sql": index "users.idx_users_email" already exists`,
		},
		{
			name:    "a later HCL file that declares the same table refuses",
			dialect: "sqlite",
			files: map[string]string{
				"a.hcl": "schema \"main\" {}\ntable \"users\" {\n  schema = schema.main\n  column \"id\" {\n    type = int\n  }\n}\n",
				"b.hcl": "table \"users\" {\n  schema = schema.main\n  column \"id\" {\n    type = int\n  }\n}\n",
			},
			wantErr: `read state from "b.hcl": table "main.users" already exists`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := writeSchemaDir(c, test.files)

			db, err := schemafile.LoadPath(dir, schemafile.Options{Dialect: test.dialect})

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			// A refused directory hands back no desired state: a caller that
			// ignored the error would otherwise apply the half-read merge this
			// refusal exists to prevent.
			c.Assert(db, qt.IsNil)
		})
	}
}

// TestLoadPathAdmitsADirectoryThatBuildsOnEarlierFiles is the control set the
// refusal above needs, and each row is a separate measurement on the pinned
// Atlas community binary v1.3.0 rather than a restatement of this
// implementation: a guarded redeclaration, a same-named table in two schemas, an
// index that belongs to a table an earlier file created, an ALTER of such a
// table, and an HCL directory whose files each open with the schema block are
// all exit 0 there.
//
// Each row states the whole desired state the directory produced, because "the
// load returned no error" is also what a reader that dropped the later file
// answers, and that reader loses the second file's declarations.
func TestLoadPathAdmitsADirectoryThatBuildsOnEarlierFiles(t *testing.T) {
	tests := []struct {
		name       string
		dialect    string
		files      map[string]string
		wantTables []string
		wantIndex  int
	}{
		{
			name:    "a guarded redeclaration is admitted",
			dialect: "sqlite",
			files: map[string]string{
				"1_a.sql": "CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY);\n",
				"2_b.sql": "CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, extra TEXT);\n",
			},
			wantTables: []string{"users"},
		},
		{
			name:    "the same table name in two schemas is not a redeclaration",
			dialect: "postgres",
			files: map[string]string{
				"1_a.sql": "CREATE TABLE app.users (id INTEGER PRIMARY KEY);\n",
				"2_b.sql": "CREATE TABLE other.users (id INTEGER PRIMARY KEY);\n",
			},
			wantTables: []string{"users", "users"},
		},
		{
			name:    "an index on a table an earlier file created is not a redeclaration",
			dialect: "sqlite",
			files: map[string]string{
				"1_a.sql": "CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT);\n",
				"2_b.sql": "CREATE INDEX idx_users_email ON users (email);\n",
			},
			wantTables: []string{"users"},
			wantIndex:  1,
		},
		{
			name:    "an ALTER of a table an earlier file created is not a redeclaration",
			dialect: "sqlite",
			files: map[string]string{
				"1_a.sql": "CREATE TABLE users (id INTEGER PRIMARY KEY);\n",
				"2_b.sql": "ALTER TABLE users ADD COLUMN extra TEXT;\n",
			},
			wantTables: []string{"users"},
		},
		{
			name:    "HCL files that each repeat the schema block are admitted",
			dialect: "sqlite",
			files: map[string]string{
				"a.hcl": "schema \"main\" {}\ntable \"users\" {\n  schema = schema.main\n  column \"id\" {\n    type = int\n  }\n}\n",
				"b.hcl": "schema \"main\" {}\ntable \"posts\" {\n  schema = schema.main\n  column \"id\" {\n    type = int\n  }\n}\n",
			},
			wantTables: []string{"posts", "users"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := writeSchemaDir(c, test.files)

			db, err := schemafile.LoadPath(dir, schemafile.Options{Dialect: test.dialect})

			c.Assert(err, qt.IsNil)
			c.Assert(sortedTableNames(db), qt.DeepEquals, test.wantTables)
			c.Assert(db.Indexes, qt.HasLen, test.wantIndex)
		})
	}
}

// sortedTableNames answers what the directory declared as a set, so a row states
// its expectation without depending on the order the files happened to be read
// in.
func sortedTableNames(db *goschema.Database) []string {
	return slices.Sorted(slices.Values(tableNames(db)))
}
