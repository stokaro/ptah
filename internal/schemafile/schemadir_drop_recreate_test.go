package schemafile_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/schemafile"
)

// TestLoadPathAdmitsATableALaterFileDropsFirst reads a schema directory whose
// later file drops a table an earlier file created before creating it again.
// The files run in order, so the engine accepts it: measured on PostgreSQL 18.6,
// the pinned Atlas community binary v1.3.0 exits 0 on the drop-and-create
// layout. Without the replay the reader refuses it as a second declaration of
// the table (stokaro/ptah#3914).
//
// Each row states the columns the table ends with, because a reader that kept
// the first file's table would also load without an error.
func TestLoadPathAdmitsATableALaterFileDropsFirst(t *testing.T) {
	tests := []struct {
		name        string
		dialect     string
		files       map[string]string
		wantTables  []string
		wantColumns []string
		wantIndexes int
	}{
		{
			name:    "a later file drops the table and creates it again",
			dialect: "postgres",
			files: map[string]string{
				"1_a.sql": "CREATE TABLE users (id INTEGER PRIMARY KEY);\n",
				"2_b.sql": "DROP TABLE users;\nCREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT);\n",
			},
			wantTables:  []string{"users"},
			wantColumns: []string{"id", "email"},
		},
		{
			name:    "the new table may declare the index the dropped one had",
			dialect: "sqlite",
			files: map[string]string{
				"1_a.sql": "CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT);\n" +
					"CREATE INDEX idx_users_email ON users (email);\n",
				"2_b.sql": "DROP TABLE users;\n" +
					"CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT, name TEXT);\n" +
					"CREATE INDEX idx_users_email ON users (email);\n",
			},
			wantTables:  []string{"users"},
			wantColumns: []string{"id", "email", "name"},
			wantIndexes: 1,
		},
		{
			name:    "a file that only drops the table lets a later file create it",
			dialect: "sqlite",
			files: map[string]string{
				"1_a.sql": "CREATE TABLE users (id INTEGER PRIMARY KEY);\n",
				"2_b.sql": "DROP TABLE users;\n",
				"3_c.sql": "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);\n",
			},
			wantTables:  []string{"users"},
			wantColumns: []string{"id", "name"},
		},
		{
			name:    "one DROP TABLE naming two tables drops both",
			dialect: "postgres",
			files: map[string]string{
				"1_a.sql": "CREATE TABLE posts (id INTEGER PRIMARY KEY);\n" +
					"CREATE TABLE users (id INTEGER PRIMARY KEY);\n",
				"2_b.sql": "DROP TABLE posts, users;\n" +
					"CREATE TABLE posts (id INTEGER PRIMARY KEY);\n" +
					"CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT);\n",
			},
			wantTables:  []string{"posts", "users"},
			wantColumns: []string{"id", "email"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := writeSchemaDir(c, test.files)

			db, err := schemafile.LoadPath(dir, schemafile.Options{Dialect: test.dialect})

			c.Assert(err, qt.IsNil)
			c.Assert(sortedTableNames(db), qt.DeepEquals, test.wantTables)
			c.Assert(tableColumnNames(db, "users"), qt.DeepEquals, test.wantColumns)
			c.Assert(db.Indexes, qt.HasLen, test.wantIndexes)
		})
	}
}

// TestLoadPathRefusesATableALaterFileCreatesBeforeDropping keeps the order the
// replay reads: a later file that creates the table before it drops it fails
// on the CREATE, as the engine does, although the file's final state holds no
// table of that name.
func TestLoadPathRefusesATableALaterFileCreatesBeforeDropping(t *testing.T) {
	c := qt.New(t)
	dir := writeSchemaDir(c, map[string]string{
		"1_a.sql": "CREATE TABLE users (id INTEGER PRIMARY KEY);\n",
		"2_b.sql": "CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT);\nDROP TABLE users;\n",
	})

	db, err := schemafile.LoadPath(dir, schemafile.Options{Dialect: "postgres"})

	c.Assert(err, qt.ErrorMatches, `read state from "2_b.sql": table "users" already exists`)
	c.Assert(db, qt.IsNil)
}

// TestLoadPathKeepsTheIndexesOfAQualifiedTableWhenDroppingItsSchemaName drops
// an unqualified table app and leaves the indexes of app.users declared: a
// later file that declares one of them again still refuses.
func TestLoadPathKeepsTheIndexesOfAQualifiedTableWhenDroppingItsSchemaName(t *testing.T) {
	c := qt.New(t)
	dir := writeSchemaDir(c, map[string]string{
		"1_a.sql": "CREATE TABLE app (id INTEGER PRIMARY KEY);\n" +
			"CREATE TABLE app.users (id INTEGER PRIMARY KEY, email TEXT);\n" +
			"CREATE INDEX idx_email ON app.users (email);\n",
		"2_b.sql": "DROP TABLE app;\nCREATE INDEX idx_email ON app.users (email);\n",
	})

	db, err := schemafile.LoadPath(dir, schemafile.Options{Dialect: "postgres"})

	c.Assert(err, qt.ErrorMatches, `read state from "2_b.sql": index "app.users.idx_email" already exists`)
	c.Assert(db, qt.IsNil)
}

// tableColumnNames lists the columns of the named table in the order the table
// declares them.
func tableColumnNames(db *schemamodel.Database, table string) []string {
	var structName string
	for _, candidate := range db.Tables {
		if candidate.Name == table {
			structName = candidate.StructName
		}
	}
	var names []string
	for _, field := range db.Fields {
		if field.StructName == structName && !slices.Contains(names, field.Name) {
			names = append(names, field.Name)
		}
	}
	return names
}
