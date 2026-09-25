package schemafile_test

import (
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/schemafile"
)

func fieldNames(db *schemamodel.Database) []string {
	names := make([]string, 0, len(db.Fields))
	for _, field := range db.Fields {
		names = append(names, field.Name)
	}
	return names
}

// A schema directory is one script run in file order, so a later file may add
// a column to a table an earlier file created. The column reaches the table:
// the pinned Atlas community binary v1.3.0 executes the files in order and has
// the column, and the directory loaded here without it (stokaro/ptah#3562).
func TestLoadPathAddsAColumnFromALaterFile_HappyPath(t *testing.T) {
	c := qt.New(t)
	dir := writeSchemaDir(c, map[string]string{
		"1_a.sql": "CREATE TABLE users (id INTEGER PRIMARY KEY, note TEXT);\n",
		"2_b.sql": "ALTER TABLE users ADD COLUMN IF NOT EXISTS extra TEXT;\n" +
			"ALTER TABLE users ADD COLUMN IF NOT EXISTS note TEXT;\n",
	})

	db, err := schemafile.LoadPath(dir, schemafile.Options{Dialect: "postgres"})

	c.Assert(err, qt.IsNil)
	c.Assert(tableNames(db), qt.DeepEquals, []string{"users"})
	c.Assert(fieldNames(db), qt.DeepEquals, []string{"id", "note", "extra"})
}

// A later file that adds, unguarded, a column an earlier file declared is
// refused, as the server refuses the statement.
func TestLoadPathAddsAColumnFromALaterFile_FailurePath(t *testing.T) {
	c := qt.New(t)
	dir := writeSchemaDir(c, map[string]string{
		"1_a.sql": "CREATE TABLE users (id INTEGER PRIMARY KEY, note TEXT);\n",
		"2_b.sql": "ALTER TABLE users ADD COLUMN note TEXT;\n",
	})

	db, err := schemafile.LoadPath(dir, schemafile.Options{Dialect: "postgres"})

	c.Assert(err, qt.ErrorMatches, `read SQL schema file .*2_b\.sql: ALTER TABLE users ADD COLUMN note names a column the table already declares`)
	c.Assert(db, qt.IsNil)
}

// An imported file is read against what came before it, the importing file
// included, as a later file of a directory is.
func TestLoadPathAddsAColumnFromAnImportedFile(t *testing.T) {
	c := qt.New(t)
	dir := writeSchemaDir(c, map[string]string{
		"main.sql":    "-- atlas:import ./columns.sql\nCREATE TABLE users (id INTEGER PRIMARY KEY);\n",
		"columns.sql": "ALTER TABLE users ADD COLUMN IF NOT EXISTS extra TEXT;\n",
	})

	db, err := schemafile.LoadPath(filepath.Join(dir, "main.sql"), schemafile.Options{Dialect: "postgres"})

	c.Assert(err, qt.IsNil)
	c.Assert(fieldNames(db), qt.DeepEquals, []string{"id", "extra"})
}
