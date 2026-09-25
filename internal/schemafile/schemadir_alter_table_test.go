package schemafile_test

import (
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/schemafile"
)

func fieldByName(db *schemamodel.Database, name string) schemamodel.Field {
	for _, field := range db.Fields {
		if field.Name == name {
			return field
		}
	}
	return schemamodel.Field{}
}

// A later file of a directory may change what an earlier file declared: add a
// primary key, change a column, drop one. The directory is one script, so the
// change reaches the table the earlier file created.
func TestLoadPathAltersAnEarlierTableFromALaterFile(t *testing.T) {
	c := qt.New(t)
	dir := writeSchemaDir(c, map[string]string{
		"1_a.sql": "CREATE TABLE users (id INTEGER NOT NULL, note TEXT, legacy TEXT);\n",
		"2_b.sql": "ALTER TABLE users ADD PRIMARY KEY (id);\n" +
			"ALTER TABLE users ALTER COLUMN note SET DEFAULT 'x', ALTER COLUMN note SET NOT NULL;\n" +
			"ALTER TABLE users DROP COLUMN legacy;\n",
	})

	db, err := schemafile.LoadPath(dir, schemafile.Options{Dialect: "postgres"})

	c.Assert(err, qt.IsNil)
	c.Assert(fieldNames(db), qt.DeepEquals, []string{"id", "note"})
	c.Assert(fieldByName(db, "id").Primary, qt.IsTrue)
	c.Assert(fieldByName(db, "note").Default, qt.Equals, "'x'")
	c.Assert(fieldByName(db, "note").Nullable, qt.IsFalse)
}

// A file imported after the file that created a table may change it too, and
// the importing file sees the change.
func TestLoadPathAltersATableFromAnImportedFile(t *testing.T) {
	c := qt.New(t)
	dir := writeSchemaDir(c, map[string]string{
		"main.sql":  "-- atlas:import ./alter.sql\nCREATE TABLE users (id INTEGER NOT NULL, legacy TEXT);\n",
		"alter.sql": "ALTER TABLE users ADD PRIMARY KEY (id), DROP COLUMN legacy;\n",
	})

	db, err := schemafile.LoadPath(filepath.Join(dir, "main.sql"), schemafile.Options{Dialect: "postgres"})

	c.Assert(err, qt.IsNil)
	c.Assert(fieldNames(db), qt.DeepEquals, []string{"id"})
	c.Assert(fieldByName(db, "id").Primary, qt.IsTrue)
}

// A later file that alters a column no file declares is refused by name.
func TestLoadPathAltersAnEarlierTableFromALaterFile_FailurePath(t *testing.T) {
	c := qt.New(t)
	dir := writeSchemaDir(c, map[string]string{
		"1_a.sql": "CREATE TABLE users (id INTEGER PRIMARY KEY);\n",
		"2_b.sql": "ALTER TABLE users ALTER COLUMN note SET NOT NULL;\n",
	})

	db, err := schemafile.LoadPath(dir, schemafile.Options{Dialect: "postgres"})

	c.Assert(err, qt.ErrorMatches,
		`read SQL schema file .*2_b\.sql: ALTER TABLE users ALTER COLUMN note names a column the table does not declare`)
	c.Assert(db, qt.IsNil)
}
