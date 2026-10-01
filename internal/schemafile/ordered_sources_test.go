package schemafile_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/schemafile"
)

// A src list is an ordered document, including ALTER statements in later files.
func TestLoadSourcesAltersEarlierSQL(t *testing.T) {
	c := qt.New(t)
	sources := []schemafile.Source{
		{URL: writeDocument(c, "table.sql", "CREATE TABLE users (id INTEGER NOT NULL, legacy TEXT);")},
		{URL: writeDocument(c, "alter.sql", "ALTER TABLE users ADD PRIMARY KEY (id); ALTER TABLE users DROP COLUMN legacy;")},
	}
	db, err := schemafile.LoadSources(sources, schemafile.Options{Dialect: "postgres"})
	c.Assert(err, qt.IsNil)
	c.Assert(fieldNames(db), qt.DeepEquals, []string{"id"})
	c.Assert(fieldByName(db, "id").Primary, qt.IsTrue)
}

// A source list preserves caller order; it cannot invent a missing table.
func TestLoadSourcesAltersEarlierSQLFailurePath(t *testing.T) {
	c := qt.New(t)
	sources := []schemafile.Source{
		{URL: writeDocument(c, "alter.sql", "ALTER TABLE users ADD PRIMARY KEY (id);")},
		{URL: writeDocument(c, "table.sql", "CREATE TABLE users (id INTEGER NOT NULL);")},
	}
	db, err := schemafile.LoadSources(sources, schemafile.Options{Dialect: "postgres"})
	c.Assert(err, qt.ErrorMatches, ".*names a table this schema does not declare")
	c.Assert(db, qt.IsNil)
}
