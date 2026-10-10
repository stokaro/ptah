package schemafile_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/builtintest"
	"ptah.run/internal/schemafile"
)

// A src list is an ordered document, including ALTER statements in later files.
func TestLoadSourcesAltersEarlierSQL(t *testing.T) {
	c := qt.New(t)
	sources := []schemafile.Source{
		{URL: writeDocument(c, "table.sql", "CREATE TABLE users (id INTEGER NOT NULL, legacy TEXT);")},
		{URL: writeDocument(c, "alter.sql", "ALTER TABLE users ADD PRIMARY KEY (id); ALTER TABLE users DROP COLUMN legacy;")},
	}
	db, err := schemafile.LoadSources(sources, schemafile.Options{YAML: builtintest.Runtime().YAML(), Dialect: "postgres"})
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
	db, err := schemafile.LoadSources(sources, schemafile.Options{YAML: builtintest.Runtime().YAML(), Dialect: "postgres"})
	c.Assert(err, qt.ErrorMatches, ".*names a table this schema does not declare")
	c.Assert(db, qt.IsNil)
}

func TestLoadSourcesPreservesImports(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	table := writeImportTree(c, dir, "table.sql", "CREATE TABLE users (id INTEGER NOT NULL);")
	entry := writeImportTree(c, dir, "main.sql", "-- atlas:import ./nested/alter.sql\n")
	writeImportTree(c, dir, "nested/alter.sql", "ALTER TABLE users ADD PRIMARY KEY (id); CREATE TABLE notes (note_id INTEGER PRIMARY KEY);")
	db, err := schemafile.LoadSources([]schemafile.Source{{URL: table}, {URL: entry}}, schemafile.Options{YAML: builtintest.Runtime().YAML(), Dialect: "postgres"})
	c.Assert(err, qt.IsNil)
	c.Assert(tableNames(db), qt.DeepEquals, []string{"notes", "users"})
	c.Assert(fieldByName(db, "id").Primary, qt.IsTrue)
}

func TestLoadSourcesImportFailurePath(t *testing.T) {
	tests := []struct {
		name, directive string
		want            error
	}{
		{name: "cycle", directive: "./main.sql", want: schemafile.ErrSQLImportCycle},
		{name: "escape", directive: "../outside.sql", want: schemafile.ErrSQLImportEscapes},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			entry := writeImportTree(c, c.TempDir(), "main.sql", "-- atlas:import "+test.directive+"\n")
			db, err := schemafile.LoadSources([]schemafile.Source{{URL: entry}}, schemafile.Options{YAML: builtintest.Runtime().YAML(), Dialect: "postgres"})
			c.Assert(err, qt.ErrorIs, test.want)
			c.Assert(db, qt.IsNil)
		})
	}
}
