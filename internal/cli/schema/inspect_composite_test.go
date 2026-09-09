package schema_test

import (
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

// The two halves of one schema, written as separate files. The second names a
// foreign key into the first, so a run that kept only one of them cannot
// produce both tables by luck.
const (
	inspectCompositeAuthors = "CREATE TABLE authors (id INTEGER PRIMARY KEY);\n"
	inspectCompositeBooks   = "CREATE TABLE books (\n" +
		"  id INTEGER PRIMARY KEY,\n" +
		"  author_id INTEGER NOT NULL REFERENCES authors(id)\n" +
		");\n"
)

// TestSchemaInspectMergesRepeatedSchemaFiles_HappyPath holds the flag to what
// its six siblings do (stokaro/ptah#3112).
//
// --schema-file was a scalar here, so cobra kept the last value and the first
// file was discarded without a word. Both orders are asserted because the
// defect wore a different face in each: with the referencing file last the run
// exited 0 having quietly dropped a table, and with it first the run failed
// with `invalid foreign key: field "author_id" references unknown table
// "authors"` -- a refusal that blames the schema for a flag the command threw
// away.
func TestSchemaInspectMergesRepeatedSchemaFiles_HappyPath(t *testing.T) {
	tests := []struct {
		name  string
		first string
		last  string
	}{
		{name: "referenced table first", first: "authors.sql", last: "books.sql"},
		{name: "referencing table first", first: "books.sql", last: "authors.sql"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := t.TempDir()
			paths := map[string]string{
				"authors.sql": writeSchemaSQLFile(c, dir, "authors.sql", inspectCompositeAuthors),
				"books.sql":   writeSchemaSQLFile(c, dir, "books.sql", inspectCompositeBooks),
			}

			out, err := runSchema("", "inspect",
				"--schema-file", paths[test.first],
				"--schema-file", paths[test.last],
				"--dev-url", "sqlite://"+filepath.Join(dir, "dev.db"),
				"--format", "hcl",
			)

			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			c.Assert(out, qt.Contains, `table "authors"`)
			c.Assert(out, qt.Contains, `table "books"`)
		})
	}
}

// TestSchemaInspectAcceptsOneSchemaFile_HappyPath is the control: making the
// flag repeatable must not change what a single value does.
func TestSchemaInspectAcceptsOneSchemaFile_HappyPath(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	schemaPath := writeSchemaSQLFile(c, dir, "authors.sql", inspectCompositeAuthors)

	out, err := runSchema("", "inspect",
		"--schema-file", schemaPath,
		"--dev-url", "sqlite://"+filepath.Join(dir, "dev.db"),
		"--format", "hcl",
	)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, `table "authors"`)
	c.Assert(out, qt.Not(qt.Contains), `table "books"`)
}

// TestSchemaInspectBlankSchemaFileIsNotASource_FailurePath keeps a value that
// carries nothing from counting as one. Without the trim a `--schema-file ""`
// reached the classifier and the refusal named a path nobody typed.
func TestSchemaInspectBlankSchemaFileIsNotASource_FailurePath(t *testing.T) {
	c := qt.New(t)

	out, err := runSchema("", "inspect", "--schema-file", "   ")

	c.Assert(err, qt.IsNotNil)
	c.Assert(out+err.Error(), qt.Contains, "an inspection source is required")
}
