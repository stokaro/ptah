package schemafile_test

import (
	"errors"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ptaherr"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/schemafile"
)

// A refusal names the file that holds the statement, and the line and column in
// that file, whichever way the file reached the load. A directory and an import
// each read a file by itself, so a line never counts across files.
func TestLoadPath_RefusalNamesTheFileLineAndColumn(t *testing.T) {
	tests := []struct {
		name       string
		files      map[string]string
		entry      string
		wantErr    string
		wantFile   string
		wantLine   int
		wantColumn int
		wantOffset int
	}{
		{
			name:  "one file, past a multi-line body and multi-byte text",
			files: map[string]string{"schema.sql": "-- Каталог\nCREATE TABLE a (\n  note TEXT DEFAULT 'one\ntwo'\n);\nCREATE TABLE заказы BOGUS;\n"},
			entry: "schema.sql",

			wantErr:    `read SQL schema file .*schema\.sql:6:21: unsupported table option: BOGUS`,
			wantFile:   "schema.sql",
			wantLine:   6,
			wantColumn: 21,
			wantOffset: 94,
		},
		{
			name: "the second file of a directory",
			files: map[string]string{
				"1_a.sql": "CREATE TABLE a (\n  id INTEGER\n);\n",
				"2_b.sql": "\n\nCREATE TABLE b BOGUS;\n",
			},
			entry: "",

			wantErr:    `read SQL schema file .*2_b\.sql:3:16: unsupported table option: BOGUS`,
			wantFile:   "2_b.sql",
			wantLine:   3,
			wantColumn: 16,
			wantOffset: 17,
		},
		{
			name: "an imported file",
			files: map[string]string{
				"main.sql":    "-- atlas:import ./columns.sql\nCREATE TABLE a (\n  id INTEGER\n);\n",
				"columns.sql": "CREATE TABLE b BOGUS;\n",
			},
			entry: "main.sql",

			wantErr:    `read SQL schema file .*columns\.sql:1:16: unsupported table option: BOGUS`,
			wantFile:   "columns.sql",
			wantLine:   1,
			wantColumn: 16,
			wantOffset: 15,
		},
		{
			name: "the file that imports another",
			files: map[string]string{
				"main.sql":    "-- atlas:import ./columns.sql\nCREATE TABLE a (\n  id INTEGER\n);\nCREATE TABLE c BOGUS;\n",
				"columns.sql": "CREATE TABLE b (id INTEGER);\n",
			},
			entry: "main.sql",

			wantErr:    `read SQL schema file .*main\.sql:5:16: unsupported table option: BOGUS`,
			wantFile:   "main.sql",
			wantLine:   5,
			wantColumn: 16,
			wantOffset: 78,
		},
		{
			name:  "a clause the reader refuses, on the line after its statement starts",
			files: map[string]string{"schema.sql": "CREATE TABLE users (id INTEGER PRIMARY KEY);\n\nALTER TABLE users\n  DISABLE ROW LEVEL SECURITY;\n"},
			entry: "schema.sql",

			wantErr:    `read SQL schema file .*schema\.sql:4:3: unsupported ALTER operation: DISABLE ROW LEVEL SECURITY`,
			wantFile:   "schema.sql",
			wantLine:   4,
			wantColumn: 3,
			wantOffset: 66,
		},
		{
			name:  "a refusal of what a statement declares, raised after the parse",
			files: map[string]string{"schema.sql": "CREATE TABLE users (id INTEGER PRIMARY KEY, note TEXT);\n\n  ALTER TABLE users ADD COLUMN note TEXT;\n"},
			entry: "schema.sql",

			wantErr:    `read SQL schema file .*schema\.sql:3:3: ALTER TABLE users ADD COLUMN note names a column the table already declares`,
			wantFile:   "schema.sql",
			wantLine:   3,
			wantColumn: 3,
			wantOffset: 59,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := writeSchemaDir(c, test.files)

			db, err := schemafile.LoadPath(t.Context(), filepath.Join(dir, test.entry), schemafile.Options{YAML: builtintest.Runtime().YAML(), SQL: builtintest.Runtime().SQL(), HCL: builtintest.Runtime().HCL(), Dialect: "postgres"})

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(db, qt.IsNil)
			at, ok := errors.AsType[*ptaherr.ParseError](err)
			c.Assert(ok, qt.IsTrue)
			c.Assert(filepath.Base(at.File), qt.Equals, test.wantFile)
			c.Assert(at.Line, qt.Equals, test.wantLine)
			c.Assert(at.Column, qt.Equals, test.wantColumn)
			c.Assert(at.Offset, qt.Equals, test.wantOffset)
		})
	}
}
