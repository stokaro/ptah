package schemafile_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/schemafile"
)

// writeImportTree writes one file of an import fixture, creating the
// directories it needs.
func writeImportTree(c *qt.C, dir, name, body string) string {
	c.Helper()
	path := filepath.Join(dir, filepath.FromSlash(name))
	c.Assert(os.MkdirAll(filepath.Dir(path), 0o755), qt.IsNil)
	c.Assert(os.WriteFile(path, []byte(body), 0o600), qt.IsNil)
	return path
}

// TestLoadPathReadsAnImportedSQLFile_HappyPath is the round trip the split
// export promises.
//
// `schema inspect --format '{{ sql . | split | write "out" }}'` writes an
// out/main.sql holding nothing but `-- atlas:import` lines, and Atlas documents
// that file as the way to reference the whole export. Ptah wrote the directive
// and read it nowhere, so the entry point loaded as an empty schema at exit 0
// and a diff against the source database then planned a DROP for every object
// (stokaro/ptah#3110).
func TestLoadPathReadsAnImportedSQLFile_HappyPath(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	writeImportTree(c, dir, "tables/users.sql", "CREATE TABLE users (id INTEGER PRIMARY KEY);\n")
	writeImportTree(c, dir, "tables/orders.sql", "CREATE TABLE orders (id INTEGER PRIMARY KEY);\n")
	main := writeImportTree(c, dir, "main.sql",
		"-- atlas:import ./tables/orders.sql\n-- atlas:import ./tables/users.sql\n")

	db, err := schemafile.LoadPath(main, schemafile.Options{})

	c.Assert(err, qt.IsNil)
	c.Assert(tableNames(db), qt.DeepEquals, []string{"orders", "users"})
}

// TestLoadPathReadsAnEntryPointThatAlsoDeclares_HappyPath pins that an entry
// point is a document, not only a manifest: its own statements are kept beside
// what it imports.
func TestLoadPathReadsAnEntryPointThatAlsoDeclares_HappyPath(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	writeImportTree(c, dir, "tables/users.sql", "CREATE TABLE users (id INTEGER PRIMARY KEY);\n")
	main := writeImportTree(c, dir, "main.sql",
		"-- atlas:import ./tables/users.sql\nCREATE TABLE audit (id INTEGER PRIMARY KEY);\n")

	db, err := schemafile.LoadPath(main, schemafile.Options{})

	c.Assert(err, qt.IsNil)
	c.Assert(tableNames(db), qt.DeepEquals, []string{"audit", "users"})
}

// TestLoadPathReadsANestedImportChain_HappyPath pins that an imported file may
// import in turn, which is what makes the directive a document contract rather
// than a one-level manifest for the export layout Ptah happens to write.
func TestLoadPathReadsANestedImportChain_HappyPath(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	writeImportTree(c, dir, "leaf/users.sql", "CREATE TABLE users (id INTEGER PRIMARY KEY);\n")
	writeImportTree(c, dir, "mid.sql", "-- atlas:import ./leaf/users.sql\n")
	main := writeImportTree(c, dir, "main.sql", "-- atlas:import ./mid.sql\n")

	db, err := schemafile.LoadPath(main, schemafile.Options{})

	c.Assert(err, qt.IsNil)
	c.Assert(tableNames(db), qt.DeepEquals, []string{"users"})
}

// TestLoadPathIgnoresTextThatOnlyResemblesTheDirective_HappyPath is the control
// for the parser.
//
// Without it, "a line mentioning atlas:import is a directive" would pass every
// test above while turning a comment about the feature, and a longer marker
// that merely starts with it, into a file read.
func TestLoadPathIgnoresTextThatOnlyResemblesTheDirective_HappyPath(t *testing.T) {
	rows := []struct {
		name string
		body string
	}{
		{name: "prose about the directive", body: "-- write atlas:import ./nope.sql to import\nCREATE TABLE t (id INTEGER PRIMARY KEY);\n"},
		{name: "longer marker", body: "-- atlas:importer ./nope.sql\nCREATE TABLE t (id INTEGER PRIMARY KEY);\n"},
		{name: "inside a statement", body: "CREATE TABLE t (id INTEGER PRIMARY KEY); -- atlas:import ./nope.sql\n"},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			dir := c.TempDir()
			main := writeImportTree(c, dir, "main.sql", row.body)

			db, err := schemafile.LoadPath(main, schemafile.Options{})

			c.Assert(err, qt.IsNil)
			c.Assert(tableNames(db), qt.DeepEquals, []string{"t"})
		})
	}
}

// TestLoadPathRefusesAnImportThatEscapes_FailurePath pins the confinement.
//
// An entry point is an input a reader may have received rather than written, so
// a directive must not name a file outside the export it belongs to. The
// spellings are refused by shape on every platform, because filepath.IsAbs
// answers false on Windows for "/etc/passwd" and a rule about what a document
// may name must not depend on the machine reading it.
func TestLoadPathRefusesAnImportThatEscapes_FailurePath(t *testing.T) {
	rows := []struct {
		name  string
		value string
	}{
		{name: "parent directory", value: "../outside.sql"},
		{name: "deep parent", value: "./tables/../../outside.sql"},
		{name: "absolute unix", value: "/etc/passwd"},
		{name: "absolute windows separator", value: `\Windows\system.ini`},
		{name: "drive letter", value: `C:\schema.sql`},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			dir := c.TempDir()
			writeImportTree(c, dir, "../outside.sql", "CREATE TABLE outside (id INTEGER PRIMARY KEY);\n")
			main := writeImportTree(c, dir, "main.sql", "-- atlas:import "+row.value+"\n")

			db, err := schemafile.LoadPath(main, schemafile.Options{})

			c.Assert(err, qt.ErrorIs, schemafile.ErrSQLImportEscapes)
			c.Assert(db, qt.IsNil)
		})
	}
}

// TestLoadPathRefusesAnImportCycle_FailurePath pins that a cycle is an error
// rather than a hang.
func TestLoadPathRefusesAnImportCycle_FailurePath(t *testing.T) {
	rows := []struct {
		name  string
		main  string
		other string
	}{
		{name: "self", main: "-- atlas:import ./main.sql\n", other: ""},
		{name: "two files", main: "-- atlas:import ./other.sql\n", other: "-- atlas:import ./main.sql\n"},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			dir := c.TempDir()
			writeImportTree(c, dir, "other.sql", row.other)
			main := writeImportTree(c, dir, "main.sql", row.main)

			db, err := schemafile.LoadPath(main, schemafile.Options{})

			c.Assert(err, qt.ErrorIs, schemafile.ErrSQLImportCycle)
			c.Assert(db, qt.IsNil)
		})
	}
}

// TestLoadPathRefusesAnUnusableImport_FailurePath covers the directives that
// name something the loader cannot read, each reported by name rather than as a
// silently empty schema, which is the failure this whole change exists to end.
func TestLoadPathRefusesAnUnusableImport_FailurePath(t *testing.T) {
	rows := []struct {
		name    string
		body    string
		wantErr string
	}{
		{
			name:    "missing file",
			body:    "-- atlas:import ./gone.sql\n",
			wantErr: `(?s).*atlas:import "\./gone\.sql": schema file does not exist.*`,
		},
		{
			name:    "no path",
			body:    "-- atlas:import\n",
			wantErr: `(?s).*line 1: atlas:import names no file.*`,
		},
		{
			name:    "not a SQL file",
			body:    "-- atlas:import ./schema.yaml\n",
			wantErr: `(?s).*atlas:import "\./schema\.yaml": only \.sql files can be imported.*`,
		},
		{
			name:    "a directory",
			body:    "-- atlas:import ./tables\n",
			wantErr: `(?s).*is a directory.*`,
		},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			dir := c.TempDir()
			writeImportTree(c, dir, "tables/users.sql", "CREATE TABLE users (id INTEGER PRIMARY KEY);\n")
			writeImportTree(c, dir, "schema.yaml", "tables: []\n")
			main := writeImportTree(c, dir, "main.sql", row.body)

			db, err := schemafile.LoadPath(main, schemafile.Options{})

			c.Assert(err, qt.ErrorMatches, row.wantErr)
			c.Assert(db, qt.IsNil)
		})
	}
}
