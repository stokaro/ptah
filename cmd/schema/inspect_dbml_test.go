package schema_test

import (
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
)

// TestSchemaInspectDBMLWritesTheInspectedSchema is the acceptance check for the
// format: a live database read back as DBML.
func TestSchemaInspectDBMLWritesTheInspectedSchema(t *testing.T) {
	c := qt.New(t)
	dbPath := filepath.Join(t.TempDir(), "live.db")
	seedSQLite(c, dbPath, "CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT NOT NULL);")

	out, err := runSchema("", "inspect", "--db-url", "sqlite://"+dbPath, "--format", "dbml")

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, `Table "users" {`)
	c.Assert(out, qt.Contains, `"email"`)
	c.Assert(strings.HasSuffix(out, "\n"), qt.IsTrue)
}

// TestSchemaInspectDBMLReadsTheSameSchemaSQLDoes pins that the two formats
// describe one inspection rather than two.
//
// They share a source deliberately -- the schema the reader described, with the
// schema rows dropped where SQL drops them -- so a table present in one and
// absent from the other would be a divergence inside a single read.
func TestSchemaInspectDBMLReadsTheSameSchemaSQLDoes(t *testing.T) {
	c := qt.New(t)
	dbPath := filepath.Join(t.TempDir(), "live.db")
	seedSQLite(c, dbPath,
		"CREATE TABLE users (id INTEGER PRIMARY KEY);\nCREATE TABLE posts (id INTEGER PRIMARY KEY);")

	asDBML, err := runSchema("", "inspect", "--db-url", "sqlite://"+dbPath, "--format", "dbml")
	c.Assert(err, qt.IsNil)
	asSQL, err := runSchema("", "inspect", "--db-url", "sqlite://"+dbPath, "--format", "sql")
	c.Assert(err, qt.IsNil)

	for _, table := range []string{"users", "posts"} {
		c.Assert(asSQL, qt.Contains, table)
		c.Assert(asDBML, qt.Contains, table)
	}
}

// TestSchemaInspectDBMLIsByteIdenticalAcrossRuns pins the canonical contract
// through the command, where a timestamp or a host would be easiest to leak in.
func TestSchemaInspectDBMLIsByteIdenticalAcrossRuns(t *testing.T) {
	c := qt.New(t)
	dbPath := filepath.Join(t.TempDir(), "live.db")
	seedSQLite(c, dbPath, "CREATE TABLE users (id INTEGER PRIMARY KEY);")

	first, err := runSchema("", "inspect", "--db-url", "sqlite://"+dbPath, "--format", "dbml")
	c.Assert(err, qt.IsNil)
	second, err := runSchema("", "inspect", "--db-url", "sqlite://"+dbPath, "--format", "dbml")
	c.Assert(err, qt.IsNil)

	c.Assert(second, qt.Equals, first)
}

// TestSchemaInspectDBMLRefusesOutDir pins that the split-and-write export stays
// an HCL and SQL affair.
//
// DBML is one document describing a whole schema; splitting it per object would
// produce files that are not DBML documents, and a format that silently wrote
// them would be inventing a convention nobody asked for.
func TestSchemaInspectDBMLRefusesOutDir(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "live.db")
	seedSQLite(c, dbPath, "CREATE TABLE users (id INTEGER PRIMARY KEY);")

	out, err := runSchema("", "inspect",
		"--db-url", "sqlite://"+dbPath, "--format", "dbml", "--out-dir", filepath.Join(dir, "out"))

	c.Assert(err, qt.IsNotNil)
	c.Assert(out+err.Error(), qt.Contains, "hcl and sql formats only")
}
