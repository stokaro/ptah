package schemafile_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/schemafile"
)

// TestLoadPath_ReportsARoutineNothingModeled is the half that reaches a person.
//
// The converter refuses, and this is the assertion that the refusal is not
// swallowed on the way out: a schema file carrying a routine whose body nothing
// parsed fails the read, names the file, and quotes the statement.
//
// Silently dropping it produced a desired schema one procedure short, which
// compares clean against a database that has it and plans its removal against
// one that does not (stokaro/ptah#2435).
func TestLoadPath_ReportsARoutineNothingModeled(t *testing.T) {
	c := qt.New(t)

	path := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(path, []byte(
		"CREATE TABLE counters (id INT);\n"+
			"CREATE PROCEDURE bump() SET @counter = @counter + 1;\n"), 0o600), qt.IsNil)

	_, err := schemafile.LoadPath(path, schemafile.Options{Dialect: "mysql"})

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "schema.sql")
	c.Assert(err.Error(), qt.Contains, "a mysql procedure whose body was kept as text")
}

// TestLoadPath_ReadsTheSameFileWithoutIt is the control. The refusal has to be
// about the routine and not about the file, the dialect, or MySQL.
func TestLoadPath_ReadsTheSameFileWithoutIt(t *testing.T) {
	c := qt.New(t)

	path := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(path, []byte(
		"CREATE TABLE counters (id INT);\n"+
			"CREATE PROCEDURE bump() BEGIN SET @counter = 1; END;\n"), 0o600), qt.IsNil)

	database, err := schemafile.LoadPath(path, schemafile.Options{Dialect: "mysql"})

	c.Assert(err, qt.IsNil)
	c.Assert(database.Tables, qt.HasLen, 1)
	c.Assert(database.Functions, qt.HasLen, 1)
}
