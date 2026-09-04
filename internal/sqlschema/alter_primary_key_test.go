package sqlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/internal/parser"
	"go.5x5.cz/ptah/internal/sqlschema"
)

// parseDialectToDatabase reads one SQL document as a named dialect. The
// dialect matters here: `a(7)` is MySQL's prefix length, and a reader with no
// dialect does not produce the same key parts.
func parseDialectToDatabase(c *qt.C, dialect, sql string) schemamodel.Database {
	c.Helper()
	statements, err := parser.NewParser(sql, parser.WithDialect(dialect)).Parse()
	c.Assert(err, qt.IsNil)
	database, err := sqlschema.ToDatabase(statements, "")
	c.Assert(err, qt.IsNil)
	return database
}

const (
	alterAddPrimaryKeySQL = "CREATE TABLE t (a VARCHAR(32) NOT NULL);\n" +
		"ALTER TABLE t ADD PRIMARY KEY (a(7) DESC);"
	inlinePrimaryKeySQL = "CREATE TABLE t (a VARCHAR(32) NOT NULL, PRIMARY KEY (a(7) DESC));"
)

// TestToDatabase_AlterTableAddPrimaryKeyReachesTheTable covers
// stokaro/ptah#2772.
//
// `ToConstraint` declines a PRIMARY KEY on purpose, because a table-level one
// belongs on [schemamodel.Table.PrimaryKey] rather than in the constraint list.
// That reasoning is a CREATE TABLE's, and the ALTER path called the same
// function: the key was declined there too and nothing else collected it, so
// parsing, conversion and rendering all reported success and the desired schema
// had no primary key at all.
//
// Both engines accept the statement and keep the prefix length and the
// direction, so this is semantic loss rather than a construct no server has.
func TestToDatabase_AlterTableAddPrimaryKeyReachesTheTable(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
	}{
		{name: "mysql", dialect: "mysql"},
		{name: "mariadb", dialect: "mariadb"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			database := parseDialectToDatabase(c, test.dialect, alterAddPrimaryKeySQL)

			c.Assert(database.Tables, qt.HasLen, 1)
			c.Assert(database.Tables[0].PrimaryKey, qt.DeepEquals, []string{"a"})
			c.Assert(database.Tables[0].PrimaryKeyParts, qt.DeepEquals, []schemamodel.PrimaryKeyPart{
				{Name: "a", Prefix: "7", Desc: true},
			})
		})
	}
}

// TestToDatabase_AnAlteredPrimaryKeyMatchesAnInlineOne is the assertion that
// keeps the repair from inventing a second representation.
//
// Making the ALTER path merely produce *something* is easy, and a test that
// only asserts "the key is not empty" would accept a key stored differently
// from the one a CREATE TABLE produces -- which every later comparison,
// renderer and planner reads. The two documents declare the same table, so the
// two models must be equal, field flags included.
func TestToDatabase_AnAlteredPrimaryKeyMatchesAnInlineOne(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
	}{
		{name: "mysql", dialect: "mysql"},
		{name: "mariadb", dialect: "mariadb"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			altered := parseDialectToDatabase(c, test.dialect, alterAddPrimaryKeySQL)
			inline := parseDialectToDatabase(c, test.dialect, inlinePrimaryKeySQL)

			c.Assert(altered.Tables, qt.DeepEquals, inline.Tables)
			c.Assert(altered.Fields, qt.DeepEquals, inline.Fields)
			c.Assert(altered.Constraints, qt.DeepEquals, inline.Constraints)
		})
	}
}

// TestToDatabase_AnAlteredPrimaryKeyOnAnUndeclaredTableIsRefused keeps the
// repair from replacing one silent loss with another.
//
// A primary key has nowhere to go without its table -- unlike a foreign key,
// which lives in the constraint list and is refused later by the renderer with
// "has no owning table". Dropping it here instead would leave a second hole of
// exactly the kind this change closes, and the document is not one any engine
// would execute either: ALTER TABLE on a table that does not exist is an error
// on the server too.
func TestToDatabase_AnAlteredPrimaryKeyOnAnUndeclaredTableIsRefused(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
	}{
		{name: "mysql", dialect: "mysql"},
		{name: "mariadb", dialect: "mariadb"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			statements, err := parser.NewParser(
				"ALTER TABLE nosuch ADD PRIMARY KEY (a);",
				parser.WithDialect(test.dialect),
			).Parse()
			c.Assert(err, qt.IsNil)

			_, err = sqlschema.ToDatabase(statements, "")
			c.Assert(err, qt.ErrorIs, sqlschema.ErrUnmodeledStatement)
			c.Assert(err.Error(), qt.Contains, "nosuch")
		})
	}
}

// TestToDatabase_AnAlteredSingleColumnPrimaryKeyMarksItsField pins the half of
// the CREATE TABLE behavior that lives on the column rather than on the table.
//
// A one-column primary key sets that column's Primary flag, and renderers read
// the flag rather than the table's list when they write the column line. An
// ALTER path that filled only Table.PrimaryKey would render a table whose
// column is not primary and whose key is declared twice over.
func TestToDatabase_AnAlteredSingleColumnPrimaryKeyMarksItsField(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
	}{
		{name: "mysql", dialect: "mysql"},
		{name: "mariadb", dialect: "mariadb"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			database := parseDialectToDatabase(c, test.dialect, alterAddPrimaryKeySQL)

			c.Assert(database.Fields, qt.HasLen, 1)
			c.Assert(database.Fields[0].Name, qt.Equals, "a")
			c.Assert(database.Fields[0].Primary, qt.IsTrue)
		})
	}
}
