package sqlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/internal/parser"
	"go.5x5.cz/ptah/internal/sqlschema"
)

// TestToDatabase_RefusesARoutineWhoseBodyNothingParsed is stokaro/ptah#2435.
//
// A MySQL routine whose body the parser did not model becomes an opaque
// routine: the outer boundary is understood, the body is kept as text nothing
// read. It reached this package and was dropped, so a desired schema was one
// procedure short and every comparison against it reported no difference.
//
// Refusing costs a read that fails on one exotic routine. Dropping costs a
// migration that removes the routine, which is the outcome nobody can see
// coming.
func TestToDatabase_RefusesARoutineWhoseBodyNothingParsed(t *testing.T) {
	c := qt.New(t)

	statements, err := parser.NewParser(
		"CREATE PROCEDURE bump() SET @counter = @counter + 1;",
		parser.WithDialect("mysql")).Parse()
	c.Assert(err, qt.IsNil)

	_, err = sqlschema.ToDatabase(statements, "mysql")

	c.Assert(err, qt.ErrorIs, sqlschema.ErrUnmodeledStatement)
	// Named in the author's terms, and quoting the statement, because "*ast.
	// OpaqueRoutineNode" sends a reader to this repository rather than to their
	// own file.
	c.Assert(err.Error(), qt.Contains, "a mysql procedure whose body was kept as text")
	c.Assert(err.Error(), qt.Contains, "CREATE PROCEDURE bump()")
}

// TestToDatabase_AModeledRoutineStillReachesTheModel is the control.
//
// The same dialect, the same verb, a body the parser reads. Without it, a
// refusal written to reject every MySQL routine would pass the test above.
func TestToDatabase_AModeledRoutineStillReachesTheModel(t *testing.T) {
	c := qt.New(t)

	statements, err := parser.NewParser(
		"CREATE PROCEDURE bump() BEGIN SET @counter = 1; END;",
		parser.WithDialect("mysql")).Parse()
	c.Assert(err, qt.IsNil)

	database, err := sqlschema.ToDatabase(statements, "mysql")

	c.Assert(err, qt.IsNil)
	c.Assert(database.Functions, qt.HasLen, 1)
	c.Assert(database.Functions[0].Name, qt.Equals, "bump")
}

// TestToDatabase_AStatementThatNamesNoObjectIsNotRefused keeps the refusal from
// becoming a refusal of everything this package does not append.
//
// Each of these is a decision rather than an omission: a DROP names an object
// by its absence, which a desired schema expresses by not declaring it, and a DO
// block does work rather than declare a thing. They have cases of their own, so
// the default means "nobody decided" and not "somebody decided not to".
func TestToDatabase_AStatementThatNamesNoObjectIsNotRefused(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		dialect string
	}{
		{name: "a dropped table", sql: "DROP TABLE users;", dialect: "postgres"},
		{name: "a dropped index", sql: "DROP INDEX idx_users_email;", dialect: "postgres"},
		{name: "a DO block", sql: "DO $$ BEGIN PERFORM 1; END $$;", dialect: "postgres"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := parser.NewParser(test.sql, parser.WithDialect(test.dialect)).Parse()
			c.Assert(err, qt.IsNil)

			database, err := sqlschema.ToDatabase(statements, test.dialect)

			c.Assert(err, qt.IsNil)
			c.Assert(database.Tables, qt.HasLen, 0)
			c.Assert(database.Indexes, qt.HasLen, 0)
		})
	}
}

// TestToDatabase_TheRefusalCarriesNothingHalfBuilt keeps a failed read from
// answering with the objects it managed to reach before the one it could not.
//
// A partial database is the worst of the two outcomes this issue is about: it
// is a desired schema missing exactly one object, offered as though it were
// whole.
func TestToDatabase_TheRefusalCarriesNothingHalfBuilt(t *testing.T) {
	c := qt.New(t)

	statements, err := parser.NewParser(
		"CREATE TABLE counters (id INT);\n"+
			"CREATE PROCEDURE bump() SET @counter = @counter + 1;",
		parser.WithDialect("mysql")).Parse()
	c.Assert(err, qt.IsNil)

	database, err := sqlschema.ToDatabase(statements, "mysql")

	c.Assert(err, qt.ErrorIs, sqlschema.ErrUnmodeledStatement)
	c.Assert(database, qt.DeepEquals, schemamodel.Database{})
}
