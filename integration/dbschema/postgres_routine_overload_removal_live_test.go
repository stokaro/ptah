//go:build integration

package dbschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

// PostgreSQL identifies a routine by its argument list as well as by its name.
// `DROP FUNCTION f` is refused with `function name "f" is not unique`
// (SQLSTATE 42725) wherever the name is overloaded, and IF EXISTS does not
// help, because the refusal is about ambiguity rather than existence. The
// overload taking no arguments is no exception: `f()` is what names it.

// overloadedRoutines declares `scalar()` and `scalar(n integer)` in one schema.
func overloadedRoutines(c *qt.C, schemaName string) *schemamodel.Database {
	c.Helper()
	description := returnTypeDocument(c, schemaName, "integer")
	withArgument := description.Functions[0]
	withArgument.Parameters = "n integer"
	withArgument.Body = "SELECT n"
	description.Functions = append(description.Functions, withArgument)
	return description
}

// routineWithArgumentAlone declares `scalar(n integer)` and nothing else, so a
// comparison against overloadedRoutines removes the zero-argument overload.
func routineWithArgumentAlone(c *qt.C, schemaName string) *schemamodel.Database {
	c.Helper()
	description := returnTypeDocument(c, schemaName, "integer")
	description.Functions[0].Parameters = "n integer"
	description.Functions[0].Body = "SELECT n"
	return description
}

// TestPostgresLiveZeroArgumentOverloadRemovalApplies plans the removal of the
// overload that takes no arguments and lets the server answer the statement.
//
// The precondition is what makes the test able to fail: both overloads exist
// when the drop runs, so a statement naming the routine alone is ambiguous and
// the server refuses the whole apply. The overload that stays has to survive
// it.
func TestPostgresLiveZeroArgumentOverloadRemovalApplies(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.PostgreSQL)
	c := qt.New(t)

	conn := returnTypeConnection(c, dbURL)
	schemaName := returnTypeSchema(c, conn, "ptah_retremoval")
	applyDeclaration(c, conn, overloadedRoutines(c, schemaName))
	c.Assert(readReturnTypes(c, conn, schemaName), qt.DeepEquals, map[string]string{
		"scalar()":          "integer",
		"scalar(n integer)": "integer",
	})

	live, err := dbschema.ReadSchemaWithSchemasContext(t.Context(), conn, []string{schemaName})
	c.Assert(err, qt.IsNil)
	diff := schemadiff.CompareWithDialect(routineWithArgumentAlone(c, schemaName), live, platform.Postgres)
	c.Assert(diff.FunctionsRemoved, qt.HasLen, 1)
	statements, err := planner.GenerateSchemaDiffSQLStatements(diff, platform.Postgres)
	c.Assert(err, qt.IsNil)

	applyStatements(c, conn, statements)

	c.Assert(readReturnTypes(c, conn, schemaName), qt.DeepEquals, map[string]string{
		"scalar(n integer)": "integer",
	})
}
