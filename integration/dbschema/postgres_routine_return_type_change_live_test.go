//go:build integration

package dbschema_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/renderer"
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/internal/atlashcl"
	"ptah.run/internal/dbtarget"
	"ptah.run/migration/generator"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

// PostgreSQL keeps a routine's return type as part of what CREATE OR REPLACE
// may not change. The statement is refused with
// `cannot change return type of existing function (SQLSTATE 42P13)`, so a plan
// that answers a return-type change with a replacement cannot be applied in
// either direction (stokaro/ptah#3288). These tests apply the plan rather than
// reading it, because only the server decides which changes a replacement may
// carry.

// returnTypeDocument declares one zero-argument routine returning the given type.
func returnTypeDocument(c *qt.C, schemaName, returns string) *schemamodel.Database {
	c.Helper()
	description, err := atlashcl.Parse([]byte(`
schema "`+schemaName+`" {}

function "scalar" {
  schema = schema.`+schemaName+`
  lang   = SQL
  return = `+returns+`
  as     = "SELECT 1"
}
`), "schema.hcl")
	c.Assert(err, qt.IsNil)
	return description
}

// returnTypeConnection opens the connection one test uses and closes it after
// every cleanup that test registers.
//
// A deferred close would not: a deferred call runs when the test function
// returns, which is before any t.Cleanup, so the schema drop registered by
// returnTypeSchema would reach a closed pool. Registering the close first puts
// it last, because cleanups run in reverse order of registration.
func returnTypeConnection(c *qt.C, dbURL string) *dbschema.DatabaseConnection {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), dbURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	return conn
}

// returnTypeSchema creates an empty schema for one test and removes it when the
// test ends.
//
// The drop is asserted rather than discarded. The server is shared, so a drop
// that silently failed left the schema for the next test in the job to plan a
// drop for, and its own context is gone by then, which is why this one runs on
// a fresh context.
func returnTypeSchema(c *qt.C, conn *dbschema.DatabaseConnection, prefix string) string {
	c.Helper()
	schemaName := fmt.Sprintf("%s_%d", prefix, time.Now().UnixNano())
	_, err := conn.ExecContext(c.Context(), `CREATE SCHEMA "`+schemaName+`"`)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		_, err := conn.ExecContext(context.Background(), `DROP SCHEMA IF EXISTS "`+schemaName+`" CASCADE`)
		c.Check(err, qt.IsNil)
	})
	return schemaName
}

// applyDeclaration renders a declaration and executes every statement.
func applyDeclaration(c *qt.C, conn *dbschema.DatabaseConnection, description *schemamodel.Database) {
	c.Helper()
	statements, err := renderer.GetOrderedCreateStatements(description, platform.Postgres)
	c.Assert(err, qt.IsNil)
	applyStatements(c, conn, statements)
}

// applyStatements executes planned statements in order, naming the one the
// server refuses.
func applyStatements(c *qt.C, conn *dbschema.DatabaseConnection, statements []string) {
	c.Helper()
	for _, statement := range statements {
		_, err := conn.ExecContext(c.Context(), statement)
		c.Assert(err, qt.IsNil, qt.Commentf("statement:\n%s", statement))
	}
}

// planReturnTypeChange compares a declaration with the live schema and plans
// both directions of the change the way `ptah migrations generate` does.
func planReturnTypeChange(
	c *qt.C,
	wanted *schemamodel.Database,
	live *catalog.Database,
) (forward, reverse []string) {
	c.Helper()
	diff := schemadiff.CompareWithDialect(wanted, live, platform.Postgres)
	c.Assert(diff.FunctionsModified, qt.HasLen, 1)

	plan, err := generator.PlanBidirectionalSchemaDiff(generator.BidirectionalSchemaPlanOptions{
		Diff:          diff,
		DesiredSchema: wanted,
		CurrentSchema: live,
		Dialect:       platform.Postgres,
		Policy: generator.BidirectionalPlanPolicy{
			Create: generator.ConcurrentIndexDisabled,
			Drop:   generator.ConcurrentIndexDisabled,
		},
	})
	c.Assert(err, qt.IsNil)

	forward, err = planner.GenerateSchemaDiffSQLStatements(plan.Forward.Diff, platform.Postgres)
	c.Assert(err, qt.IsNil)
	reverse, err = planner.GenerateSchemaDiffSQLStatements(plan.Reverse.Diff, platform.Postgres)
	c.Assert(err, qt.IsNil)
	return forward, reverse
}

// readReturnTypes reads the schema back and reports each routine's return type
// by its identity arguments, which is what tells two overloads apart.
func readReturnTypes(c *qt.C, conn *dbschema.DatabaseConnection, schemaName string) map[string]string {
	c.Helper()
	live, err := dbschema.ReadSchemaWithSchemasContext(c.Context(), conn, []string{schemaName})
	c.Assert(err, qt.IsNil)
	returns := make(map[string]string, len(live.Functions))
	for _, function := range live.Functions {
		c.Assert(function.IdentityArguments, qt.IsNotNil)
		returns[function.Name+"("+*function.IdentityArguments+")"] = function.Returns
	}
	return returns
}

// TestPostgresLiveRoutineReturnTypeChangeApplies applies the forward plan for a
// return-type change and checks that the routine converges on the declaration.
func TestPostgresLiveRoutineReturnTypeChangeApplies(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.PostgreSQL)
	c := qt.New(t)

	conn := returnTypeConnection(c, dbURL)

	schemaName := returnTypeSchema(c, conn, "ptah_retchange")
	applyDeclaration(c, conn, returnTypeDocument(c, schemaName, "integer"))

	live, err := dbschema.ReadSchemaWithSchemasContext(t.Context(), conn, []string{schemaName})
	c.Assert(err, qt.IsNil)
	wanted := returnTypeDocument(c, schemaName, "bigint")
	forward, _ := planReturnTypeChange(c, wanted, live)

	applyStatements(c, conn, forward)

	c.Assert(readReturnTypes(c, conn, schemaName), qt.DeepEquals, map[string]string{"scalar()": "bigint"})
	after, err := dbschema.ReadSchemaWithSchemasContext(t.Context(), conn, []string{schemaName})
	c.Assert(err, qt.IsNil)
	settled := schemadiff.CompareWithDialect(wanted, after, platform.Postgres)
	c.Assert(settled.FunctionsModified, qt.HasLen, 0)
	c.Assert(settled.FunctionsAdded, qt.HasLen, 0)
	c.Assert(settled.FunctionsRemoved, qt.HasLen, 0)
}

// TestPostgresLiveRoutineReturnTypeRollbackApplies applies the reverse plan on
// its own, against a database moved to the forward state by hand, so the
// rollback is measured independently of the forward statements.
func TestPostgresLiveRoutineReturnTypeRollbackApplies(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.PostgreSQL)
	c := qt.New(t)

	conn := returnTypeConnection(c, dbURL)

	schemaName := returnTypeSchema(c, conn, "ptah_retrollback")
	applyDeclaration(c, conn, returnTypeDocument(c, schemaName, "integer"))

	live, err := dbschema.ReadSchemaWithSchemasContext(t.Context(), conn, []string{schemaName})
	c.Assert(err, qt.IsNil)
	_, reverse := planReturnTypeChange(c, returnTypeDocument(c, schemaName, "bigint"), live)

	applyStatements(c, conn, []string{
		`DROP FUNCTION "` + schemaName + `".scalar()`,
		`CREATE FUNCTION "` + schemaName + `".scalar() RETURNS bigint LANGUAGE sql AS 'SELECT 1'`,
	})

	applyStatements(c, conn, reverse)

	c.Assert(readReturnTypes(c, conn, schemaName), qt.DeepEquals, map[string]string{"scalar()": "integer"})
}

// TestPostgresLiveOverloadedRoutineReturnTypeChangeApplies changes the return
// type of one overload while the other stays.
//
// The drop half of the rebuild has to name the overload it removes. Both
// overloads exist when it runs, and PostgreSQL refuses a bare name then with
// `function name "..." is not unique (SQLSTATE 42725)`. The overload that does
// not change has to survive both directions untouched.
func TestPostgresLiveOverloadedRoutineReturnTypeChangeApplies(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.PostgreSQL)
	c := qt.New(t)

	conn := returnTypeConnection(c, dbURL)

	schemaName := returnTypeSchema(c, conn, "ptah_retoverload")
	overloaded := func(returns string) *schemamodel.Database {
		description := returnTypeDocument(c, schemaName, "integer")
		overload := description.Functions[0]
		overload.Parameters = "n integer"
		overload.Returns = returns
		overload.Body = "SELECT n"
		description.Functions = append(description.Functions, overload)
		return description
	}
	applyDeclaration(c, conn, overloaded("integer"))

	live, err := dbschema.ReadSchemaWithSchemasContext(t.Context(), conn, []string{schemaName})
	c.Assert(err, qt.IsNil)
	forward, reverse := planReturnTypeChange(c, overloaded("bigint"), live)

	applyStatements(c, conn, forward)
	c.Assert(readReturnTypes(c, conn, schemaName), qt.DeepEquals, map[string]string{
		"scalar()":          "integer",
		"scalar(n integer)": "bigint",
	})

	applyStatements(c, conn, reverse)
	c.Assert(readReturnTypes(c, conn, schemaName), qt.DeepEquals, map[string]string{
		"scalar()":          "integer",
		"scalar(n integer)": "integer",
	})
}
