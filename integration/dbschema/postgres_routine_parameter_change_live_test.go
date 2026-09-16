//go:build integration

package dbschema_test

// What a plan does when a routine's PARAMETER list changes, which is the other
// half of what CREATE OR REPLACE may not do.
//
// Measured on PostgreSQL 18 with the routine `scalar(n integer)` in place:
//
//	CREATE OR REPLACE ... scalar(m integer)  -> ERROR: cannot change name of
//	                                            input parameter "n" (42P13)
//	CREATE OR REPLACE ... scalar(n bigint)   -> accepted, and the schema then
//	                                            holds scalar(integer) AND
//	                                            scalar(bigint)
//
// The second is the one worth a live test: nothing fails, so only the catalog
// says the declaration of one routine left two behind (stokaro/ptah#3327).

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/internal/atlashcl"
	"ptah.run/internal/dbtarget"
	"ptah.run/migration/schemadiff"
)

// parameterDocument declares one routine with the given parameter list and a
// body that only uses its own parameter, so a rename has to reach the body too.
func parameterDocument(c *qt.C, schemaName, parameter, parameterType, body string) *schemamodel.Database {
	c.Helper()
	description, err := atlashcl.Parse([]byte(`
schema "`+schemaName+`" {}

function "scalar" {
  schema = schema.`+schemaName+`
  lang   = SQL
  arg "`+parameter+`" {
    type = `+parameterType+`
  }
  return = integer
  as     = "`+body+`"
}
`), "schema.hcl")
	c.Assert(err, qt.IsNil)
	return description
}

// TestPostgresLiveRoutineParameterRenameApplies is the refusal: PostgreSQL will
// not rename an input parameter through a replacement.
func TestPostgresLiveRoutineParameterRenameApplies(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.PostgreSQL)
	c := qt.New(t)

	conn := returnTypeConnection(c, dbURL)
	schemaName := returnTypeSchema(c, conn, "ptah_paramrename")
	applyDeclaration(c, conn, parameterDocument(c, schemaName, "n", "integer", "SELECT n"))

	live, err := dbschema.ReadSchemaWithSchemasContext(t.Context(), conn, []string{schemaName})
	c.Assert(err, qt.IsNil)
	wanted := parameterDocument(c, schemaName, "m", "integer", "SELECT m")
	forward, _ := planReturnTypeChange(c, wanted, live)

	applyStatements(c, conn, forward)

	after, err := dbschema.ReadSchemaWithSchemasContext(t.Context(), conn, []string{schemaName})
	c.Assert(err, qt.IsNil)
	c.Assert(after.Functions, qt.HasLen, 1)
	settled := schemadiff.CompareWithDialect(wanted, after, platform.Postgres)
	c.Assert(settled.FunctionsModified, qt.HasLen, 0)
	c.Assert(settled.FunctionsAdded, qt.HasLen, 0)
	c.Assert(settled.FunctionsRemoved, qt.HasLen, 0)
}

// TestPostgresLiveRoutineParameterTypeChangeApplies is the silent one.
//
// PostgreSQL accepts a replacement that changes a parameter's type, and creates
// a second overload rather than changing the first. The declaration names one
// routine, so the catalog holding two is the defect, and nothing in the apply
// reports it.
func TestPostgresLiveRoutineParameterTypeChangeApplies(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.PostgreSQL)
	c := qt.New(t)

	conn := returnTypeConnection(c, dbURL)
	schemaName := returnTypeSchema(c, conn, "ptah_paramtype")
	applyDeclaration(c, conn, parameterDocument(c, schemaName, "n", "integer", "SELECT n"))

	live, err := dbschema.ReadSchemaWithSchemasContext(t.Context(), conn, []string{schemaName})
	c.Assert(err, qt.IsNil)
	wanted := parameterDocument(c, schemaName, "n", "bigint", "SELECT n::integer")
	forward, _ := planReturnTypeChange(c, wanted, live)

	applyStatements(c, conn, forward)

	// One routine was declared, so one has to be there. Under a replacement the
	// schema holds scalar(integer) and scalar(bigint).
	after, err := dbschema.ReadSchemaWithSchemasContext(t.Context(), conn, []string{schemaName})
	c.Assert(err, qt.IsNil)
	c.Assert(after.Functions, qt.HasLen, 1)
	c.Assert(readReturnTypes(c, conn, schemaName), qt.DeepEquals, map[string]string{"scalar(n bigint)": "integer"})
	settled := schemadiff.CompareWithDialect(wanted, after, platform.Postgres)
	c.Assert(settled.FunctionsModified, qt.HasLen, 0)
	c.Assert(settled.FunctionsAdded, qt.HasLen, 0)
	c.Assert(settled.FunctionsRemoved, qt.HasLen, 0)
}
