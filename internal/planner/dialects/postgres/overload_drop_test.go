package postgres_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/renderer"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/planner/dialects/postgres"
	"ptah.run/migration/schemadiff/difftypes"
)

// droppedRoutineSQL plans the removals and returns the statements as one string.
func droppedRoutineSQL(c *qt.C, diff *difftypes.SchemaDiff) string {
	c.Helper()

	nodes, err := postgres.New().GenerateMigrationAST(diff)
	c.Assert(err, qt.IsNil)

	var statements []string
	for _, node := range nodes {
		rendered, err := renderer.RenderSQL("postgres", node)
		c.Assert(err, qt.IsNil)
		statements = append(statements, rendered)
	}
	return strings.Join(statements, "\n")
}

// TestRemoveFunctions_ADroppedOverloadNamesItsArguments pins the statement that
// selects one overload of a name.
//
// A name alone does not. Measured on PostgreSQL 18.6 with two overloads of `f`,
// `DROP FUNCTION IF EXISTS f` answers
//
//	ERROR:  function name "f" is not unique
//	HINT:  Specify the argument list to select the function unambiguously.
//
// and IF EXISTS does not help, because the refusal is about ambiguity rather
// than existence -- so a schema that overloads could not have one removed by
// `schema apply` at all (stokaro/ptah#2296).
func TestRemoveFunctions_ADroppedOverloadNamesItsArguments(t *testing.T) {
	c := qt.New(t)

	sql := droppedRoutineSQL(c, &difftypes.SchemaDiff{
		// The signature travels WITH the removal now; it used to need a
		// parallel list beside this one (stokaro/ptah#2315).
		FunctionsRemoved: difftypes.FunctionChanges{
			{Function: schemamodel.Function{Name: "f"}, Signature: "a text"},
		},
	})

	c.Assert(sql, qt.Contains, `DROP FUNCTION IF EXISTS "f"(a text)`)
}

// TestRemoveFunctions_ADroppedProcedureNamesItsArguments is the same for the
// other routine kind, which takes its own verb.
func TestRemoveFunctions_ADroppedProcedureNamesItsArguments(t *testing.T) {
	c := qt.New(t)

	sql := droppedRoutineSQL(c, &difftypes.SchemaDiff{
		ProceduresRemoved: difftypes.FunctionChanges{
			{Function: schemamodel.Function{Name: "p"}, Signature: "a integer"},
		},
	})

	c.Assert(sql, qt.Contains, "DROP PROCEDURE")
	c.Assert(sql, qt.Contains, "(a integer)")
}

// TestRemoveFunctions_ARoutineWithNoSignatureIsStillDropped is the control on
// the fallback.
//
// A SchemaDiff can be decoded from JSON written before the richer list existed,
// and a bare name still drops a routine correctly on every schema that does not
// overload it. Requiring a signature would turn a working plan into no plan.
func TestRemoveFunctions_ARoutineWithNoSignatureIsStillDropped(t *testing.T) {
	c := qt.New(t)

	sql := droppedRoutineSQL(c, &difftypes.SchemaDiff{FunctionsRemoved: difftypes.FunctionChanges{{Function: schemamodel.Function{Name: "solo"}}}})

	c.Assert(sql, qt.Contains, `DROP FUNCTION IF EXISTS "solo"`)
	c.Assert(sql, qt.Not(qt.Contains), "()")
}
