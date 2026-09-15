package postgres_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/renderer"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/planner/dialects/postgres"
	"ptah.run/migration/schemadiff/difftypes"
)

// PostgreSQL refuses CREATE OR REPLACE for a routine whose return type changes,
// with `cannot change return type of existing function` (SQLSTATE 42P13). A plan
// that answered the change with the replacement alone could be applied in
// neither direction (stokaro/ptah#3288).

// renderFunctionModification plans one modified routine and renders what the
// server would be given.
func renderFunctionModification(c *qt.C, change difftypes.FunctionDiff) string {
	c.Helper()
	nodes, err := postgres.New().GenerateMigrationAST(&difftypes.SchemaDiff{
		FunctionsModified: []difftypes.FunctionDiff{change},
	})
	c.Assert(err, qt.IsNil)
	sql, err := renderer.RenderSQL("postgres", nodes...)
	c.Assert(err, qt.IsNil)
	return sql
}

// totalFunction is a routine declaration with one argument.
func totalFunction(parameters, returns string) schemamodel.Function {
	return schemamodel.Function{
		Name:       "billing.total",
		Parameters: parameters,
		Returns:    returns,
		Language:   "sql",
		Security:   "INVOKER",
		Volatility: "VOLATILE",
		Body:       "SELECT 1",
	}
}

func TestPlanner_FunctionReturnTypeChange_DropsTheRoutineBeforeCreatingIt(t *testing.T) {
	c := qt.New(t)

	sql := renderFunctionModification(c, difftypes.FunctionDiff{
		FunctionName:     "billing.total",
		Changes:          map[string]string{"returns": "integer -> bigint"},
		CurrentSignature: "n integer",
		Desired:          totalFunction("n integer", "bigint"),
	})

	c.Assert(sql, qt.Matches,
		`(?s).*DROP FUNCTION "billing"\."total"\(n integer\).*CREATE OR REPLACE FUNCTION "billing"\."total"\(n integer\) RETURNS bigint.*`)
	// The routine was just read, so a drop that matches nothing is a wrong
	// signature and must fail rather than be skipped; and a dependent object
	// must stop the plan rather than be removed with the routine.
	c.Assert(sql, qt.Not(qt.Contains), "IF EXISTS")
	c.Assert(sql, qt.Not(qt.Contains), "CASCADE")
}

// The drop addresses the routine the database holds, not the one the change
// creates. When the arguments change too, the declared list names an overload
// that does not exist yet.
func TestPlanner_FunctionReturnTypeChange_DropsByTheCurrentSignature(t *testing.T) {
	c := qt.New(t)

	sql := renderFunctionModification(c, difftypes.FunctionDiff{
		FunctionName: "billing.total",
		Changes: map[string]string{
			"parameters": "n integer -> n bigint",
			"returns":    "integer -> bigint",
		},
		CurrentSignature: "n integer",
		Desired:          totalFunction("n bigint", "bigint"),
	})

	c.Assert(sql, qt.Matches,
		`(?s).*DROP FUNCTION "billing"\."total"\(n integer\).*CREATE OR REPLACE FUNCTION "billing"\."total"\(n bigint\) RETURNS bigint.*`)
}

// A change a replacement accepts keeps the replacement alone. A drop fails on
// any routine a view, policy or trigger uses, so it is not planned where it is
// not needed.
func TestPlanner_FunctionBodyChange_ReplacesWithoutDropping(t *testing.T) {
	c := qt.New(t)

	sql := renderFunctionModification(c, difftypes.FunctionDiff{
		FunctionName:     "billing.total",
		Changes:          map[string]string{"body": "SELECT 0 -> SELECT 1"},
		CurrentSignature: "n integer",
		Desired:          totalFunction("n integer", "integer"),
	})

	c.Assert(sql, qt.Contains, `CREATE OR REPLACE FUNCTION "billing"."total"(n integer) RETURNS integer`)
	c.Assert(sql, qt.Not(qt.Contains), "DROP FUNCTION")
}
