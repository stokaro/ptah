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
		CurrentSignature: new("n integer"),
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
		CurrentSignature: new("n integer"),
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
		CurrentSignature: new("n integer"),
		Desired:          totalFunction("n integer", "integer"),
	})

	c.Assert(sql, qt.Contains, `CREATE OR REPLACE FUNCTION "billing"."total"(n integer) RETURNS integer`)
	c.Assert(sql, qt.Not(qt.Contains), "DROP FUNCTION")
}

// A rebuilt routine that takes no arguments is dropped as `f()`.
//
// Both overloads are present when the drop runs, so the statement has to select
// one of them. The empty list is what names the routine being rebuilt; the bare
// name is refused with `function name "total" is not unique`.
func TestPlanner_FunctionReturnTypeChange_DropsAZeroArgumentRoutineByItsEmptyList(t *testing.T) {
	c := qt.New(t)

	sql := renderFunctionModification(c, difftypes.FunctionDiff{
		FunctionName:     "billing.total",
		Changes:          map[string]string{"returns": "integer -> bigint"},
		CurrentSignature: new(""),
		Desired:          totalFunction("", "bigint"),
	})

	c.Assert(sql, qt.Contains, `DROP FUNCTION "billing"."total"()`)
}

// A parameter change alone rebuilds the routine too, for two reasons the server
// gives separately: a rename is refused with `cannot change name of input
// parameter` (42P13), and a type change is ACCEPTED and leaves a second
// overload behind, so the declaration of one routine ends with two
// (stokaro/ptah#3327).
func TestPlanner_FunctionParameterChange_DropsTheRoutineBeforeCreatingIt(t *testing.T) {
	tests := []struct {
		name    string
		changes map[string]string
		current string
		desired schemamodel.Function
		want    string
	}{
		{
			name:    "a renamed parameter",
			changes: map[string]string{"parameters": "n integer -> m integer"},
			current: "n integer",
			desired: totalFunction("m integer", "integer"),
			want: `(?s).*DROP FUNCTION "billing"\."total"\(n integer\).*` +
				`CREATE OR REPLACE FUNCTION "billing"\."total"\(m integer\) RETURNS integer.*`,
		},
		{
			name:    "a retyped parameter",
			changes: map[string]string{"parameters": "n integer -> n bigint"},
			current: "n integer",
			desired: totalFunction("n bigint", "integer"),
			want: `(?s).*DROP FUNCTION "billing"\."total"\(n integer\).*` +
				`CREATE OR REPLACE FUNCTION "billing"\."total"\(n bigint\) RETURNS integer.*`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			sql := renderFunctionModification(c, difftypes.FunctionDiff{
				FunctionName:     "billing.total",
				Changes:          tt.changes,
				CurrentSignature: new(tt.current),
				Desired:          tt.desired,
			})

			c.Assert(sql, qt.Matches, tt.want)
			c.Assert(sql, qt.Not(qt.Contains), "IF EXISTS")
			c.Assert(sql, qt.Not(qt.Contains), "CASCADE")
		})
	}
}
