package postgres_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/core/renderer"
	"github.com/stokaro/ptah/migration/planner/dialects/postgres"
	"github.com/stokaro/ptah/migration/schemadiff/types"
)

func TestPlanner_GenerateMigrationAST_FunctionsModified_EmitsCreateOrReplace(t *testing.T) {
	c := qt.New(t)

	diff := &types.SchemaDiff{
		FunctionsModified: []types.FunctionDiff{
			{
				FunctionName: "set_tenant_context",
				Changes: map[string]string{
					"body":     "OLD BODY -> NEW BODY",
					"security": "DEFINER -> INVOKER",
				},
			},
		},
	}
	generated := &goschema.Database{
		Functions: []goschema.Function{
			{
				Name:       "set_tenant_context",
				Parameters: "tenant_id_param TEXT",
				Returns:    "VOID",
				Language:   "plpgsql",
				Security:   "INVOKER",
				Volatility: "VOLATILE",
				Body:       "BEGIN PERFORM set_config('app.current_tenant_id', tenant_id_param, true); END;",
			},
		},
	}

	planner := postgres.New()
	nodes := planner.GenerateMigrationAST(diff, generated)
	c.Assert(nodes, qt.Not(qt.HasLen), 0)

	sql, err := renderer.RenderSQL("postgres", nodes...)
	c.Assert(err, qt.IsNil)

	c.Assert(sql, qt.Contains, "CREATE OR REPLACE FUNCTION set_tenant_context(tenant_id_param TEXT)")
	c.Assert(sql, qt.Contains, "RETURNS VOID")
	c.Assert(sql, qt.Contains, "LANGUAGE plpgsql")
	c.Assert(sql, qt.Contains, "SECURITY INVOKER")
	c.Assert(sql, qt.Contains, "set_config('app.current_tenant_id', tenant_id_param, true)")
	// Diff summary appears as a comment for traceability.
	c.Assert(sql, qt.Contains, "Modify function set_tenant_context")
	c.Assert(sql, qt.Contains, "body, security")
}

func TestPlanner_GenerateMigrationAST_FunctionsModified_SkippedWhenTargetMissing(t *testing.T) {
	c := qt.New(t)

	// FunctionsModified references a function not present in generated.Functions:
	// the planner must skip silently rather than emitting a malformed CREATE.
	diff := &types.SchemaDiff{
		FunctionsModified: []types.FunctionDiff{
			{
				FunctionName: "ghost",
				Changes:      map[string]string{"body": "x -> y"},
			},
		},
	}
	generated := &goschema.Database{}

	planner := postgres.New()
	nodes := planner.GenerateMigrationAST(diff, generated)

	sql, err := renderer.RenderSQL("postgres", nodes...)
	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Not(qt.Contains), "ghost",
		qt.Commentf("planner must not emit SQL for a modified function whose target definition is missing"))
}
