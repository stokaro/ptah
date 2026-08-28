package postgres_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/renderer"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/internal/planner/dialects/postgres"
	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
)

func TestPlanner_GenerateMigrationAST_FunctionsModified_EmitsCreateOrReplace(t *testing.T) {
	c := qt.New(t)

	diff := &difftypes.SchemaDiff{
		FunctionsModified: []difftypes.FunctionDiff{
			{
				FunctionName: "set_tenant_context",
				Changes: map[string]string{
					"body":     "OLD BODY -> NEW BODY",
					"security": "DEFINER -> INVOKER",
				},
				// The definition travels with the change, so the schema below
				// is empty (stokaro/ptah#2315).
				Desired: schemamodel.Function{
					Name:       "set_tenant_context",
					Parameters: "tenant_id_param TEXT",
					Returns:    "VOID",
					Language:   "plpgsql",
					Security:   "INVOKER",
					Volatility: "VOLATILE",
					Body:       "BEGIN PERFORM set_config('app.current_tenant_id', tenant_id_param, true); END;",
				},
			},
		},
	}

	planner := postgres.New()
	nodes, err := planner.GenerateMigrationAST(diff, &schemamodel.Database{})
	c.Assert(err, qt.IsNil)
	c.Assert(nodes, qt.Not(qt.HasLen), 0)

	sql, err := renderer.RenderSQL("postgres", nodes...)
	c.Assert(err, qt.IsNil)
	sql = legacyRenderedSQL(sql)

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

	// A FunctionsModified entry carrying no definition: the planner must skip
	// silently rather than emitting a malformed CREATE.
	diff := &difftypes.SchemaDiff{
		FunctionsModified: []difftypes.FunctionDiff{
			{
				FunctionName: "ghost",
				Changes:      map[string]string{"body": "x -> y"},
			},
		},
	}
	desired := &schemamodel.Database{}

	planner := postgres.New()
	nodes, err := planner.GenerateMigrationAST(diff, desired)
	c.Assert(err, qt.IsNil)

	sql, err := renderer.RenderSQL("postgres", nodes...)
	c.Assert(err, qt.IsNil)
	sql = legacyRenderedSQL(sql)
	c.Assert(sql, qt.Not(qt.Contains), "ghost",
		qt.Commentf("planner must not emit SQL for a modified function the change carries no definition for"))
}
