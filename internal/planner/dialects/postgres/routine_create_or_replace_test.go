package postgres_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/internal/planner/dialects/postgres"
	"ptah.run/migration/schemadiff/difftypes"
)

// renderPlan plans a diff and renders what the server would be given.
func renderPlan(c *qt.C, diff *difftypes.SchemaDiff) string {
	c.Helper()
	nodes, err := postgres.New().GenerateMigrationAST(diff)
	c.Assert(err, qt.IsNil)
	sql, err := builtin.RenderSQL("postgres", nodes...)
	c.Assert(err, qt.IsNil)
	return sql
}

// A plan says whether it creates a routine or replaces one. A routine the
// database does not have is a plain CREATE, and CREATE OR REPLACE is kept for
// one it has. Without the plain form, every plan that adds a function reads as
// one that rewrites a function other code already calls, and it overwrites a
// routine that appeared after the plan was made instead of failing on it.
//
// A trigger's own function follows the trigger: created with a new one,
// replaced with a changed one. A routine the plan drops first because
// PostgreSQL refuses the replacement is still a replacement: it existed, and
// the drop took its grants with it.
func TestPlanner_CreatesANewRoutineAndReplacesAnExistingOne(t *testing.T) {
	trigger := func(body string) schemamodel.Trigger {
		return schemamodel.Trigger{Name: "touch", Table: "users", Timing: "BEFORE", Event: "UPDATE", ForEach: "ROW", Body: body}
	}
	tests := []struct {
		name string
		diff *difftypes.SchemaDiff
		want string
	}{
		{
			name: "an added function",
			diff: &difftypes.SchemaDiff{FunctionsAdded: difftypes.FunctionChanges{{Function: schemamodel.Function{
				Name: "add_one", Parameters: "n integer", Returns: "integer", Language: "sql", Body: "SELECT n + 1",
			}}}},
			want: `(?s)CREATE FUNCTION "add_one"\(n integer\) RETURNS integer.*`,
		},
		{
			name: "an added procedure",
			diff: &difftypes.SchemaDiff{FunctionsAdded: difftypes.FunctionChanges{{Function: schemamodel.Function{
				Name: "reset", Kind: schemamodel.FunctionKindProcedure, Language: "sql", Body: "SELECT 1",
			}}}},
			want: `(?s)CREATE PROCEDURE "reset"\(\).*`,
		},
		{
			name: "a modified function",
			diff: &difftypes.SchemaDiff{FunctionsModified: []difftypes.FunctionDiff{{
				FunctionName: "add_one",
				Changes:      map[string]string{"body": "SELECT n + 1 -> SELECT n + 2"},
				Desired: schemamodel.Function{
					Name: "add_one", Parameters: "n integer", Returns: "integer", Language: "sql", Body: "SELECT n + 2",
				},
			}}},
			want: `(?s).*\nCREATE OR REPLACE FUNCTION "add_one"\(n integer\) RETURNS integer.*`,
		},
		{
			name: "a function dropped first because its return type changed",
			diff: &difftypes.SchemaDiff{FunctionsModified: []difftypes.FunctionDiff{{
				FunctionName:     "add_one",
				Changes:          map[string]string{"returns": "integer -> bigint"},
				CurrentSignature: new("n integer"),
				Desired: schemamodel.Function{
					Name: "add_one", Parameters: "n integer", Returns: "bigint", Language: "sql", Body: "SELECT n + 1",
				},
			}}},
			want: `(?s).*DROP FUNCTION "add_one"\(n integer\);.*\nCREATE OR REPLACE FUNCTION "add_one"\(n integer\) RETURNS bigint.*`,
		},
		{
			name: "an added trigger",
			diff: &difftypes.SchemaDiff{TriggersAdded: []difftypes.TriggerRef{{
				TriggerName: "touch", TableName: "users", Desired: trigger("RETURN NEW;"),
			}}},
			want: `(?s)CREATE FUNCTION "ptah_trigger_users_touch"\(\).*\nCREATE TRIGGER "touch".*`,
		},
		{
			name: "a modified trigger",
			diff: &difftypes.SchemaDiff{TriggersModified: []difftypes.TriggerDiff{{
				TriggerName: "touch", TableName: "users",
				Changes: map[string]string{"body": "RETURN NEW; -> RETURN OLD;"},
				Desired: trigger("RETURN OLD;"),
			}}},
			want: `(?s)CREATE OR REPLACE FUNCTION "ptah_trigger_users_touch"\(\).*\nCREATE OR REPLACE TRIGGER "touch".*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			sql := renderPlan(c, test.diff)

			c.Assert(sql, qt.Matches, test.want)
		})
	}
}
