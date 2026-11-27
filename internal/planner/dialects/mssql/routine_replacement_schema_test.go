package mssql_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/difftypes"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/migration/planner"
)

// SQL Server resolves an unqualified routine name in the session's default
// schema first, and that is not dbo for every account. A routine the
// comparison paired with one the read found in a schema is replaced in that
// schema, so the plan drops and creates the routine it compared, whatever the
// session resolves a bare name to (stokaro/ptah#6786).

// routineReplacement is the change of the function declared as declared, which
// the read found as held.
func routineReplacement(declared, held string) *difftypes.SchemaDiff {
	return &difftypes.SchemaDiff{
		FunctionsModified: []difftypes.FunctionDiff{{
			FunctionName: declared,
			CurrentName:  held,
			Changes:      map[string]string{"body": "old -> new"},
			Desired: schemamodel.Function{
				StructName: "F", Name: declared, Returns: "int", Language: "sql",
				Body: "BEGIN RETURN 5 END",
			},
		}},
	}
}

func TestPlanner_RoutineReplacementNamesTheHeldSchema(t *testing.T) {
	tests := []struct {
		name           string
		declared, held string
		want           []string
	}{
		{
			name:     "an unqualified declaration of a routine in dbo",
			declared: "f", held: "dbo.f",
			want: []string{"DROP FUNCTION IF EXISTS [dbo].[f]", "CREATE OR ALTER FUNCTION [dbo].[f]()"},
		},
		{
			// The drop names the routine the read found, and the create keeps
			// the declared spelling, which the server then stores.
			name:     "a declaration in another case under a case-insensitive collation",
			declared: "sales.MIXEDCASE", held: "Sales.MixedCase",
			want: []string{"DROP FUNCTION IF EXISTS [Sales].[MixedCase]", "CREATE OR ALTER FUNCTION [Sales].[MIXEDCASE]()"},
		},
		{
			name:     "a change no read recorded the name of",
			declared: "f", held: "",
			want: []string{"DROP FUNCTION IF EXISTS [f]", "CREATE OR ALTER FUNCTION [f]()"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := planner.GenerateSchemaDiffSQLStatements(
				t.Context(), must.Must(builtin.New()), routineReplacement(test.declared, test.held), platform.SQLServer,
			)

			c.Assert(err, qt.IsNil)
			c.Assert(firstLines(statements), qt.DeepEquals, test.want)
		})
	}
}

// firstLines is the first line of each statement past its comments, the line
// that names the object.
func firstLines(statements []string) []string {
	lines := make([]string, 0, len(statements))
	for _, statement := range statements {
		for line := range strings.Lines(statement) {
			line = strings.TrimSpace(line)
			if line != "" && !strings.HasPrefix(line, "--") {
				lines = append(lines, line)
				break
			}
		}
	}
	return lines
}
