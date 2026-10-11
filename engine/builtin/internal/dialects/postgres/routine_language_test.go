package postgres_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/engine/builtin"
)

// TestRenderSQL_ARoutineWithoutALanguageIsInPLpgSQL pins that a routine the
// model hands over with no language is written in plpgsql. PostgreSQL refuses a
// routine without a LANGUAGE clause ("no language specified"), and the model
// keeps an omitted language empty because only the target knows what it means.
// A stated language is written as declared.
func TestRenderSQL_ARoutineWithoutALanguageIsInPLpgSQL(t *testing.T) {
	for _, test := range []struct {
		name     string
		language string
		want     string
	}{
		{name: "no language", language: "", want: "LANGUAGE plpgsql"},
		{name: "a stated language", language: "sql", want: "LANGUAGE sql"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			out, err := builtin.RenderSQL("postgres", &ast.CreateFunctionNode{
				Name: "f", Returns: "integer", Language: test.language, Body: "SELECT 1",
			})

			c.Assert(err, qt.IsNil)
			c.Assert(out, qt.Contains, test.want)
		})
	}
}
