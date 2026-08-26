package atlashclrender_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/internal/atlashcl"
	"go.5x5.cz/ptah/internal/atlashclrender"
)

// TestRenderInspected_ARoutineIsWrittenAsWhatItIs pins the block a routine is
// described with.
//
// A procedure written as a `function` block is not a smaller description, it is
// a wrong one: the reader of that document cannot tell the two apart, and the
// comparator keys routines by kind. Applying a database's own description
// therefore dropped every procedure it had and put a CREATE FUNCTION with an
// empty RETURNS clause in its place. Measured on MySQL 9.7.2, inspecting a
// database with two procedures and applying the output back:
//
//	CREATE FUNCTION `mysrc`.`p_out`(a int, out b int, inout c int) RETURNS  ...
//	DROP PROCEDURE IF EXISTS `mysrc`.`p_in`;
//	DROP PROCEDURE IF EXISTS `mysrc`.`p_out`;
//
// `RETURNS ` with nothing after it is not valid MySQL, so the plan dropped two
// working procedures and then failed to create anything (stokaro/ptah#2209).
func TestRenderInspected_ARoutineIsWrittenAsWhatItIs(t *testing.T) {
	tests := []struct {
		name     string
		routine  schemamodel.Function
		want     string
		unwanted string
	}{
		{
			name: "a procedure is a procedure block",
			routine: schemamodel.Function{
				Name: "p_touch", Kind: schemamodel.FunctionKindProcedure,
				Language: "plpgsql", Body: "BEGIN END;",
			},
			want:     `procedure "p_touch" {`,
			unwanted: `function "p_touch" {`,
		},
		{
			name: "a function is still a function block",
			routine: schemamodel.Function{
				Name: "f_one", Returns: "integer", Language: "sql", Body: "SELECT 1",
			},
			want:     `function "f_one" {`,
			unwanted: `procedure "f_one" {`,
		},
		{
			name: "a function keeps the return type that identifies it",
			routine: schemamodel.Function{
				Name: "f_one", Returns: "integer", Language: "sql", Body: "SELECT 1",
			},
			want:     `return = "integer"`,
			unwanted: `procedure`,
		},
		{
			// A procedure returns nothing, so there is nothing to write. The
			// row varies the IR rather than the spelling: a procedure carrying
			// a return type describes an object no engine can create, and the
			// renderer must not pass it on just because the field was set.
			name: "a procedure carrying a return type is still written without one",
			routine: schemamodel.Function{
				Name: "p_touch", Kind: schemamodel.FunctionKindProcedure,
				Returns: "integer", Language: "plpgsql", Body: "BEGIN END;",
			},
			want:     `procedure "p_touch" {`,
			unwanted: `return =`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db := &schemamodel.Database{Functions: []schemamodel.Function{test.routine}}

			result, err := atlashclrender.RenderInspected(db, platform.Postgres, "public")

			c.Assert(err, qt.IsNil)
			c.Assert(string(result.Data), qt.Contains, test.want)
			c.Assert(string(result.Data), qt.Not(qt.Contains), test.unwanted)
		})
	}
}

// TestRenderInspected_AProcedureSurvivesItsOwnDescription is the property the
// issue is actually about: a description of a database, read back, still says
// the same thing.
//
// Rendering alone cannot show this. The block could be spelled `procedure` and
// parse back as a function, which is exactly what the surface did before -- the
// kind had no way through, in either direction.
func TestRenderInspected_AProcedureSurvivesItsOwnDescription(t *testing.T) {
	tests := []struct {
		name string
		kind string
	}{
		{name: "a procedure comes back a procedure", kind: schemamodel.FunctionKindProcedure},
		{name: "a function comes back a function", kind: ""},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db := &schemamodel.Database{Functions: []schemamodel.Function{{
				Name:       "public.r",
				Kind:       test.kind,
				Parameters: "a integer",
				Language:   "plpgsql",
				Body:       "BEGIN END;",
			}}}

			result, err := atlashclrender.RenderInspected(db, platform.Postgres, "public")
			c.Assert(err, qt.IsNil)

			reparsed, err := atlashcl.Parse(result.Data, "schema.hcl")

			c.Assert(err, qt.IsNil)
			c.Assert(reparsed.Functions, qt.HasLen, 1)
			c.Assert(reparsed.Functions[0].Kind, qt.Equals, test.kind)
			c.Assert(reparsed.Functions[0].Parameters, qt.Equals, "a integer")
		})
	}
}
