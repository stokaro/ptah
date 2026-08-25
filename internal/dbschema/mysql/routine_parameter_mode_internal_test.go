package mysql

// White-box testing required: the declaration is assembled inside the reader
// while the catalog rows are still in hand, and by the time a caller sees the
// routine the parameter list is one string with nothing left to distinguish a
// mode that was dropped from one that was never there.

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

// TestParameterDeclaration_ProceduresCarryTheirModes pins what a routine
// parameter is spelled as.
//
// Losing a mode is not cosmetic. A parameter with no mode is IN, so a replayed
// procedure assigns to a local copy that is discarded at return: the procedure
// still exists, still accepts the same call, still succeeds, and no longer
// returns its result. Measured on MySQL 9.7.2 -- `CALL p_out(7, @o, @io)`
// answered 7 against the source and NULL against the replay
// (stokaro/ptah#2208).
//
// A function is the row that keeps this honest. MySQL reports IN for its
// parameters in information_schema.PARAMETERS the same way, and writing that
// back is not a longer spelling of the same thing -- it does not parse.
// Measured on the same server:
//
//	CREATE FUNCTION f_bad(IN a INT) RETURNS INT DETERMINISTIC RETURN a + 1;
//	ERROR 1064 (42000): You have an error in your SQL syntax
func TestParameterDeclaration_ProceduresCarryTheirModes(t *testing.T) {
	tests := []struct {
		name        string
		routineType string
		mode        string
		want        string
	}{
		{
			name:        "a procedure's OUT is kept",
			routineType: "PROCEDURE",
			mode:        "OUT",
			want:        "OUT b int",
		},
		{
			name:        "a procedure's INOUT is kept",
			routineType: "PROCEDURE",
			mode:        "INOUT",
			want:        "INOUT b int",
		},
		{
			name:        "a procedure's IN is left implicit",
			routineType: "PROCEDURE",
			mode:        "IN",
			want:        "b int",
		},
		{
			name:        "a mode the catalog did not report",
			routineType: "PROCEDURE",
			mode:        "",
			want:        "b int",
		},
		{
			name:        "a function's IN is discarded, because the syntax has no place for it",
			routineType: "FUNCTION",
			mode:        "IN",
			want:        "b int",
		},
		{
			name:        "a function is not given a mode whatever the catalog says",
			routineType: "FUNCTION",
			mode:        "OUT",
			want:        "b int",
		},
		{
			name:        "the routine type is matched without regard to case",
			routineType: "procedure",
			mode:        "out",
			want:        "OUT b int",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			declaration := parameterDeclaration(test.routineType, test.mode, "b", "int")

			c.Assert(declaration, qt.Equals, test.want)
		})
	}
}
