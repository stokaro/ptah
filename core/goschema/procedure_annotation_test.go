package goschema_test

import (
	"os"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
)

// TestParseProcedureAnnotation covers the declaration a procedure needs and the
// one attribute it must not carry.
//
// A procedure returns nothing -- that is the property separating it from a
// function, in the grammar and in both catalogs -- so `returns=` is refused
// rather than dropped. Accepting and ignoring it is the shape of failure
// stokaro/ptah#1722 is about: a declaration that means one thing and a database
// that ends up holding another, with nothing said.
func TestParseProcedureAnnotation(t *testing.T) {
	tests := []struct {
		name       string
		annotation string
		wantErr    bool
		wantKind   string
	}{
		{
			name:       "a procedure is parsed as one",
			annotation: `//ptah:schema:procedure name="bump" params="n integer" language="sql" body="SELECT n"`,
			wantKind:   goschema.FunctionKindProcedure,
		},
		{
			name:       "a function is still a function",
			annotation: `//ptah:schema:function name="addone" params="n integer" returns="integer" language="sql" body="SELECT n + 1"`,
			wantKind:   "",
		},
		{
			name:       "a procedure declaring a return type is refused",
			annotation: `//ptah:schema:procedure name="bump" returns="integer" language="sql" body="SELECT 1"`,
			wantErr:    true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			source := "package p\n" + test.annotation + "\ntype TestStruct struct {}\n"
			path := t.TempDir() + "/routine.go"
			c.Assert(os.WriteFile(path, []byte(source), 0o644), qt.IsNil) // #nosec G306 -- a test fixture

			database, err := goschema.ParseFile(path)

			c.Assert(err != nil, qt.Equals, test.wantErr, qt.Commentf("err: %v", err))
			c.Assert(len(database.Functions) == 1, qt.Equals, !test.wantErr)
			c.Assert(routineKindOf(database), qt.Equals, test.wantKind)
		})
	}
}

// routineKindOf returns the kind of the single parsed routine, or empty when
// the parse was refused.
func routineKindOf(database goschema.Database) string {
	if len(database.Functions) != 1 {
		return ""
	}
	return database.Functions[0].Kind
}
