package routineidentity_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/internal/routineidentity"
)

// TestSubject_ADeclarationAndItsCatalogRowAreOneRoutine pins that a setting
// keyed from a declaration finds the routine the catalog reports: the two
// spell the arguments differently, and the subject reads both to their input
// types.
func TestSubject_ADeclarationAndItsCatalogRowAreOneRoutine(t *testing.T) {
	builder := objectidentity.NewBuilder(identifier.ForDialect("postgres"))
	for _, test := range []struct {
		name     string
		kind     string
		declared string
		catalog  string
	}{
		{name: "a function", kind: "function", declared: "a int DEFAULT 1, OUT b text", catalog: "a integer, OUT b text"},
		{name: "a procedure", kind: "PROCEDURE", declared: "IN a varchar(20)", catalog: "a character varying"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			declared := routineidentity.Subject(builder, test.kind, "public.f", test.declared)
			reported := routineidentity.Subject(builder, test.kind, "f", test.catalog)

			c.Assert(declared.Equal(reported), qt.IsTrue, qt.Commentf("%s and %s", declared, reported))
		})
	}
}

// TestSubject_KeepsAFunctionApartFromAProcedure pins the kind: a setting of a
// procedure is not one of the function with its name and arguments.
func TestSubject_KeepsAFunctionApartFromAProcedure(t *testing.T) {
	c := qt.New(t)
	builder := objectidentity.NewBuilder(identifier.ForDialect("sqlserver"))

	function := routineidentity.Subject(builder, "", "dbo.f", "")
	procedure := routineidentity.Subject(builder, "procedure", "dbo.f", "")

	c.Assert(function.Kind, qt.Equals, objectidentity.KindFunction)
	c.Assert(procedure.Kind, qt.Equals, objectidentity.KindProcedure)
	c.Assert(function.Equal(procedure), qt.IsFalse)
}

// TestSubject_TellsOverloadsApart pins that the input types, and only they,
// separate two routines of one name.
func TestSubject_TellsOverloadsApart(t *testing.T) {
	c := qt.New(t)
	builder := objectidentity.NewBuilder(identifier.ForDialect("postgres"))

	integer := routineidentity.Subject(builder, "function", "public.f", "a int")
	text := routineidentity.Subject(builder, "function", "public.f", "a text")

	c.Assert(integer.Equal(text), qt.IsFalse)
}
