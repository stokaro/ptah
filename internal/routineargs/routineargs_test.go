package routineargs_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/routineargs"
)

// TestSignature_AgreesWithTheCatalog is the measured regression
// set: every row is a declaration and the identity arguments PostgreSQL 18
// actually reported for the function created from it.
//
// The normalizer's job is to make the two sides comparable, not to reproduce
// the catalog, so the assertion is that both sides normalize to ONE value
// rather than to any particular spelling.
func TestSignature_AgreesWithTheCatalog(t *testing.T) {
	tests := []struct {
		name     string
		declared string
		identity string
	}{
		{name: "a canonical type is unchanged", declared: "a integer", identity: "a integer"},
		{name: "the int alias", declared: "a int", identity: "a integer"},
		{name: "the int4 alias", declared: "a int4", identity: "a integer"},
		{name: "a type modifier is dropped", declared: "a varchar(50)", identity: "a character varying"},
		{name: "a default is dropped", declared: "a text DEFAULT (quote_literal('x'))", identity: "a text"},
		{name: "the redundant IN mode is dropped and OUT is kept", declared: "IN a int, OUT b int", identity: "a integer, OUT b integer"},
		{name: "no arguments", declared: "", identity: ""},
		{name: "variadic keeps its mode and array", declared: "VARIADIC a int[]", identity: "VARIADIC a integer[]"},
		{name: "two arguments with modifiers", declared: "a bool, b numeric(10,2)", identity: "a boolean, b numeric"},
		{name: "inout is kept", declared: "INOUT a int", identity: "INOUT a integer"},
		{name: "a quoted type survives", declared: `a timestamptz, b "char"`, identity: `a timestamp with time zone, b "char"`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(routineargs.Signature(test.declared), qt.Equals,
				routineargs.Signature(test.identity))
		})
	}
}

// TestSignature_KeepsDistinctSignaturesApart is the control for
// the table above.
//
// A normalizer that reduced everything to one value would satisfy every row
// there and make every overload compare equal, which is the defect this exists
// to fix rather than a stricter version of it.
func TestSignature_KeepsDistinctSignaturesApart(t *testing.T) {
	tests := []struct {
		name  string
		left  string
		right string
	}{
		{name: "different types", left: "a integer", right: "a text"},
		{name: "different arity", left: "a integer", right: "a integer, b integer"},
		{name: "an array is not its element", left: "a integer", right: "a integer[]"},
		{name: "a mode is part of the identity", left: "a integer", right: "INOUT a integer"},
		{name: "no arguments is not one argument", left: "", right: "a integer"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(routineargs.Signature(test.left), qt.Not(qt.Equals),
				routineargs.Signature(test.right))
		})
	}
}

// TestInputTypes_AgreesWithTheCatalog is the measured regression set for grant
// targets: every row is an argument list a GRANT or REVOKE may name a routine
// by and the identity arguments PostgreSQL 18 reported for that routine.
// PostgreSQL resolved each spelling on the left to the routine on the right.
func TestInputTypes_AgreesWithTheCatalog(t *testing.T) {
	tests := []struct {
		name     string
		declared string
		identity string
	}{
		{name: "a bare type against a named parameter", declared: "uuid", identity: "p_id uuid"},
		{name: "a named parameter against itself", declared: "p_id uuid", identity: "p_id uuid"},
		{name: "a different parameter name", declared: "other uuid", identity: "p_id uuid"},
		{name: "the IN mode", declared: "IN p_id uuid", identity: "p_id uuid"},
		{name: "an OUT argument is not part of the target", declared: "uuid", identity: "p_id uuid, OUT n integer"},
		{name: "an alias", declared: "int, text", identity: "a integer, b text"},
		{name: "a multi-word type with no name", declared: "double precision", identity: "x double precision"},
		{name: "a multi-word type with a name", declared: "at timestamp with time zone", identity: "at timestamp with time zone"},
		{name: "a modifier is dropped", declared: "varchar(10)", identity: "code character varying"},
		{name: "an array", declared: "uuid[]", identity: "ids uuid[]"},
		{name: "no arguments", declared: "", identity: ""},
		{name: "case and spacing", declared: "UUID ,  TEXT", identity: "a uuid, b text"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(routineargs.InputTypes(test.declared), qt.Equals, routineargs.InputTypes(test.identity))
		})
	}
}

// TestInputTypes_KeepsDistinctTargetsApart is the control for the table above.
func TestInputTypes_KeepsDistinctTargetsApart(t *testing.T) {
	tests := []struct {
		name  string
		left  string
		right string
	}{
		{name: "different types", left: "uuid", right: "text"},
		{name: "different arity", left: "uuid", right: "uuid, uuid"},
		{name: "an array is not its element", left: "uuid", right: "uuid[]"},
		{name: "no arguments is not one argument", left: "", right: "uuid"},
		{name: "an INOUT argument is an input", left: "", right: "INOUT n integer"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(routineargs.InputTypes(test.left), qt.Not(qt.Equals), routineargs.InputTypes(test.right))
		})
	}
}
