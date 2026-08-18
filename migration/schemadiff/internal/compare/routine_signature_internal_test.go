package compare

// White-box testing required: the signature normalizer and the overload pairing
// are unexported by design — they are an implementation detail of how the
// comparator keys routines, and the exported surface only shows their effect on
// a diff. Pinning the normalizer directly is what makes the measured catalog
// answers a regression set rather than a comment.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/dbschema/types"
)

// TestNormalizeRoutineSignatureAgreesWithTheCatalog is the measured regression
// set: every row is a declaration and the identity arguments PostgreSQL 18
// actually reported for the function created from it.
//
// The normalizer's job is to make the two sides comparable, not to reproduce
// the catalog, so the assertion is that both sides normalize to ONE value
// rather than to any particular spelling.
func TestNormalizeRoutineSignatureAgreesWithTheCatalog(t *testing.T) {
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

			c.Assert(normalizeRoutineSignature(test.declared), qt.Equals,
				normalizeRoutineSignature(test.identity))
		})
	}
}

// TestNormalizeRoutineSignatureKeepsDistinctSignaturesApart is the control for
// the table above.
//
// A normalizer that reduced everything to one value would satisfy every row
// there and make every overload compare equal, which is the defect this exists
// to fix rather than a stricter version of it.
func TestNormalizeRoutineSignatureKeepsDistinctSignaturesApart(t *testing.T) {
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

			c.Assert(normalizeRoutineSignature(test.left), qt.Not(qt.Equals),
				normalizeRoutineSignature(test.right))
		})
	}
}

func declaredFn(parameters, body string) goschema.Function {
	return goschema.Function{Name: "f", Parameters: parameters, Body: body}
}

func recordedFn(identityArguments, body string) types.DBFunction {
	arguments := identityArguments
	return types.DBFunction{Name: "f", Parameters: arguments, IdentityArguments: &arguments, Body: body}
}

// TestPairRoutineOverloads pins the pairing, including the case that decides
// the whole safety argument: a name with one routine on each side pairs without
// consulting the signature at all.
//
// That is what keeps the common case unable to regress. A schema whose single
// routine spells its parameters differently from the catalog paired before this
// change and still pairs now, because the signature is never asked.
func TestPairRoutineOverloads(t *testing.T) {
	tests := []struct {
		name          string
		declared      []goschema.Function
		recorded      []types.DBFunction
		wantPairs     int
		wantAdded     int
		wantRemoved   int
		wantFirstPair string
	}{
		{
			name:          "one on each side pairs without reading the signature",
			declared:      []goschema.Function{declaredFn("a whatever-this-is", "SELECT 1")},
			recorded:      []types.DBFunction{recordedFn("a integer", "SELECT 1")},
			wantPairs:     1,
			wantFirstPair: "SELECT 1",
		},
		{
			name: "an overload set pairs on the signature, not on order",
			declared: []goschema.Function{
				declaredFn("a text", "text body"),
				declaredFn("a int", "int body"),
			},
			recorded: []types.DBFunction{
				recordedFn("a integer", "int body"),
				recordedFn("a text", "text body"),
			},
			wantPairs:     2,
			wantFirstPair: "text body",
		},
		{
			name:          "a declared overload the database lacks is an addition",
			declared:      []goschema.Function{declaredFn("a int", "x"), declaredFn("a text", "y")},
			recorded:      []types.DBFunction{recordedFn("a integer", "x")},
			wantPairs:     1,
			wantAdded:     1,
			wantFirstPair: "x",
		},
		{
			name:          "a recorded overload the schema lacks is a removal",
			declared:      []goschema.Function{declaredFn("a int", "x")},
			recorded:      []types.DBFunction{recordedFn("a integer", "x"), recordedFn("a text", "y")},
			wantPairs:     1,
			wantRemoved:   1,
			wantFirstPair: "x",
		},
		{
			name:        "a routine the schema no longer declares at all",
			declared:    nil,
			recorded:    []types.DBFunction{recordedFn("a integer", "x")},
			wantRemoved: 1,
		},
		{
			name:      "a routine the database does not have at all",
			declared:  []goschema.Function{declaredFn("a int", "x")},
			recorded:  nil,
			wantAdded: 1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			pairs, added, removed := pairRoutineOverloads(test.declared, test.recorded)

			c.Assert(pairs, qt.HasLen, test.wantPairs)
			c.Assert(added, qt.Equals, test.wantAdded)
			c.Assert(removed, qt.Equals, test.wantRemoved)
			c.Assert(firstPairBody(pairs), qt.Equals, test.wantFirstPair)
		})
	}
}

// firstPairBody returns the recorded body of the first pair, or "" when the
// row does not assert on pairing order. It keeps the loop body branch-free.
func firstPairBody(pairs []routinePair) string {
	bodies := map[bool]func() string{
		true:  func() string { return "" },
		false: func() string { return pairs[0].recorded.Body },
	}
	return bodies[len(pairs) == 0]()
}
