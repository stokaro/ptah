package compare

// White-box testing required: the overload pairing is unexported by design --
// it is an implementation detail of how the comparator keys routines, and the
// exported surface only shows its effect on a diff.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
)

func declaredFn(parameters, body string) schemamodel.Function {
	return schemamodel.Function{Name: "f", Parameters: parameters, Body: body}
}

func recordedFn(identityArguments, body string) catalog.Function {
	arguments := identityArguments
	return catalog.Function{Name: "f", Parameters: arguments, IdentityArguments: &arguments, Body: body}
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
		declared      []schemamodel.Function
		recorded      []catalog.Function
		wantPairs     int
		wantAdded     int
		wantRemoved   int
		wantFirstPair string
	}{
		{
			name:          "one on each side pairs without reading the signature",
			declared:      []schemamodel.Function{declaredFn("a whatever-this-is", "SELECT 1")},
			recorded:      []catalog.Function{recordedFn("a integer", "SELECT 1")},
			wantPairs:     1,
			wantFirstPair: "SELECT 1",
		},
		{
			name: "an overload set pairs on the signature, not on order",
			declared: []schemamodel.Function{
				declaredFn("a text", "text body"),
				declaredFn("a int", "int body"),
			},
			recorded: []catalog.Function{
				recordedFn("a integer", "int body"),
				recordedFn("a text", "text body"),
			},
			wantPairs:     2,
			wantFirstPair: "text body",
		},
		{
			name:          "a declared overload the database lacks is an addition",
			declared:      []schemamodel.Function{declaredFn("a int", "x"), declaredFn("a text", "y")},
			recorded:      []catalog.Function{recordedFn("a integer", "x")},
			wantPairs:     1,
			wantAdded:     1,
			wantFirstPair: "x",
		},
		{
			name:          "a recorded overload the schema lacks is a removal",
			declared:      []schemamodel.Function{declaredFn("a int", "x")},
			recorded:      []catalog.Function{recordedFn("a integer", "x"), recordedFn("a text", "y")},
			wantPairs:     1,
			wantRemoved:   1,
			wantFirstPair: "x",
		},
		{
			name:        "a routine the schema no longer declares at all",
			declared:    nil,
			recorded:    []catalog.Function{recordedFn("a integer", "x")},
			wantRemoved: 1,
		},
		{
			name:      "a routine the database does not have at all",
			declared:  []schemamodel.Function{declaredFn("a int", "x")},
			recorded:  nil,
			wantAdded: 1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			pairs, added, removed := pairRoutineOverloads(test.declared, test.recorded)

			c.Assert(pairs, qt.HasLen, test.wantPairs)
			// Both sides are the routines themselves rather than counts. The
			// removals carry the signature a DROP needs to address an overload
			// (stokaro/ptah#2296); the additions carry the declaration a CREATE
			// is written from, without which two overloads of one name were
			// created as the same one twice (stokaro/ptah#2408).
			c.Assert(added, qt.HasLen, test.wantAdded)
			c.Assert(removed, qt.HasLen, test.wantRemoved)
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
