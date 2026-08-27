package difftypes_test

import (
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
)

// TestRangeChanges_TheWireShapeIsUnchanged pins the promise the type makes.
//
// `RangesAdded` and `RangesRemoved` now carry the range type instead of its
// name, the first family to do so under stokaro/ptah#2315. `ptah schema diff
// --format json` serializes the comparator's model as it stands, so the change
// would have altered a document stamped `format_version: 1` -- and 33 of these
// families remain, which is 33 format changes for one architectural move.
//
// So the encoding stays what it was. This test is what makes that a promise
// rather than an intention: it fails the moment the operands reach the wire,
// which is the moment a version bump becomes due.
func TestRangeChanges_TheWireShapeIsUnchanged(t *testing.T) {
	tests := []struct {
		name    string
		changes difftypes.RangeChanges
		want    string
		why     string
	}{
		{
			name:    "nil is null",
			changes: nil,
			want:    "null",
			why:     "null is a comparison that did not run, which every field of this type distinguishes from []",
		},
		{
			name:    "empty is an empty array",
			changes: difftypes.RangeChanges{},
			want:    "[]",
			why:     "[] is a comparison that ran and found nothing",
		},
		{
			name: "the operands do not reach the wire",
			changes: difftypes.RangeChanges{
				{Name: "r", Subtype: "integer", SubtypeOpClass: "int4_ops", Canonical: "r_canonical"},
			},
			want: `["r"]`,
			why:  "a name list is what format_version 1 has always carried here",
		},
		{
			name: "a schema-qualified range keeps its qualified spelling",
			changes: difftypes.RangeChanges{
				{Name: "r", Schema: "app", Subtype: "integer"},
			},
			want: `["app.r"]`,
			why:  "the name lists carried qualified names, and the identity a consumer keys on is unchanged",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			encoded, err := json.Marshal(test.changes)

			c.Assert(err, qt.IsNil)
			c.Assert(string(encoded), qt.Equals, test.want, qt.Commentf("%s", test.why))
		})
	}
}

// TestRangeChanges_TheDefinitionSurvivesInMemory is the other half: the wire
// staying flat must not mean the operands were dropped on the way in.
//
// Without this, the marshaller above would pass against a type that had thrown
// the definition away, which is exactly the state this change exists to leave.
func TestRangeChanges_TheDefinitionSurvivesInMemory(t *testing.T) {
	c := qt.New(t)

	changes := difftypes.RangeChanges{
		{Name: "r", Schema: "app", Subtype: "integer", SubtypeOpClass: "int4_ops"},
	}

	c.Assert(changes[0].Subtype, qt.Equals, "integer")
	c.Assert(changes[0].SubtypeOpClass, qt.Equals, "int4_ops")
	c.Assert(changes.Names(), qt.DeepEquals, []string{"app.r"})
	c.Assert(schemamodel.Range(changes[0]).QualifiedName(), qt.Equals, "app.r")
}
