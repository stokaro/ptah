package pgtable_test

import (
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/postgres/pgtable"
)

func TestExclusionChangeCodec_RoundTrip(t *testing.T) {
	for _, test := range []struct {
		name    string
		change  *pgtable.ExclusionChange
		removes bool
	}{
		{"create", &pgtable.ExclusionChange{After: &pgtable.DesiredExclusion{Exclusion: exclusionDefinition()}}, false},
		{"drop", &pgtable.ExclusionChange{Before: new(pgtable.ObservedExclusion(exclusionDefinition()))}, true},
		{"replace", &pgtable.ExclusionChange{Before: new(pgtable.ObservedExclusion(exclusionDefinition())), After: &pgtable.DesiredExclusion{Exclusion: pgtable.Exclusion{UsingMethod: "gist", Elements: "during WITH &&"}}}, false},
		{"comment", &pgtable.ExclusionChange{Before: new(pgtable.ObservedExclusion(exclusionDefinition())), After: &pgtable.DesiredExclusion{Exclusion: pgtable.Exclusion{UsingMethod: "gist", Elements: "room WITH =, during WITH &&", Comment: "Room schedule"}}, CommentOnly: true}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			codec := pgtable.ExclusionChangeCodec()
			encoded, err := codec.Encode(test.change)
			c.Assert(err, qt.IsNil)
			restored, err := codec.Decode(encoded)
			c.Assert(err, qt.IsNil)
			c.Assert(restored, qt.DeepEquals, test.change)
			c.Assert(restored, qt.Not(qt.Equals), test.change)
			c.Assert(test.change.RemovesState(), qt.Equals, test.removes)
			inverse, err := (pgtable.ExclusionService{}).ReverseChanges(t.Context(), schemaext.ReversalRequest{Target: "postgres", Changes: []schemaext.ChangeRecord{{Subject: exclusionSubject(), Value: test.change}}})
			c.Assert(err, qt.IsNil)
			c.Assert(inverse, qt.HasLen, 1)
			c.Assert(inverse[0].Change.Subject, qt.DeepEquals, exclusionSubject())
			c.Assert(inverse[0].ForwardState, qt.HasLen, 1)
			c.Assert(inverse[0].ForwardState[0].Placement, qt.Equals, schemaext.ObjectPlacement)
			c.Assert(inverse[0].ForwardState[0].Kind, qt.Equals, pgtable.ExclusionKind)
			roundtrip, err := (pgtable.ExclusionService{}).ReverseChanges(t.Context(), schemaext.ReversalRequest{Target: "postgres", Changes: []schemaext.ChangeRecord{inverse[0].Change}})
			c.Assert(err, qt.IsNil)
			c.Assert(roundtrip[0].Change.Value, qt.DeepEquals, test.change)
		})
	}
}

func TestExclusionChange_InvalidClassification(t *testing.T) {
	for _, test := range []struct {
		name   string
		change *pgtable.ExclusionChange
	}{
		{"nil", nil}, {"empty", &pgtable.ExclusionChange{}},
		{"same", &pgtable.ExclusionChange{Before: new(pgtable.ObservedExclusion(exclusionDefinition())), After: &pgtable.DesiredExclusion{Exclusion: exclusionDefinition()}}},
		{"comment-only creation", &pgtable.ExclusionChange{After: &pgtable.DesiredExclusion{Exclusion: exclusionDefinition()}, CommentOnly: true}},
		{"comment-only replacement", &pgtable.ExclusionChange{Before: new(pgtable.ObservedExclusion(exclusionDefinition())), After: &pgtable.DesiredExclusion{Exclusion: pgtable.Exclusion{UsingMethod: "gist", Elements: "during WITH &&"}}, CommentOnly: true}},
		{"replacement for a comment", &pgtable.ExclusionChange{Before: new(pgtable.ObservedExclusion(exclusionDefinition())), After: &pgtable.DesiredExclusion{Exclusion: pgtable.Exclusion{UsingMethod: "gist", Elements: "room WITH =, during WITH &&", Comment: "Room schedule"}}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(pgtable.ValidateExclusionChange(test.change), qt.ErrorIs, schemaext.ErrInvalidValue)
			wire, err := pgtable.ExclusionChangeCodec().Encode(test.change)
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(wire, qt.IsNil)
		})
	}
}

func TestExclusionChangeCodec_FailurePath(t *testing.T) {
	for _, wire := range []string{
		`null`, `{}`, `{"before":null,"after":null}`, `{"after":{"using_method":"gist","elements":"during WITH &&"}}`,
		`{"before":null,"after":{"using_method":"gist","elements":"during WITH &&","extra":1}}`,
		`{"before":null,"after":{"using_method":"gist","elements":"during WITH &&"},"comment_only":false}`,
	} {
		t.Run(wire, func(t *testing.T) {
			c := qt.New(t)
			value, err := pgtable.ExclusionChangeCodec().Decode(json.RawMessage(wire))
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(value, qt.IsNil)
		})
	}
}

func TestExclusionReversal_NormalizedCommentChange(t *testing.T) {
	c := qt.New(t)
	desired := &pgtable.DesiredExclusion{Exclusion: exclusionDefinition(), StructName: "Room"}
	desired.WhereCondition = "state = 'open'"
	desired.Comment = "Room schedule"
	desired.Normalized = &pgtable.NormalizedExclusion{SourceKey: pgtable.ExclusionComparisonKey(desired.Exclusion), Elements: desired.Elements, WhereCondition: "state = 'open'::text"}
	observed := pgtable.ObservedExclusion(desired.Exclusion)
	observed.WhereCondition = desired.Normalized.WhereCondition
	observed.Comment = ""
	change := &pgtable.ExclusionChange{Before: &observed, After: desired, CommentOnly: true}
	result, err := (pgtable.ExclusionService{}).ReverseChanges(t.Context(), schemaext.ReversalRequest{Target: "postgres", Changes: []schemaext.ChangeRecord{{Subject: exclusionSubject(), Value: change}}})
	c.Assert(err, qt.IsNil)
	inverse := result[0].Change.Value.(*pgtable.ExclusionChange)
	c.Assert(inverse.CommentOnly, qt.IsTrue)
	c.Assert(inverse.Before.WhereCondition, qt.Equals, "state = 'open'::text")
	c.Assert(inverse.After.Comment, qt.Equals, "")
	c.Assert(inverse.After.StructName, qt.Equals, "")
	c.Assert(inverse.After.Normalized, qt.IsNil)
	c.Assert(result[0].Limitations, qt.HasLen, 0)
	c.Assert(desired.WhereCondition, qt.Equals, "state = 'open'")
	inverse.Before.Comment = "mutated"
	c.Assert(result[0].ForwardState[0].Value.(*pgtable.ObservedExclusion).Comment, qt.Equals, "Room schedule")
}

func TestExclusionReversal_ReportsRestorationLimit(t *testing.T) {
	c := qt.New(t)
	change := &pgtable.ExclusionChange{Before: new(pgtable.ObservedExclusion(exclusionDefinition()))}
	result, err := (pgtable.ExclusionService{}).ReverseChanges(t.Context(), schemaext.ReversalRequest{Target: "postgres", Changes: []schemaext.ChangeRecord{{Subject: exclusionSubject(), Value: change}}})
	c.Assert(err, qt.IsNil)
	c.Assert(result[0].Limitations, qt.DeepEquals, []string{"Rows written under the changed constraints can prevent restoration; the plan does not remove those rows."})
	c.Assert(result[0].ForwardState[0].Value, qt.IsNil)
}

func TestExclusionReversal_RefusesPartialBatch(t *testing.T) {
	c := qt.New(t)
	change := &pgtable.ExclusionChange{Before: new(pgtable.ObservedExclusion(exclusionDefinition()))}
	result, err := (pgtable.ExclusionService{}).ReverseChanges(t.Context(), schemaext.ReversalRequest{Target: "postgres", Changes: []schemaext.ChangeRecord{{Subject: exclusionSubject(), Value: change}, {Subject: exclusionSubject(), Value: &pgtable.ExclusionChange{}}}})
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(result, qt.IsNil)
}
