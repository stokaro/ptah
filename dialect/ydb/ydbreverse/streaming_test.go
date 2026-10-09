package ydbreverse_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbreverse"
	"ptah.run/dialect/ydb/ydbstreaming"
)

func TestStreamingReversalCarriesPermissionAndReportsLostState(t *testing.T) {
	c := qt.New(t)
	before := ydbstreaming.Spec{Text: "SELECT 1;", Run: new(false), ResourcePool: "batch"}
	after := ydbstreaming.Spec{Text: "SELECT 2;", Run: new(true), ResourcePool: "default"}
	change := &ydbdiff.StreamingQuery{Before: &ydbstreaming.Observed{Spec: before}, After: &ydbstreaming.Desired{Spec: after, AllowStateReset: true}}
	result, err := (ydbreverse.StreamingService{}).ReverseChanges(t.Context(), streamingReverseRequest(change))
	c.Assert(err, qt.IsNil)
	c.Assert(result, qt.HasLen, 1)
	reverse := result[0].Change.Value.(*ydbdiff.StreamingQuery)
	c.Assert(reverse.Before.Spec, qt.DeepEquals, after)
	c.Assert(reverse.After.Spec, qt.DeepEquals, before)
	c.Assert(reverse.After.AllowStateReset, qt.IsTrue)
	c.Assert(result[0].Limitations, qt.DeepEquals, []string{ydbstreaming.CheckpointLoss})
	c.Assert(result[0].ForwardState, qt.HasLen, 1)
	c.Assert(result[0].ForwardState[0].Value, qt.DeepEquals, &ydbstreaming.Observed{Spec: after})
	*reverse.Before.Spec.Run = false
	*reverse.After.Spec.Run = true
	c.Assert(*change.Before.Spec.Run, qt.IsFalse)
	c.Assert(*change.After.Spec.Run, qt.IsTrue)
	c.Assert(*result[0].ForwardState[0].Value.(*ydbstreaming.Observed).Spec.Run, qt.IsTrue)
}

func TestStreamingSettingReversalDoesNotClaimCheckpointLoss(t *testing.T) {
	c := qt.New(t)
	change := &ydbdiff.StreamingQuery{Before: &ydbstreaming.Observed{Spec: ydbstreaming.Spec{Text: "SELECT 1;"}},
		After: &ydbstreaming.Desired{Spec: ydbstreaming.Spec{Text: "SELECT 1;", Run: new(false)}}}
	result, err := (ydbreverse.StreamingService{}).ReverseChanges(t.Context(), streamingReverseRequest(change))
	c.Assert(err, qt.IsNil)
	c.Assert(result, qt.HasLen, 1)
	c.Assert(result[0].Limitations, qt.HasLen, 0)
	c.Assert(result[0].Change.Value.(*ydbdiff.StreamingQuery).After.AllowStateReset, qt.IsFalse)
}

func TestStreamingReversalCannotInventResetPermission(t *testing.T) {
	c := qt.New(t)
	change := &ydbdiff.StreamingQuery{Before: &ydbstreaming.Observed{Spec: ydbstreaming.Spec{Text: "SELECT 1;"}},
		After: &ydbstreaming.Desired{Spec: ydbstreaming.Spec{Text: "SELECT 2;"}}}
	result, err := (ydbreverse.StreamingService{}).ReverseChanges(t.Context(), streamingReverseRequest(change))
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(err, qt.ErrorMatches, `.*allow_state_reset=true.*`)
	c.Assert(result, qt.IsNil)
}

func TestStreamingChangeCloneDoesNotShareOptionalSettings(t *testing.T) {
	c := qt.New(t)
	original := &ydbdiff.StreamingQuery{Before: &ydbstreaming.Observed{Spec: ydbstreaming.Spec{Text: "SELECT 1;", Run: new(false)}},
		After: &ydbstreaming.Desired{Spec: ydbstreaming.Spec{Text: "SELECT 2;", Run: new(true)}, AllowStateReset: true}}
	encoded, err := ydbdiff.StreamingCodec().Encode(original)
	c.Assert(err, qt.IsNil)
	decoded, err := ydbdiff.StreamingCodec().Decode(encoded)
	c.Assert(err, qt.IsNil)
	c.Assert(decoded, qt.DeepEquals, original)
	cloned := decoded.(*ydbdiff.StreamingQuery).CloneChange().(*ydbdiff.StreamingQuery)
	*cloned.Before.Spec.Run = true
	*cloned.After.Spec.Run = false
	c.Assert(decoded, qt.DeepEquals, original)
}

func streamingReverseRequest(change *ydbdiff.StreamingQuery) schemaext.ReversalRequest {
	return schemaext.ReversalRequest{Target: "ydb", Identifiers: identifier.ForDialect("ydb"),
		Capabilities: capability.Capabilities{capability.StreamingQueries: true},
		Changes:      []schemaext.ChangeRecord{{Subject: ydbstreaming.Ref("app", "query"), Value: change}},
	}
}
