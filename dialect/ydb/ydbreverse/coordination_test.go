package ydbreverse_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbcoordination"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbreverse"
	"ptah.run/engine"
)

func TestCoordinationReversalProjectsStoredSettingsAndReportsRuntimeLoss(t *testing.T) {
	strict := ydbcoordination.Spec{ReadConsistencyMode: "strict", SelfCheckPeriodMillis: 1000}
	cases := []struct {
		name        string
		change      *ydbdiff.CoordinationNode
		forward     schemaext.Value
		stored      *ydbcoordination.Observed
		strategy    string
		limitations bool
	}{
		{name: "create unset", change: &ydbdiff.CoordinationNode{After: &ydbcoordination.Desired{}}, stored: &ydbcoordination.Observed{}, forward: &ydbcoordination.Observed{}, strategy: "drop the created coordination node", limitations: true},
		{name: "drop", change: &ydbdiff.CoordinationNode{Before: &ydbcoordination.Observed{Spec: strict}}, strategy: "recreate the dropped coordination node", limitations: true},
		{name: "reset only changed setting", change: &ydbdiff.CoordinationNode{Before: &ydbcoordination.Observed{Spec: strict}, After: &ydbcoordination.Desired{}}, stored: &ydbcoordination.Observed{Spec: ydbcoordination.Spec{ReadConsistencyMode: "relaxed", SelfCheckPeriodMillis: 1000}}, forward: &ydbcoordination.Observed{Spec: ydbcoordination.Spec{ReadConsistencyMode: "relaxed", SelfCheckPeriodMillis: 1000}}, strategy: "restore coordination settings in place"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			runtime, err := engine.New(engine.Provider{ID: "ptah.run/ydb", Targets: []engine.Target{{Name: "ydb"}},
				Codecs:    append(ydbcoordination.Codecs(), ydbdiff.Codecs()...),
				Reversals: []engine.Reversal{{Target: "ydb", Kinds: []schemaext.Kind{ydbdiff.CoordinationNodeKind}, Service: ydbreverse.CoordinationService{}}},
			})
			c.Assert(err, qt.IsNil)
			record := schemaext.ChangeRecord{Subject: ydbcoordination.Ref("app", "locks"), Value: test.change}
			original, err := record.Clone()
			c.Assert(err, qt.IsNil)
			result, err := runtime.ReverseChanges(t.Context(), schemaext.ReversalRequest{Target: "ydb", Identifiers: identifier.ForDialect("ydb"), Capabilities: capability.YDB262(), Changes: []schemaext.ChangeRecord{record}})
			c.Assert(err, qt.IsNil)
			c.Assert(result, qt.HasLen, 1)
			c.Assert(result[0].Change.Subject, qt.Equals, record.Subject)
			reversed := result[0].Change.Value.(*ydbdiff.CoordinationNode)
			c.Assert(reversed.Before, qt.DeepEquals, test.stored)
			c.Assert(reversed.After, qt.DeepEquals, test.change.Before.Desired())
			c.Assert(result[0].Strategy, qt.Equals, test.strategy)
			c.Assert(len(result[0].Limitations) > 0, qt.Equals, test.limitations)
			c.Assert(result[0].ForwardState, qt.HasLen, 1)
			c.Assert(result[0].ForwardState[0].Value, qt.DeepEquals, test.forward)
			c.Assert(record, qt.DeepEquals, original)
		})
	}
}

func TestCoordinationReversalRejectsAnInvalidSuffixAtomically(t *testing.T) {
	c := qt.New(t)
	good := schemaext.ChangeRecord{Subject: ydbcoordination.Ref("app", "locks"), Value: &ydbdiff.CoordinationNode{After: &ydbcoordination.Desired{}}}
	bad := schemaext.ChangeRecord{Subject: ydbcoordination.Ref("app", "other"), Value: &ydbdiff.CoordinationNode{}}
	result, err := (ydbreverse.CoordinationService{}).ReverseChanges(t.Context(), schemaext.ReversalRequest{Target: "ydb", Identifiers: identifier.ForDialect("ydb"), Capabilities: capability.YDB262(), Changes: []schemaext.ChangeRecord{good, bad}})
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(result, qt.IsNil)
}

func TestCoordinationReversalSnapshotsTheForwardState(t *testing.T) {
	c := qt.New(t)
	before := &ydbcoordination.Observed{Spec: ydbcoordination.Spec{ReadConsistencyMode: "strict"}}
	request := schemaext.ReversalRequest{Target: "ydb", Identifiers: identifier.ForDialect("ydb"), Capabilities: capability.YDB262(),
		Changes: []schemaext.ChangeRecord{{Subject: ydbcoordination.Ref("app", "locks"), Value: &ydbdiff.CoordinationNode{Before: before, After: &ydbcoordination.Desired{}}}},
	}
	result, err := (ydbreverse.CoordinationService{}).ReverseChanges(t.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(result, qt.HasLen, 1)
	change := result[0].Change.Value.(*ydbdiff.CoordinationNode)
	change.Before.Spec.ReadConsistencyMode = "mutated"
	before.Spec.ReadConsistencyMode = "changed input"
	c.Assert(result[0].ForwardState[0].Value, qt.DeepEquals, &ydbcoordination.Observed{Spec: ydbcoordination.Spec{ReadConsistencyMode: "relaxed"}})
}
