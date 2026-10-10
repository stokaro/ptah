package ydbplan_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/featureplan"
	"ptah.run/core/plangraph"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbast"
	"ptah.run/dialect/ydb/ydbcoordination"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbplan"
	"ptah.run/dialect/ydb/ydbscheme"
	"ptah.run/dialect/ydb/ydbstreaming"
	"ptah.run/engine"
)

// standaloneRuntime plans coordination nodes and streaming queries, two
// standalone owners that order their statements against the scheme paths
// above them the way a secret does.
func standaloneRuntime(c *qt.C) *engine.Runtime {
	c.Helper()
	codecs := append(ydbcoordination.Codecs(), ydbdiff.Codecs()...)
	codecs = append(codecs, ydbast.CoordinationCodec())
	codecs = append(codecs, ydbstreaming.Codecs()...)
	codecs = append(codecs, ydbast.StreamingCodec())
	runtime, err := engine.New(engine.Provider{ID: "ptah.run/ydb", Targets: []engine.Target{{Name: "ydb"}}, Codecs: codecs,
		Planning: []engine.Planning{
			{Target: "ydb", Kinds: []schemaext.Kind{ydbdiff.CoordinationNodeKind}, OperationKinds: []schemaext.Kind{ydbast.CoordinationNodeKind}, Service: ydbplan.CoordinationService{}},
			{Target: "ydb", Kinds: []schemaext.Kind{ydbdiff.StreamingQueryKind}, OperationKinds: []schemaext.Kind{ydbast.StreamingQueryKind}, Service: ydbplan.StreamingService{}},
		}})
	c.Assert(err, qt.IsNil)
	return runtime
}

// The changes the ordering tests plan: each owner's creation and drop of an
// object in app/ext, below the directories app and app/ext.
var (
	coordinationNodeCreated = schemaext.ChangeRecord{Subject: ydbcoordination.Ref("app/ext", "lock"),
		Value: &ydbdiff.CoordinationNode{After: &ydbcoordination.Desired{}}}
	coordinationNodeDropped = schemaext.ChangeRecord{Subject: ydbcoordination.Ref("app/ext", "lock"),
		Value: &ydbdiff.CoordinationNode{Before: &ydbcoordination.Observed{}}}
	streamingQueryCreated = schemaext.ChangeRecord{Subject: ydbstreaming.Ref("app/ext", "copy"),
		Value: &ydbdiff.StreamingQuery{After: &ydbstreaming.Desired{Spec: ydbstreaming.Spec{Text: "SELECT 1;"}}}}
	streamingQueryDropped = schemaext.ChangeRecord{Subject: ydbstreaming.Ref("app/ext", "copy"),
		Value: &ydbdiff.StreamingQuery{Before: &ydbstreaming.Observed{Spec: ydbstreaming.Spec{Text: "SELECT 1;"}}}}
)

// TestStandalonePlan_CreationFollowsADropAbove creates a coordination node and
// a streaming query below two paths the common plan drops: each creation
// follows both drops, since YDB needs every directory above an object to be a
// directory.
func TestStandalonePlan_CreationFollowsADropAbove(t *testing.T) {
	for _, change := range []schemaext.ChangeRecord{coordinationNodeCreated, streamingQueryCreated} {
		t.Run(string(change.Subject.Kind), func(t *testing.T) {
			c := qt.New(t)
			chain := commonChain([]plangraph.Effect{{Subject: ydbscheme.Path("", "app"), Action: plangraph.Drop}},
				[]plangraph.Effect{{Subject: ydbscheme.Path("app", "ext"), Action: plangraph.Drop}})
			request := featureplan.Request{Target: "ydb", Identifiers: identifier.ForDialect("ydb"), Capabilities: capability.YDB262().With(capability.StreamingQueries, true),
				CommonSteps: chain.steps, Changes: []schemaext.ChangeRecord{change}}

			result, err := standaloneRuntime(c).PlanFeatures(t.Context(), request)

			c.Assert(err, qt.IsNil)
			c.Assert(result.Err(request), qt.IsNil)
			create := result.Changes[0].Steps[0]
			c.Assert(result.Contributions[0].Dependencies, qt.Contains, plangraph.Dependency{Before: chain.steps[0].ID, After: create})
			c.Assert(result.Contributions[0].Dependencies, qt.Contains, plangraph.Dependency{Before: chain.steps[1].ID, After: create})
		})
	}
}

// TestStandalonePlan_DropPrecedesACreationAbove drops a coordination node and
// a streaming query below two paths the common plan creates: each drop
// precedes both creations, which would otherwise find a directory that still
// holds the object.
func TestStandalonePlan_DropPrecedesACreationAbove(t *testing.T) {
	for _, change := range []schemaext.ChangeRecord{coordinationNodeDropped, streamingQueryDropped} {
		t.Run(string(change.Subject.Kind), func(t *testing.T) {
			c := qt.New(t)
			chain := commonChain([]plangraph.Effect{{Subject: ydbscheme.Path("", "app"), Action: plangraph.Create}},
				[]plangraph.Effect{{Subject: ydbscheme.Path("app", "ext"), Action: plangraph.Create}})
			request := featureplan.Request{Target: "ydb", Identifiers: identifier.ForDialect("ydb"), Capabilities: capability.YDB262().With(capability.StreamingQueries, true),
				CommonSteps: chain.steps, Changes: []schemaext.ChangeRecord{change}}

			result, err := standaloneRuntime(c).PlanFeatures(t.Context(), request)

			c.Assert(err, qt.IsNil)
			c.Assert(result.Err(request), qt.IsNil)
			drop := result.Changes[0].Steps[0]
			c.Assert(result.Contributions[0].Dependencies, qt.Contains, plangraph.Dependency{Before: drop, After: chain.steps[0].ID})
			c.Assert(result.Contributions[0].Dependencies, qt.Contains, plangraph.Dependency{Before: drop, After: chain.steps[1].ID})
		})
	}
}
