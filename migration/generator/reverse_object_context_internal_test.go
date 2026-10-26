package generator

// White-box testing required: the context projection is a private boundary
// between reversal and planning. Public SQL cannot expose its knowledge claims.

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/difftypes"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbtopic"
	"ptah.run/engine/builtin"
)

func TestReverseObjectContextUsesCompleteProjectionsAndKeepsSiblings(t *testing.T) {
	c := qt.New(t)
	runtime := must.Must(builtin.New())
	held := ydbtopic.ObservedObject("app", "changed", ydbtopic.Spec{MinActivePartitions: 2})
	sibling := ydbtopic.ObservedObject("app", "retained", ydbtopic.Spec{MinActivePartitions: 7})
	requested := ydbtopic.DesiredObject("app", "changed", "", ydbtopic.Spec{MinActivePartitions: 9})
	coverage := must.Must(ydbtopic.Coverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Uninspected, Reason: "selected topics only"}, nil))
	captured := difftypes.FeatureContext{DesiredObjects: must.Must(schemaext.NewObjects(requested)), CurrentObjects: must.Must(schemaext.NewObjects(held, sibling)), CurrentCoverage: coverage}
	projected := &ydbtopic.Observed{Spec: ydbtopic.Spec{MinActivePartitions: 4}}
	result, err := reverseObjectContext(t.Context(), captured, []schemaext.Reversal{{Change: schemaext.ChangeRecord{Subject: held.Ref}, ForwardState: []schemaext.ProjectedValue{{Placement: schemaext.ObjectPlacement, Kind: ydbtopic.Kind, Value: projected}}}}, "ydb", runtime)
	c.Assert(err, qt.IsNil)
	desired, found, err := result.DesiredObjects.Get(held.Ref)
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	c.Assert(desired.Value.(*ydbtopic.Desired).Spec.MinActivePartitions, qt.Equals, uint64(2))
	observed, found, err := result.CurrentObjects.Get(held.Ref)
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	c.Assert(observed.Value.(*ydbtopic.Observed).Spec.MinActivePartitions, qt.Equals, uint64(4))
	retained, found, err := result.CurrentObjects.Get(sibling.Ref)
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	c.Assert(retained.Value, qt.DeepEquals, sibling.Value)
	c.Assert(result.CurrentCoverage.Lookup(ydbtopic.Kind, held.Ref).State, qt.Equals, schemaext.Complete)
	c.Assert(result.CurrentCoverage.Lookup(ydbtopic.Kind, sibling.Ref).State, qt.Equals, schemaext.Uninspected)
	projected.Spec.MinActivePartitions = 99
	again, _, err := result.CurrentObjects.Get(held.Ref)
	c.Assert(err, qt.IsNil)
	c.Assert(again.Value.(*ydbtopic.Observed).Spec.MinActivePartitions, qt.Equals, uint64(4))
}

func TestReverseObjectContextMissingProjectionIsUnknown(t *testing.T) {
	for _, enrolled := range []bool{false, true} {
		t.Run(map[bool]string{false: "missing namespace", true: "complete namespace"}[enrolled], func(t *testing.T) {
			c := qt.New(t)
			runtime := must.Must(builtin.New())
			object := ydbtopic.ObservedObject("app", "topic", ydbtopic.Spec{MinActivePartitions: 2})
			captured := difftypes.FeatureContext{CurrentObjects: must.Must(schemaext.NewObjects(object))}
			if enrolled {
				captured.CurrentCoverage = must.Must(ydbtopic.Coverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Complete}, nil))
			}
			result, err := reverseObjectContext(t.Context(), captured, []schemaext.Reversal{{Change: schemaext.ChangeRecord{Subject: object.Ref}}}, "ydb", runtime)
			c.Assert(err, qt.IsNil)
			c.Assert(result.CurrentObjects.Len(), qt.Equals, 0)
			knowledge := result.CurrentCoverage.Lookup(ydbtopic.Kind, object.Ref)
			c.Assert(knowledge.State, qt.Equals, schemaext.Uninspected)
			c.Assert(knowledge.Reason, qt.Contains, "did not predict")
			c.Assert(result.DesiredObjects.Len(), qt.Equals, 1)
			sibling := ydbtopic.Ref("app", "unread")
			want := schemaext.Complete
			if !enrolled {
				want = schemaext.Uninspected
			}
			c.Assert(result.CurrentCoverage.Lookup(ydbtopic.Kind, sibling).State, qt.Equals, want)
		})
	}
}
