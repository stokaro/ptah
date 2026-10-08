package ydbcompare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/capability"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/engine/builtin"
)

func replicationStream() *ydbschema.ObservedChangefeed {
	return &ydbschema.ObservedChangefeed{Spec: feed("updates"),
		Replication: &ydbschema.ReplicationBinding{DestinationPath: "/remote/replica", ItemID: "1"}}
}

func ownedState(c *qt.C, representation schemaext.Representation, knowledge schemaext.KnowledgeState, value schemaext.Value, limits ...schemaext.SubjectCoverage) schemaext.ObjectState {
	c.Helper()
	state := source(c, representation, knowledge, nil, limits...)
	if value != nil {
		objects, err := state.Objects.With(schemaext.Object{Ref: ydbschema.ChangefeedRef("", "orders", "updates"), Value: value})
		c.Assert(err, qt.IsNil)
		state.Objects = objects
	}
	return state
}

func TestComparisonRetainsReplicationStreamsAcrossRepeatedPlanning(t *testing.T) {
	for _, knowledge := range []schemaext.KnowledgeState{schemaext.Complete, schemaext.Uninspected} {
		t.Run(string(knowledge), func(t *testing.T) {
			c := qt.New(t)
			runtime, err := builtin.New()
			c.Assert(err, qt.IsNil)
			observed := replicationStream()
			request := schemaext.ObjectComparisonRequest{Target: "ydb", Capabilities: capability.YDB262(),
				Desired: ownedState(c, schemaext.Desired, knowledge, nil),
				Current: ownedState(c, schemaext.Observed, schemaext.Complete, observed),
				Parents: []schemaext.ParentState{{Subject: table("", "orders"), Desired: true, Current: true}}}
			for range 2 {
				result, err := runtime.CompareObjects(t.Context(), request)
				c.Assert(err, qt.IsNil)
				c.Assert(result.Complete, qt.IsTrue)
				c.Assert(result.Changes, qt.HasLen, 0)
				c.Assert(result.Undecided, qt.HasLen, 0)
				object, found, err := result.Desired.Objects.Get(ydbschema.ChangefeedRef("", "orders", "updates"))
				c.Assert(err, qt.IsNil)
				c.Assert(found, qt.IsTrue)
				c.Assert(object.Value.Equal(observed.Desired()), qt.IsTrue)
				request.Desired = result.Desired
			}
		})
	}
}

func TestComparisonWithholdsIndependentReplicationStreamChanges(t *testing.T) {
	changedBinding := replicationStream().Desired()
	changedBinding.RetainedReplication.ItemID = "2"
	changedSpec := replicationStream().Desired()
	changedSpec.Spec.RetentionPeriod = "PT12H"
	for _, test := range []struct {
		name    string
		desired schemaext.Value
		current schemaext.Value
		limits  []schemaext.SubjectCoverage
	}{
		{name: "take ownership", desired: &ydbschema.DesiredChangefeed{Spec: feed("updates")}, current: replicationStream()},
		{name: "changed binding", desired: changedBinding, current: replicationStream()},
		{name: "changed retained settings", desired: changedSpec, current: replicationStream()},
		{name: "missing retained stream", desired: replicationStream().Desired()},
		{name: "binding disappeared", desired: replicationStream().Desired(), current: &ydbschema.ObservedChangefeed{Spec: feed("updates")}},
		{name: "explicit absence", current: replicationStream(), limits: []schemaext.SubjectCoverage{limit(schemaext.Absent)}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			runtime, err := builtin.New()
			c.Assert(err, qt.IsNil)
			result, err := runtime.CompareObjects(t.Context(), schemaext.ObjectComparisonRequest{Target: "ydb", Capabilities: capability.YDB262(),
				Desired: ownedState(c, schemaext.Desired, schemaext.Complete, test.desired, test.limits...),
				Current: ownedState(c, schemaext.Observed, schemaext.Complete, test.current),
				Parents: []schemaext.ParentState{{Subject: table("", "orders"), Desired: true, Current: true}}})
			c.Assert(err, qt.IsNil)
			c.Assert(result.Changes, qt.HasLen, 0)
			c.Assert(result.Undecided, qt.HasLen, 1)
			c.Assert(result.Undecided[0].Reason, qt.Contains, "independently of its controller")
		})
	}
}

func TestReplicationBindingDoesNotOverrideIncompleteInspection(t *testing.T) {
	c := qt.New(t)
	runtime, err := builtin.New()
	c.Assert(err, qt.IsNil)
	result, err := runtime.CompareObjects(t.Context(), schemaext.ObjectComparisonRequest{Target: "ydb", Capabilities: capability.YDB262(),
		Desired: ownedState(c, schemaext.Desired, schemaext.Complete, nil),
		Current: ownedState(c, schemaext.Observed, schemaext.Complete, replicationStream(), limit(schemaext.Unrepresentable)),
		Parents: []schemaext.ParentState{{Subject: table("", "orders"), Desired: true, Current: true}}})
	c.Assert(err, qt.IsNil)
	c.Assert(result.Changes, qt.HasLen, 0)
	c.Assert(result.Undecided, qt.HasLen, 1)
	c.Assert(result.Desired.Objects.Len(), qt.Equals, 0)
}
