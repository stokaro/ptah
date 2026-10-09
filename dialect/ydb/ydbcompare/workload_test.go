package ydbcompare_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbcompare"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbworkload"
	"ptah.run/engine"
)

func workloadRuntime() *engine.Runtime {
	return must.Must(engine.New(engine.Provider{ID: "ptah.run/ydb", Targets: []engine.Target{{Name: "ydb"}},
		Codecs: append(ydbworkload.Codecs(), ydbdiff.ResourcePoolCodec(), ydbdiff.ResourcePoolClassifierCodec()),
		Comparisons: []engine.ObjectComparison{
			{Target: "ydb", Kinds: []schemaext.Kind{ydbworkload.PoolKind}, ChangeKinds: []schemaext.Kind{ydbdiff.ResourcePoolKind}, Service: ydbcompare.PoolService{}},
			{Target: "ydb", Kinds: []schemaext.Kind{ydbworkload.ClassifierKind}, ChangeKinds: []schemaext.Kind{ydbdiff.ResourcePoolClassifierKind}, Service: ydbcompare.ClassifierService{}},
		},
	}))
}

func workloadState(kind schemaext.Kind, representation schemaext.Representation, knowledge schemaext.KnowledgeState, value schemaext.Value, subjects ...schemaext.SubjectCoverage) schemaext.ObjectState {
	state := schemaext.ObjectState{Coverage: must.Must(ydbworkload.Coverage(kind, representation,
		schemaext.Knowledge{State: knowledge, Reason: "namespace evidence"}, subjects))}
	if value != nil {
		ref := ydbworkload.PoolRef("workload")
		if kind == ydbworkload.ClassifierKind {
			ref = ydbworkload.ClassifierRef("workload")
		}
		state.Objects = must.Must(schemaext.NewObjects(schemaext.Object{Ref: ref, Value: value}))
	}
	return state
}

func TestWorkloadComparisonKeepsDatabaseObjectsOutsideApplicationScope(t *testing.T) {
	for _, family := range []struct {
		kind    schemaext.Kind
		ref     objectidentity.ID
		current schemaext.Value
		adopted schemaext.Value
	}{
		{ydbworkload.PoolKind, ydbworkload.PoolRef("workload"), &ydbworkload.ObservedPool{Spec: ydbworkload.PoolSpec{ResourceWeight: new(25.5)}}, &ydbworkload.DesiredPool{Spec: ydbworkload.PoolSpec{ResourceWeight: new(25.5)}}},
		{ydbworkload.ClassifierKind, ydbworkload.ClassifierRef("workload"), &ydbworkload.ObservedClassifier{Spec: ydbworkload.ClassifierSpec{ResourcePool: "another-app", Rank: 0}}, &ydbworkload.DesiredClassifier{Spec: ydbworkload.ClassifierSpec{ResourcePool: "another-app", Rank: 0}}},
	} {
		for _, knowledge := range []schemaext.KnowledgeState{schemaext.Complete, schemaext.Uninspected} {
			t.Run(string(family.kind)+"/"+string(knowledge), func(t *testing.T) {
				c := qt.New(t)
				result, err := workloadRuntime().CompareObjects(t.Context(), schemaext.ObjectComparisonRequest{
					Target: "ydb", Identifiers: identifier.ForDialect("ydb"), Capabilities: capability.Capabilities{capability.ResourcePools: true}, Kinds: []schemaext.Kind{family.kind},
					Desired: workloadState(family.kind, schemaext.Desired, knowledge, nil),
					Current: workloadState(family.kind, schemaext.Observed, schemaext.Complete, family.current),
				})
				c.Assert(err, qt.IsNil)
				c.Assert(result.Complete, qt.IsTrue)
				c.Assert(result.Changes, qt.HasLen, 0)
				c.Assert(result.Undecided, qt.HasLen, 0)
				c.Assert(must.Must(result.Desired.Objects.All()), qt.DeepEquals, []schemaext.Object{{Ref: family.ref, Value: family.adopted}})
				c.Assert(result.Desired.Coverage.Lookup(family.kind, family.ref).State, qt.Equals, schemaext.Complete)
			})
		}
	}
}

func TestPoolComparisonPreservesLimitsAndEvidence(t *testing.T) {
	zero := &ydbworkload.DesiredPool{Spec: ydbworkload.PoolSpec{ConcurrentQueryLimit: new(int32(0))}, StructName: "Pool"}
	unlimited := &ydbworkload.ObservedPool{}
	limited := func(state schemaext.KnowledgeState) []schemaext.SubjectCoverage {
		return []schemaext.SubjectCoverage{{Kind: ydbworkload.PoolKind, Subject: ydbworkload.PoolRef("workload"), Knowledge: schemaext.Knowledge{State: state, Reason: "settings were not described"}}}
	}
	for _, test := range []struct {
		name                               string
		desired                            *ydbworkload.DesiredPool
		current                            *ydbworkload.ObservedPool
		desiredKnowledge, currentKnowledge schemaext.KnowledgeState
		desiredLimits, currentLimits       []schemaext.SubjectCoverage
		changes, undecided                 int
	}{
		{name: "create explicit zero", desired: zero, desiredKnowledge: schemaext.Complete, currentKnowledge: schemaext.Complete, changes: 1},
		{name: "zero changes unlimited", desired: zero, current: unlimited, desiredKnowledge: schemaext.Complete, currentKnowledge: schemaext.Complete, changes: 1},
		{name: "matching zero", desired: zero, current: zero.Observed(), desiredKnowledge: schemaext.Complete, currentKnowledge: schemaext.Complete},
		{name: "unknown namespace withholds create", desired: zero, desiredKnowledge: schemaext.Complete, currentKnowledge: schemaext.Uninspected, undecided: 1},
		{name: "unknown subject withholds alter", desired: zero, current: unlimited, desiredKnowledge: schemaext.Complete, currentKnowledge: schemaext.Complete, currentLimits: limited(schemaext.Unrepresentable), undecided: 1},
		{name: "known object in incomplete namespace", desired: zero, current: unlimited, desiredKnowledge: schemaext.Complete, currentKnowledge: schemaext.Uninspected, changes: 1},
		{name: "incomplete declaration withholds alter", desired: zero, current: unlimited, desiredKnowledge: schemaext.Complete, currentKnowledge: schemaext.Complete, desiredLimits: limited(schemaext.Unrepresentable), undecided: 1},
		{name: "known absence permits create", desired: zero, desiredKnowledge: schemaext.Complete, currentKnowledge: schemaext.Uninspected, currentLimits: limited(schemaext.Absent), changes: 1},
		{name: "empty uninspected namespace", desiredKnowledge: schemaext.Complete, currentKnowledge: schemaext.Uninspected},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired := workloadState(ydbworkload.PoolKind, schemaext.Desired, test.desiredKnowledge, nil, test.desiredLimits...)
			current := workloadState(ydbworkload.PoolKind, schemaext.Observed, test.currentKnowledge, nil, test.currentLimits...)
			setPoolOperands(&desired, &current, test.desired, test.current)
			result, err := workloadRuntime().CompareObjects(t.Context(), schemaext.ObjectComparisonRequest{
				Target: "ydb", Identifiers: identifier.ForDialect("ydb"), Capabilities: capability.Capabilities{capability.ResourcePools: true}, Kinds: []schemaext.Kind{ydbworkload.PoolKind}, Desired: desired, Current: current,
			})
			c.Assert(err, qt.IsNil)
			c.Assert(result.Complete, qt.IsTrue)
			c.Assert(result.Changes, qt.HasLen, test.changes)
			c.Assert(result.Undecided, qt.HasLen, test.undecided)
			for _, record := range result.Changes {
				c.Assert(record, qt.DeepEquals, schemaext.ChangeRecord{Subject: ydbworkload.PoolRef("workload"), Value: &ydbdiff.ResourcePool{Before: test.current, After: test.desired}})
			}
		})
	}
}

func setPoolOperands(desired, current *schemaext.ObjectState, declaration *ydbworkload.DesiredPool, observation *ydbworkload.ObservedPool) {
	if declaration != nil {
		desired.Objects = must.Must(schemaext.NewObjects(schemaext.Object{Ref: ydbworkload.PoolRef("workload"), Value: declaration}))
	}
	if observation != nil {
		current.Objects = must.Must(schemaext.NewObjects(schemaext.Object{Ref: ydbworkload.PoolRef("workload"), Value: observation}))
	}
}

func TestWorkloadComparisonDoesNotCreateAnUninspectedDefaultPool(t *testing.T) {
	c := qt.New(t)
	desired := workloadState(ydbworkload.PoolKind, schemaext.Desired, schemaext.Complete, nil)
	desired.Objects = must.Must(schemaext.NewObjects(ydbworkload.DesiredPoolObject("default", "", ydbworkload.PoolSpec{ResourceWeight: new(50.0)})))
	result, err := workloadRuntime().CompareObjects(t.Context(), schemaext.ObjectComparisonRequest{
		Target: "ydb", Identifiers: identifier.ForDialect("ydb"), Capabilities: capability.Capabilities{capability.ResourcePools: true}, Kinds: []schemaext.Kind{ydbworkload.PoolKind},
		Desired: desired, Current: workloadState(ydbworkload.PoolKind, schemaext.Observed, schemaext.Complete, nil),
	})
	c.Assert(err, qt.IsNil)
	c.Assert(result.Changes, qt.HasLen, 0)
	c.Assert(result.Undecided, qt.HasLen, 1)
	c.Assert(result.Undecided[0].Subject, qt.DeepEquals, ydbworkload.PoolRef("default"))
}

func TestWorkloadExplicitAbsenceRequiresAnOperationWithoutChangingSourceEvidence(t *testing.T) {
	c := qt.New(t)
	ref := ydbworkload.PoolRef("workload")
	absent := schemaext.Knowledge{State: schemaext.Absent, Reason: "source requests absence"}
	desired := workloadState(ydbworkload.PoolKind, schemaext.Desired, schemaext.Complete, nil,
		schemaext.SubjectCoverage{Kind: ydbworkload.PoolKind, Subject: ref, Knowledge: absent})
	result, err := workloadRuntime().CompareObjects(t.Context(), schemaext.ObjectComparisonRequest{
		Target: "ydb", Identifiers: identifier.ForDialect("ydb"), Capabilities: capability.Capabilities{capability.ResourcePools: true}, Kinds: []schemaext.Kind{ydbworkload.PoolKind},
		Desired: desired, Current: workloadState(ydbworkload.PoolKind, schemaext.Observed, schemaext.Complete, &ydbworkload.ObservedPool{}),
	})
	c.Assert(err, qt.IsNil)
	c.Assert(result.Changes, qt.HasLen, 0)
	c.Assert(result.Undecided, qt.HasLen, 1)
	c.Assert(result.Desired.Objects.Len(), qt.Equals, 0)
	c.Assert(result.Desired.Coverage.Lookup(ydbworkload.PoolKind, ref), qt.DeepEquals, absent)
}

func TestClassifierComparisonCapturesRoutingAndRankOperands(t *testing.T) {
	for _, test := range []struct {
		name    string
		before  *ydbworkload.ObservedClassifier
		after   *ydbworkload.DesiredClassifier
		changes int
	}{
		{"create zero rank", nil, &ydbworkload.DesiredClassifier{Spec: ydbworkload.ClassifierSpec{ResourcePool: "Pool", Rank: 0}}, 1},
		{"member reset", &ydbworkload.ObservedClassifier{Spec: ydbworkload.ClassifierSpec{ResourcePool: "Pool", Rank: 0, MemberName: "team"}}, &ydbworkload.DesiredClassifier{Spec: ydbworkload.ClassifierSpec{ResourcePool: "Pool", Rank: 0}}, 1},
		{"rank change", &ydbworkload.ObservedClassifier{Spec: ydbworkload.ClassifierSpec{ResourcePool: "Pool", Rank: 10}}, &ydbworkload.DesiredClassifier{Spec: ydbworkload.ClassifierSpec{ResourcePool: "Pool", Rank: 0}}, 1},
		{"case-sensitive routing", &ydbworkload.ObservedClassifier{Spec: ydbworkload.ClassifierSpec{ResourcePool: "pool", Rank: 0}}, &ydbworkload.DesiredClassifier{Spec: ydbworkload.ClassifierSpec{ResourcePool: "Pool", Rank: 0}}, 1},
		{"unchanged settings", &ydbworkload.ObservedClassifier{Spec: ydbworkload.ClassifierSpec{ResourcePool: "Pool", Rank: 0}}, &ydbworkload.DesiredClassifier{Spec: ydbworkload.ClassifierSpec{ResourcePool: "Pool", Rank: 0}, StructName: "Holder"}, 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			current := workloadState(ydbworkload.ClassifierKind, schemaext.Observed, schemaext.Complete, nil)
			current.Objects = observedClassifierObjects(test.before)
			result, err := workloadRuntime().CompareObjects(t.Context(), schemaext.ObjectComparisonRequest{
				Target: "ydb", Identifiers: identifier.ForDialect("ydb"), Capabilities: capability.Capabilities{capability.ResourcePools: true}, Kinds: []schemaext.Kind{ydbworkload.ClassifierKind},
				Desired: workloadState(ydbworkload.ClassifierKind, schemaext.Desired, schemaext.Complete, test.after), Current: current,
			})
			c.Assert(err, qt.IsNil)
			c.Assert(result.Changes, qt.HasLen, test.changes)
			c.Assert(result.Undecided, qt.HasLen, 0)
			for _, record := range result.Changes {
				c.Assert(record, qt.DeepEquals, schemaext.ChangeRecord{Subject: ydbworkload.ClassifierRef("workload"), Value: &ydbdiff.ResourcePoolClassifier{Before: test.before, After: test.after}})
			}
		})
	}
}

func observedClassifierObjects(value *ydbworkload.ObservedClassifier) schemaext.Objects {
	if value == nil {
		return schemaext.Objects{}
	}
	return must.Must(schemaext.NewObjects(schemaext.Object{Ref: ydbworkload.ClassifierRef("workload"), Value: value}))
}

func TestWorkloadComparisonRefusesInvalidRequestsWithoutPartialOutput(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*schemaext.ObjectComparisonRequest)
		want   error
	}{
		{"wrong target", func(r *schemaext.ObjectComparisonRequest) { r.Target = "postgres" }, ptaherr.ErrUnsupportedDialect},
		{"missing capability", func(r *schemaext.ObjectComparisonRequest) { r.Capabilities = nil }, ptaherr.ErrUnsupportedFeature},
		{"wrong identifiers", func(r *schemaext.ObjectComparisonRequest) { r.Identifiers = identifier.ForDialect("postgres") }, schemaext.ErrInvalidValue},
		{"foreign vocabulary", func(r *schemaext.ObjectComparisonRequest) { r.Kinds = []schemaext.Kind{ydbworkload.ClassifierKind} }, schemaext.ErrInvalidValue},
		{"default limit", func(r *schemaext.ObjectComparisonRequest) {
			r.Desired.Objects = must.Must(schemaext.NewObjects(ydbworkload.DesiredPoolObject("default", "", ydbworkload.PoolSpec{ConcurrentQueryLimit: new(int32(0))})))
		}, schemaext.ErrInvalidValue},
		{"directory scope", func(r *schemaext.ObjectComparisonRequest) {
			ref := ydbworkload.PoolRef("pool")
			ref.Schema = objectidentity.Part{Source: "app", Normalized: "app"}
			r.Desired.Objects = must.Must(schemaext.NewObjects(schemaext.Object{Ref: ref, Value: &ydbworkload.DesiredPool{}}))
		}, schemaext.ErrInvalidValue},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := schemaext.ObjectComparisonRequest{Target: "ydb", Identifiers: identifier.ForDialect("ydb"), Capabilities: capability.Capabilities{capability.ResourcePools: true}, Kinds: []schemaext.Kind{ydbworkload.PoolKind},
				Desired: workloadState(ydbworkload.PoolKind, schemaext.Desired, schemaext.Complete, &ydbworkload.DesiredPool{}),
				Current: workloadState(ydbworkload.PoolKind, schemaext.Observed, schemaext.Complete, nil)}
			test.mutate(&request)
			result, err := (ydbcompare.PoolService{}).CompareObjects(t.Context(), request)
			c.Assert(err, qt.ErrorIs, test.want)
			c.Assert(result, qt.DeepEquals, schemaext.ObjectComparisonResult{})
		})
	}
	c := qt.New(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	result, err := (ydbcompare.PoolService{}).CompareObjects(ctx, schemaext.ObjectComparisonRequest{})
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(result, qt.DeepEquals, schemaext.ObjectComparisonResult{})
}

// Seeing a global object is not a request to alter it. Disabled DDL and partial
// enumeration must preserve unrelated application work and the unknown state.
func TestWorkloadComparisonRetainsUnmanagedObjectsWithoutDDLSupport(t *testing.T) {
	for _, family := range []struct {
		kind    schemaext.Kind
		ref     objectidentity.ID
		current schemaext.Value
		adopted schemaext.Value
	}{
		{ydbworkload.PoolKind, ydbworkload.PoolRef("workload"), &ydbworkload.ObservedPool{}, &ydbworkload.DesiredPool{}},
		{ydbworkload.ClassifierKind, ydbworkload.ClassifierRef("workload"), &ydbworkload.ObservedClassifier{Spec: ydbworkload.ClassifierSpec{ResourcePool: "default"}}, &ydbworkload.DesiredClassifier{Spec: ydbworkload.ClassifierSpec{ResourcePool: "default"}}},
	} {
		for _, knowledge := range []schemaext.KnowledgeState{schemaext.Complete, schemaext.Uninspected} {
			t.Run(string(family.kind)+"/"+string(knowledge), func(t *testing.T) {
				c := qt.New(t)
				request := schemaext.ObjectComparisonRequest{
					Target: "ydb", Identifiers: identifier.ForDialect("ydb"), Kinds: []schemaext.Kind{family.kind},
					Desired: workloadState(family.kind, schemaext.Desired, schemaext.Complete, nil),
					Current: workloadState(family.kind, schemaext.Observed, knowledge, family.current),
				}
				result, err := workloadRuntime().CompareObjects(t.Context(), request)
				c.Assert(err, qt.IsNil)
				c.Assert(result.Changes, qt.HasLen, 0)
				c.Assert(result.Undecided, qt.HasLen, 0)
				c.Assert(must.Must(result.Desired.Objects.All()), qt.DeepEquals, []schemaext.Object{{Ref: family.ref, Value: family.adopted}})
				c.Assert(result.Desired.Coverage.Lookup(family.kind, family.ref).State, qt.Equals, schemaext.Complete)
				c.Assert(result.Desired.Coverage.Lookup(family.kind, objectidentity.ID{}).State, qt.Equals, schemaext.Complete)
				c.Assert(request.Current.Coverage.Lookup(family.kind, objectidentity.ID{}).State, qt.Equals, knowledge)
				request.Desired = result.Desired
				repeated, err := workloadRuntime().CompareObjects(t.Context(), request)
				c.Assert(err, qt.IsNil)
				c.Assert(repeated.Changes, qt.HasLen, 0)
				c.Assert(repeated.Undecided, qt.HasLen, 0)
			})
		}
	}
}

func TestWorkloadComparisonKeepsUnmanagedSubjectLimits(t *testing.T) {
	c := qt.New(t)
	ref := ydbworkload.PoolRef("workload")
	limit := schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "settings unavailable"}
	result, err := workloadRuntime().CompareObjects(t.Context(), schemaext.ObjectComparisonRequest{
		Target: "ydb", Identifiers: identifier.ForDialect("ydb"), Kinds: []schemaext.Kind{ydbworkload.PoolKind},
		Desired: workloadState(ydbworkload.PoolKind, schemaext.Desired, schemaext.Uninspected, nil),
		Current: workloadState(ydbworkload.PoolKind, schemaext.Observed, schemaext.Uninspected, &ydbworkload.ObservedPool{},
			schemaext.SubjectCoverage{Kind: ydbworkload.PoolKind, Subject: ref, Knowledge: limit}),
	})
	c.Assert(err, qt.IsNil)
	c.Assert(result.Changes, qt.HasLen, 0)
	c.Assert(result.Undecided, qt.HasLen, 1)
	c.Assert(result.Desired.Objects.Len(), qt.Equals, 0)
	c.Assert(result.Desired.Coverage.Lookup(ydbworkload.PoolKind, ref), qt.DeepEquals, limit)
}
