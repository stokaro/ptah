package ydbreverse_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbreverse"
	"ptah.run/dialect/ydb/ydbworkload"
)

func workloadReverseRequest(ref objectidentity.ID, change schemaext.ChangeValue) schemaext.ReversalRequest {
	return schemaext.ReversalRequest{Target: "ydb", Identifiers: identifier.ForDialect("ydb"),
		Capabilities: capability.Capabilities{capability.ResourcePools: true}, Changes: []schemaext.ChangeRecord{{Subject: ref, Value: change}}}
}

func TestPoolReversalPreservesResetAndZeroWithoutSharingPointers(t *testing.T) {
	c := qt.New(t)
	change := &ydbdiff.ResourcePool{
		Before: &ydbworkload.ObservedPool{Spec: ydbworkload.PoolSpec{ConcurrentQueryLimit: new(int32(0)), QueueSize: new(int32(0))}},
		After:  &ydbworkload.DesiredPool{Spec: ydbworkload.PoolSpec{ResourceWeight: new(12.5)}, StructName: "Holder"},
	}
	result, err := (ydbreverse.PoolService{}).ReverseChanges(t.Context(), workloadReverseRequest(ydbworkload.PoolRef("pool"), change))
	c.Assert(err, qt.IsNil)
	c.Assert(result, qt.HasLen, 1)
	reverse := result[0].Change.Value.(*ydbdiff.ResourcePool)
	c.Assert(reverse.Before.Spec, qt.DeepEquals, change.After.Spec)
	c.Assert(reverse.After, qt.DeepEquals, &ydbworkload.DesiredPool{Spec: change.Before.Spec})
	c.Assert(result[0].ForwardState, qt.DeepEquals, []schemaext.ProjectedValue{{Placement: schemaext.ObjectPlacement, Kind: ydbworkload.PoolKind, Value: change.After.Observed()}})
	c.Assert(result[0].Limitations, qt.HasLen, 1)
	*reverse.Before.Spec.ResourceWeight = 40
	*reverse.After.Spec.ConcurrentQueryLimit, *reverse.After.Spec.QueueSize = 10, 20
	c.Assert(*change.Before.Spec.ConcurrentQueryLimit, qt.Equals, int32(0))
	c.Assert(*change.Before.Spec.QueueSize, qt.Equals, int32(0))
	c.Assert(*change.After.Spec.ResourceWeight, qt.Equals, 12.5)
	c.Assert(*result[0].ForwardState[0].Value.(*ydbworkload.ObservedPool).Spec.ResourceWeight, qt.Equals, 12.5)
}

func TestWorkloadReversalHandlesExplicitCreationAndRemoval(t *testing.T) {
	for _, test := range []struct {
		name       string
		service    schemaext.ReversalService
		ref        objectidentity.ID
		change     schemaext.ChangeValue
		reverse    schemaext.ChangeValue
		projection schemaext.ProjectedValue
	}{
		{"pool creation", ydbreverse.PoolService{}, ydbworkload.PoolRef("pool"),
			&ydbdiff.ResourcePool{After: &ydbworkload.DesiredPool{}}, &ydbdiff.ResourcePool{Before: &ydbworkload.ObservedPool{}},
			schemaext.ProjectedValue{Placement: schemaext.ObjectPlacement, Kind: ydbworkload.PoolKind, Value: &ydbworkload.ObservedPool{}}},
		{"pool removal", ydbreverse.PoolService{}, ydbworkload.PoolRef("pool"),
			&ydbdiff.ResourcePool{Before: &ydbworkload.ObservedPool{}}, &ydbdiff.ResourcePool{After: &ydbworkload.DesiredPool{}},
			schemaext.ProjectedValue{Placement: schemaext.ObjectPlacement, Kind: ydbworkload.PoolKind}},
		{"classifier creation", ydbreverse.ClassifierService{}, ydbworkload.ClassifierRef("route"),
			&ydbdiff.ResourcePoolClassifier{After: &ydbworkload.DesiredClassifier{Spec: ydbworkload.ClassifierSpec{ResourcePool: "pool", Rank: 0}}},
			&ydbdiff.ResourcePoolClassifier{Before: &ydbworkload.ObservedClassifier{Spec: ydbworkload.ClassifierSpec{ResourcePool: "pool", Rank: 0}}},
			schemaext.ProjectedValue{Placement: schemaext.ObjectPlacement, Kind: ydbworkload.ClassifierKind, Value: &ydbworkload.ObservedClassifier{Spec: ydbworkload.ClassifierSpec{ResourcePool: "pool", Rank: 0}}}},
		{"classifier removal", ydbreverse.ClassifierService{}, ydbworkload.ClassifierRef("route"),
			&ydbdiff.ResourcePoolClassifier{Before: &ydbworkload.ObservedClassifier{Spec: ydbworkload.ClassifierSpec{ResourcePool: "pool", Rank: 20, MemberName: "team"}}},
			&ydbdiff.ResourcePoolClassifier{After: &ydbworkload.DesiredClassifier{Spec: ydbworkload.ClassifierSpec{ResourcePool: "pool", Rank: 20, MemberName: "team"}}},
			schemaext.ProjectedValue{Placement: schemaext.ObjectPlacement, Kind: ydbworkload.ClassifierKind}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			result, err := test.service.ReverseChanges(t.Context(), workloadReverseRequest(test.ref, test.change))
			c.Assert(err, qt.IsNil)
			c.Assert(result, qt.HasLen, 1)
			c.Assert(result[0].Change, qt.DeepEquals, schemaext.ChangeRecord{Subject: test.ref, Value: test.reverse})
			c.Assert(result[0].ForwardState, qt.DeepEquals, []schemaext.ProjectedValue{test.projection})
			c.Assert(result[0].Strategy, qt.Not(qt.Equals), "")
		})
	}
}

func TestWorkloadReversalRejectsInvalidBatchesWithoutAPrefix(t *testing.T) {
	for _, invalid := range []schemaext.ChangeRecord{
		{Subject: ydbworkload.PoolRef("default"), Value: &ydbdiff.ResourcePool{After: &ydbworkload.DesiredPool{}}},
		{Subject: ydbworkload.PoolRef("default"), Value: &ydbdiff.ResourcePool{Before: &ydbworkload.ObservedPool{}}},
		{Subject: ydbworkload.PoolRef("pool"), Value: &ydbdiff.ResourcePool{}},
		{Subject: ydbworkload.PoolRef("pool"), Value: &ydbdiff.ResourcePool{Before: &ydbworkload.ObservedPool{}, After: &ydbworkload.DesiredPool{}}},
		{Subject: ydbworkload.ClassifierRef("pool"), Value: &ydbdiff.ResourcePool{After: &ydbworkload.DesiredPool{}}},
	} {
		t.Run(invalid.Subject.String(), func(t *testing.T) {
			c := qt.New(t)
			request := workloadReverseRequest(ydbworkload.PoolRef("valid"), &ydbdiff.ResourcePool{After: &ydbworkload.DesiredPool{}})
			request.Changes = append(request.Changes, invalid)
			result, err := (ydbreverse.PoolService{}).ReverseChanges(t.Context(), request)
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(result, qt.IsNil)
		})
	}
	c := qt.New(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	result, err := (ydbreverse.ClassifierService{}).ReverseChanges(ctx, schemaext.ReversalRequest{})
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(result, qt.IsNil)
}
