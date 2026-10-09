package ydbconvert_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbconvert"
	"ptah.run/dialect/ydb/ydbworkload"
	"ptah.run/engine"
)

func TestWorkloadConversionUsesTheSelectedFamilyAndPreservesInputOrder(t *testing.T) {
	c := qt.New(t)
	runtime, err := engine.New(engine.Provider{ID: "ptah.run/ydb", Targets: []engine.Target{{Name: "ydb"}}, Codecs: ydbworkload.Codecs(),
		Conversions: []engine.Conversion{
			{Target: "ydb", Kinds: []schemaext.Kind{ydbworkload.PoolKind}, Service: ydbconvert.PoolService{}},
			{Target: "ydb", Kinds: []schemaext.Kind{ydbworkload.ClassifierKind}, Service: ydbconvert.ClassifierService{}},
		}})
	c.Assert(err, qt.IsNil)
	pool := &ydbworkload.DesiredPool{Spec: ydbworkload.PoolSpec{ConcurrentQueryLimit: new(int32(0))}, StructName: "Holder"}
	classifier := &ydbworkload.DesiredClassifier{Spec: ydbworkload.ClassifierSpec{ResourcePool: "pool", Rank: 0, MemberName: "team"}, StructName: "Routing"}
	converted, err := runtime.ConvertFeatures(t.Context(), schemaext.ConversionRequest{Target: "ydb", From: schemaext.Desired, To: schemaext.Observed,
		Values: []schemaext.Value{classifier, pool}})
	c.Assert(err, qt.IsNil)
	c.Assert(converted, qt.DeepEquals, []schemaext.Value{classifier.Observed(), pool.Observed()})
	restored, err := runtime.ConvertFeatures(t.Context(), schemaext.ConversionRequest{Target: "ydb", From: schemaext.Observed, To: schemaext.Desired, Values: converted})
	c.Assert(err, qt.IsNil)
	c.Assert(restored, qt.DeepEquals, []schemaext.Value{classifier.Observed().Desired(), pool.Observed().Desired()})
	*converted[1].(*ydbworkload.ObservedPool).Spec.ConcurrentQueryLimit = 30
	c.Assert(*pool.Spec.ConcurrentQueryLimit, qt.Equals, int32(0))
	c.Assert(*restored[1].(*ydbworkload.DesiredPool).Spec.ConcurrentQueryLimit, qt.Equals, int32(0))
}

func TestWorkloadConversionDiscardsPrefixBeforeInvalidValue(t *testing.T) {
	for _, invalid := range []schemaext.Value{&ydbworkload.ObservedPool{}, &ydbworkload.DesiredClassifier{}, (*ydbworkload.DesiredPool)(nil)} {
		t.Run(string(invalid.Kind()), func(t *testing.T) {
			c := qt.New(t)
			result, err := (ydbconvert.PoolService{}).ConvertFeatures(t.Context(), schemaext.ConversionRequest{Target: "ydb", From: schemaext.Desired, To: schemaext.Observed,
				Values: []schemaext.Value{&ydbworkload.DesiredPool{}, invalid}})
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(result, qt.IsNil)
		})
	}
	c := qt.New(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	result, err := (ydbconvert.ClassifierService{}).ConvertFeatures(ctx, schemaext.ConversionRequest{Target: "ydb", From: schemaext.Desired, To: schemaext.Observed})
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(result, qt.IsNil)
}
