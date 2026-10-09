package ydbast_test

import (
	"encoding/json"
	"errors"
	"math"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbast"
)

func TestPoolOperationRoundTripPreservesZeroUnsetAndBothOperands(t *testing.T) {
	c := qt.New(t)
	value := &ydbast.ResourcePool{Operation: ydbast.PoolAlter, Name: "batch.jobs",
		Spec:     &ast.ResourcePoolSpec{ConcurrentQueryLimit: new(int32(0)), ResourceWeight: new(0.0), DatabaseLoadCPUThreshold: new(80.5), QueryMemoryLimitPercentPerNode: new(20.25), QueryCPULimitPercentPerNode: new(10.75), TotalCPULimitPercentPerNode: new(60.5)},
		Previous: &ast.ResourcePoolSpec{ConcurrentQueryLimit: new(int32(7)), QueueSize: new(int32(9)), ResourceWeight: new(12.5)}}
	registry, err := schemaext.NewRegistry(schemaext.OwnedCodec{Owner: "ptah.run/ydb", Codec: ydbast.ResourcePoolCodec()})
	c.Assert(err, qt.IsNil)
	data, err := registry.Marshal(c.Context(), schemaext.Operation, []schemaext.Payload{value})
	c.Assert(err, qt.IsNil)
	c.Assert(string(data), qt.Contains, `"concurrent_query_limit":0`)
	decoded, err := registry.Unmarshal(c.Context(), data)
	c.Assert(err, qt.IsNil)
	c.Assert(decoded, qt.DeepEquals, []schemaext.Payload{value})
	clone := decoded[0].(*ydbast.ResourcePool).CloneExtension().(*ydbast.ResourcePool)
	*clone.Spec.ConcurrentQueryLimit = 3
	*clone.Previous.QueueSize = 4
	*clone.Spec.ResourceWeight = 25
	c.Assert(*value.Spec.ConcurrentQueryLimit, qt.Equals, int32(0))
	c.Assert(*decoded[0].(*ydbast.ResourcePool).Previous.QueueSize, qt.Equals, int32(9))
	c.Assert(*decoded[0].(*ydbast.ResourcePool).Spec.ResourceWeight, qt.Equals, 0.0)
	c.Assert(decoded[0].(*ydbast.ResourcePool).Spec.QueueSize, qt.IsNil)
}

func TestClassifierOperationRoundTripPreservesRankAndMemberReset(t *testing.T) {
	c := qt.New(t)
	value := &ydbast.ResourcePoolClassifier{Operation: ydbast.PoolAlter, Name: "route.jobs",
		Spec:     &ast.ResourcePoolClassifierSpec{ResourcePool: "default", Rank: 0},
		Previous: &ast.ResourcePoolClassifierSpec{ResourcePool: "batch", MemberName: "etl", Rank: math.MaxInt64}}
	codec := ydbast.ResourcePoolClassifierCodec()
	data, err := codec.Encode(value)
	c.Assert(err, qt.IsNil)
	c.Assert(string(data), qt.Contains, `"rank":9223372036854775807`)
	decoded, err := codec.Decode(data)
	c.Assert(err, qt.IsNil)
	c.Assert(decoded, qt.DeepEquals, value)
	clone, err := codec.Clone(decoded)
	c.Assert(err, qt.IsNil)
	clone.(*ydbast.ResourcePoolClassifier).Spec.Rank = 15
	clone.(*ydbast.ResourcePoolClassifier).Previous.MemberName = "other"
	c.Assert(decoded.(*ydbast.ResourcePoolClassifier).Spec.Rank, qt.Equals, int64(0))
	c.Assert(decoded.(*ydbast.ResourcePoolClassifier).Previous.MemberName, qt.Equals, "etl")
}

func TestWorkloadCodecsRejectIncompleteOrAmbiguousWire(t *testing.T) {
	for _, test := range []struct {
		name  string
		codec schemaext.Codec
		data  string
	}{
		{"missing operand", ydbast.ResourcePoolCodec(), `{"operation":"create","name":"batch"}`},
		{"missing previous", ydbast.ResourcePoolCodec(), `{"operation":"alter","name":"batch","spec":{}}`},
		{"irrelevant previous", ydbast.ResourcePoolCodec(), `{"operation":"create","name":"batch","spec":{},"previous":{}}`},
		{"drop settings", ydbast.ResourcePoolCodec(), `{"operation":"drop","name":"batch","spec":{}}`},
		{"null operand", ydbast.ResourcePoolCodec(), `{"operation":"drop","name":"batch","spec":null}`},
		{"null setting", ydbast.ResourcePoolCodec(), `{"operation":"create","name":"batch","spec":{"queue_size":null}}`},
		{"unknown setting", ydbast.ResourcePoolCodec(), `{"operation":"create","name":"batch","spec":{"unknown":2}}`},
		{"case-variant name", ydbast.ResourcePoolCodec(), `{"operation":"drop","name":"batch","Name":"other"}`},
		{"case-variant setting", ydbast.ResourcePoolCodec(), `{"operation":"create","name":"batch","spec":{"Resource_Weight":25}}`},
		{"duplicate name", ydbast.ResourcePoolCodec(), `{"operation":"drop","name":"batch","name":"other"}`},
		{"fractional count", ydbast.ResourcePoolCodec(), `{"operation":"create","name":"batch","spec":{"concurrent_query_limit":0.5}}`},
		{"missing rank", ydbast.ResourcePoolClassifierCodec(), `{"operation":"create","name":"route","spec":{"resource_pool":"default"}}`},
		{"null rank", ydbast.ResourcePoolClassifierCodec(), `{"operation":"create","name":"route","spec":{"resource_pool":"default","rank":null}}`},
		{"rank overflow", ydbast.ResourcePoolClassifierCodec(), `{"operation":"create","name":"route","spec":{"resource_pool":"default","rank":9223372036854775808}}`},
		{"invalid UTF-8", ydbast.ResourcePoolClassifierCodec(), "{\"operation\":\"drop\",\"name\":\"\xff\"}"},
		{"unknown operation", ydbast.ResourcePoolCodec(), `{"operation":"pause","name":"batch"}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			value, err := test.codec.Decode(json.RawMessage(test.data))
			c.Assert(err, qt.IsNotNil)
			c.Assert(value, qt.IsNil)
		})
	}
}

func TestPoolCodecsRefuseInvalidModelsAndUnknownEffects(t *testing.T) {
	for _, test := range []struct {
		name  string
		value *ydbast.ResourcePool
	}{
		{"nil", nil},
		{"default drop", &ydbast.ResourcePool{Operation: ydbast.PoolDrop, Name: "default"}},
		{"path", &ydbast.ResourcePool{Operation: ydbast.PoolDrop, Name: "directory/batch"}},
		{"queue without limit", &ydbast.ResourcePool{Operation: ydbast.PoolCreate, Name: "batch", Spec: &ast.ResourcePoolSpec{QueueSize: new(int32(1))}}},
		{"nonfinite", &ydbast.ResourcePool{Operation: ydbast.PoolCreate, Name: "batch", Spec: &ast.ResourcePoolSpec{ResourceWeight: new(math.Inf(1))}}},
		{"invalid previous", &ydbast.ResourcePool{Operation: ydbast.PoolAlter, Name: "batch", Spec: &ast.ResourcePoolSpec{}, Previous: &ast.ResourcePoolSpec{ResourceWeight: new(math.NaN())}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			data, err := ydbast.ResourcePoolCodec().Encode(test.value)
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			model, ok := errors.AsType[*schemaext.InvalidModelError](err)
			c.Assert(ok, qt.IsTrue)
			c.Assert(model.Kind, qt.Equals, ydbast.ResourcePoolKind)
			c.Assert(data, qt.IsNil)
			c.Assert(test.value.Effect().Impact, qt.Equals, schemaext.Impact(""))
		})
	}
}
