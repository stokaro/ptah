package ydbdiff_test

import (
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbworkload"
)

func TestWorkloadChangeCodecsRequireExplicitOperands(t *testing.T) {
	for _, codec := range []schemaext.Codec{ydbdiff.ResourcePoolCodec(), ydbdiff.ResourcePoolClassifierCodec()} {
		for _, data := range []string{
			`null`, `{}`, `{"before":null}`, `{"after":null}`, `{"before":null,"after":null}`,
			`{"Before":null,"after":{"spec":{}}}`, `{"before":{},"after":null}`,
			`{"before":null,"before":null,"after":{"spec":{}}}`, `{"before":null,"after":{"spec":null}}`,
			`{"before":null,"after":{"spec":{}},"unknown":1}`,
		} {
			t.Run(string(codec.Prototype.Kind())+"/"+data, func(t *testing.T) {
				c := qt.New(t)
				value, err := codec.Decode(json.RawMessage(data))
				c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
				c.Assert(value, qt.IsNil)
			})
		}
	}
}

func TestPoolChangeRetainsZeroAndResetOperands(t *testing.T) {
	c := qt.New(t)
	before := &ydbworkload.ObservedPool{Spec: ydbworkload.PoolSpec{ConcurrentQueryLimit: new(int32(0)), QueueSize: new(int32(0))}}
	after := &ydbworkload.DesiredPool{StructName: "Holder"}
	change := &ydbdiff.ResourcePool{Before: before, After: after}
	codec := ydbdiff.ResourcePoolCodec()
	wire := must.Must(codec.Encode(change))
	c.Assert(string(wire), qt.Equals, `{"before":{"spec":{"concurrent_query_limit":0,"queue_size":0}},"after":{"spec":{},"struct_name":"Holder"}}`)
	decoded, err := codec.Decode(wire)
	c.Assert(err, qt.IsNil)
	c.Assert(decoded, qt.DeepEquals, change)
	clone := change.CloneChange().(*ydbdiff.ResourcePool)
	*clone.Before.Spec.QueueSize = 30
	c.Assert(*before.Spec.QueueSize, qt.Equals, int32(0))
	c.Assert(change.Effect().Impact, qt.Equals, schemaext.Behavioral)
}

func TestWorkloadChangeTransportAndImpact(t *testing.T) {
	for _, test := range []struct {
		name   string
		codec  schemaext.Codec
		value  schemaext.ChangeValue
		impact schemaext.Impact
	}{
		{"pool create", ydbdiff.ResourcePoolCodec(), &ydbdiff.ResourcePool{After: &ydbworkload.DesiredPool{}}, schemaext.Additive},
		{"pool remove", ydbdiff.ResourcePoolCodec(), &ydbdiff.ResourcePool{Before: &ydbworkload.ObservedPool{}}, schemaext.Behavioral},
		{"classifier create", ydbdiff.ResourcePoolClassifierCodec(), &ydbdiff.ResourcePoolClassifier{After: &ydbworkload.DesiredClassifier{Spec: ydbworkload.ClassifierSpec{ResourcePool: "default"}}}, schemaext.Behavioral},
		{"classifier remove", ydbdiff.ResourcePoolClassifierCodec(), &ydbdiff.ResourcePoolClassifier{Before: &ydbworkload.ObservedClassifier{Spec: ydbworkload.ClassifierSpec{ResourcePool: "pool", Rank: 20}}}, schemaext.Behavioral},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			wire := must.Must(test.codec.Encode(test.value))
			decoded, err := test.codec.Decode(wire)
			c.Assert(err, qt.IsNil)
			c.Assert(decoded, qt.DeepEquals, test.value)
			c.Assert(test.value.(interface{ Effect() schemaext.Effect }).Effect().Impact, qt.Equals, test.impact)
		})
	}
}
