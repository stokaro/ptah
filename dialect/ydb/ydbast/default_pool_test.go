package ydbast_test

import (
	"encoding/json"
	"math"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbast"
	"ptah.run/dialect/ydb/ydbworkload"
)

func TestDefaultPoolSettingsWireHasNoInventedObservation(t *testing.T) {
	c := qt.New(t)
	value := &ydbast.DefaultPoolSettings{Spec: ydbworkload.PoolSpec{ResourceWeight: new(0.0), QueryCPULimitPercentPerNode: new(12.5)}}
	codec := ydbast.DefaultPoolSettingsCodec()
	encoded, err := codec.Encode(value)
	c.Assert(err, qt.IsNil)
	c.Assert(string(encoded), qt.Equals, `{"spec":{"query_cpu_limit_percent_per_node":12.5,"resource_weight":0}}`)
	decoded, err := codec.Decode(encoded)
	c.Assert(err, qt.IsNil)
	c.Assert(decoded, qt.DeepEquals, value)
	clone, err := codec.Clone(decoded)
	c.Assert(err, qt.IsNil)
	*clone.(*ydbast.DefaultPoolSettings).Spec.ResourceWeight = 99
	c.Assert(*decoded.(*ydbast.DefaultPoolSettings).Spec.ResourceWeight, qt.Equals, 0.0)
	c.Assert(value.Subject(), qt.DeepEquals, ydbworkload.PoolRef("default"))
	c.Assert(value.Effect().Impact, qt.Equals, schemaext.Behavioral)
}

func TestDefaultPoolSettingsRefusesMalformedWire(t *testing.T) {
	for _, tc := range []struct {
		name string
		wire string
	}{
		{"missing settings", `{}`},
		{"null settings", `{"spec":null}`},
		{"invented observation", `{"spec":{},"previous":{}}`},
		{"named pool", `{"spec":{},"name":"batch"}`},
		{"null limit", `{"spec":{"resource_weight":null}}`},
		{"unknown limit", `{"spec":{"weight":2}}`},
		{"case variant", `{"spec":{"Resource_Weight":2}}`},
		{"duplicate limit", `{"spec":{"resource_weight":2,"resource_weight":3}}`},
		{"invalid limit", `{"spec":{"resource_weight":101}}`},
		{"default concurrency", `{"spec":{"concurrent_query_limit":0}}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			value, err := ydbast.DefaultPoolSettingsCodec().Decode(json.RawMessage(tc.wire))
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(value, qt.IsNil)
		})
	}
}

func TestDefaultPoolSettingsInvalidValuesHaveUnknownEffects(t *testing.T) {
	for _, tc := range []struct {
		name  string
		value *ydbast.DefaultPoolSettings
	}{
		{"nil", nil},
		{"infinite limit", &ydbast.DefaultPoolSettings{Spec: ydbworkload.PoolSpec{ResourceWeight: new(math.Inf(1))}}},
		{"queue", &ydbast.DefaultPoolSettings{Spec: ydbworkload.PoolSpec{QueueSize: new(int32(1)), ConcurrentQueryLimit: new(int32(1))}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			encoded, err := ydbast.DefaultPoolSettingsCodec().Encode(tc.value)
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(encoded, qt.IsNil)
			c.Assert(tc.value.Effect(), qt.DeepEquals, schemaext.Effect{})
		})
	}
}
