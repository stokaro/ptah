package ydbast_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbast"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbschema"
)

// TestAlterTablePartitioning_SettingsAndCodec pins the operation's wire form,
// which is the owner's change, and the settings it lowers to: the declaration
// read over what the table holds, naming every setting of each group it
// touches.
func TestAlterTablePartitioning_SettingsAndCodec(t *testing.T) {
	c := qt.New(t)
	op := &ydbast.AlterTablePartitioning{Change: ydbdiff.TablePartitioning{
		Before: &ydbschema.ObservedTablePartitioning{TablePartitioning: ydbschema.TablePartitioning{ByLoad: new(true), MinPartitions: 6}},
		After:  &ydbschema.DesiredTablePartitioning{TablePartitioning: ydbschema.TablePartitioning{MinPartitions: 4, KeyBloomFilter: new(true)}},
	}}
	codec := ydbast.TablePartitioningCodec()

	encoded, err := codec.Encode(op)
	c.Assert(err, qt.IsNil)
	decoded, err := codec.Decode(encoded)
	c.Assert(err, qt.IsNil)
	settings, err := op.Settings()
	c.Assert(err, qt.IsNil)
	cloned := op.CloneExtension().(*ydbast.AlterTablePartitioning)
	cloned.Change.After.MinPartitions = 9

	c.Assert(string(encoded), qt.Equals, `{"change":{"before":{"by_load":true,"min_partitions":6},"after":{"min_partitions":4,"key_bloom_filter":true}}}`)
	c.Assert(decoded, qt.DeepEquals, op)
	c.Assert(settings, qt.DeepEquals, []string{
		"AUTO_PARTITIONING_BY_SIZE = ENABLED", "AUTO_PARTITIONING_PARTITION_SIZE_MB = 2048",
		"AUTO_PARTITIONING_BY_LOAD = ENABLED", "AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 4", "KEY_BLOOM_FILTER = ENABLED",
	})
	c.Assert(op.Change.After.MinPartitions, qt.Equals, uint64(4))
	c.Assert(op.Effect().Impact, qt.Equals, schemaext.Additive)
	c.Assert(op.Validate(), qt.IsNil)
	c.Assert((*ydbast.AlterTablePartitioning)(nil).Copy(), qt.IsNil)
}

// TestAlterTablePartitioning_CodecFailurePath refuses an operation that is
// not the owner's change, one after which the table holds what it held, and
// one with a side YDB could not hold.
func TestAlterTablePartitioning_CodecFailurePath(t *testing.T) {
	tests := []struct {
		name string
		data string
	}{
		{name: "null", data: `null`},
		{name: "no change", data: `{}`},
		{name: "an extra property", data: `{"change":{"after":{"min_partitions":4}},"table":"t"}`},
		{name: "a change that changes nothing", data: `{"change":{"before":{"min_partitions":4},"after":{"min_partitions":4}}}`},
		{name: "a declaration stating nothing", data: `{"change":{"before":{"by_load":true},"after":{}}}`},
		{name: "a size YDB refuses", data: `{"change":{"after":{"by_size":false,"partition_size_mb":100}}}`},
		{name: "two starting layouts", data: `{"change":{"after":{"uniform_partitions":4,"partition_at_keys":[["10"]]}}}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			decoded, err := ydbast.TablePartitioningCodec().Decode([]byte(test.data))

			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(decoded, qt.IsNil)
		})
	}
	c := qt.New(t)
	c.Assert((*ydbast.AlterTablePartitioning)(nil).Validate(), qt.ErrorIs, schemaext.ErrInvalidValue)
}
