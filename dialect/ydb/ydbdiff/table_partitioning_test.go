package ydbdiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbschema"
)

// TestTablePartitioningCodec_RoundTripsEachTransition pins that an omitted
// before is a table holding YDB's defaults, and that a change decodes to
// itself.
func TestTablePartitioningCodec_RoundTripsEachTransition(t *testing.T) {
	declared := &ydbschema.DesiredTablePartitioning{TablePartitioning: ydbschema.TablePartitioning{MinPartitions: 4}}
	tests := []struct {
		name   string
		change *ydbdiff.TablePartitioning
		wire   string
	}{
		{name: "settings on a table holding the defaults", change: &ydbdiff.TablePartitioning{After: declared},
			wire: `{"after":{"min_partitions":4}}`},
		{name: "a change", change: &ydbdiff.TablePartitioning{
			Before: &ydbschema.ObservedTablePartitioning{TablePartitioning: ydbschema.TablePartitioning{ByLoad: new(true)}}, After: declared},
			wire: `{"before":{"by_load":true},"after":{"min_partitions":4}}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			codec := ydbdiff.TablePartitioningCodec()

			encoded, err := codec.Encode(test.change)
			c.Assert(err, qt.IsNil)
			decoded, err := codec.Decode(encoded)

			c.Assert(err, qt.IsNil)
			c.Assert(string(encoded), qt.Equals, test.wire)
			c.Assert(decoded, qt.DeepEquals, test.change)
		})
	}
}

// TestTablePartitioningCodec_FailurePath refuses a change without the
// declaration, a null or unknown operand, and an operand its model's codec
// or the change's own validation refuses.
func TestTablePartitioningCodec_FailurePath(t *testing.T) {
	for _, wire := range []string{
		`null`,
		`{}`,
		`{"before":{}}`,
		`{"before":null,"after":{}}`,
		`{"after":{},"during":{}}`,
		`{"after":{"read_replicas":"per_az:1"}}`,
		`{"before":{"uniform_partitions":4},"after":{}}`,
	} {
		t.Run(wire, func(t *testing.T) {
			c := qt.New(t)

			decoded, err := ydbdiff.TablePartitioningCodec().Decode([]byte(wire))

			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(decoded, qt.IsNil)
		})
	}
	c := qt.New(t)
	c.Assert(ydbdiff.ValidateTablePartitioning(nil), qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(ydbdiff.ValidateTablePartitioning(&ydbdiff.TablePartitioning{
		Before: &ydbschema.ObservedTablePartitioning{TablePartitioning: ydbschema.TablePartitioning{UniformPartitions: 4}},
		After:  &ydbschema.DesiredTablePartitioning{},
	}), qt.ErrorMatches, `.*observed model "ptah.run/ydb/table-partitioning": .*starting layout.*`)
}

// TestTablePartitioning_CopySharesNoOperand pins that a copied change cannot
// reach back into its source through an operand.
func TestTablePartitioning_CopySharesNoOperand(t *testing.T) {
	c := qt.New(t)
	original := &ydbdiff.TablePartitioning{
		Before: &ydbschema.ObservedTablePartitioning{TablePartitioning: ydbschema.TablePartitioning{ByLoad: new(true)}},
		After:  &ydbschema.DesiredTablePartitioning{TablePartitioning: ydbschema.TablePartitioning{KeyBloomFilter: new(true)}},
	}

	cloned := original.CloneChange().(*ydbdiff.TablePartitioning)
	*cloned.Before.ByLoad, *cloned.After.KeyBloomFilter = false, false

	c.Assert(*original.Before.ByLoad, qt.IsTrue)
	c.Assert(*original.After.KeyBloomFilter, qt.IsTrue)
	c.Assert((*ydbdiff.TablePartitioning)(nil).Copy(), qt.IsNil)
}
