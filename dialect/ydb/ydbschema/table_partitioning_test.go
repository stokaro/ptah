package ydbschema_test

import (
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbschema"
)

// TestTablePartitioning_CodecsRoundTrip pins the wire form of each
// representation: one object of the settings it states, a false switch
// written as a value, and the split points in order.
func TestTablePartitioning_CodecsRoundTrip(t *testing.T) {
	tests := []struct {
		name  string
		codec int
		value schemaext.Value
		wire  string
	}{
		{name: "a declaration", codec: 0, value: &ydbschema.DesiredTablePartitioning{TablePartitioning: ydbschema.TablePartitioning{
			BySize: new(false), ByLoad: new(true), MinPartitions: 3, MaxPartitions: 9, ReadReplicas: "PER_AZ:0",
			KeyBloomFilter: new(false), PartitionAtKeys: [][]string{{"20"}, {"10", "a"}},
		}}, wire: `{"by_size":false,"by_load":true,"min_partitions":3,"max_partitions":9,"read_replicas":"PER_AZ:0",` +
			`"key_bloom_filter":false,"partition_at_keys":[["20"],["10","a"]]}`},
		{name: "a declaration of a starting layout", codec: 0, value: &ydbschema.DesiredTablePartitioning{TablePartitioning: ydbschema.TablePartitioning{
			PartitionSizeMB: 100, UniformPartitions: 4,
		}}, wire: `{"partition_size_mb":100,"uniform_partitions":4}`},
		{name: "an observation", codec: 1, value: &ydbschema.ObservedTablePartitioning{TablePartitioning: ydbschema.TablePartitioning{
			ReadReplicas: "ANY_AZ:2", KeyBloomFilter: new(true),
		}}, wire: `{"read_replicas":"ANY_AZ:2","key_bloom_filter":true}`},
		{name: "nothing stated", codec: 1, value: &ydbschema.ObservedTablePartitioning{}, wire: `{}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			codec := ydbschema.TablePartitioningCodecs()[test.codec]

			encoded, err := codec.Encode(test.value)
			c.Assert(err, qt.IsNil)
			decoded, err := codec.Decode(encoded)

			c.Assert(err, qt.IsNil)
			c.Assert(string(encoded), qt.Equals, test.wire)
			c.Assert(codec.Prototype.Kind(), qt.Equals, ydbschema.TablePartitioningKind)
			c.Assert(decoded.(schemaext.Value).Equal(test.value), qt.IsTrue)
			c.Assert(test.value.Clone().Equal(test.value), qt.IsTrue)
		})
	}
}

// TestTablePartitioning_EqualReadsSwitchesByValue compares two switches by what
// they hold, and an absent switch differs from a false one.
func TestTablePartitioning_EqualReadsSwitchesByValue(t *testing.T) {
	c := qt.New(t)
	base := ydbschema.TablePartitioning{BySize: new(false), PartitionAtKeys: [][]string{{"10"}}}

	c.Assert(base.Equal(ydbschema.TablePartitioning{BySize: new(false), PartitionAtKeys: [][]string{{"10"}}}), qt.IsTrue)
	c.Assert(base.Equal(ydbschema.TablePartitioning{PartitionAtKeys: [][]string{{"10"}}}), qt.IsFalse)
	c.Assert(base.Equal(ydbschema.TablePartitioning{BySize: new(true), PartitionAtKeys: [][]string{{"10"}}}), qt.IsFalse)
	c.Assert(base.Equal(ydbschema.TablePartitioning{BySize: new(false), PartitionAtKeys: [][]string{{"20"}}}), qt.IsFalse)
	c.Assert(ydbschema.TablePartitioning{KeyBloomFilter: new(false)}.Equal(ydbschema.TablePartitioning{}), qt.IsFalse)
	c.Assert(ydbschema.TablePartitioning{ByLoad: new(true)}.Equal(ydbschema.TablePartitioning{ByLoad: new(false)}), qt.IsFalse)
	c.Assert(ydbschema.TablePartitioning{MaxPartitions: 2}.Equal(ydbschema.TablePartitioning{MaxPartitions: 3}), qt.IsFalse)
	c.Assert(ydbschema.TablePartitioning{}.IsZero(), qt.IsTrue)
	c.Assert(ydbschema.TablePartitioning{ByLoad: new(false)}.IsZero(), qt.IsFalse)
}

// TestTablePartitioning_CloneSharesNothing pins that a clone reaches no
// switch or split point of its source.
func TestTablePartitioning_CloneSharesNothing(t *testing.T) {
	c := qt.New(t)
	original := &ydbschema.DesiredTablePartitioning{TablePartitioning: ydbschema.TablePartitioning{
		BySize: new(true), ByLoad: new(true), KeyBloomFilter: new(true), PartitionAtKeys: [][]string{{"10"}},
	}}

	cloned := original.Copy()
	*cloned.BySize, *cloned.ByLoad, *cloned.KeyBloomFilter = false, false, false
	cloned.PartitionAtKeys[0][0] = "99"
	observed := &ydbschema.ObservedTablePartitioning{TablePartitioning: ydbschema.TablePartitioning{ByLoad: new(true)}}
	declared := observed.Desired()
	*declared.ByLoad = false

	c.Assert(*original.BySize && *original.ByLoad && *original.KeyBloomFilter, qt.IsTrue)
	c.Assert(original.PartitionAtKeys, qt.DeepEquals, [][]string{{"10"}})
	c.Assert(*observed.ByLoad, qt.IsTrue)
	c.Assert((*ydbschema.DesiredTablePartitioning)(nil).Copy(), qt.IsNil)
	c.Assert((*ydbschema.ObservedTablePartitioning)(nil).Desired(), qt.IsNil)
}

// TestValidateTablePartitioning_FailurePath refuses what no source writes,
// and an observation stating a starting layout, which no read finds.
func TestValidateTablePartitioning_FailurePath(t *testing.T) {
	tests := []struct {
		name     string
		settings ydbschema.TablePartitioning
		wantErr  string
	}{
		{"read replicas in lower case", ydbschema.TablePartitioning{ReadReplicas: "per_az:1"}, `.*read replicas "per_az:1" are not PER_AZ:<n> or ANY_AZ:<n>`},
		{"read replicas without a count", ydbschema.TablePartitioning{ReadReplicas: "PER_AZ:"}, `.*read replicas "PER_AZ:" .*`},
		{"read replicas with a padded count", ydbschema.TablePartitioning{ReadReplicas: "PER_AZ:01"}, `.*read replicas "PER_AZ:01" .*`},
		{"another replica mode", ydbschema.TablePartitioning{ReadReplicas: "ALL_AZ:1"}, `.*read replicas "ALL_AZ:1" .*`},
		{"no split point", ydbschema.TablePartitioning{PartitionAtKeys: make([][]string, 0)}, `.*partition_at_keys lists no split point`},
		{"an empty split point", ydbschema.TablePartitioning{PartitionAtKeys: [][]string{{"10"}, {}}}, `.*a split point of partition_at_keys holds no value`},
		{"a NUL in a split point", ydbschema.TablePartitioning{PartitionAtKeys: [][]string{{"a\x00b"}}}, `.*partition_at_keys value contains a NUL byte`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			desired := ydbschema.ValidateDesiredTablePartitioning(&ydbschema.DesiredTablePartitioning{TablePartitioning: test.settings})
			observed := ydbschema.ValidateObservedTablePartitioning(&ydbschema.ObservedTablePartitioning{TablePartitioning: test.settings})

			c.Assert(desired, qt.ErrorMatches, test.wantErr)
			var invalid *schemaext.InvalidModelError
			c.Assert(desired, qt.ErrorAs, &invalid)
			c.Assert(invalid.Kind, qt.Equals, ydbschema.TablePartitioningKind)
			c.Assert(observed, qt.ErrorMatches, test.wantErr)
		})
	}
	c := qt.New(t)
	c.Assert(ydbschema.ValidateDesiredTablePartitioning(nil), qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(ydbschema.ValidateObservedTablePartitioning(nil), qt.ErrorIs, schemaext.ErrInvalidValue)
	for _, layout := range []ydbschema.TablePartitioning{{UniformPartitions: 4}, {PartitionAtKeys: [][]string{{"10"}}}} {
		c.Assert(ydbschema.ValidateObservedTablePartitioning(&ydbschema.ObservedTablePartitioning{TablePartitioning: layout}),
			qt.ErrorMatches, `.*a read finds no starting layout, which YDB keeps no record of`)
		c.Assert(ydbschema.ValidateDesiredTablePartitioning(&ydbschema.DesiredTablePartitioning{TablePartitioning: layout}), qt.IsNil)
	}
}

// TestTablePartitioningCodecs_RefuseMalformedValues accepts only the spelling
// the encoder writes.
func TestTablePartitioningCodecs_RefuseMalformedValues(t *testing.T) {
	for _, wire := range []string{
		`null`,
		`{"BySize":true}`,
		`{"min_partitions":0}`,
		`{"read_replicas":""}`,
		`{"partition_at_keys":null}`,
		`{"partitions":4}`,
		`{"read_replicas":"per_az:1"}`,
	} {
		t.Run(wire, func(t *testing.T) {
			c := qt.New(t)

			decoded, err := ydbschema.TablePartitioningCodecs()[0].Decode(json.RawMessage(wire))

			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(decoded, qt.IsNil)
		})
	}
}

// TestTablePartitioningCoverage_RecordsTheClaimItIsGiven pins coverage: the
// claim for every table, and a subject's own knowledge over it.
func TestTablePartitioningCoverage_RecordsTheClaimItIsGiven(t *testing.T) {
	c := qt.New(t)
	builder := objectidentity.NewBuilder(identifier.ForDialect("ydb"))

	coverage := must.Must(ydbschema.TablePartitioningCoverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Uninspected, Reason: "only returned tables"},
		[]schemaext.SubjectCoverage{{Kind: ydbschema.TablePartitioningKind, Subject: builder.TableParts("app", "events"), Knowledge: schemaext.Knowledge{State: schemaext.Complete}}}))
	_, refused := ydbschema.TablePartitioningCoverage(schemaext.Change, schemaext.Knowledge{State: schemaext.Complete}, nil)

	c.Assert(coverage.Lookup(ydbschema.TablePartitioningKind, builder.TableParts("app", "events")).State, qt.Equals, schemaext.Complete)
	c.Assert(coverage.Lookup(ydbschema.TablePartitioningKind, builder.TableParts("app", "other")).State, qt.Equals, schemaext.Uninspected)
	c.Assert(refused, qt.ErrorIs, schemaext.ErrInvalidValue)
}
