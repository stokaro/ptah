package ydbast_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbast"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbschema"
)

// TestAlterColumnFamilies_CodecRoundTripsAndClonesIndependently pins the
// operation's wire form, which is the owner's change, and its statement
// actions: a family the table lacks is added, a setting it holds otherwise is
// set, and a column whose family differs is moved.
func TestAlterColumnFamilies_CodecRoundTripsAndClonesIndependently(t *testing.T) {
	c := qt.New(t)
	op := &ydbast.AlterColumnFamilies{Change: ydbdiff.ColumnFamilies{
		Before: &ydbschema.ObservedColumnFamilies{Families: []ydbschema.ColumnFamily{
			{Name: "default", Compression: "off"},
			{Name: "cold", Data: "hdd", Compression: "off", Columns: []string{"body"}},
		}},
		After: &ydbschema.DesiredColumnFamilies{Families: []ydbschema.ColumnFamily{
			{Name: "default", Compression: "off"},
			{Name: "cold", Data: "hdd", Compression: "lz4"},
			{Name: "warm", CacheMode: "in_memory", Columns: []string{"body"}},
		}},
	}}
	codec := ydbast.ColumnFamiliesCodec()

	encoded, err := codec.Encode(op)
	c.Assert(err, qt.IsNil)
	decoded, err := codec.Decode(encoded)
	c.Assert(err, qt.IsNil)
	reencoded, err := codec.Encode(decoded)
	c.Assert(err, qt.IsNil)
	cloned := op.CloneExtension().(*ydbast.AlterColumnFamilies)
	cloned.Change.After.Families[2].Columns[0] = "mutated"
	cloned.Change.Before.Families[1].Name = "mutated"

	c.Assert(string(encoded), qt.Equals, `{"change":{"before":{"families":[{"name":"cold","data":"hdd","compression":"off","columns":["body"]},`+
		`{"name":"default","compression":"off"}]},"after":{"families":[{"name":"cold","data":"hdd","compression":"lz4"},`+
		`{"name":"default","compression":"off"},{"name":"warm","cache_mode":"in_memory","columns":["body"]}]}}}`)
	c.Assert(string(reencoded), qt.Equals, string(encoded))
	c.Assert(op.Actions(), qt.DeepEquals, []string{
		"ADD FAMILY `warm` (CACHE_MODE = 'in_memory')",
		"ALTER FAMILY `cold` SET COMPRESSION 'lz4'",
		"ALTER COLUMN `body` SET FAMILY `warm`",
	})
	c.Assert(op.Change.After.Families[2].Columns, qt.DeepEquals, []string{"body"})
	c.Assert(op.Change.Before.Families[1].Name, qt.Equals, "cold")
	c.Assert(op.Effect().Impact, qt.Equals, schemaext.Additive)
	c.Assert(op.Validate(), qt.IsNil)
	c.Assert((*ydbast.AlterColumnFamilies)(nil).Copy(), qt.IsNil)
}

// TestAlterColumnFamilies_CodecFailurePath refuses an operation that is not
// the owner's change, and a transition that writes no statement: the table
// already holds every setting and placement After states.
func TestAlterColumnFamilies_CodecFailurePath(t *testing.T) {
	tests := []struct {
		name string
		data string
	}{
		{name: "null", data: `null`},
		{name: "no change", data: `{}`},
		{name: "an extra property", data: `{"change":{"after":{"families":[{"name":"cold"}]}},"table":"t"}`},
		{name: "a change without after", data: `{"change":{"before":{"families":[]}}}`},
		{name: "a change that changes nothing", data: `{"change":{"after":{"families":[{"name":"cold","compression":"lz4"}]},` +
			`"before":{"families":[{"name":"cold","data":"hdd","compression":"lz4"}]}}}`},
		{name: "no family on either side", data: `{"change":{"after":{"families":[]}}}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			decoded, err := ydbast.ColumnFamiliesCodec().Decode([]byte(test.data))

			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			var invalid *schemaext.InvalidModelError
			c.Assert(err, qt.ErrorAs, &invalid)
			c.Assert(decoded, qt.IsNil)
		})
	}
	c := qt.New(t)
	c.Assert((*ydbast.AlterColumnFamilies)(nil).Validate(), qt.ErrorIs, schemaext.ErrInvalidValue)
}
