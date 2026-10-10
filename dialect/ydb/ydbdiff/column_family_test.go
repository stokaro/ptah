package ydbdiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbschema"
)

// TestColumnFamiliesCodec_RoundTripsEachTransition pins that an omitted before
// is a table the read found without families, and that each transition
// decodes to itself, the observed keep_in_memory included.
func TestColumnFamiliesCodec_RoundTripsEachTransition(t *testing.T) {
	held := &ydbschema.ObservedColumnFamilies{Families: []ydbschema.ColumnFamily{
		{Name: "default", Compression: "lz4", KeepInMemory: true},
		{Name: "cold", Data: "hdd", Columns: []string{"body"}},
	}}
	declared := &ydbschema.DesiredColumnFamilies{Families: []ydbschema.ColumnFamily{{Name: "cold", Data: "hdd", Columns: []string{"blob", "body"}}}}
	tests := []struct {
		name   string
		change *ydbdiff.ColumnFamilies
		wire   string
	}{
		{name: "families on a table that had none", change: &ydbdiff.ColumnFamilies{After: declared},
			wire: `{"after":{"families":[{"name":"cold","data":"hdd","columns":["blob","body"]}]}}`},
		{name: "a change", change: &ydbdiff.ColumnFamilies{Before: held, After: declared},
			wire: `{"before":{"families":[{"name":"cold","data":"hdd","columns":["body"]},{"name":"default","compression":"lz4","keep_in_memory":true}]},` +
				`"after":{"families":[{"name":"cold","data":"hdd","columns":["blob","body"]}]}}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			codec := ydbdiff.ColumnFamiliesCodec()

			encoded, err := codec.Encode(test.change)
			c.Assert(err, qt.IsNil)
			c.Assert(string(encoded), qt.Equals, test.wire)
			decoded, err := codec.Decode(encoded)
			c.Assert(err, qt.IsNil)
			c.Assert(decoded.(*ydbdiff.ColumnFamilies).After.Equal(test.change.After), qt.IsTrue)
			reencoded, err := codec.Encode(decoded)
			c.Assert(err, qt.IsNil)
			c.Assert(string(reencoded), qt.Equals, test.wire)
		})
	}
}

// TestColumnFamiliesCodec_FailurePath refuses a change without the families
// the table ends up holding, a null or unknown operand, and an operand its
// model's codec refuses.
func TestColumnFamiliesCodec_FailurePath(t *testing.T) {
	tests := []struct {
		name string
		data string
	}{
		{name: "null", data: `null`},
		{name: "no operand", data: `{}`},
		{name: "only before", data: `{"before":{"families":[]}}`},
		{name: "a null before", data: `{"before":null,"after":{"families":[]}}`},
		{name: "a null after", data: `{"after":null}`},
		{name: "an unknown property", data: `{"after":{"families":[]},"during":{}}`},
		{name: "keep_in_memory written out as false", data: `{"after":{"families":[{"name":"cold","keep_in_memory":false}]}}`},
		{name: "an invalid declaration", data: `{"after":{"families":[{"name":"cold","compression":"zstd"}]}}`},
		{name: "an invalid observation", data: `{"before":{"families":[{"name":"default","columns":["a"]}]},"after":{"families":[]}}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			decoded, err := ydbdiff.ColumnFamiliesCodec().Decode([]byte(test.data))

			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			var invalid *schemaext.InvalidModelError
			c.Assert(err, qt.ErrorAs, &invalid)
			c.Assert(decoded, qt.IsNil)
		})
	}
	c := qt.New(t)
	c.Assert(ydbdiff.ValidateColumnFamilies(nil), qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(ydbdiff.ValidateColumnFamilies(&ydbdiff.ColumnFamilies{}), qt.ErrorMatches, `.*requires the families the table ends up holding`)
	c.Assert(ydbdiff.ValidateColumnFamilies(&ydbdiff.ColumnFamilies{
		Before: &ydbschema.ObservedColumnFamilies{Families: []ydbschema.ColumnFamily{{Name: "default", Columns: []string{"a"}}}},
		After:  &ydbschema.DesiredColumnFamilies{},
	}), qt.ErrorMatches, `.*observed model "ptah.run/ydb/column-families": .*the default column family lists columns.*`)
}

// TestColumnFamilies_CopySharesNoOperand pins that a copied change cannot
// reach back into its source through an operand or a column list.
func TestColumnFamilies_CopySharesNoOperand(t *testing.T) {
	c := qt.New(t)
	original := &ydbdiff.ColumnFamilies{
		Before: &ydbschema.ObservedColumnFamilies{Families: []ydbschema.ColumnFamily{{Name: "cold", Columns: []string{"a"}}}},
		After:  &ydbschema.DesiredColumnFamilies{Families: []ydbschema.ColumnFamily{{Name: "cold", Columns: []string{"a", "b"}}}},
	}

	cloned := original.CloneChange().(*ydbdiff.ColumnFamilies)
	cloned.Before.Families[0].Columns[0] = "x"
	cloned.After.Families[0].Name = "warm"
	cloned.After.Families[0].Columns[1] = "y"

	c.Assert(original.Before.Families[0].Columns, qt.DeepEquals, []string{"a"})
	c.Assert(original.After.Families, qt.DeepEquals, []ydbschema.ColumnFamily{{Name: "cold", Columns: []string{"a", "b"}}})
	c.Assert((*ydbdiff.ColumnFamilies)(nil).Copy(), qt.IsNil)
	c.Assert((&ydbdiff.ColumnFamilies{}).Copy(), qt.DeepEquals, &ydbdiff.ColumnFamilies{})
}
