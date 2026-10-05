package ydb_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Scheme"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Table"
	"google.golang.org/protobuf/encoding/protowire"

	"ptah.run/core/ast"
	"ptah.run/core/coverage"
)

// family is a column family as DescribeTable reports one with no settings of
// its own, which is how a family named without settings reads back on
// local-ydb 25.1.4.7 and 26.2.1.14.
func family(name string) *Ydb_Table.ColumnFamily {
	return &Ydb_Table.ColumnFamily{Name: name, Compression: Ydb_Table.ColumnFamily_COMPRESSION_NONE}
}

// withFamilyField appends a varint field the pinned protocol buffers do not
// model, as a newer server sends it.
func withFamilyField(spec *Ydb_Table.ColumnFamily, number protowire.Number, value uint64) *Ydb_Table.ColumnFamily {
	raw := protowire.AppendTag(spec.ProtoReflect().GetUnknown(), number, protowire.VarintType)
	raw = protowire.AppendVarint(raw, value)
	spec.ProtoReflect().SetUnknown(raw)
	return spec
}

// inFamily is a nullable Utf8 column that DescribeTable names family for.
func inFamily(name, familyName string) *Ydb_Table.ColumnMeta {
	return &Ydb_Table.ColumnMeta{Name: name, Type: optional(primitive(Ydb.Type_UTF8)), Family: familyName}
}

// familyTable is a table holding columns a and b, with the families and the
// column families given.
func familyTable(families []*Ydb_Table.ColumnFamily, columns ...*Ydb_Table.ColumnMeta) fakeSource {
	described := plainTable(columns...)
	described.ColumnFamilies = families
	return fakeSource{
		directories: map[string][]*Ydb_Scheme.Entry{"/local": {entry("t", Ydb_Scheme.Entry_TABLE)}},
		tables:      map[string]*Ydb_Table.DescribeTableResult{"/local/t": described},
	}
}

// A table's column families are read as the table holds them, with the
// columns DescribeTable names each with: sorted, each setting kept at the
// value the table holds, YDB's own (`off`, `regular`) included, and
// keep_in_memory read, which a table profile's `column_cache` turns on for
// every new table. A comparison then sees what a profile gave the table.
// Reading them records nothing.
func TestReader_ReadsTheColumnFamilies(t *testing.T) {
	cold := family("cold")
	off := func(name string, columns ...string) ast.YDBColumnFamilySpec {
		return ast.YDBColumnFamilySpec{Name: name, Compression: "off", Columns: columns}
	}
	tests := []struct {
		name     string
		families []*Ydb_Table.ColumnFamily
		columns  []*Ydb_Table.ColumnMeta
		want     []ast.YDBColumnFamilySpec
	}{
		{
			name:     "families holding columns",
			families: []*Ydb_Table.ColumnFamily{family("default"), family("warm"), cold},
			columns:  []*Ydb_Table.ColumnMeta{inFamily("b", "cold"), inFamily("a", "cold"), inFamily("c", "warm")},
			want:     []ast.YDBColumnFamilySpec{off("cold", "a", "b"), off("default"), off("warm", "c")},
		},
		{
			name:     "a column the description names in the default family",
			families: []*Ydb_Table.ColumnFamily{family("default"), cold},
			columns:  []*Ydb_Table.ColumnMeta{inFamily("a", "default"), inFamily("b", "cold")},
			want:     []ast.YDBColumnFamilySpec{off("cold", "b"), off("default")},
		},
		{
			name:     "an unused family",
			families: []*Ydb_Table.ColumnFamily{family("default"), {Name: "cold", Compression: Ydb_Table.ColumnFamily_COMPRESSION_LZ4}},
			want:     []ast.YDBColumnFamilySpec{{Name: "cold", Compression: "lz4"}, off("default")},
		},
		{
			name:     "a compressed default family",
			families: []*Ydb_Table.ColumnFamily{{Name: "default", Compression: Ydb_Table.ColumnFamily_COMPRESSION_LZ4}},
			want:     []ast.YDBColumnFamilySpec{{Name: "default", Compression: "lz4"}},
		},
		{
			name: "a default family on a named pool",
			families: []*Ydb_Table.ColumnFamily{
				{Name: "default", Compression: Ydb_Table.ColumnFamily_COMPRESSION_NONE, Data: &Ydb_Table.StoragePool{Media: "ssd"}},
			},
			want: []ast.YDBColumnFamilySpec{{Name: "default", Data: "ssd", Compression: "off"}},
		},
		{
			name:     "a family kept in memory by its cache mode",
			families: []*Ydb_Table.ColumnFamily{family("default"), withFamilyField(family("cold"), 6, 2)},
			want:     []ast.YDBColumnFamilySpec{{Name: "cold", Compression: "off", CacheMode: "in_memory"}, off("default")},
		},
		{
			name:     "a family set to the regular cache",
			families: []*Ydb_Table.ColumnFamily{withFamilyField(family("default"), 6, 1)},
			want:     []ast.YDBColumnFamilySpec{{Name: "default", Compression: "off", CacheMode: "regular"}},
		},
		{
			name: "a family a table profile keeps in memory",
			families: []*Ydb_Table.ColumnFamily{
				{Name: "default", Compression: Ydb_Table.ColumnFamily_COMPRESSION_LZ4, KeepInMemory: Ydb.FeatureFlag_ENABLED},
			},
			want: []ast.YDBColumnFamilySpec{{Name: "default", Compression: "lz4", KeepInMemory: true}},
		},
		{
			name:     "a family with no compression given",
			families: []*Ydb_Table.ColumnFamily{family("default"), {Name: "cold"}},
			want:     []ast.YDBColumnFamilySpec{{Name: "cold"}, off("default")},
		},
		{
			name:     "only the default family, as a table created without families has",
			families: []*Ydb_Table.ColumnFamily{family("default")},
			want:     []ast.YDBColumnFamilySpec{off("default")},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db := readFrom(c, familyTable(test.families, test.columns...))

			c.Assert(db.Tables, qt.HasLen, 1)
			c.Assert(db.Tables[0].YDBColumnFamilies, qt.DeepEquals, test.want)
			c.Assert(db.NotDescribed.Describes(coverage.ColumnFamily, "t"), qt.IsTrue)
			c.Assert(db.NotDescribed.Describes(coverage.TableOption, "t"), qt.IsTrue)
		})
	}
}

// A table whose families hold something Ptah does not read lists no family,
// and its families are recorded as not described, so a plan neither changes
// nor drops them and a rebuild refuses the table. A row table cannot be given
// zstd or a compression level (`Unsupported compression value 3`, `is not
// supported for OLTP tables`).
func TestReader_RecordsColumnFamiliesItDoesNotRead(t *testing.T) {
	pool := &Ydb_Table.StoragePool{Media: "ssd"}
	pool.ProtoReflect().SetUnknown(protowire.AppendVarint(protowire.AppendTag(nil, 2, protowire.VarintType), 1))
	tests := []struct {
		name     string
		families []*Ydb_Table.ColumnFamily
		columns  []*Ydb_Table.ColumnMeta
	}{
		{name: "zstd", families: []*Ydb_Table.ColumnFamily{family("default"), {Name: "cold", Compression: 3}}},
		{name: "a compression level", families: []*Ydb_Table.ColumnFamily{withFamilyField(family("cold"), 5, 3)}},
		{name: "a cache mode Ptah does not name", families: []*Ydb_Table.ColumnFamily{withFamilyField(family("cold"), 6, 9)}},
		{name: "a field Ptah does not know", families: []*Ydb_Table.ColumnFamily{withFamilyField(family("cold"), 7, 1)}},
		{name: "a storage pool with a field Ptah does not know", families: []*Ydb_Table.ColumnFamily{
			{Name: "cold", Compression: Ydb_Table.ColumnFamily_COMPRESSION_NONE, Data: pool},
		}},
		{
			name:     "a column in a family the table does not list",
			families: []*Ydb_Table.ColumnFamily{family("default")},
			columns:  []*Ydb_Table.ColumnMeta{inFamily("a", "cold")},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db := readFrom(c, familyTable(test.families, test.columns...))

			c.Assert(db.Tables, qt.HasLen, 1)
			c.Assert(db.Tables[0].YDBColumnFamilies, qt.IsNil)
			c.Assert(db.NotDescribed.Describes(coverage.ColumnFamily, "t"), qt.IsFalse)
			c.Assert(db.NotDescribed.Describes(coverage.TableOption, "t"), qt.IsTrue)
		})
	}
}
