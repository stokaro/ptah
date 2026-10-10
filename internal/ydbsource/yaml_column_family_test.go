package ydbsource_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/core/yamlschema"
	"ptah.run/dialect/ydb/ydbschema"
)

// A YAML table declares its YDB column families under column_families, keyed
// by name, with the keys the annotation takes, read in the order written.
func TestParse_ColumnFamilies_HappyPath(t *testing.T) {
	c := qt.New(t)
	db, err := yamlschema.Parse(ydbYAMLOwners, []byte(`tables:
  items:
    fields:
      id: {type: BIGINT, primary: true}
      body: {type: TEXT}
    column_families:
      default:
        compression: LZ4
      cold:
        data: hdd
        cache_mode: in_memory
        fields: [body]
`))

	c.Assert(err, qt.IsNil)
	c.Assert(db.Tables, qt.HasLen, 1)
	declared, found, err := schemaext.FacetAs[*ydbschema.DesiredColumnFamilies](db.Tables[0].Facets, ydbschema.ColumnFamiliesKind)
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	c.Assert(declared.Families, qt.DeepEquals, []ydbschema.ColumnFamily{
		{Name: "default", Compression: "lz4"},
		{Name: "cold", Data: "hdd", CacheMode: "in_memory", Columns: []string{"body"}},
	})
}

// A family a YAML document cannot mean is refused, naming the table, the
// family and the key, and an empty value is refused rather than read as none.
func TestParse_ColumnFamilies_FailurePath(t *testing.T) {
	tests := []struct {
		name     string
		families string
		wantErr  string
	}{
		{name: "an empty pool", families: "cold: {data: ''}",
			wantErr: `table "items": column family "cold": invalid data "": names the kind of storage pool .*`},
		{name: "zstd", families: "cold: {compression: zstd}",
			wantErr: `table "items": column family "cold": invalid compression "zstd": takes off or lz4: .*`},
		{name: "columns in the default family", families: "default: {fields: [body]}",
			wantErr: `table "items": column family "default": invalid fields "body": the default family holds .*`},
		{name: "an unknown key", families: "cold: {compression_level: 3}",
			wantErr: `(?s).*compression_level.*`},
		{name: "a column in two families", families: "cold: {fields: [body]}\n      warm: {fields: [body]}",
			wantErr: `table "items": column families: .*column "body" is in two column families, "cold" and "warm"`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db, err := yamlschema.Parse(ydbYAMLOwners, []byte("tables:\n  items:\n    fields:\n      id: {type: BIGINT, primary: true}\n"+
				"      body: {type: TEXT}\n    column_families:\n      "+test.families+"\n"))
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(db, qt.IsNil)
		})
	}
}
