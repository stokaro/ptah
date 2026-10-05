package yamlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/yamlschema"
)

// A YAML table declares its YDB column families under column_families, keyed
// by name, with the keys the annotation takes, read in the order written.
func TestParse_ColumnFamilies_HappyPath(t *testing.T) {
	c := qt.New(t)
	db, err := yamlschema.Parse([]byte(`tables:
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
	c.Assert(db.Tables[0].YDBColumnFamilies, qt.DeepEquals, []ast.YDBColumnFamilySpec{
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
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db, err := yamlschema.Parse([]byte("tables:\n  items:\n    fields:\n      id: {type: BIGINT, primary: true}\n" +
				"      body: {type: TEXT}\n    column_families:\n      " + test.families + "\n"))
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(db, qt.IsNil)
		})
	}
}
