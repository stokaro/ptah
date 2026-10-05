package ydbcolumn_test

import (
	"os"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/internal/ydbcolumn"
)

// The fixtures contain the schema portion of live monitoring responses. Runtime
// ACL and tablet metrics are left out; column and index properties are unchanged.
func TestDecode_ColumnLayout(t *testing.T) {
	for _, test := range []struct {
		name, path string
		indexes    int
	}{
		{name: "25-1", path: "/local/col_probe", indexes: 0},
		{name: "26-2-indexes", path: "/local/col_index_probe", indexes: 5},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			body, err := os.ReadFile("testdata/" + test.name + ".json")
			c.Assert(err, qt.IsNil)
			got, err := ydbcolumn.Decode(body, test.path)
			c.Assert(err, qt.IsNil)
			c.Assert(got.Spec.HashColumns, qt.DeepEquals, []string{"id"})
			c.Assert(got.Spec.Partitions, qt.Equals, uint64(64))
			c.Assert(got.Indexes, qt.HasLen, test.indexes)
		})
	}
}

func TestDecode_LocalIndexSettings(t *testing.T) {
	c := qt.New(t)
	body, err := os.ReadFile("testdata/26-2-indexes.json")
	c.Assert(err, qt.IsNil)
	got, err := ydbcolumn.Decode(body, "/local/col_index_probe")
	c.Assert(err, qt.IsNil)
	c.Assert(got.Indexes, qt.DeepEquals, []ydbcolumn.LocalIndex{
		{Name: "bf", Method: "bloom_filter", Columns: []string{"body"}, Options: map[string]string{"false_positive_probability": "0.01"}},
		{Name: "bf_default", Method: "bloom_filter", Columns: []string{"body"}, Options: map[string]string{"false_positive_probability": "0.1"}},
		{Name: "mm", Method: "min_max", Columns: []string{"id"}, Options: make(map[string]string)},
		{Name: "ng", Method: "bloom_ngram_filter", Columns: []string{"body"}, Options: map[string]string{"false_positive_probability": "0.01", "ngram_size": "3", "case_sensitive": "true"}},
		{Name: "ng_default", Method: "bloom_ngram_filter", Columns: []string{"body"}, Options: map[string]string{"false_positive_probability": "0.1", "ngram_size": "3", "case_sensitive": "true"}},
	})
}

func TestDecode_TieredTTL(t *testing.T) {
	for _, test := range []struct{ name, path, unit string }{
		{name: "26-2-ttl", path: "/local/col_tier_probe"},
		{name: "26-2-epoch", path: "/local/col_epoch_probe", unit: "SECONDS"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			body, err := os.ReadFile("testdata/" + test.name + ".json")
			c.Assert(err, qt.IsNil)
			got, err := ydbcolumn.Decode(body, test.path)
			c.Assert(err, qt.IsNil)
			c.Assert(got.Spec.TTL, qt.DeepEquals, &ast.YDBTieredTTLSpec{Column: "at", Unit: test.unit, Tiers: []ast.YDBTTLTierSpec{{Interval: "PT86400S", ExternalSource: "/local/col_archive"}, {Interval: "PT604800S"}}})
		})
	}
}

func TestDecode_UnknownIndexField(t *testing.T) {
	c := qt.New(t)
	body, err := os.ReadFile("testdata/26-2-indexes.json")
	c.Assert(err, qt.IsNil)
	changed := strings.Replace(string(body), `"BloomFilter": {`, `"BloomFilter": {"FutureOption": true,`, 1)
	got, err := ydbcolumn.Decode([]byte(changed), "/local/col_index_probe")
	c.Assert(err, qt.ErrorMatches, `.*unknown field "FutureOption"`)
	c.Assert(got, qt.IsNil)
}

func TestDecode_UnknownStorageField(t *testing.T) {
	c := qt.New(t)
	body, err := os.ReadFile("testdata/26-2-indexes.json")
	c.Assert(err, qt.IsNil)
	changed := strings.Replace(string(body), `"StorageConfig": {`, `"StorageConfig": {"FutureStorage": true,`, 1)
	got, err := ydbcolumn.Decode([]byte(changed), "/local/col_index_probe")
	c.Assert(err, qt.ErrorMatches, `.*unknown field "FutureStorage"`)
	c.Assert(got, qt.IsNil)
}
