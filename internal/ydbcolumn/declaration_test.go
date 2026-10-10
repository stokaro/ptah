package ydbcolumn_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/internal/ydbcolumn"
)

func TestParse_HappyPath(t *testing.T) {
	tests := []struct {
		name   string
		values map[string]string
		want   *ydbschema.ColumnStore
	}{
		{name: "row default"},
		{name: "explicit row", values: map[string]string{"store": "ROW"}},
		{name: "column defaults", values: map[string]string{"store": "column"}, want: &ydbschema.ColumnStore{}},
		{name: "hash and shards", values: map[string]string{"store": "column", "partition_by_hash": "tenant, id", "column_shards": "8"}, want: &ydbschema.ColumnStore{HashColumns: []string{"tenant", "id"}, Partitions: 8}},
		{name: "retention", values: map[string]string{"store": "column", "column_ttl": `{"column":"at","tiers":[{"interval":"P1D","external_source":"/local/archive"},{"interval":"P7D"}]}`}, want: &ydbschema.ColumnStore{TTL: &ydbschema.TieredTTL{Column: "at", Tiers: []ydbschema.TTLTier{{Interval: "P1D", ExternalSource: "/local/archive"}, {Interval: "P7D"}}}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := ydbcolumn.Parse(test.values)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

func TestParse_FailurePath(t *testing.T) {
	tests := []struct {
		name   string
		values map[string]string
		want   string
	}{
		{name: "empty store", values: map[string]string{"store": " "}, want: `store must be ROW or COLUMN`},
		{name: "relative tier source", values: map[string]string{"store": "column", "column_ttl": `{"column":"at","tiers":[{"interval":"P1D","external_source":"archive"}]}`}, want: `TTL tier external source must be an absolute database path`},
		{name: "storage typo", values: map[string]string{"store": "colum"}, want: `store "colum" must be ROW or COLUMN`},
		{name: "hash on row", values: map[string]string{"partition_by_hash": "id"}, want: `partition_by_hash requires store=column`},
		{name: "zero shards", values: map[string]string{"store": "column", "column_shards": "0"}, want: `column_shards must be a positive 32-bit integer`},
		{name: "duplicate key", values: map[string]string{"store": "column", "partition_by_hash": "id,id"}, want: `hash partitioning column "id" is empty or repeated`},
		{name: "empty key", values: map[string]string{"store": "column", "partition_by_hash": ""}, want: `partition_by_hash contains an empty column name`},
		{name: "unknown TTL field", values: map[string]string{"store": "column", "column_ttl": `{"colum":"at"}`}, want: `column_ttl: json: unknown field "colum"`},
		{name: "null TTL", values: map[string]string{"store": "column", "column_ttl": "null"}, want: `column_ttl must be an object, not null`},
		{name: "trailing TTL document", values: map[string]string{"store": "column", "column_ttl": "{} {}"}, want: `column_ttl must contain exactly one JSON object`},
		{name: "delete before eviction", values: map[string]string{"store": "column", "column_ttl": `{"column":"at","tiers":[{"interval":"P1D"},{"interval":"P7D","external_source":"/local/archive"}]}`}, want: `only the last TTL tier may delete data`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := ydbcolumn.Parse(test.values)
			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(got, qt.IsNil)
		})
	}
}
