package yamlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/core/yamlschema"
	"ptah.run/dialect/ydb/ydbschema"
)

func TestParse_ColumnStore(t *testing.T) {
	c := qt.New(t)
	db, err := yamlschema.Parse([]byte(`tables:
  events:
    schema: analytics
    fields:
      id: {type: Uint64, primary: true}
      at: {type: Timestamp}
    column_store:
      hash_columns: [id]
      partitions: 8
      ttl:
        column: at
        tiers:
          - {interval: P1D, external_source: /local/archive}
          - {interval: P7D}
`))
	c.Assert(err, qt.IsNil)
	c.Assert(db.Tables, qt.HasLen, 1)
	c.Assert(db.Tables[0].Schema, qt.Equals, "analytics")
	store, held, err := schemaext.FacetAs[*ydbschema.DesiredColumnStore](db.Tables[0].Facets, ydbschema.ColumnStoreKind)
	c.Assert(err, qt.IsNil)
	c.Assert(held, qt.IsTrue)
	c.Assert(store, qt.DeepEquals, &ydbschema.DesiredColumnStore{ColumnStore: ydbschema.ColumnStore{HashColumns: []string{"id"}, Partitions: 8,
		TTL: &ydbschema.TieredTTL{Column: "at", Tiers: []ydbschema.TTLTier{{Interval: "P1D", ExternalSource: "/local/archive"}, {Interval: "P7D"}}}}})
}
