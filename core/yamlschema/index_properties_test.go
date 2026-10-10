package yamlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/yamlschema"
)

func TestParseIndexPlatformProperties(t *testing.T) {
	c := qt.New(t)
	db, err := yamlschema.Parse(noOwners, []byte(`
tables:
  events:
    columns:
      id: {type: UInt64}
    indexes:
      by_id:
        fields: [id]
        platform:
          clickhouse:
            type.state: default
            granularity: 18446744073709551615
          other:
            future: ""
indexes:
  by_id_external:
    table: events
    fields: [id]
    platform:
      clickhouse:
        granularity.state: default
`))
	c.Assert(err, qt.IsNil)
	c.Assert(db.Indexes, qt.HasLen, 2)
	c.Assert(db.Indexes[0].Overrides, qt.DeepEquals, map[string]map[string]string{
		"clickhouse": {"type.state": "default", "granularity": "18446744073709551615"},
		"other":      {"future": ""},
	})
	c.Assert(db.Indexes[1].Overrides, qt.DeepEquals, map[string]map[string]string{"clickhouse": {"granularity.state": "default"}})
	c.Assert(db.Indexes[0].Facets.IsZero(), qt.IsTrue)
	c.Assert(db.Indexes[1].Facets.IsZero(), qt.IsTrue)
}
