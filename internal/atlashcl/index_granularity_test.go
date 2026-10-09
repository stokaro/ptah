package atlashcl_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/goschema"
	"ptah.run/internal/atlashcl"
)

// TestParseIndexGranularityAsAClickHousePlatformProperty reads a skipping
// index's granularity where the ClickHouse owner decodes it: a property of the
// index's `platform "clickhouse"` block. The common `type` stays on the index.
func TestParseIndexGranularityAsAClickHousePlatformProperty(t *testing.T) {
	c := qt.New(t)

	db, err := atlashcl.Parse([]byte(`
table "events" {
  column "payload" {
    type = text
  }
  index "idx_events_payload" {
    columns = [column.payload]
    type    = bloom_filter
    platform "clickhouse" {
      override "granularity" { value = "64" }
    }
  }
}
`), "schema.hcl")

	c.Assert(err, qt.IsNil)
	c.Assert(db.Indexes, qt.HasLen, 1)
	c.Assert(db.Indexes[0].Name, qt.Equals, "idx_events_payload")
	c.Assert(db.Indexes[0].Type, qt.Equals, "bloom_filter")
	c.Assert(db.Indexes[0].Overrides, qt.DeepEquals, map[string]map[string]string{"clickhouse": {"granularity": "64"}})
}

// TestParseIndexGranularityAttribute_FailurePath pins that the bare attribute
// is gone rather than read into a second representation of the setting.
func TestParseIndexGranularityAttribute_FailurePath(t *testing.T) {
	c := qt.New(t)

	db, err := atlashcl.Parse([]byte(`
table "events" {
  column "payload" {
    type = text
  }
  index "idx_events_payload" {
    columns     = [column.payload]
    granularity = 64
  }
}
`), "schema.hcl")

	c.Assert(err, qt.ErrorMatches, `.*unsupported index attribute "granularity".*`)
	c.Assert(db, qt.IsNil)
}

// TestIndexGranularityGoAnnotationParity asserts that the Go annotation frontend
// and the Atlas HCL frontend declare the same ClickHouse skipping-index
// settings for the same schema, closing the #684 parity gap. Both carry the
// granularity as a ClickHouse source property for the owner to decode.
func TestIndexGranularityGoAnnotationParity(t *testing.T) {
	c := qt.New(t)

	goDB, err := goschema.ParseSource("events.go", `package models

//ptah:schema:table name="events"
type Event struct {
	//ptah:schema:field name="payload" type="String"
	Payload string

	//ptah:schema:index name="idx_events_payload" fields="payload" type="bloom_filter" platform.clickhouse.granularity="64"
	_ int
}
`)
	c.Assert(err, qt.IsNil)
	c.Assert(goDB.Indexes, qt.HasLen, 1)

	hclDB, err := atlashcl.Parse([]byte(`
table "events" {
  column "payload" {
    type = String
  }
  index "idx_events_payload" {
    columns = [column.payload]
    type    = bloom_filter
    platform "clickhouse" {
      override "granularity" { value = "64" }
    }
  }
}
`), "schema.hcl")
	c.Assert(err, qt.IsNil)
	c.Assert(hclDB.Indexes, qt.HasLen, 1)

	goIndex := goDB.Indexes[0]
	hclIndex := hclDB.Indexes[0]
	c.Assert(hclIndex.Name, qt.Equals, goIndex.Name)
	c.Assert(hclIndex.Fields, qt.DeepEquals, goIndex.Fields)
	c.Assert(hclIndex.Type, qt.Equals, goIndex.Type)
	c.Assert(hclIndex.Overrides, qt.DeepEquals, goIndex.Overrides)
	c.Assert(goIndex.Overrides, qt.DeepEquals, map[string]map[string]string{"clickhouse": {"granularity": "64"}})
}
