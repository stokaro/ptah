package atlashcl_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/internal/atlashcl"
)

func TestParseIndexGranularity(t *testing.T) {
	c := qt.New(t)

	db, err := atlashcl.Parse([]byte(`
table "events" {
  column "payload" {
    type = text
  }
  index "idx_events_payload" {
    columns     = [column.payload]
    type        = bloom_filter
    granularity = 64
  }
}
`), "schema.hcl")

	c.Assert(err, qt.IsNil)
	c.Assert(db.Indexes, qt.HasLen, 1)
	c.Assert(db.Indexes[0].Name, qt.Equals, "idx_events_payload")
	c.Assert(db.Indexes[0].Type, qt.Equals, "bloom_filter")
	c.Assert(db.Indexes[0].Granularity, qt.Equals, 64)
}

func TestParseIndexGranularityAbsentIsZero(t *testing.T) {
	c := qt.New(t)

	db, err := atlashcl.Parse([]byte(`
table "events" {
  column "payload" {
    type = text
  }
  index "idx_events_payload" {
    columns = [column.payload]
  }
}
`), "schema.hcl")

	c.Assert(err, qt.IsNil)
	c.Assert(db.Indexes, qt.HasLen, 1)
	c.Assert(db.Indexes[0].Granularity, qt.Equals, 0)
}

func TestParseIndexGranularityRejectsNegative(t *testing.T) {
	c := qt.New(t)

	_, err := atlashcl.Parse([]byte(`
table "events" {
  column "payload" {
    type = text
  }
  index "idx_events_payload" {
    columns     = [column.payload]
    granularity = -1
  }
}
`), "schema.hcl")

	c.Assert(err, qt.ErrorMatches, `.*index attribute "granularity" must be a non-negative integer.*`)
}

func TestParseIndexGranularityRejectsNonInteger(t *testing.T) {
	c := qt.New(t)

	_, err := atlashcl.Parse([]byte(`
table "events" {
  column "payload" {
    type = text
  }
  index "idx_events_payload" {
    columns     = [column.payload]
    granularity = 1.5
  }
}
`), "schema.hcl")

	c.Assert(err, qt.ErrorMatches, `.*index attribute "granularity" must be an integer.*`)
}

func TestParseIndexGranularityRejectsOverflow(t *testing.T) {
	c := qt.New(t)

	_, err := atlashcl.Parse([]byte(`
table "events" {
  column "payload" {
    type = text
  }
  index "idx_events_payload" {
    columns     = [column.payload]
    granularity = 99999999999999999999
  }
}
`), "schema.hcl")

	c.Assert(err, qt.ErrorMatches, `.*index attribute "granularity" must be an integer within the int64 range.*`)
}

// TestIndexGranularityGoAnnotationParity asserts that the Go annotation frontend
// and the Atlas HCL frontend produce an equivalent ClickHouse data-skipping
// index granularity for the same schema, closing the #684 parity gap. Like the
// Go path (parseIndexComment), both frontends reject negative or non-integer
// values and default an absent granularity to 0.
func TestIndexGranularityGoAnnotationParity(t *testing.T) {
	c := qt.New(t)

	goDB, err := goschema.ParseSource("events.go", `package models

//ptah:schema:table name="events"
type Event struct {
	//ptah:schema:field name="payload" type="String"
	Payload string

	//ptah:schema:index name="idx_events_payload" fields="payload" type="bloom_filter" granularity="64"
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
    columns     = [column.payload]
    type        = bloom_filter
    granularity = 64
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
	c.Assert(hclIndex.Granularity, qt.Equals, goIndex.Granularity)
}
