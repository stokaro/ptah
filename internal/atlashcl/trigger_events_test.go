package atlashcl_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlashcl"
)

// triggerEventsDocument is a table and a trigger whose timing block holds the
// given attributes.
func triggerEventsDocument(eventLines string) []byte {
	return []byte(`
schema "public" {}

table "t" {
  schema = schema.public
  column "id" { type = int }
}

trigger "t_touch" {
  on = table.t
  before {
    ` + eventLines + `
  }
  for = ROW
  as  = "BEGIN RETURN NEW; END;"
}
`)
}

// TestParseTriggerEvents_HappyPath reads every event a timing block sets
// (stokaro/ptah#3692). Reading the first one alone created a trigger that
// fired on fewer events than the document declared, and said nothing.
func TestParseTriggerEvents_HappyPath(t *testing.T) {
	tests := []struct {
		name   string
		events string
		want   string
	}{
		{name: "one event", events: "update = true", want: "UPDATE"},
		{name: "two events", events: "insert = true\n    update = true", want: "INSERT OR UPDATE"},
		{
			name:   "written in another order, read in the order PostgreSQL reports",
			events: "update = true\n    delete = true\n    insert = true",
			want:   "INSERT OR DELETE OR UPDATE",
		},
		{
			name:   "every event",
			events: "truncate = true\n    update = true\n    delete = true\n    insert = true",
			want:   "INSERT OR DELETE OR UPDATE OR TRUNCATE",
		},
		{name: "an event set to false is not one", events: "insert = false\n    update = true", want: "UPDATE"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db, err := atlashcl.Parse(triggerEventsDocument(test.events), "schema.hcl")
			c.Assert(err, qt.IsNil)
			c.Assert(db.Triggers, qt.HasLen, 1)
			c.Assert(db.Triggers[0].Event, qt.Equals, test.want)
		})
	}
}

// TestParseTriggerEvents_FailurePath refuses an event attribute it cannot
// read as set or unset, rather than reading it as unset.
func TestParseTriggerEvents_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		events  string
		wantErr string
	}{
		{
			name:    "an event that is not a bool",
			events:  "insert = true\n    update = \"yes\"",
			wantErr: `parse HCL schema at .*: trigger event attribute "update" must be a bool`,
		},
		{
			name:    "no event set",
			events:  "insert = false",
			wantErr: `parse HCL schema at .*: trigger timing block requires an event`,
		},
		{
			name:    "an attribute naming no event",
			events:  "insert = true\n    upsert = true",
			wantErr: `parse HCL schema at .*: unsupported trigger event attribute "upsert"`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db, err := atlashcl.Parse(triggerEventsDocument(test.events), "schema.hcl")
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(db, qt.IsNil)
		})
	}
}
