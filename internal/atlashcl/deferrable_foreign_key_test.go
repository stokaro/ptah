package atlashcl_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/internal/atlashcl"
)

// deferrableDocument puts the two attributes on a foreign key, which is the
// only place they are written.
func deferrableDocument(body string) []byte {
	return []byte(`schema "public" {
}

table "parent" {
  schema = schema.public
  column "id" {
    type = int
  }
  primary_key {
    columns = [column.id]
  }
}

table "child" {
  schema = schema.public
  column "parent_id" {
    type = int
  }
  foreign_key "fk_child_parent" {
    columns     = [column.parent_id]
    ref_columns = [table.parent.column.id]
` + body + `
  }
}
`)
}

// TestParseForeignKeyDeferral covers the authoring half of stokaro/ptah#1624:
// DEFERRABLE was absent from the IR, so no surface could express one.
//
// The rows separate the two attributes and their interaction. `deferrable` and
// `initially` are separate fields rather than one tri-state because the clauses
// are separate: DEFERRABLE alone is legal and means the check CAN be deferred
// while still running immediately.
func TestParseForeignKeyDeferral(t *testing.T) {
	tests := []struct {
		name           string
		body           string
		wantDeferrable bool
		wantInitially  string
	}{
		{name: "neither attribute leaves both empty", body: ""},
		{
			name:           "deferrable alone",
			body:           "    deferrable = true",
			wantDeferrable: true,
		},
		{
			name:           "deferrable with a timing",
			body:           "    deferrable = true\n    initially = \"deferred\"",
			wantDeferrable: true,
			wantInitially:  "deferred",
		},
		{
			// The timing implies the clause: INITIALLY DEFERRED on a constraint
			// that is not deferrable is not something an engine accepts, and a
			// file writing only the timing plainly means the check deferrable.
			name:           "a timing alone implies deferrable",
			body:           "    initially = \"deferred\"",
			wantDeferrable: true,
			wantInitially:  "deferred",
		},
		{
			name:           "the timing is folded to lower case",
			body:           "    initially = \"IMMEDIATE\"",
			wantDeferrable: true,
			wantInitially:  "immediate",
		},
		{
			// deferrable = false is a statement, not an absence: it says the
			// author considered deferral and declined it, and it must not turn
			// into a DEFERRABLE clause.
			name: "deferrable false stays false",
			body: "    deferrable = false",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			db, err := atlashcl.Parse(deferrableDocument(test.body), "schema.hcl")

			c.Assert(err, qt.IsNil)
			field := fieldNamed1624(c, db, "parent_id")
			c.Assert(field.Deferrable, qt.Equals, test.wantDeferrable)
			c.Assert(field.Initially, qt.Equals, test.wantInitially)
		})
	}
}

// TestParseForeignKeyDeferralRefusesAnUnknownTiming keeps the vocabulary
// closed. A value nothing understands must not reach a renderer, where it would
// become either a dropped clause or invalid SQL.
func TestParseForeignKeyDeferralRefusesAnUnknownTiming(t *testing.T) {
	c := qt.New(t)

	_, err := atlashcl.Parse(deferrableDocument(`    initially = "whenever"`), "schema.hcl")

	c.Assert(err, qt.ErrorMatches, `.*initially "whenever" is neither deferred nor immediate.*`)
}

// fieldNamed1624 returns the one field with a name, failing when the document
// carries none. Selecting inside a loop in the test body would let a document
// that lost the column assert nothing.
func fieldNamed1624(c *qt.C, db *goschema.Database, name string) goschema.Field {
	c.Helper()
	matched := make([]goschema.Field, 0, 1)
	for _, field := range db.Fields {
		if field.Name == name {
			matched = append(matched, field)
		}
	}
	c.Assert(matched, qt.HasLen, 1, qt.Commentf("fields: %+v", db.Fields))
	return matched[0]
}
