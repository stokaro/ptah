package atlashcl_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/internal/atlashcl"
)

// TestParseHypertable reads the block that says a table is partitioned.
//
// The label is the TABLE, because a hypertable has no name of its own:
// `timescaledb_information.hypertables` is keyed by the relation, and there is
// nothing to rename. The schema folds back into the table name, which is how
// [goschema.Hypertable] carries it — a declaration names a table, and a table
// is named the way every other reference to one is.
func TestParseHypertable(t *testing.T) {
	tests := []struct {
		name     string
		document string
		want     goschema.Hypertable
	}{
		{
			name: "the whole block",
			document: `
schema "app" {
}

hypertable "readings" {
  schema         = schema.app
  column         = "time"
  chunk_interval = "1 day"
  if_not_exists  = true
  comment        = "partitioned by hour of arrival"
}
`,
			want: goschema.Hypertable{
				Table: "app.readings", Column: "time", ChunkInterval: "1 day",
				IfNotExists: true, Comment: "partitioned by hour of arrival",
			},
		},
		{
			// No interval takes TimescaleDB's own default, so an omitted one is
			// a declaration rather than a gap.
			name: "the smallest one there is",
			document: `
hypertable "readings" {
  column = "time"
}
`,
			want: goschema.Hypertable{Table: "readings", Column: "time"},
		},
		{
			name: "the two-label spelling",
			document: `
hypertable "app" "readings" {
  column = "time"
}
`,
			want: goschema.Hypertable{Table: "app.readings", Column: "time"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			db, err := atlashcl.Parse([]byte(test.document), "schema.hcl")

			c.Assert(err, qt.IsNil)
			c.Assert(db.Hypertables, qt.DeepEquals, []goschema.Hypertable{test.want})
		})
	}
}

// TestParseHypertable_RefusesADeclarationThatPartitionsOnNothing pins the one
// required attribute.
//
// A hypertable with no dimension is not one, and the server has no default to
// fall back on: `create_hypertable` takes the dimension as its second argument.
func TestParseHypertable_RefusesADeclarationThatPartitionsOnNothing(t *testing.T) {
	tests := []struct {
		name     string
		document string
		want     string
	}{
		{
			name:     "no column",
			document: "hypertable \"readings\" {\n}\n",
			want:     "requires a column to partition on",
		},
		{
			name:     "an attribute the block does not have",
			document: "hypertable \"readings\" {\n  column = \"time\"\n  target = \"x\"\n}\n",
			want:     "target",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			_, err := atlashcl.Parse([]byte(test.document), "schema.hcl")

			c.Assert(err, qt.ErrorMatches, ".*"+test.want+".*")
		})
	}
}
