package atlashcl_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/internal/atlashcl"
)

// TestParseContinuousAggregate reads the block that names a TimescaleDB
// continuous aggregate.
//
// Unlike a hypertable this object has a name of its own, so the label is that
// name and `schema` says where it lives -- the shape a view block already has.
func TestParseContinuousAggregate(t *testing.T) {
	tests := []struct {
		name     string
		document string
		want     goschema.ContinuousAggregate
	}{
		{
			name: "the whole block",
			document: `
schema "app" {
}

continuous_aggregate "hourly" {
  schema            = schema.app
  as                = "SELECT time_bucket('1 hour', time) AS bucket FROM readings GROUP BY bucket"
  materialized_only = true
  comment           = "one row per hour"
}
`,
			want: goschema.ContinuousAggregate{
				Name: "hourly", Schema: "app",
				Body:             "SELECT time_bucket('1 hour', time) AS bucket FROM readings GROUP BY bucket",
				MaterializedOnly: new(true), Comment: "one row per hour",
			},
		},
		{
			name: "the smallest one there is",
			document: `
continuous_aggregate "hourly" {
  as = "SELECT 1"
}
`,
			want: goschema.ContinuousAggregate{Name: "hourly", Body: "SELECT 1"},
		},
		{
			name: "the two-label spelling",
			document: `
continuous_aggregate "app" "hourly" {
  as = "SELECT 1"
}
`,
			want: goschema.ContinuousAggregate{Name: "hourly", Schema: "app", Body: "SELECT 1"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			db, err := atlashcl.Parse([]byte(test.document), "schema.hcl")

			c.Assert(err, qt.IsNil)
			c.Assert(db.ContinuousAggregates, qt.DeepEquals, []goschema.ContinuousAggregate{test.want})
		})
	}
}

// TestParseContinuousAggregate_RefusesADeclarationWithNoBody pins the one
// required attribute.
//
// A continuous aggregate IS its SELECT. There is no default body and nothing to
// create without one, so an empty declaration is a document error rather than
// an object with a gap.
func TestParseContinuousAggregate_RefusesADeclarationWithNoBody(t *testing.T) {
	tests := []struct {
		name     string
		document string
		want     string
	}{
		{
			name:     "no body",
			document: "continuous_aggregate \"hourly\" {\n}\n",
			want:     "requires an `as` body",
		},
		{
			name:     "an attribute the block does not have",
			document: "continuous_aggregate \"hourly\" {\n  as = \"SELECT 1\"\n  column = \"time\"\n}\n",
			want:     "column",
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
