package atlashcl_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/timescaledb/tsschema"
	"ptah.run/internal/atlashcl"
)

// TestParseHypertable reads the block that says a table is partitioned, and
// gives the settings to the table it names.
//
// The label is the TABLE, because a hypertable has no name of its own:
// `timescaledb_information.hypertables` is keyed by the relation, and there is
// nothing to rename.
func TestParseHypertable(t *testing.T) {
	tests := []struct {
		name     string
		document string
		want     tsschema.DesiredHypertable
	}{
		{
			name: "the whole block",
			document: `
schema "app" {
}

table "readings" {
  schema = schema.app
  column "time" {
    type = timestamptz
  }
}

hypertable "readings" {
  schema         = schema.app
  column         = "time"
  chunk_interval = "1 day"
  if_not_exists  = true
  comment        = "partitioned by hour of arrival"
}
`,
			want: tsschema.DesiredHypertable{Column: "time", ChunkInterval: "1 day", IfNotExists: true, Comment: "partitioned by hour of arrival"},
		},
		{
			// No interval takes TimescaleDB's own default, so an omitted one is
			// a declaration rather than a gap.
			name: "the smallest one there is",
			document: `
table "readings" {
  column "time" {
    type = timestamptz
  }
}

hypertable "readings" {
  column = "time"
}
`,
			want: tsschema.DesiredHypertable{Column: "time"},
		},
		{
			// A document exported from a server names every table's schema,
			// and a block written by hand often does not.
			name: "a table in the default schema, named without it",
			document: `
schema "public" {
}

table "readings" {
  schema = schema.public
  column "time" {
    type = timestamptz
  }
}

hypertable "readings" {
  column = "time"
}
`,
			want: tsschema.DesiredHypertable{Column: "time"},
		},
		{
			// The block names no schema, and the one table of that name is
			// in another schema than the default.
			name: "a table in another schema, named without it",
			document: `
schema "app" {
}

table "readings" {
  schema = schema.app
  column "time" {
    type = timestamptz
  }
}

hypertable "readings" {
  column = "time"
}
`,
			want: tsschema.DesiredHypertable{Column: "time"},
		},
		{
			name: "the two-label spelling, before its table",
			document: `
hypertable "app" "readings" {
  column = "time"
}

schema "app" {
}

table "readings" {
  schema = schema.app
  column "time" {
    type = timestamptz
  }
}
`,
			want: tsschema.DesiredHypertable{Column: "time"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			db, err := atlashcl.Parse([]byte(test.document), "schema.hcl")

			c.Assert(err, qt.IsNil)
			c.Assert(db.Tables, qt.HasLen, 1)
			hypertable, found := must.Must2(schemaext.FacetAs[*tsschema.DesiredHypertable](db.Tables[0].Facets, tsschema.HypertableKind))
			c.Assert(found, qt.IsTrue)
			c.Assert(hypertable, qt.DeepEquals, &test.want)
		})
	}
}

// TestParseHypertable_RefusesWhatItCannotPlace pins the refusals: the one
// required attribute, an attribute the block does not have, a table the
// document does not declare, and a table partitioned twice.
//
// A hypertable with no dimension is not one, and the server has no default to
// fall back on: `create_hypertable` takes the dimension as its second argument.
func TestParseHypertable_RefusesWhatItCannotPlace(t *testing.T) {
	const table = "table \"readings\" {\n  column \"time\" {\n    type = timestamptz\n  }\n}\n"
	tests := []struct {
		name     string
		document string
		want     string
	}{
		{
			name:     "no column",
			document: table + "hypertable \"readings\" {\n}\n",
			want:     `hypertable "readings" requires a column to partition on`,
		},
		{
			name:     "an attribute the block does not have",
			document: table + "hypertable \"readings\" {\n  column = \"time\"\n  target = \"x\"\n}\n",
			want:     "target",
		},
		{
			name:     "a table the document does not declare",
			document: "hypertable \"readings\" {\n  column = \"time\"\n}\n",
			want:     `hypertable "readings" names a table this schema does not declare`,
		},
		{
			name: "a table name two schemas declare",
			document: "schema \"app\" {\n}\nschema \"public\" {\n}\n" +
				"table \"readings\" {\n  schema = schema.app\n  column \"time\" {\n    type = timestamptz\n  }\n}\n" +
				"table \"readings\" {\n  schema = schema.public\n  column \"time\" {\n    type = timestamptz\n  }\n}\n" +
				"hypertable \"readings\" {\n  column = \"time\"\n}\n",
			want: `hypertable "readings" names a table that schemas "app" and "public" each declare; name the schema it belongs to`,
		},
		{
			name:     "a schema the table is not in",
			document: "schema \"app\" {\n}\n" + table + "hypertable \"app\" \"readings\" {\n  column = \"time\"\n}\n",
			want:     `hypertable "app.readings" names a table this schema does not declare`,
		},
		{
			name:     "a table partitioned twice",
			document: table + "hypertable \"readings\" {\n  column = \"time\"\n}\nhypertable \"readings\" {\n  column = \"at\"\n}\n",
			want:     `table "readings" declares two hypertable blocks`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			db, err := atlashcl.Parse([]byte(test.document), "schema.hcl")

			c.Assert(err, qt.ErrorMatches, "(?s).*"+test.want+".*")
			c.Assert(db, qt.IsNil)
		})
	}
}

// TestParseContinuousAggregate reads the block that names a TimescaleDB
// continuous aggregate.
//
// Unlike a hypertable this object has a name of its own, so the label is that
// name and `schema` says where it lives -- the shape a view block already has.
func TestParseContinuousAggregate(t *testing.T) {
	tests := []struct {
		name     string
		document string
		want     schemaext.Object
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
			want: tsschema.DesiredContinuousAggregateObject("app", "hourly", tsschema.DesiredContinuousAggregate{
				Body:             "SELECT time_bucket('1 hour', time) AS bucket FROM readings GROUP BY bucket",
				MaterializedOnly: new(true), Comment: "one row per hour",
			}),
		},
		{
			name: "the smallest one there is",
			document: `
continuous_aggregate "hourly" {
  as = "SELECT 1"
}
`,
			want: tsschema.DesiredContinuousAggregateObject("", "hourly", tsschema.DesiredContinuousAggregate{Body: "SELECT 1"}),
		},
		{
			// A dotted label names the schema too, as the block's other
			// spellings do.
			name: "a qualified label",
			document: `
continuous_aggregate "app.hourly" {
  as = "SELECT 1"
}
`,
			want: tsschema.DesiredContinuousAggregateObject("app", "hourly", tsschema.DesiredContinuousAggregate{Body: "SELECT 1"}),
		},
		{
			name: "the two-label spelling",
			document: `
continuous_aggregate "app" "hourly" {
  as = "SELECT 1"
}
`,
			want: tsschema.DesiredContinuousAggregateObject("app", "hourly", tsschema.DesiredContinuousAggregate{Body: "SELECT 1"}),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			db, err := atlashcl.Parse([]byte(test.document), "schema.hcl")

			c.Assert(err, qt.IsNil)
			c.Assert(must.Must(db.FeatureObjects.All()), qt.DeepEquals, []schemaext.Object{test.want})
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
		{
			name:     "a second declaration",
			document: "continuous_aggregate \"hourly\" {\n  as = \"SELECT 1\"\n}\ncontinuous_aggregate \"hourly\" {\n  as = \"SELECT 2\"\n}\n",
			want:     "hourly",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			db, err := atlashcl.Parse([]byte(test.document), "schema.hcl")

			c.Assert(err, qt.ErrorMatches, "(?s).*"+test.want+".*")
			c.Assert(db, qt.IsNil)
		})
	}
}

// TestParse_AnHCLDocumentDescribesBothTimescaleModels pins the coverage the
// document records: blocks exist for both models, so a document declaring
// neither describes a database without either.
func TestParse_AnHCLDocumentDescribesBothTimescaleModels(t *testing.T) {
	tests := []struct {
		name string
		kind schemaext.Kind
	}{
		{name: "hypertables", kind: tsschema.HypertableKind},
		{name: "continuous aggregates", kind: tsschema.ContinuousAggregateKind},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			db, err := atlashcl.Parse([]byte("table \"plain\" {\n  column \"id\" {\n    type = int\n  }\n}\n"), "schema.hcl")

			c.Assert(err, qt.IsNil)
			// The claim is per kind, so it answers for a subject the document never names.
			c.Assert(db.FeatureCoverage.Lookup(test.kind, tsschema.ContinuousAggregateRef("", "absent")).State, qt.Equals, schemaext.Complete)
		})
	}
}
