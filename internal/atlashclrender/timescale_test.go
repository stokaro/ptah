package atlashclrender_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/timescaledb/tsschema"
	"ptah.run/internal/atlashcl"
	"ptah.run/internal/atlashclrender"
)

// timescaleDocument is a partitioned table and one aggregate over it.
func timescaleDocument(c *qt.C, hypertable tsschema.DesiredHypertable, aggregate tsschema.DesiredContinuousAggregate) *schemamodel.Database {
	c.Helper()
	return &schemamodel.Database{
		Schemas: []schemamodel.Schema{{Name: "public"}},
		Tables: []schemamodel.Table{{
			StructName: "T", Name: "readings", Schema: "public",
			Facets: must.Must(schemaext.NewFacets(&hypertable)),
		}},
		Fields: []schemamodel.Field{{StructName: "T", Name: "time", Type: "TIMESTAMPTZ", Primary: true}},
		FeatureObjects: must.Must(schemaext.NewObjects(
			tsschema.DesiredContinuousAggregateObject("public", "hourly", aggregate),
		)),
		FeatureCoverage: must.Must(tsschema.CompleteCoverage(schemaext.Desired)),
	}
}

// TestRenderWritesTheTimescaleBlocks pins that a description says which tables
// are partitioned and which views are continuous aggregates.
//
// Nothing else in a document can. Measured on TimescaleDB 2.29.2 / PostgreSQL
// 17.11, a hypertable answers `relkind = 'r'` and carries no extension
// ownership in `pg_depend`, so a document that describes the table describes an
// ORDINARY table -- complete on its face, and wrong. To PostgreSQL a continuous
// aggregate IS a view, and describing it as one replays the rewritten body
// TimescaleDB stores (stokaro/ptah#1026).
//
// The interval row keeps a round trip converging: an omitted interval takes
// TimescaleDB's own default, and writing the default back would turn "whatever
// the server chooses" into a fixed value the next comparison holds the server
// to.
func TestRenderWritesTheTimescaleBlocks(t *testing.T) {
	tests := []struct {
		name       string
		hypertable tsschema.DesiredHypertable
		aggregate  tsschema.DesiredContinuousAggregate
		want       string
	}{
		{
			name:       "every setting",
			hypertable: tsschema.DesiredHypertable{Column: "time", ChunkInterval: "1 day", IfNotExists: true, Comment: "by time"},
			aggregate:  tsschema.DesiredContinuousAggregate{Body: "SELECT 1", MaterializedOnly: new(true), Comment: "hourly"},
			want: "hypertable \"readings\" {\n  schema = schema.public\n  column = \"time\"\n  chunk_interval = \"1 day\"\n" +
				"  if_not_exists = true\n  comment = \"by time\"\n}\n\n" +
				"continuous_aggregate \"hourly\" {\n  schema = schema.public\n  as = \"SELECT 1\"\n" +
				"  materialized_only = true\n  comment = \"hourly\"\n}\n",
		},
		{
			name:       "the server's defaults",
			hypertable: tsschema.DesiredHypertable{Column: "time"},
			aggregate:  tsschema.DesiredContinuousAggregate{Body: "SELECT 1"},
			want: "hypertable \"readings\" {\n  schema = schema.public\n  column = \"time\"\n}\n\n" +
				"continuous_aggregate \"hourly\" {\n  schema = schema.public\n  as = \"SELECT 1\"\n}\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result, err := atlashclrender.RenderInspected(timescaleDocument(c, test.hypertable, test.aggregate), platform.Postgres, "public")

			c.Assert(err, qt.IsNil)
			c.Assert(string(result.Data), qt.Contains, test.want)
		})
	}
}

// TestRenderTheTimescaleStateSurvivesItsOwnDocument is the round trip the
// blocks exist for: what is written is read back as the same declaration.
func TestRenderTheTimescaleStateSurvivesItsOwnDocument(t *testing.T) {
	c := qt.New(t)
	db := timescaleDocument(c,
		tsschema.DesiredHypertable{Column: "time", ChunkInterval: "1 day", IfNotExists: true, Comment: "by time"},
		tsschema.DesiredContinuousAggregate{
			Body:             "SELECT time_bucket('1 hour', time) AS bucket FROM readings GROUP BY bucket",
			MaterializedOnly: new(false),
		})

	result, err := atlashclrender.RenderInspected(db, platform.Postgres, "public")
	c.Assert(err, qt.IsNil)
	reread, err := atlashcl.Parse(result.Data, "schema.hcl")
	c.Assert(err, qt.IsNil)

	c.Assert(reread.Tables, qt.HasLen, 1)
	c.Assert(reread.Tables[0].Facets.Equal(db.Tables[0].Facets), qt.IsTrue)
	c.Assert(must.Must(reread.FeatureObjects.All()), qt.DeepEquals, must.Must(db.FeatureObjects.All()))
}

// TestRenderRefusesATimescaleLimitItCannotWrite pins the export refusal: a
// parsed document claims to describe every hypertable and aggregate, so a
// description that could not describe one is refused rather than written as
// absent.
func TestRenderRefusesATimescaleLimitItCannotWrite(t *testing.T) {
	c := qt.New(t)
	db := timescaleDocument(c, tsschema.DesiredHypertable{Column: "time"}, tsschema.DesiredContinuousAggregate{Body: "SELECT 1"})
	db.FeatureCoverage = must.Must(schemaext.NewCoverage(schemaext.Desired, db.FeatureCoverage.KindRecords(), []schemaext.SubjectCoverage{{
		Kind: tsschema.ContinuousAggregateKind, Subject: tsschema.ContinuousAggregateRef("public", "daily"),
		Knowledge: schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "the definition could not be read"},
	}}))

	result, err := atlashclrender.RenderInspected(db, platform.Postgres, "public")

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, `(?s).*daily.*cannot be exported without losing its coverage record.*`)
	c.Assert(result.Data, qt.IsNil)
}
