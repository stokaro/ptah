package goschematogo_test

import (
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/goschema"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/timescaledb/tsschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/convert/goschematogo"
)

// timescaleSchema is a partitioned table, a second table without settings,
// and an aggregate over the first.
func timescaleSchema(c *qt.C) *schemamodel.Database {
	c.Helper()
	return &schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "Reading", Name: "readings", Facets: must.Must(schemaext.NewFacets(
				&tsschema.DesiredHypertable{Column: "time", ChunkInterval: "1 day", IfNotExists: true, Comment: "by time"}))},
			{StructName: "Device", Name: "devices"},
		},
		Fields: []schemamodel.Field{
			{StructName: "Reading", FieldName: "Time", Name: "time", Type: "TIMESTAMPTZ", Primary: true},
			{StructName: "Device", FieldName: "ID", Name: "id", Type: "INTEGER", Primary: true},
		},
		FeatureObjects: must.Must(schemaext.NewObjects(tsschema.DesiredContinuousAggregateObject("metrics", "hourly",
			tsschema.DesiredContinuousAggregate{Body: "SELECT time_bucket('1 hour', time) AS bucket FROM readings GROUP BY bucket",
				MaterializedOnly: new(false), Comment: "hourly", StructName: "Hourly"}))),
		FeatureCoverage: must.Must(tsschema.CompleteCoverage(schemaext.Desired)),
	}
}

// TestRender_TimescaleStateSurvivesItsOwnAnnotations pins the round trip the
// annotations exist for, in each file layout: a hypertable annotation is
// written beside its table, because the parser attaches it only to a table the
// same file declares.
func TestRender_TimescaleStateSurvivesItsOwnAnnotations(t *testing.T) {
	tests := []struct {
		name    string
		options goschematogo.Options
	}{
		{name: "one file", options: goschematogo.Options{SingleFile: true, Dialect: "postgres", Runtime: must.Must(builtin.New())}},
		{name: "a file per table", options: goschematogo.Options{PerTable: true, Dialect: "postgres", Runtime: must.Must(builtin.New())}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db := timescaleSchema(c)

			files, err := goschematogo.Render(t.Context(), db, test.options)
			c.Assert(err, qt.IsNil)
			source := fstest.MapFS{}
			for _, file := range files {
				source["models/"+file.Name] = &fstest.MapFile{Data: file.Data}
			}
			parsed, err := goschema.ParseFS(builtintest.Annotations(), source, "models")
			c.Assert(err, qt.IsNil)

			c.Assert(tableFacets(c, parsed), qt.DeepEquals, tableFacets(c, db))
			c.Assert(aggregates(c, parsed), qt.DeepEquals, aggregates(c, db))
		})
	}
}

// TestRender_RefusesATimescaleLimitItCannotWrite pins the export refusal: an
// annotation file claims to describe every hypertable and aggregate, so a
// description that could not describe one is refused rather than written as
// absent.
func TestRender_RefusesATimescaleLimitItCannotWrite(t *testing.T) {
	c := qt.New(t)
	db := timescaleSchema(c)
	table := objectidentity.NewBuilder(identifier.ForDialect("postgres")).TableParts("", "devices")
	db.FeatureCoverage = must.Must(schemaext.NewCoverage(schemaext.Desired, db.FeatureCoverage.KindRecords(), []schemaext.SubjectCoverage{{
		Kind: tsschema.HypertableKind, Subject: table,
		Knowledge: schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "the catalog reported no dimension for this hypertable"},
	}}))

	files, err := goschematogo.Render(t.Context(), db, goschematogo.Options{SingleFile: true, Dialect: "postgres", Runtime: must.Must(builtin.New())})

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, `(?s).*devices.*cannot be exported without losing its coverage record.*the catalog reported no dimension.*`)
	c.Assert(files, qt.IsNil)
}

// aggregates names each aggregate with its declaration, less the struct the
// annotation sits on: that is where the source put it, and the export chooses
// its own.
func aggregates(c *qt.C, db *schemamodel.Database) map[string]tsschema.DesiredContinuousAggregate {
	c.Helper()
	result := make(map[string]tsschema.DesiredContinuousAggregate)
	for _, object := range must.Must(db.FeatureObjects.All()) {
		value := *object.Value.(*tsschema.DesiredContinuousAggregate)
		value.StructName = ""
		result[tsschema.QualifiedName(object.Ref)] = value
	}
	return result
}

// tableFacets keys each table's settings by its name, so two schemas whose
// tables came back in another order still compare.
func tableFacets(c *qt.C, db *schemamodel.Database) map[string]*tsschema.DesiredHypertable {
	c.Helper()
	result := make(map[string]*tsschema.DesiredHypertable, len(db.Tables))
	for _, table := range db.Tables {
		value, _, err := schemaext.FacetAs[*tsschema.DesiredHypertable](table.Facets, tsschema.HypertableKind)
		c.Assert(err, qt.IsNil)
		result[table.Name] = value
	}
	return result
}
