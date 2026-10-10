package tssource_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/annotation"
	"ptah.run/core/goschema"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/timescaledb/tsschema"
	"ptah.run/dialect/timescaledb/tssource"
)

// timescaleOwner selects this owner alone, so each test reads TimescaleDB's
// directives through the frontend the way a runtime that registers the owner
// does.
func timescaleOwner(c *qt.C) annotation.Set {
	c.Helper()
	set, err := annotation.NewSet(tssource.Annotations())
	c.Assert(err, qt.IsNil)
	return set
}

func parse(c *qt.C, filename string, source any) schemamodel.Database {
	c.Helper()
	db, err := goschema.ParseSource(timescaleOwner(c), filename, source)
	c.Assert(err, qt.IsNil)
	return db
}

// TestParseSource_WithoutTheOwnerTheDirectivesDeclareNothing is the control
// on the selection: a parse that selects no owner reads a TimescaleDB
// directive no more than one it does not know, and claims no knowledge of
// either model, so a comparison leaves an existing hypertable alone rather
// than reading its absence as a removal.
func TestParseSource_WithoutTheOwnerTheDirectivesDeclareNothing(t *testing.T) {
	c := qt.New(t)
	source := "package models\n\n//ptah:schema:table name=\"readings\"\ntype Reading struct{}\n\n" +
		"//ptah:schema:hypertable table=\"readings\" column=\"time\"\ntype H struct{}\n" +
		"//ptah:schema:continuousaggregate name=\"hourly\" body=\"SELECT 1\"\ntype Hourly struct{}\n"

	db, err := goschema.ParseSource(annotation.None(), "readings.go", source)

	c.Assert(err, qt.IsNil)
	c.Assert(db.Tables, qt.HasLen, 1)
	c.Assert(db.Tables[0].Facets.IsZero(), qt.IsTrue)
	c.Assert(db.FeatureObjects.Len(), qt.Equals, 0)
	c.Assert(db.FeatureCoverage.Lookup(tsschema.HypertableKind, tsschema.ContinuousAggregateRef("", "hourly")).State, qt.Not(qt.Equals), schemaext.Complete)
}

// TestParseSource_ReadsTheContinuousAggregateAnnotation pins what the
// annotation carries, and that the body is kept as it was WRITTEN.
//
// The catalog stores a rewritten SELECT, and the comparison puts the
// declaration through the same rewrite rather than folding either text -- so a
// body normalized here would be normalized twice and match nothing
// (stokaro/ptah#1026).
func TestParseSource_ReadsTheContinuousAggregateAnnotation(t *testing.T) {
	c := qt.New(t)
	const body = "SELECT time_bucket('1 hour', time) AS bucket, avg(value) AS v " +
		"FROM readings GROUP BY bucket"
	source := "package models\n\n" +
		"//ptah:schema:continuousaggregate name=\"hourly\" schema=\"metrics\" body=\"" + body + "\" " +
		"materialized_only=\"true\" comment=\"one row per hour\"\n" +
		"type Hourly struct{}\n"

	db := parse(c, "aggregate.go", source)

	c.Assert(must.Must(db.FeatureObjects.All()), qt.DeepEquals, []schemaext.Object{
		tsschema.DesiredContinuousAggregateObject("metrics", "hourly", tsschema.DesiredContinuousAggregate{
			StructName: "Hourly", Body: body, MaterializedOnly: new(true), Comment: "one row per hour",
		}),
	})
}

// TestParseSource_TheAggregateOptionDefaultsOff is the control on the boolean:
// an omitted attribute is the server's own default, not true.
func TestParseSource_TheAggregateOptionDefaultsOff(t *testing.T) {
	c := qt.New(t)
	source := "package models\n\n" +
		"//ptah:schema:continuousaggregate name=\"hourly\" body=\"SELECT 1\"\n" +
		"type Hourly struct{}\n"

	db := parse(c, "aggregate.go", source)

	c.Assert(must.Must(db.FeatureObjects.All()), qt.DeepEquals, []schemaext.Object{
		tsschema.DesiredContinuousAggregateObject("", "hourly", tsschema.DesiredContinuousAggregate{StructName: "Hourly", Body: "SELECT 1"}),
	})
}

// TestParseSource_AttachesTheHypertableToItsTable pins that the settings
// belong to the table the annotation names, wherever in the file the table is
// declared, and that the interval is kept as it was written.
func TestParseSource_AttachesTheHypertableToItsTable(t *testing.T) {
	tests := []struct {
		name   string
		source string
		want   tsschema.DesiredHypertable
	}{
		{
			name: "after the table",
			source: "package models\n\n//ptah:schema:table name=\"readings\"\ntype Reading struct {\n\t//ptah:schema:field name=\"time\" type=\"TIMESTAMPTZ\"\n\tTime string\n}\n\n" +
				"//ptah:schema:hypertable table=\"readings\" column=\"time\" chunk_interval=\"1 day\" if_not_exists=\"true\" comment=\"by time\"\n" +
				"type ReadingsHypertable struct{}\n",
			want: tsschema.DesiredHypertable{Column: "time", ChunkInterval: "1 day", IfNotExists: true, Comment: "by time"},
		},
		{
			name: "before the table",
			source: "package models\n\n//ptah:schema:hypertable table=\"readings\" column=\"time\"\ntype ReadingsHypertable struct{}\n\n" +
				"//ptah:schema:table name=\"readings\"\ntype Reading struct {\n\t//ptah:schema:field name=\"time\" type=\"TIMESTAMPTZ\"\n\tTime string\n}\n",
			want: tsschema.DesiredHypertable{Column: "time"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			db := parse(c, "readings.go", test.source)

			c.Assert(db.Tables, qt.HasLen, 1)
			hypertable, found := must.Must2(schemaext.FacetAs[*tsschema.DesiredHypertable](db.Tables[0].Facets, tsschema.HypertableKind))
			c.Assert(found, qt.IsTrue)
			c.Assert(hypertable, qt.DeepEquals, &test.want)
		})
	}
}

// TestParseSource_AGoSchemaDescribesBothTimescaleModels pins the coverage a Go
// schema records: annotations exist for both models, so a schema declaring
// neither describes a database without either.
func TestParseSource_AGoSchemaDescribesBothTimescaleModels(t *testing.T) {
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

			db := parse(c, "plain.go", "package models\n\n//ptah:schema:table name=\"plain\"\ntype Plain struct{}\n")

			// The claim is per kind, so it answers for a subject the schema never names.
			knowledge := db.FeatureCoverage.Lookup(test.kind, tsschema.ContinuousAggregateRef("", "absent"))
			c.Assert(knowledge.State, qt.Equals, schemaext.Complete)
		})
	}
}

// TestParseSource_TimescaleAnnotationsRefuseWhatTheyCannotPlace pins the
// refusals: the required attributes, a hypertable whose table the file does not
// declare, and a second declaration of the same object.
func TestParseSource_TimescaleAnnotationsRefuseWhatTheyCannotPlace(t *testing.T) {
	const table = "//ptah:schema:table name=\"readings\"\ntype Reading struct{}\n"
	tests := []struct {
		name    string
		source  string
		wantIs  error
		wantErr string
	}{
		{
			name:    "an aggregate without a name",
			source:  "//ptah:schema:continuousaggregate body=\"SELECT 1\"\ntype Hourly struct{}\n",
			wantIs:  ptaherr.ErrMissingRequiredAttribute,
			wantErr: `(?s).*name.*`,
		},
		{
			name:    "an aggregate without a body",
			source:  "//ptah:schema:continuousaggregate name=\"hourly\"\ntype Hourly struct{}\n",
			wantIs:  ptaherr.ErrMissingRequiredAttribute,
			wantErr: `(?s).*body.*`,
		},
		{
			name:    "a hypertable without a column",
			source:  table + "//ptah:schema:hypertable table=\"readings\"\ntype H struct{}\n",
			wantIs:  ptaherr.ErrMissingRequiredAttribute,
			wantErr: `(?s).*column.*`,
		},
		{
			name:    "a hypertable for a table the file does not declare",
			source:  "//ptah:schema:hypertable table=\"readings\" column=\"time\"\ntype H struct{}\n",
			wantIs:  ptaherr.ErrInvalidAttributeValue,
			wantErr: `(?s).*table "readings" is not declared in this file, and a hypertable is declared beside its table.*`,
		},
		{
			name:    "a table declared a hypertable twice",
			source:  table + "//ptah:schema:hypertable table=\"readings\" column=\"time\"\ntype H struct{}\n//ptah:schema:hypertable table=\"readings\" column=\"at\"\ntype G struct{}\n",
			wantIs:  ptaherr.ErrInvalidAttributeValue,
			wantErr: `(?s).*table "readings" declares a hypertable twice.*`,
		},
		{
			name:    "an aggregate declared twice",
			source:  "//ptah:schema:continuousaggregate name=\"hourly\" body=\"SELECT 1\"\ntype H struct{}\n//ptah:schema:continuousaggregate name=\"hourly\" body=\"SELECT 2\"\ntype G struct{}\n",
			wantIs:  ptaherr.ErrInvalidAttributeValue,
			wantErr: `(?s).*continuous aggregate "hourly" is declared twice.*`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			db, err := goschema.ParseSource(timescaleOwner(c), "timescale.go", "package models\n\n"+test.source)

			c.Assert(err, qt.ErrorIs, test.wantIs)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(db.Tables, qt.HasLen, 0)
		})
	}
}

// twoReadings declares a readings table in public and another in archive, the
// shape an unqualified table attribute cannot choose between.
const twoReadings = "package models\n\n" +
	"//ptah:schema:table name=\"readings\" schema=\"public\"\ntype Reading struct {\n\t//ptah:schema:field name=\"time\" type=\"TIMESTAMPTZ\"\n\tTime string\n}\n\n" +
	"//ptah:schema:table name=\"readings\" schema=\"archive\"\ntype ArchivedReading struct {\n\t//ptah:schema:field name=\"time\" type=\"TIMESTAMPTZ\"\n\tTime string\n}\n\n"

// TestParseSource_ANamedTableResolvesInTheSchemaItNames pins the control on
// the ambiguity refusal below: a schema-qualified table attribute partitions
// the table in that schema and leaves the other one alone.
func TestParseSource_ANamedTableResolvesInTheSchemaItNames(t *testing.T) {
	c := qt.New(t)

	db := parse(c, "readings.go", twoReadings+
		"//ptah:schema:hypertable table=\"archive.readings\" column=\"time\"\ntype ArchiveHypertable struct{}\n")

	c.Assert(db.Tables, qt.HasLen, 2)
	c.Assert(db.Tables[0].Facets.IsZero(), qt.IsTrue)
	c.Assert(db.Tables[1].Schema, qt.Equals, "archive")
	hypertable, found := must.Must2(schemaext.FacetAs[*tsschema.DesiredHypertable](db.Tables[1].Facets, tsschema.HypertableKind))
	c.Assert(found, qt.IsTrue)
	c.Assert(hypertable.Column, qt.Equals, "time")
}

// TestParseSource_AnUnqualifiedTableTwoSchemasDeclareIsRefused pins the
// refusal for an annotation that names a table without its schema when the
// file declares the name in two schemas. Attaching the part to whichever came
// first partitions -- or streams from -- a table the author may not have
// meant.
func TestParseSource_AnUnqualifiedTableTwoSchemasDeclareIsRefused(t *testing.T) {
	tests := []struct {
		name       string
		annotation string
		directive  string
	}{
		{name: "a hypertable", annotation: "//ptah:schema:hypertable table=\"readings\" column=\"time\"\ntype H struct{}\n", directive: "hypertable"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			db, err := goschema.ParseSource(timescaleOwner(c), "readings.go", twoReadings+test.annotation)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidAttributeValue)
			c.Assert(err, qt.ErrorMatches, `(?s).*table "readings" is declared in schemas "public" and "archive"; name the schema in the table attribute.*`+test.directive+`.*`)
			c.Assert(db.Tables, qt.HasLen, 0)
		})
	}
}

// TestParseSource_AQualifiedAggregateNameNamesItsSchema pins that a dotted name
// without a schema attribute is split the way a table annotation's is. Kept as
// one name, it would create a relation literally called "metrics.hourly" in the
// default schema, and complete coverage would plan dropping the real
// metrics.hourly.
func TestParseSource_AQualifiedAggregateNameNamesItsSchema(t *testing.T) {
	tests := []struct {
		name       string
		attributes string
		want       string
	}{
		{name: "a qualified name", attributes: `name="metrics.hourly"`, want: "metrics.hourly"},
		{name: "a schema attribute", attributes: `name="hourly" schema="metrics"`, want: "metrics.hourly"},
		{name: "an unqualified name", attributes: `name="hourly"`, want: "hourly"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			source := "package models\n\n//ptah:schema:continuousaggregate " + test.attributes + " body=\"SELECT 1\"\ntype Hourly struct{}\n"

			db := parse(c, "aggregate.go", source)

			refs := db.FeatureObjects.Refs()
			c.Assert(refs, qt.HasLen, 1)
			c.Assert(tsschema.QualifiedName(refs[0]), qt.Equals, test.want)
			c.Assert(refs[0].Name.Source, qt.Equals, "hourly")
		})
	}
}
