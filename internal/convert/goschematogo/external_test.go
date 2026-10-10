package goschematogo_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/goschema"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbexternal"
	"ptah.run/internal/convert/goschematogo"
)

// TestRenderExternalObjectsRoundTripThroughParser writes each YDB external
// data source and external table as the annotation the parser reads back as
// the same object, a semicolon and a backslash in an option's value and a
// column name that needs quoting included.
func TestRenderExternalObjectsRoundTripThroughParser(t *testing.T) {
	c := qt.New(t)
	warehouse := ydbexternal.DataSource{SourceType: "PostgreSQL", Location: "pg:5432", AuthMethod: "BASIC",
		Options: map[string]string{"DATABASE_NAME": "app", "LOGIN": `a\b`, "PASSWORD_SECRET_PATH": "ext/pw"}}
	bucket := ydbexternal.DataSource{SourceType: "ObjectStorage", Location: "https://s3.example.test/b/", AuthMethod: "NONE"}
	events := ydbexternal.Table{DataSource: "bucket", Location: "e/",
		Columns: []ydbexternal.Column{
			{Name: "id", Type: "Int64", NotNull: true}, {Name: "the kind", Type: "Utf8"}, {Name: "amount", Type: "Decimal(22,9)"},
		},
		Options: map[string]string{"FORMAT": "csv_with_names", "CSV_DELIMITER": ";", "PARTITIONED_BY": `["id"]`}}
	database := &schemamodel.Database{FeatureObjects: must.Must(schemaext.NewObjects(
		ydbexternal.DesiredSourceObject("ext", "warehouse", "", warehouse),
		ydbexternal.DesiredSourceObject("", "bucket", "", bucket),
		ydbexternal.DesiredTableObject("ext", "events", "", events),
	))}
	files, err := goschematogo.Render(c.Context(), database, goschematogo.Options{PackageName: "models", SingleFile: true})
	c.Assert(err, qt.IsNil)
	dir := t.TempDir()
	c.Assert(goschematogo.WriteDir(dir, files), qt.IsNil)

	parsed, err := goschema.ParseDir(dir)

	c.Assert(err, qt.IsNil)
	objects, err := parsed.FeatureObjects.All()
	c.Assert(err, qt.IsNil)
	c.Assert(objects, qt.DeepEquals, []schemaext.Object{
		ydbexternal.DesiredSourceObject("", "bucket", "PtahSchemaObjects", bucket),
		ydbexternal.DesiredSourceObject("ext", "warehouse", "PtahSchemaObjects", warehouse),
		ydbexternal.DesiredTableObject("ext", "events", "PtahSchemaObjects", events),
	})
}
