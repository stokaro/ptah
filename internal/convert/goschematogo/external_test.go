package goschematogo_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/goschema"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/convert/goschematogo"
)

// TestRenderExternalObjectsRoundTripThroughParser writes each YDB external
// data source and external table as the annotation the parser reads back as
// the same object, a semicolon and a backslash in an option's value and a
// column name that needs quoting included.
func TestRenderExternalObjectsRoundTripThroughParser(t *testing.T) {
	c := qt.New(t)
	database := &schemamodel.Database{
		ExternalDataSources: []schemamodel.ExternalDataSource{
			{Name: "warehouse", Schema: "ext", SourceType: "PostgreSQL", Location: "pg:5432", AuthMethod: "BASIC",
				Options: map[string]string{"DATABASE_NAME": "app", "LOGIN": `a\b`, "PASSWORD_SECRET_PATH": "ext/pw"}},
			{Name: "bucket", SourceType: "ObjectStorage", Location: "https://s3.example.test/b/", AuthMethod: "NONE"},
		},
		ExternalTables: []schemamodel.ExternalTable{{
			Name: "events", Schema: "ext", DataSource: "bucket", Location: "e/",
			Columns: []schemamodel.ExternalColumn{
				{Name: "id", Type: "Int64", NotNull: true}, {Name: "the kind", Type: "Utf8"},
				{Name: "amount", Type: "Decimal(22,9)"},
			},
			Options: map[string]string{"FORMAT": "csv_with_names", "CSV_DELIMITER": ";", "PARTITIONED_BY": `["id"]`},
		}},
	}
	files, err := goschematogo.Render(c.Context(), database, goschematogo.Options{PackageName: "models", SingleFile: true})
	c.Assert(err, qt.IsNil)
	dir := t.TempDir()
	c.Assert(goschematogo.WriteDir(dir, files), qt.IsNil)

	parsed, err := goschema.ParseDir(dir)

	c.Assert(err, qt.IsNil)
	want := database.ExternalTables[0]
	want.StructName = "PtahSchemaObjects"
	c.Assert(parsed.ExternalTables, qt.DeepEquals, []schemamodel.ExternalTable{want})
	bucket, warehouse := database.ExternalDataSources[1], database.ExternalDataSources[0]
	bucket.StructName, warehouse.StructName = "PtahSchemaObjects", "PtahSchemaObjects"
	c.Assert(parsed.ExternalDataSources, qt.DeepEquals, []schemamodel.ExternalDataSource{bucket, warehouse})
}
