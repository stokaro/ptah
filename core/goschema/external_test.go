package goschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/goschema"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
)

// externalSource is an entity file whose holder struct carries annotation.
func externalSource(annotation string) string {
	return "package entities\n\n" + annotation + "\ntype Holder struct{}\n"
}

// TestParseSource_ExternalObjects_HappyPath reads the fixture's data sources
// and external table, options and columns included.
func TestParseSource_ExternalObjects_HappyPath(t *testing.T) {
	c := qt.New(t)
	db, err := goschema.ParseDir("../../integration/internal/fixtures/entities/053-ydb-external-sources")
	c.Assert(err, qt.IsNil)
	c.Assert(db.ExternalDataSources, qt.DeepEquals, []schemamodel.ExternalDataSource{
		{StructName: "Warehouse", Name: "warehouse", Schema: "ext", SourceType: "PostgreSQL",
			Location: "pg.example.test:5432", AuthMethod: "BASIC", Options: map[string]string{
				"DATABASE_NAME": "app", "LOGIN": "reader", "PASSWORD_SECRET_PATH": "ext/pg_password",
			}},
		{StructName: "Warehouse", Name: "events_bucket", Schema: "ext", SourceType: "ObjectStorage",
			Location: "https://storage.example.test/events/", AuthMethod: "NONE"},
	})
	c.Assert(db.ExternalTables, qt.DeepEquals, []schemamodel.ExternalTable{{
		StructName: "Event", Name: "events", Schema: "ext", DataSource: "ext/events_bucket", Location: "2026/",
		Columns: []schemamodel.ExternalColumn{
			{Name: "id", Type: "Int64", NotNull: true}, {Name: "kind", Type: "Utf8"}, {Name: "amount", Type: "Decimal(22,9)"},
		},
		Options: map[string]string{"FORMAT": "json_each_row", "COMPRESSION": "gzip"},
	}})
}

// TestParseSource_ExternalObjects_FailurePath refuses what a declaration of
// either object cannot carry, naming the attribute.
func TestParseSource_ExternalObjects_FailurePath(t *testing.T) {
	tests := []struct {
		name       string
		annotation string
		attribute  string
		wantErr    string
	}{
		{
			name:       "a data source without a source type",
			annotation: `//ptah:schema:externaldatasource name="s3" auth_method="NONE"`,
			attribute:  "source_type",
			wantErr:    `missing required annotation attribute "source_type" on //ptah:schema:externaldatasource at Holder`,
		},
		{
			name:       "a location written as an option",
			annotation: `//ptah:schema:externaldatasource name="s3" source_type="ObjectStorage" auth_method="NONE" options="LOCATION=x"`,
			attribute:  "options",
			wantErr: `invalid options: LOCATION is written with its own attribute, location ` +
				`on //ptah:schema:externaldatasource at Holder`,
		},
		{
			name:       "a path in a data source's name",
			annotation: `//ptah:schema:externaldatasource name="ext/s3" source_type="ObjectStorage" auth_method="NONE"`,
			attribute:  "name",
			wantErr:    `invalid name: "ext/s3" holds a slash; name the directory with schema .*`,
		},
		{
			name:       "a column with a default",
			annotation: `//ptah:schema:externaltable name="events" data_source="s3" location="e/" columns="id Int64 DEFAULT 0"`,
			attribute:  "columns",
			wantErr: `invalid columns: column id carries DEFAULT, which an external table does not take ` +
				`on //ptah:schema:externaltable at Holder`,
		},
		{
			name:       "a table without columns",
			annotation: `//ptah:schema:externaltable name="events" data_source="s3" location="e/"`,
			attribute:  "columns",
			wantErr:    `missing required annotation attribute "columns" on //ptah:schema:externaltable at Holder`,
		},
		{
			name:       "a data source written as an option",
			annotation: `//ptah:schema:externaltable name="events" data_source="s3" location="e/" columns="id Int64" options="DATA_SOURCE=s3"`,
			attribute:  "options",
			wantErr:    `invalid options: DATA_SOURCE is written with its own attribute, data_source .*`,
		},
		{
			name:       "an attribute an external table does not take",
			annotation: `//ptah:schema:externaltable name="events" data_source="s3" location="e/" columns="id Int64" format="csv"`,
			attribute:  "format",
			wantErr:    `.*format.*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db, err := goschema.ParseSource("external.go", externalSource(test.annotation))
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			var parseErr *ptaherr.ParseError
			c.Assert(err, qt.ErrorAs, &parseErr)
			c.Assert(parseErr.Attribute, qt.Equals, test.attribute)
			c.Assert(db.ExternalDataSources, qt.HasLen, 0)
			c.Assert(db.ExternalTables, qt.HasLen, 0)
		})
	}
}
