package yamlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/core/yamlschema"
)

// TestParse_YDBExternalObjects_HappyPath reads a data source and an external
// table, keyed by name, with option names in any letter case.
func TestParse_YDBExternalObjects_HappyPath(t *testing.T) {
	c := qt.New(t)
	db, err := yamlschema.Parse([]byte(`external_data_sources:
  warehouse:
    schema: ext
    source_type: PostgreSQL
    location: pg.example.test:5432
    auth_method: BASIC
    options:
      database_name: app
      LOGIN: reader
      PASSWORD_SECRET_PATH: ext/pg_password
external_tables:
  events:
    schema: /ext/
    data_source: ext/events_bucket
    location: 2026/
    columns:
      - {name: id, type: Int64, not_null: true}
      - {name: amount, type: "Decimal(22,9)"}
    options:
      FORMAT: json_each_row
`))
	c.Assert(err, qt.IsNil)
	c.Assert(db.ExternalDataSources, qt.DeepEquals, []schemamodel.ExternalDataSource{{
		Name: "warehouse", Schema: "ext", SourceType: "PostgreSQL", Location: "pg.example.test:5432", AuthMethod: "BASIC",
		Options: map[string]string{"DATABASE_NAME": "app", "LOGIN": "reader", "PASSWORD_SECRET_PATH": "ext/pg_password"},
	}})
	c.Assert(db.ExternalTables, qt.DeepEquals, []schemamodel.ExternalTable{{
		Name: "events", Schema: "ext", DataSource: "ext/events_bucket", Location: "2026/",
		Columns: []schemamodel.ExternalColumn{{Name: "id", Type: "Int64", NotNull: true}, {Name: "amount", Type: "Decimal(22,9)"}},
		Options: map[string]string{"FORMAT": "json_each_row"},
	}})
}

// TestParse_YDBExternalObjects_FailurePath refuses what the annotation
// refuses, naming the entry.
func TestParse_YDBExternalObjects_FailurePath(t *testing.T) {
	tests := []struct {
		name     string
		document string
		wantErr  string
	}{
		{
			name:     "a reserved option",
			document: "external_data_sources:\n  s3:\n    source_type: ObjectStorage\n    auth_method: NONE\n    options:\n      location: x\n",
			wantErr:  `external data source "s3": invalid options: LOCATION is written with its own attribute, location`,
		},
		{
			name:     "an option in two letter cases",
			document: "external_data_sources:\n  pg:\n    options:\n      login: a\n      LOGIN: b\n",
			wantErr:  `external data source "pg": invalid options: LOGIN is written twice`,
		},
		{
			name:     "a table without columns",
			document: "external_tables:\n  events:\n    data_source: s3\n    location: e/\n",
			wantErr:  `external table "events": invalid columns: an external table needs a column`,
		},
		{
			name:     "a column without a type",
			document: "external_tables:\n  events:\n    columns:\n      - {name: id}\n",
			wantErr:  `external table "events": invalid columns: column id has no type`,
		},
		{
			name:     "a path in a name",
			document: "external_tables:\n  events:\n    name: ext/events\n    columns:\n      - {name: id, type: Int64}\n",
			wantErr:  `external table "events": invalid name: "ext/events" holds a slash; name the directory with schema`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			db, err := yamlschema.Parse([]byte(tc.document))
			c.Assert(err, qt.ErrorMatches, tc.wantErr)
			c.Assert(db, qt.IsNil)
		})
	}
}
