package builtin_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
)

// externalSchema declares a secret, an object storage source and a
// PostgreSQL source that names the secret by its path, an external table
// over the first, and a row table.
func externalSchema() *schemamodel.Database {
	schema := &schemamodel.Database{
		Tables:  []schemamodel.Table{{StructName: "T", Name: "notes", Schema: "app"}},
		Fields:  []schemamodel.Field{{StructName: "T", Name: "id", Type: "BIGINT", Primary: true}},
		Secrets: []schemamodel.Secret{{Name: "pw", Schema: "ext", ValueEnv: "PTAH_SECRET_PW"}},
		ExternalDataSources: []schemamodel.ExternalDataSource{
			{Name: "bucket", Schema: "ext", SourceType: "ObjectStorage", Location: "https://s3.example.test/b/",
				AuthMethod: "NONE"},
			{Name: "pg", Schema: "ext", SourceType: "PostgreSQL", Location: "pg:5432", AuthMethod: "BASIC",
				Options: map[string]string{"LOGIN": "u", "PASSWORD_SECRET_PATH": "ext/pw"}},
		},
		ExternalTables: []schemamodel.ExternalTable{{Name: "events", Schema: "ext", DataSource: "ext/bucket",
			Location: "e/", Columns: []schemamodel.ExternalColumn{{Name: "id", Type: "Int64"}}}},
	}
	schemamodel.Finalize(schema)
	return schema
}

// A declared external object renders after the secret a data source names
// and before the tables, an external table after its source.
func TestRender_External_HappyPath(t *testing.T) {
	c := qt.New(t)
	caps := capability.YDB262().With(capability.ExternalDataSources, true)

	statements, err := builtin.GetOrderedCreateStatementsWithCapabilities(externalSchema(), platform.YDB, caps)

	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.HasLen, 5)
	c.Assert(statements[0], qt.Equals, "CREATE SECRET `ext/pw` WITH (value = $PTAH_SECRET_PW);\n")
	c.Assert(statements[1], qt.Matches, "(?s)CREATE EXTERNAL DATA SOURCE `ext/bucket` WITH .*")
	c.Assert(statements[2], qt.Matches, "(?s)CREATE EXTERNAL DATA SOURCE `ext/pg` WITH .*PASSWORD_SECRET_PATH = 'ext/pw'.*")
	c.Assert(statements[3], qt.Matches, "(?s)CREATE EXTERNAL TABLE `ext/events` .*")
	c.Assert(statements[4], qt.Matches, "(?s)CREATE TABLE `app/notes` .*")
}

// Every target without the key refuses a declared external object before
// anything is written, YDB at its default flags among them.
func TestRender_External_RefusedWithoutTheKey(t *testing.T) {
	targets := append(secretlessTargets, struct {
		dialect string
		caps    capability.Capabilities
	}{dialect: platform.YDB, caps: capability.YDB262()})
	for _, test := range targets {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)
			schema := externalSchema()
			schema.Secrets = nil
			schema.ExternalDataSources[1].Options = nil

			statements, err := builtin.GetOrderedCreateStatementsWithCapabilities(schema, test.dialect, test.caps)

			c.Assert(err, qt.ErrorMatches, `external data source ext\.bucket, which requires target capability `+
				`external_data_sources, unavailable on this \w+ target`)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(statements, qt.IsNil)
		})
	}
}

// A declaration the server would refuse is refused before anything is
// written: an external object on a path another declared object holds, and
// an external table over a declared source that is not object storage.
func TestRender_External_FailurePath(t *testing.T) {
	onTablePath := externalSchema()
	onTablePath.ExternalDataSources[0].Schema, onTablePath.ExternalDataSources[0].Name = "app", "notes"
	onTablePath.ExternalTables[0].DataSource = "app/notes"
	onSecretPath := externalSchema()
	onSecretPath.ExternalTables[0].Name = "pw"
	overPostgres := externalSchema()
	overPostgres.ExternalTables[0].DataSource = "ext/pg"
	tests := []struct {
		name   string
		schema *schemamodel.Database
		want   string
	}{
		{name: "a data source on a table's path", schema: onTablePath,
			want: "external data source app.notes has the path of a declared table, and YDB keeps one object at a " +
				"path \\(`unexpected path type`\\)"},
		{name: "an external table on a secret's path", schema: onSecretPath,
			want: "external table ext.pw has the path of a declared secret, .*"},
		{name: "an external table over a PostgreSQL source", schema: overPostgres,
			want: "external table ext.events reads data source ext/pg, a PostgreSQL source; an external table reads " +
				"files, from an ObjectStorage source \\(`Only ObjectStorage source type supported`\\)"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			caps := capability.YDB262().With(capability.ExternalDataSources, true)

			statements, err := builtin.GetOrderedCreateStatementsWithCapabilities(test.schema, platform.YDB, caps)

			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(statements, qt.IsNil)
		})
	}
}
