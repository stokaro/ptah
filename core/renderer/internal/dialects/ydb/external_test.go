package ydb_test

import (
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer/internal/dialects/ydb"
)

// externalCaps is a 26.2 server with external data sources turned on, and
// with CREATE OR REPLACE of them too.
func externalCaps() capability.Capabilities {
	return capability.YDB262().With(capability.ExternalDataSources, true).With(capability.ExternalObjectReplace, true)
}

// s3Source is an object storage data source.
func s3Source(replace bool) *ast.CreateExternalDataSourceNode {
	return &ast.CreateExternalDataSourceNode{Name: "ext.events_bucket", SourceType: "ObjectStorage",
		Location: "https://storage.example.test/events/", AuthMethod: "NONE", Replace: replace}
}

// eventsTable is an external table over that source.
func eventsTable(replace bool) *ast.CreateExternalTableNode {
	return &ast.CreateExternalTableNode{Name: "ext.events", DataSource: "ext/events_bucket", Location: "2026/",
		Columns: []ast.ExternalColumn{{Name: "id", Type: "Int64", NotNull: true}, {Name: "amount", Type: "Decimal(22,9)"}},
		Options: map[string]string{"FORMAT": "json_each_row"}, Replace: replace}
}

// TestRender_External_HappyPath pins how each external object is written.
// Each rendering was applied to local-ydb 25.1.4.7 and 26.2.1.14 with
// EnableExternalDataSources on, and the replacements with
// EnableReplaceIfExistsForExternalEntities on too.
func TestRender_External_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		node ast.Node
		want string
	}{
		{name: "a data source", node: s3Source(false),
			want: "CREATE EXTERNAL DATA SOURCE `ext/events_bucket` WITH (\n    SOURCE_TYPE = 'ObjectStorage',\n" +
				"    LOCATION = 'https://storage.example.test/events/',\n    AUTH_METHOD = 'NONE'\n);\n"},
		{name: "a data source replaced", node: s3Source(true),
			want: "CREATE OR REPLACE EXTERNAL DATA SOURCE `ext/events_bucket` WITH (\n    SOURCE_TYPE = 'ObjectStorage',\n" +
				"    LOCATION = 'https://storage.example.test/events/',\n    AUTH_METHOD = 'NONE'\n);\n"},
		{name: "an external table", node: eventsTable(false),
			want: "CREATE EXTERNAL TABLE `ext/events` (\n    `id` Int64 NOT NULL,\n    `amount` Decimal(22,9)\n) WITH (\n" +
				"    DATA_SOURCE = 'ext/events_bucket',\n    LOCATION = '2026/',\n    FORMAT = 'json_each_row'\n);\n"},
		{name: "an external table replaced", node: eventsTable(true),
			want: "CREATE OR REPLACE EXTERNAL TABLE `ext/events` (\n    `id` Int64 NOT NULL,\n    `amount` Decimal(22,9)\n" +
				") WITH (\n    DATA_SOURCE = 'ext/events_bucket',\n    LOCATION = '2026/',\n    FORMAT = 'json_each_row'\n);\n"},
		{name: "a data source dropped", node: ast.NewDropExternalDataSource("ext.events_bucket"),
			want: "DROP EXTERNAL DATA SOURCE `ext/events_bucket`;\n"},
		{name: "an external table dropped", node: ast.NewDropExternalTable("ext.events"),
			want: "DROP EXTERNAL TABLE `ext/events`;\n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := ydb.NewWithCapabilities(externalCaps()).Render(test.node)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// TestRender_External_RefusesByCapability names the key a line lacks: every
// line has external data sources behind a flag, and CREATE OR REPLACE of them
// behind another.
func TestRender_External_RefusesByCapability(t *testing.T) {
	tests := []struct {
		name    string
		caps    capability.Capabilities
		node    ast.Node
		key     capability.Capability
		wantErr string
	}{
		{name: "a data source with the flag off", caps: capability.YDB262(), node: s3Source(false),
			key: capability.ExternalDataSources,
			wantErr: "external data source ext.events_bucket, which requires target capability external_data_sources, " +
				"unavailable on this ydb target"},
		{name: "a table with the flag off", caps: capability.YDB251(), node: eventsTable(false),
			key:     capability.ExternalDataSources,
			wantErr: "external table ext.events, which requires target capability external_data_sources, .*"},
		{name: "a drop with the flag off", caps: capability.YDB262(), node: ast.NewDropExternalTable("ext.events"),
			key:     capability.ExternalDataSources,
			wantErr: "DROP EXTERNAL TABLE ext.events, which requires target capability external_data_sources, .*"},
		{name: "a replacement with the replace flag off",
			caps: capability.YDB262().With(capability.ExternalDataSources, true), node: s3Source(true),
			key: capability.ExternalObjectReplace,
			wantErr: "external data source ext.events_bucket replaced in place, which requires target capability " +
				"external_object_replace, .*"},
		{name: "a secret path on 25.1", caps: capability.YDB251().With(capability.ExternalDataSources, true),
			node: &ast.CreateExternalDataSourceNode{Name: "pg", SourceType: "PostgreSQL", AuthMethod: "BASIC",
				Options: map[string]string{"PASSWORD_SECRET_PATH": "pw"}},
			key:     capability.ExternalDataSourceSecretPaths,
			wantErr: "external data source pg option PASSWORD_SECRET_PATH, which requires target capability .*"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := ydb.NewWithCapabilities(test.caps).Render(test.node)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			refusal, ok := errors.AsType[*ptaherr.CapabilityError](err)
			c.Assert(ok, qt.IsTrue)
			c.Assert(refusal.Feature, qt.Equals, string(test.key))
			c.Assert(got, qt.Equals, "")
		})
	}
}
