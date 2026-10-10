package ydbrender_test

import (
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/dialect/ydb/ydbast"
	"ptah.run/dialect/ydb/ydbexternal"
	"ptah.run/dialect/ydb/ydbrender"
)

func externalRegistry(c *qt.C) renderer.Extensions {
	registry, err := renderer.NewExtensions(ydbrender.ExternalDataSourceHandler(), ydbrender.ExternalTableHandler())
	c.Assert(err, qt.IsNil)
	return registry
}

// externalCaps is a 26.2 server with external data sources turned on, and
// with CREATE OR REPLACE of them too.
func externalCaps() capability.Capabilities {
	return capability.YDB262().With(capability.ExternalDataSources, true).With(capability.ExternalObjectReplace, true)
}

// s3Source is an object storage data source in ext.
func s3Source(operation ydbast.ExternalOperation) *ydbast.ExternalDataSource {
	return &ydbast.ExternalDataSource{Operation: operation, Schema: "ext", Name: "events_bucket", Spec: ydbexternal.DataSource{
		SourceType: "ObjectStorage", Location: "https://storage.example.test/events/", AuthMethod: "NONE"}}
}

// eventsTable is an external table over that source.
func eventsTable(operation ydbast.ExternalOperation) *ydbast.ExternalTable {
	return &ydbast.ExternalTable{Operation: operation, Schema: "ext", Name: "events", Spec: ydbexternal.Table{
		DataSource: "ext/events_bucket", Location: "2026/",
		Columns: []ydbexternal.Column{{Name: "id", Type: "Int64", NotNull: true}, {Name: "amount", Type: "Decimal(22,9)"}},
		Options: map[string]string{"FORMAT": "json_each_row"}}}
}

// TestExternalHandlers_HappyPath pins how each statement on an external object
// is written. Each rendering was applied to local-ydb 25.1.4.7 and 26.2.1.14
// with EnableExternalDataSources on, and the replacements with
// EnableReplaceIfExistsForExternalEntities on too. A dot stays part of the
// name it is written in.
func TestExternalHandlers_HappyPath(t *testing.T) {
	tests := []struct {
		name      string
		operation ast.ExtensionPayload
		want      []string
	}{
		{name: "a data source", operation: s3Source(ydbast.ExternalCreate),
			want: []string{"CREATE EXTERNAL DATA SOURCE `ext/events_bucket` WITH (\n    SOURCE_TYPE = 'ObjectStorage',\n" +
				"    LOCATION = 'https://storage.example.test/events/',\n    AUTH_METHOD = 'NONE'\n);"}},
		{name: "a data source replaced", operation: s3Source(ydbast.ExternalReplace),
			want: []string{"CREATE OR REPLACE EXTERNAL DATA SOURCE `ext/events_bucket` WITH (\n    SOURCE_TYPE = 'ObjectStorage',\n" +
				"    LOCATION = 'https://storage.example.test/events/',\n    AUTH_METHOD = 'NONE'\n);"}},
		{name: "an external table", operation: eventsTable(ydbast.ExternalCreate),
			want: []string{"CREATE EXTERNAL TABLE `ext/events` (\n    `id` Int64 NOT NULL,\n    `amount` Decimal(22,9)\n) WITH (\n" +
				"    DATA_SOURCE = 'ext/events_bucket',\n    LOCATION = '2026/',\n    FORMAT = 'json_each_row'\n);"}},
		{name: "an external table replaced", operation: eventsTable(ydbast.ExternalReplace),
			want: []string{"CREATE OR REPLACE EXTERNAL TABLE `ext/events` (\n    `id` Int64 NOT NULL,\n    `amount` Decimal(22,9)\n" +
				") WITH (\n    DATA_SOURCE = 'ext/events_bucket',\n    LOCATION = '2026/',\n    FORMAT = 'json_each_row'\n);"}},
		{name: "a data source dropped", operation: &ydbast.ExternalDataSource{Operation: ydbast.ExternalDrop, Schema: "ext", Name: "events_bucket"},
			want: []string{"DROP EXTERNAL DATA SOURCE `ext/events_bucket`;"}},
		{name: "an external table at the root with a dot dropped",
			operation: &ydbast.ExternalTable{Operation: ydbast.ExternalDrop, Name: "events.v1"},
			want:      []string{"DROP EXTERNAL TABLE `events.v1`;"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := externalRegistry(c).Render(renderer.ExtensionContext{Target: "ydb", Capabilities: externalCaps()}, ast.StatementExtension, test.operation)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

// TestExternalHandlers_RefusesByCapability names the key a line lacks: every
// line has external data sources behind a flag, CREATE OR REPLACE of them
// behind another, and 25.1 names no secret by its path. A target other than
// YDB has none of them.
func TestExternalHandlers_RefusesByCapability(t *testing.T) {
	tests := []struct {
		name      string
		target    string
		caps      capability.Capabilities
		operation ast.ExtensionPayload
		key       capability.Capability
		wantErr   string
	}{
		{name: "a data source with the flag off", target: "ydb", caps: capability.YDB262(), operation: s3Source(ydbast.ExternalCreate),
			key: capability.ExternalDataSources,
			wantErr: "external data source ext/events_bucket, which requires target capability external_data_sources, " +
				"unavailable on this ydb target"},
		{name: "a table with the flag off", target: "ydb", caps: capability.YDB251(), operation: eventsTable(ydbast.ExternalCreate),
			key: capability.ExternalDataSources, wantErr: "external table ext/events, which requires target capability external_data_sources, .*"},
		{name: "a drop with the flag off", target: "ydb", caps: capability.YDB262(),
			operation: &ydbast.ExternalTable{Operation: ydbast.ExternalDrop, Schema: "ext", Name: "events"},
			key:       capability.ExternalDataSources,
			wantErr:   "DROP EXTERNAL TABLE ext/events, which requires target capability external_data_sources, .*"},
		{name: "a replacement with the replace flag off", target: "ydb",
			caps: capability.YDB262().With(capability.ExternalDataSources, true), operation: s3Source(ydbast.ExternalReplace),
			key: capability.ExternalObjectReplace,
			wantErr: "external data source ext/events_bucket replaced in place, which requires target capability " +
				"external_object_replace, .*"},
		{name: "a table replacement with the replace flag off", target: "ydb",
			caps: capability.YDB262().With(capability.ExternalDataSources, true), operation: eventsTable(ydbast.ExternalReplace),
			key:     capability.ExternalObjectReplace,
			wantErr: "external table ext/events replaced in place, which requires target capability external_object_replace, .*"},
		{name: "a secret path on 25.1", target: "ydb", caps: capability.YDB251().With(capability.ExternalDataSources, true),
			operation: &ydbast.ExternalDataSource{Operation: ydbast.ExternalCreate, Name: "pg", Spec: ydbexternal.DataSource{
				SourceType: "PostgreSQL", AuthMethod: "BASIC", Options: map[string]string{"PASSWORD_SECRET_PATH": "pw"}}},
			key:     capability.ExternalDataSourceSecretPaths,
			wantErr: "external data source pg option PASSWORD_SECRET_PATH, which requires target capability .*"},
		{name: "another target", target: "postgres", caps: externalCaps(), operation: s3Source(ydbast.ExternalCreate),
			key: capability.ExternalDataSources, wantErr: "external data source ext/events_bucket, which requires target capability external_data_sources, .*"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := externalRegistry(c).Render(renderer.ExtensionContext{Target: test.target, Capabilities: test.caps}, ast.StatementExtension, test.operation)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			refusal, ok := errors.AsType[*ptaherr.CapabilityError](err)
			c.Assert(ok, qt.IsTrue)
			c.Assert(refusal.Feature, qt.Equals, string(test.key))
			c.Assert(got, qt.IsNil)
		})
	}
}

// TestExternalHandlers_FailurePath refuses an operation no statement can carry
// before anything is written: a drop that carries a spec, an unknown
// operation, and a creation whose spec is incomplete.
func TestExternalHandlers_FailurePath(t *testing.T) {
	tests := []struct {
		name      string
		operation ast.ExtensionPayload
		wantErr   string
	}{
		{name: "a drop with a spec", operation: &ydbast.ExternalDataSource{Operation: ydbast.ExternalDrop, Name: "s3",
			Spec: ydbexternal.DataSource{SourceType: "ObjectStorage", AuthMethod: "NONE"}},
			wantErr: `.*a drop of .*s3.* carries no spec`},
		{name: "an unknown operation", operation: &ydbast.ExternalTable{Operation: "alter", Name: "events"},
			wantErr: `.*unknown external object operation "alter"`},
		{name: "a table without a location", operation: &ydbast.ExternalTable{Operation: ydbast.ExternalCreate, Name: "events",
			Spec: ydbexternal.Table{DataSource: "s3", Columns: []ydbexternal.Column{{Name: "id", Type: "Int64"}}}},
			wantErr: `.*an external table needs a location`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := externalRegistry(c).Render(renderer.ExtensionContext{Target: "ydb", Capabilities: externalCaps()}, ast.StatementExtension, test.operation)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
			c.Assert(got, qt.IsNil)
		})
	}
}
