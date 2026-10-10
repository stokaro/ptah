package builtin_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbexternal"
	"ptah.run/dialect/ydb/ydbsecret"
	"ptah.run/engine/builtin"
)

// externalSchema declares a secret, an object storage source and a
// PostgreSQL source that names the secret by its path, an external table
// over the first, and a row table. Each of sources and tables replaces its
// family when it is not nil.
func externalSchema(sources, tables []schemaext.Object) *schemamodel.Database {
	if sources == nil {
		sources = []schemaext.Object{
			ydbexternal.DesiredSourceObject("ext", "bucket", "", ydbexternal.DataSource{SourceType: "ObjectStorage",
				Location: "https://s3.example.test/b/", AuthMethod: "NONE"}),
			ydbexternal.DesiredSourceObject("ext", "pg", "", ydbexternal.DataSource{SourceType: "PostgreSQL", Location: "pg:5432",
				AuthMethod: "BASIC", Options: map[string]string{"LOGIN": "u", "PASSWORD_SECRET_PATH": "ext/pw"}}),
		}
	}
	if tables == nil {
		tables = []schemaext.Object{eventsTable("ext", "events", "ext/bucket")}
	}
	objects := append([]schemaext.Object{ydbsecret.DesiredObject("ext", "pw", "", "PTAH_SECRET_PW")}, sources...)
	coverage := must.Must(ydbsecret.Coverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil))
	coverage = must.Must(coverage.Combine(must.Must(ydbexternal.SourceCoverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil))))
	coverage = must.Must(coverage.Combine(must.Must(ydbexternal.TableCoverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil))))
	schema := &schemamodel.Database{
		Tables:          []schemamodel.Table{{StructName: "T", Name: "notes", Schema: "app"}},
		Fields:          []schemamodel.Field{{StructName: "T", Name: "id", Type: "BIGINT", Primary: true}},
		FeatureObjects:  must.Must(schemaext.NewObjects(append(objects, tables...)...)),
		FeatureCoverage: coverage,
	}
	schemamodel.Finalize(schema)
	return schema
}

// eventsTable is an external table name in the directory schema over source.
func eventsTable(schema, name, source string) schemaext.Object {
	return ydbexternal.DesiredTableObject(schema, name, "", ydbexternal.Table{DataSource: source, Location: "e/",
		Columns: []ydbexternal.Column{{Name: "id", Type: "Int64"}}})
}

// A declared external object renders through its owner ahead of the common
// statements: a data source follows the secret it names, and an external
// table follows its source. Among the statements their reads leave free, the
// owners' steps come in owner and name order.
func TestRender_External_HappyPath(t *testing.T) {
	c := qt.New(t)
	caps := capability.YDB262().With(capability.ExternalDataSources, true)

	statements, err := builtin.GetOrderedCreateStatementsWithCapabilities(externalSchema(nil, nil), platform.YDB, caps)

	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.HasLen, 5)
	c.Assert(statements[0], qt.Matches, "(?s)CREATE EXTERNAL DATA SOURCE `ext/bucket` WITH .*")
	c.Assert(statements[1], qt.Matches, "(?s)CREATE EXTERNAL TABLE `ext/events` .*DATA_SOURCE = 'ext/bucket'.*")
	c.Assert(statements[2], qt.Equals, "CREATE SECRET `ext/pw` WITH (value = $PTAH_SECRET_PW);\n")
	c.Assert(statements[3], qt.Matches, "(?s)CREATE EXTERNAL DATA SOURCE `ext/pg` WITH .*PASSWORD_SECRET_PATH = 'ext/pw'.*")
	c.Assert(statements[4], qt.Matches, "(?s)CREATE TABLE `app/notes` .*")
}

// Every target without the key refuses a declared external object before
// anything is written: YDB at its default flags by the key, and every other
// target because no owner of feature objects is registered for it.
func TestRender_External_RefusedWithoutTheKey(t *testing.T) {
	type target struct {
		dialect string
		caps    capability.Capabilities
		wantErr string
	}
	tests := []target{{dialect: platform.YDB, caps: capability.YDB262(),
		wantErr: `external data source ext/bucket, which requires target capability external_data_sources, unavailable on this ydb target`}}
	for _, other := range secretlessTargets {
		tests = append(tests, target{dialect: other.dialect, caps: other.caps.With(capability.ExternalDataSources, true),
			wantErr: `unsupported feature: feature objects are not registered for target "` + other.dialect + `"`})
	}
	for _, test := range tests {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)
			schema := externalSchema([]schemaext.Object{ydbexternal.DesiredSourceObject("ext", "bucket", "", ydbexternal.DataSource{
				SourceType: "ObjectStorage", AuthMethod: "NONE"})}, make([]schemaext.Object, 0))
			schema.FeatureObjects = schema.FeatureObjects.Without(ydbsecret.Ref("ext", "pw"))

			statements, err := builtin.GetOrderedCreateStatementsWithCapabilities(schema, test.dialect, test.caps)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(statements, qt.IsNil)
		})
	}
}

// A declaration the server would refuse is refused before anything is
// written: an external object on a path a declared table, another external
// object or a secret holds, and an external table over a declared source that
// is not object storage.
func TestRender_External_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		schema  *schemamodel.Database
		wantErr string
		wantIs  error
	}{
		{name: "a data source on a table's path",
			schema: externalSchema([]schemaext.Object{ydbexternal.DesiredSourceObject("app", "notes", "", ydbexternal.DataSource{
				SourceType: "ObjectStorage", AuthMethod: "NONE"})}, make([]schemaext.Object, 0)),
			wantErr: `.*external data source create conflicts with create at scheme path ptah\.run/ydb/scheme-path app\.notes`,
			wantIs:  ptaherr.ErrInvalidSchemaDiff},
		{name: "an external table on a data source's path",
			schema:  externalSchema(nil, []schemaext.Object{eventsTable("ext", "bucket", "ext/bucket")}),
			wantErr: ".*external table ext/bucket has the path of external data source ext/bucket, and YDB keeps one object at a path \\(`unexpected path type`\\)",
			wantIs:  ptaherr.ErrInvalidSchemaDiff},
		{name: "an external table over a PostgreSQL source",
			schema: externalSchema(nil, []schemaext.Object{eventsTable("ext", "events", "ext/pg")}),
			wantErr: ".*external table ext/events reads data source ext/pg, a PostgreSQL source; an external table reads " +
				"files, from an ObjectStorage source \\(`Only ObjectStorage source type supported`\\)",
			wantIs: ptaherr.ErrInvalidSchemaDiff},
		{name: "an external table on a secret's path",
			schema:  externalSchema(nil, []schemaext.Object{eventsTable("ext", "pw", "ext/bucket")}),
			wantErr: `invalid schema diff: conflicting plan effects: unordered effects on ptah\.run/ydb/scheme-path ext\.pw .*`,
			wantIs:  ptaherr.ErrInvalidSchemaDiff},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			caps := capability.YDB262().With(capability.ExternalDataSources, true)

			statements, err := builtin.GetOrderedCreateStatementsWithCapabilities(test.schema, platform.YDB, caps)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(err, qt.ErrorIs, test.wantIs)
			c.Assert(statements, qt.IsNil)
		})
	}
}
