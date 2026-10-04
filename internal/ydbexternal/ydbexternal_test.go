package ydbexternal_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/capability"
	"ptah.run/internal/ydbexternal"
)

func TestParseOptions_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		text string
		want map[string]string
	}{
		{name: "nothing", text: "  "},
		{name: "names in any case, values as written", text: "database_name=app; Login = reader ;USE_TLS=true",
			want: map[string]string{"DATABASE_NAME": "app", "LOGIN": "reader", "USE_TLS": "true"}},
		{name: "a value holding an equals sign", text: "FILE_PATTERN=a=b*.csv",
			want: map[string]string{"FILE_PATTERN": "a=b*.csv"}},
		{name: "a trailing separator", text: "FORMAT=json_each_row;", want: map[string]string{"FORMAT": "json_each_row"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := ydbexternal.ParseOptions(tc.text, ydbexternal.DataSourceReserved...)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, tc.want)
		})
	}
}

func TestParseOptions_FailurePath(t *testing.T) {
	tests := []struct {
		name string
		text string
		want string
	}{
		{name: "no equals sign", text: "FORMAT", want: `invalid options: "FORMAT" is not NAME=value`},
		{name: "no name", text: "=x", want: `invalid options: "=x" is not NAME=value`},
		{name: "a name with a space", text: "DATABASE NAME=x", want: `invalid options: "DATABASE NAME" is not an option name: .*`},
		{name: "a reserved name", text: "location=x", want: `invalid options: LOCATION is written with its own attribute, location`},
		{name: "the server's list", text: "REFERENCES=[]", want: `invalid options: REFERENCES lists the external tables over a data source, and the server keeps it`},
		{name: "a name written twice", text: "LOGIN=a;login=b", want: `invalid options: LOGIN is written twice`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := ydbexternal.ParseOptions(tc.text, ydbexternal.DataSourceReserved...)
			c.Assert(err, qt.ErrorMatches, tc.want)
			c.Assert(got, qt.IsNil)
		})
	}
}

func TestParseColumns_HappyPath(t *testing.T) {
	c := qt.New(t)
	got, err := ydbexternal.ParseColumns("id Int64 NOT NULL, `the name` Utf8, amount Decimal(22, 9) null, tags List<Utf8>")
	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.DeepEquals, []ydbexternal.Column{
		{Name: "id", Type: "Int64", NotNull: true},
		{Name: "the name", Type: "Utf8"},
		{Name: "amount", Type: "Decimal(22, 9)"},
		{Name: "tags", Type: "List<Utf8>"},
	})
}

func TestParseColumns_FailurePath(t *testing.T) {
	tests := []struct {
		name string
		text string
		want string
	}{
		{name: "nothing", text: "", want: `invalid columns: an entry is empty`},
		{name: "an empty entry", text: "id Int64,,b Utf8", want: `invalid columns: an entry is empty`},
		{name: "no type", text: "id", want: `invalid columns: column id has no type`},
		{name: "only NOT NULL", text: "id NOT NULL", want: `invalid columns: column id has no type`},
		{name: "a default the server drops", text: "id Int64 DEFAULT 1",
			want: `invalid columns: column id carries DEFAULT, which an external table does not take`},
		{name: "a key", text: "id Int64 PRIMARY KEY", want: `invalid columns: column id carries PRIMARY KEY, .*`},
		{name: "a family", text: "id Int64 FAMILY f", want: `invalid columns: column id carries FAMILY, .*`},
		{name: "a name written twice", text: "id Int64, id Utf8", want: `invalid columns: id is written twice`},
		{name: "an unclosed quote", text: "`id Int64", want: `invalid columns: "` + "`" + `id Int64" opens a quoted name it does not close`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := ydbexternal.ParseColumns(tc.text)
			c.Assert(err, qt.ErrorMatches, tc.want)
			c.Assert(got, qt.IsNil)
		})
	}
}

// postgresSource is a PostgreSQL data source whose password a YDB secret holds.
var postgresSource = ydbexternal.DataSource{
	SourceType: "PostgreSQL",
	Location:   "pg:5432",
	AuthMethod: "BASIC",
	Options:    map[string]string{"DATABASE_NAME": "app", "LOGIN": "reader", "PASSWORD_SECRET_PATH": "ext/pg_password"},
}

// eventsTable is an external table over files in an object storage source.
var eventsTable = ydbexternal.Table{
	DataSource: "ext/s3",
	Location:   "events/",
	Columns:    []ydbexternal.Column{{Name: "id", Type: "Int64", NotNull: true}, {Name: "name", Type: "Utf8"}},
	Options:    map[string]string{"FORMAT": "json_each_row", "PARTITIONED_BY": `["id"]`},
}

func TestCheckDataSource_HappyPath(t *testing.T) {
	c := qt.New(t)
	caps := capability.YDB262().With(capability.ExternalDataSources, true)
	c.Assert(ydbexternal.CheckDataSource("ext.pg", postgresSource, caps), qt.IsNil)
	c.Assert(ydbexternal.CheckTable("ext.events", eventsTable, caps), qt.IsNil)
}

func TestCheckDataSource_FailurePath(t *testing.T) {
	withSources := capability.YDB262().With(capability.ExternalDataSources, true)
	tests := []struct {
		name   string
		source ydbexternal.DataSource
		caps   capability.Capabilities
		want   ydbexternal.Refusal
	}{
		{name: "a line with the flag off", source: postgresSource, caps: capability.YDB262(),
			want: ydbexternal.Refusal{Subject: "external data source ext.pg", Key: capability.ExternalDataSources}},
		{name: "a secret path on a line that reads it as a name", source: postgresSource,
			caps: capability.YDB251().With(capability.ExternalDataSources, true),
			want: ydbexternal.Refusal{Subject: "external data source ext.pg option PASSWORD_SECRET_PATH",
				Key: capability.ExternalDataSourceSecretPaths}},
		{name: "no source type", source: ydbexternal.DataSource{AuthMethod: "NONE"}, caps: withSources,
			want: ydbexternal.Refusal{Subject: "external data source ext.pg",
				Reason: "it needs a source_type, such as ObjectStorage or PostgreSQL"}},
		{name: "no auth method", source: ydbexternal.DataSource{SourceType: "ObjectStorage"}, caps: withSources,
			want: ydbexternal.Refusal{Subject: "external data source ext.pg",
				Reason: "it needs an auth_method, such as NONE or BASIC (`AUTH_METHOD requires key`)"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			got := ydbexternal.CheckDataSource("ext.pg", tc.source, tc.caps)
			c.Assert(got, qt.IsNotNil)
			c.Assert(*got, qt.DeepEquals, tc.want)
		})
	}
}

func TestCheckTable_FailurePath(t *testing.T) {
	withSources := capability.YDB262().With(capability.ExternalDataSources, true)
	noLocation := eventsTable
	noLocation.Location = ""
	noSource := eventsTable
	noSource.DataSource = ""
	noColumns := eventsTable
	noColumns.Columns = nil
	tests := []struct {
		name  string
		table ydbexternal.Table
		caps  capability.Capabilities
		want  ydbexternal.Refusal
	}{
		{name: "a line with the flag off", table: eventsTable, caps: capability.YDB262(),
			want: ydbexternal.Refusal{Subject: "external table ext.events", Key: capability.ExternalDataSources}},
		{name: "no data source", table: noSource, caps: withSources,
			want: ydbexternal.Refusal{Subject: "external table ext.events",
				Reason: "it needs a data_source, the path of the data source it reads"}},
		{name: "no location", table: noLocation, caps: withSources,
			want: ydbexternal.Refusal{Subject: "external table ext.events", Reason: "it needs a location (`LOCATION requires key`)"}},
		{name: "no column", table: noColumns, caps: withSources,
			want: ydbexternal.Refusal{Subject: "external table ext.events", Reason: "it needs a column"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			got := ydbexternal.CheckTable("ext.events", tc.table, tc.caps)
			c.Assert(got, qt.IsNotNil)
			c.Assert(*got, qt.DeepEquals, tc.want)
		})
	}
}

func TestStatements(t *testing.T) {
	tests := []struct {
		name string
		got  string
		want string
	}{
		{name: "a data source", got: ydbexternal.CreateDataSourceStatement("ext.pg", postgresSource, false),
			want: "CREATE EXTERNAL DATA SOURCE `ext/pg` WITH (\n    SOURCE_TYPE = 'PostgreSQL',\n    LOCATION = 'pg:5432',\n" +
				"    AUTH_METHOD = 'BASIC',\n    DATABASE_NAME = 'app',\n    LOGIN = 'reader',\n" +
				"    PASSWORD_SECRET_PATH = 'ext/pg_password'\n);"},
		{name: "a data source replaced, with no location", got: ydbexternal.CreateDataSourceStatement("cluster",
			ydbexternal.DataSource{SourceType: "PostgreSQL", AuthMethod: "MDB_BASIC",
				Options: map[string]string{"MDB_CLUSTER_ID": "c'1"}}, true),
			want: "CREATE OR REPLACE EXTERNAL DATA SOURCE `cluster` WITH (\n    SOURCE_TYPE = 'PostgreSQL',\n" +
				"    AUTH_METHOD = 'MDB_BASIC',\n    MDB_CLUSTER_ID = 'c\\'1'\n);"},
		{name: "an external table", got: ydbexternal.CreateTableStatement("ext.events", eventsTable, false),
			want: "CREATE EXTERNAL TABLE `ext/events` (\n    `id` Int64 NOT NULL,\n    `name` Utf8\n) WITH (\n" +
				"    DATA_SOURCE = 'ext/s3',\n    LOCATION = 'events/',\n    FORMAT = 'json_each_row',\n" +
				"    PARTITIONED_BY = '[\"id\"]'\n);"},
		{name: "an external table replaced", got: ydbexternal.CreateTableStatement("events",
			ydbexternal.Table{DataSource: "s3", Location: "e/", Columns: []ydbexternal.Column{{Name: "id", Type: "Int64"}}}, true),
			want: "CREATE OR REPLACE EXTERNAL TABLE `events` (\n    `id` Int64\n) WITH (\n    DATA_SOURCE = 's3',\n" +
				"    LOCATION = 'e/'\n);"},
		{name: "a data source dropped", got: ydbexternal.DropDataSourceStatement("ext.pg"),
			want: "DROP EXTERNAL DATA SOURCE `ext/pg`;"},
		{name: "an external table dropped", got: ydbexternal.DropTableStatement("ext.events"),
			want: "DROP EXTERNAL TABLE `ext/events`;"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(tc.got, qt.Equals, tc.want)
		})
	}
}

// The server keeps a data source's values as written, its option names in
// upper case and a secret's path as an absolute one, and adds REFERENCES.
func TestSameDataSource(t *testing.T) {
	described := ydbexternal.DataSource{
		SourceType: "PostgreSQL",
		Location:   "pg:5432",
		AuthMethod: "BASIC",
		Options: ydbexternal.DescribedSourceOptions(map[string]string{
			"DATABASE_NAME": "app", "LOGIN": "reader", "PASSWORD_SECRET_PATH": "/local/ext/pg_password",
			"REFERENCES": `["/local/ext/events"]`,
		}, "/local"),
	}
	otherLogin := described
	otherLogin.Options = map[string]string{"DATABASE_NAME": "app", "LOGIN": "writer", "PASSWORD_SECRET_PATH": "ext/pg_password"}
	otherSecret := described
	otherSecret.Options = map[string]string{"DATABASE_NAME": "app", "LOGIN": "reader", "PASSWORD_SECRET_PATH": "ext/other"}
	lessOptions := described
	lessOptions.Options = map[string]string{"DATABASE_NAME": "app", "LOGIN": "reader"}
	otherCase := described
	otherCase.SourceType = "postgresql"
	tests := []struct {
		name    string
		current ydbexternal.DataSource
		want    bool
	}{
		{name: "as described", current: described, want: true},
		{name: "another login", current: otherLogin},
		{name: "another secret", current: otherSecret},
		{name: "an option left out", current: lessOptions},
		{name: "a source type in another case", current: otherCase},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbexternal.SameDataSource(postgresSource, tc.current, "/local"), qt.Equals, tc.want)
		})
	}
}

// The server writes a table's data source as an absolute path and every
// option as a JSON array, and PARTITIONED_BY as the array it was given.
func TestSameTable(t *testing.T) {
	format, err := ydbexternal.DescribedTableOption("FORMAT", `["json_each_row"]`)
	c := qt.New(t)
	c.Assert(err, qt.IsNil)
	partitions, err := ydbexternal.DescribedTableOption("PARTITIONED_BY", `["id"]`)
	c.Assert(err, qt.IsNil)
	described := ydbexternal.Table{
		DataSource: "/local/ext/s3",
		Location:   "events/",
		Columns:    []ydbexternal.Column{{Name: "id", Type: "INT64", NotNull: true}, {Name: "name", Type: "utf8"}},
		Options:    map[string]string{"FORMAT": format, "PARTITIONED_BY": partitions},
	}
	spaced := eventsTable
	spaced.Options = map[string]string{"format": "json_each_row", "PARTITIONED_BY": `[ "id" ]`}
	otherSource := described
	otherSource.DataSource = "/local/ext/other"
	nullable := described
	nullable.Columns = []ydbexternal.Column{{Name: "id", Type: "Int64"}, {Name: "name", Type: "Utf8"}}
	reordered := described
	reordered.Columns = []ydbexternal.Column{{Name: "name", Type: "Utf8"}, {Name: "id", Type: "Int64", NotNull: true}}
	otherFormat := described
	otherFormat.Options = map[string]string{"FORMAT": "csv_with_names", "PARTITIONED_BY": partitions}
	tests := []struct {
		name     string
		declared ydbexternal.Table
		current  ydbexternal.Table
		want     bool
	}{
		{name: "as described", declared: eventsTable, current: described, want: true},
		{name: "options written another way", declared: spaced, current: described, want: true},
		{name: "another data source", declared: eventsTable, current: otherSource},
		{name: "a column that may be null", declared: eventsTable, current: nullable},
		{name: "columns in another order", declared: eventsTable, current: reordered},
		{name: "another format", declared: eventsTable, current: otherFormat},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbexternal.SameTable(tc.declared, tc.current, "/local"), qt.Equals, tc.want)
		})
	}
}

func TestDescribedTableOption_FailurePath(t *testing.T) {
	for _, described := range []string{`json_each_row`, `["a","b"]`, `[]`} {
		t.Run(described, func(t *testing.T) {
			c := qt.New(t)
			got, err := ydbexternal.DescribedTableOption("FORMAT", described)
			c.Assert(err, qt.ErrorMatches, `external table option FORMAT is .*, where the server writes a JSON array of one string`)
			c.Assert(got, qt.Equals, "")
		})
	}
}

func TestRelativePath(t *testing.T) {
	tests := []struct {
		value string
		want  string
	}{
		{value: "/local/ext/s3", want: "ext/s3"},
		{value: "/local/s3", want: "s3"},
		{value: "ext/s3", want: "ext/s3"},
		{value: "/other/ext/s3", want: "/other/ext/s3"},
		{value: "/localx/s3", want: "/localx/s3"},
	}
	for _, tc := range tests {
		t.Run(tc.value, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbexternal.RelativePath(tc.value, "/local"), qt.Equals, tc.want)
		})
	}
}
