package ydb_test

import (
	"context"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbexternal"
	"ptah.run/engine/builtin"
	"ptah.run/internal/planner/dialects/ydb"
	"ptah.run/migration/schemadiff/difftypes"
)

// The objects the external plans below change: an object storage source, a
// PostgreSQL source whose password a secret holds, and an external table over
// the first.
var (
	plannedBucket    = ydbexternal.DataSource{SourceType: "ObjectStorage", Location: "https://s3.example.test/b/", AuthMethod: "NONE"}
	plannedWarehouse = ydbexternal.DataSource{SourceType: "PostgreSQL", Location: "pg:5432", AuthMethod: "BASIC",
		Options: map[string]string{"DATABASE_NAME": "app", "LOGIN": "reader", "PASSWORD_SECRET_PATH": "ext/pw"}}
	plannedEvents = ydbexternal.Table{DataSource: "ext/bucket", Location: "e/",
		Columns: []ydbexternal.Column{{Name: "id", Type: "Int64", NotNull: true}}, Options: map[string]string{"FORMAT": "json_each_row"}}
)

// externalPlanCaps is 26.2 with external data sources turned on, and with
// CREATE OR REPLACE of them when replace is set.
func externalPlanCaps(replace bool) capability.Capabilities {
	return capability.YDB262().With(capability.ExternalDataSources, true).With(capability.ExternalObjectReplace, replace)
}

// sourceChange is a change of the data source name in the directory schema
// from before to after; a nil side is the source's absence.
func sourceChange(schema, name string, before, after *ydbexternal.DataSource) schemaext.ChangeRecord {
	change := &ydbdiff.ExternalDataSource{}
	if before != nil {
		change.Before = &ydbexternal.ObservedSource{Spec: *before}
	}
	if after != nil {
		change.After = &ydbexternal.DesiredSource{Spec: *after}
	}
	return schemaext.ChangeRecord{Subject: ydbexternal.SourceRef(schema, name), Value: change}
}

// tableChange is a change of the external table name in the directory schema,
// as sourceChange is of a data source.
func tableChange(schema, name string, before, after *ydbexternal.Table) schemaext.ChangeRecord {
	change := &ydbdiff.ExternalTable{}
	if before != nil {
		change.Before = &ydbexternal.ObservedTable{Spec: *before}
	}
	if after != nil {
		change.After = &ydbexternal.DesiredTable{Spec: *after}
	}
	return schemaext.ChangeRecord{Subject: ydbexternal.TableRef(schema, name), Value: change}
}

// movedBucket is plannedBucket at another location.
func movedBucket() *ydbexternal.DataSource {
	moved := plannedBucket.Clone()
	moved.Location = "https://s3.example.test/other/"
	return &moved
}

// TestGenerateMigrationAST_External_Order pins where the external owner's
// statements go in a YDB plan. Each is early, so it runs as soon as what it
// depends on allows, ahead of the common statements: an external table is
// dropped before the data source it read and created after the one it reads,
// and a data source follows the secret it names, which the secret's owner
// creates. Among the statements their dependencies leave free, the owners'
// steps come in name order. The views, which may read an external table, come
// after all of them.
func TestGenerateMigrationAST_External_Order(t *testing.T) {
	c := qt.New(t)
	stale := ydbexternal.Table{DataSource: "old", Location: "x/", Columns: []ydbexternal.Column{{Name: "id", Type: "Int64"}}}
	diff := &difftypes.SchemaDiff{
		FeatureChanges: []schemaext.ChangeRecord{
			tableChange("", "stale", &stale, nil),
			sourceChange("", "old", &ydbexternal.DataSource{SourceType: "ObjectStorage", AuthMethod: "NONE"}, nil),
			secretCreated("ext", "pw", "PTAH_SECRET_PW"),
			sourceChange("ext", "bucket", nil, &plannedBucket),
			sourceChange("ext", "warehouse", nil, &plannedWarehouse),
			tableChange("ext", "events", nil, &plannedEvents),
		},
		ViewsAdded: difftypes.ViewChanges{{Name: "v", Body: "SELECT id FROM `ext/events`"}},
	}

	got := render(c, externalPlanCaps(false), diff)

	c.Assert(got, qt.Equals, "CREATE EXTERNAL DATA SOURCE `ext/bucket` WITH (\n    SOURCE_TYPE = 'ObjectStorage',\n"+
		"    LOCATION = 'https://s3.example.test/b/',\n    AUTH_METHOD = 'NONE'\n);\n"+
		"DROP EXTERNAL TABLE `stale`;\n"+
		"DROP EXTERNAL DATA SOURCE `old`;\n"+
		"CREATE EXTERNAL TABLE `ext/events` (\n    `id` Int64 NOT NULL\n) WITH (\n    DATA_SOURCE = 'ext/bucket',\n"+
		"    LOCATION = 'e/',\n    FORMAT = 'json_each_row'\n);\n"+
		"CREATE SECRET `ext/pw` WITH (value = $PTAH_SECRET_PW);\n"+
		"CREATE EXTERNAL DATA SOURCE `ext/warehouse` WITH (\n    SOURCE_TYPE = 'PostgreSQL',\n"+
		"    LOCATION = 'pg:5432',\n    AUTH_METHOD = 'BASIC',\n    DATABASE_NAME = 'app',\n    LOGIN = 'reader',\n"+
		"    PASSWORD_SECRET_PATH = 'ext/pw'\n);\n"+
		"CREATE VIEW `v` WITH (security_invoker = TRUE) AS\nSELECT id FROM `ext/events`\n;\n")
}

// TestGenerateMigrationAST_External_TableFollowsASourceThatWaits creates an
// external table after the data source it reads even where the source waits
// for a secret another owner creates: the table's own statement would be
// free to run first.
func TestGenerateMigrationAST_External_TableFollowsASourceThatWaits(t *testing.T) {
	c := qt.New(t)
	keyed := plannedBucket.Clone()
	keyed.AuthMethod = "AWS"
	keyed.Options = map[string]string{"AWS_ACCESS_KEY_ID_SECRET_PATH": "ext/key", "AWS_SECRET_ACCESS_KEY_SECRET_PATH": "ext/key", "AWS_REGION": "x"} // #nosec G101 -- secret paths, not credentials
	diff := &difftypes.SchemaDiff{FeatureChanges: []schemaext.ChangeRecord{
		sourceChange("ext", "bucket", nil, &keyed),
		tableChange("ext", "events", nil, &plannedEvents),
		secretCreated("ext", "key", "PTAH_SECRET_KEY"),
	}}

	got := render(c, externalPlanCaps(false), diff)

	c.Assert(statementHeads(got), qt.DeepEquals, []string{"CREATE SECRET `ext/key` WITH (value = $PTAH_SECRET_KEY);",
		"CREATE EXTERNAL DATA SOURCE `ext/bucket`", "CREATE EXTERNAL TABLE `ext/events`"})
}

// TestGenerateMigrationAST_External_Replacement pins how a changed object is
// replaced. With CREATE OR REPLACE the source and the table are each replaced
// in one statement and the table over the source stays. Without it, the
// source is dropped and created again, and so is every external table over
// it, which the comparison asks for and YDB will not keep while its source is
// dropped. A source whose type changes is replaced in place; a table over it
// would read a source that is not object storage, which the planner refuses.
func TestGenerateMigrationAST_External_Replacement(t *testing.T) {
	retyped := plannedBucket.Clone()
	retyped.SourceType = "Ydb"
	changedEvents := plannedEvents.Clone()
	changedEvents.Location = "f/"
	tests := []struct {
		name    string
		replace bool
		changes []schemaext.ChangeRecord
		want    []string
	}{
		{name: "a source moved, replaced in place", replace: true,
			changes: []schemaext.ChangeRecord{sourceChange("ext", "bucket", &plannedBucket, movedBucket())},
			want:    []string{"CREATE OR REPLACE EXTERNAL DATA SOURCE `ext/bucket`"}},
		{name: "a source moved, dropped and created again with its table", replace: false,
			changes: []schemaext.ChangeRecord{sourceChange("ext", "bucket", &plannedBucket, movedBucket()),
				tableChange("ext", "events", &plannedEvents, &plannedEvents)},
			want: []string{"DROP EXTERNAL TABLE `ext/events`;", "DROP EXTERNAL DATA SOURCE `ext/bucket`;",
				"CREATE EXTERNAL DATA SOURCE `ext/bucket`", "CREATE EXTERNAL TABLE `ext/events`"}},
		{name: "a source given another type, with no table over it", replace: true,
			changes: []schemaext.ChangeRecord{sourceChange("ext", "bucket", &plannedBucket, &retyped)},
			want:    []string{"CREATE OR REPLACE EXTERNAL DATA SOURCE `ext/bucket`"}},
		{name: "a table changed, replaced in place", replace: true,
			changes: []schemaext.ChangeRecord{tableChange("ext", "events", &plannedEvents, &changedEvents)},
			want:    []string{"CREATE OR REPLACE EXTERNAL TABLE `ext/events`"}},
		{name: "a table changed, dropped and created again", replace: false,
			changes: []schemaext.ChangeRecord{tableChange("ext", "events", &plannedEvents, &changedEvents)},
			want:    []string{"DROP EXTERNAL TABLE `ext/events`;", "CREATE EXTERNAL TABLE `ext/events`"}},
		{name: "a table moved off a source the plan drops", replace: true,
			changes: []schemaext.ChangeRecord{sourceChange("ext", "bucket", &plannedBucket, nil),
				sourceChange("ext", "other", nil, &plannedBucket),
				tableChange("ext", "events", &plannedEvents, &ydbexternal.Table{DataSource: "ext/other", Location: "e/",
					Columns: plannedEvents.Columns})},
			want: []string{"CREATE EXTERNAL DATA SOURCE `ext/other`", "DROP EXTERNAL TABLE `ext/events`;",
				"DROP EXTERNAL DATA SOURCE `ext/bucket`;", "CREATE EXTERNAL TABLE `ext/events`"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got := render(c, externalPlanCaps(test.replace), &difftypes.SchemaDiff{FeatureChanges: test.changes})
			c.Assert(statementHeads(got), qt.DeepEquals, test.want)
		})
	}
}

// TestGenerateMigrationAST_External_FailurePath refuses, before any node is
// returned, an external object change on a line without the key, a secret
// path on a line that reads it as a name, and a table over a source that is
// not object storage.
func TestGenerateMigrationAST_External_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		caps    capability.Capabilities
		changes []schemaext.ChangeRecord
		wantErr string
		wantIs  error
	}{
		{name: "a data source with the flag off", caps: capability.YDB262(),
			changes: []schemaext.ChangeRecord{sourceChange("ext", "bucket", nil, &plannedBucket)}, wantIs: ptaherr.ErrUnsupportedFeature,
			wantErr: ".*external data source ext/bucket, which requires target capability external_data_sources, " +
				"unavailable on this ydb target.*"},
		{name: "a removal with the flag off", caps: capability.YDB251(),
			changes: []schemaext.ChangeRecord{tableChange("ext", "events", &plannedEvents, nil)}, wantIs: ptaherr.ErrUnsupportedFeature,
			wantErr: ".*DROP EXTERNAL TABLE ext/events, which requires target capability external_data_sources, .*"},
		{name: "a table over a source given another type", caps: externalPlanCaps(true),
			changes: []schemaext.ChangeRecord{sourceChange("ext", "bucket", &plannedBucket, &ydbexternal.DataSource{SourceType: "Ydb", AuthMethod: "NONE"}),
				tableChange("ext", "events", &plannedEvents, &plannedEvents)},
			wantErr: ".*external table ext/events reads data source ext/bucket, a Ydb source; an external table reads files, from an ObjectStorage source .*",
			wantIs:  ptaherr.ErrInvalidSchemaDiff},
		{name: "a secret path on 25.1", caps: capability.YDB251().With(capability.ExternalDataSources, true),
			changes: []schemaext.ChangeRecord{sourceChange("ext", "warehouse", nil, &plannedWarehouse)}, wantIs: ptaherr.ErrUnsupportedFeature,
			wantErr: ".*external data source ext/warehouse option PASSWORD_SECRET_PATH, which requires target capability " +
				"external_data_source_secret_paths, .*"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			nodes, err := ydb.NewWithCapabilities(test.caps).GenerateMigrationAST(context.Background(), must.Must(builtin.New()),
				&difftypes.SchemaDiff{FeatureChanges: test.changes})
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(err, qt.ErrorIs, test.wantIs)
			c.Assert(nodes, qt.IsNil)
		})
	}
}

// statementHeads is the first line of each CREATE and DROP statement in sql,
// without the parenthesis that opens its body.
func statementHeads(sql string) []string {
	var heads []string
	for line := range strings.SplitSeq(sql, "\n") {
		if !strings.HasPrefix(line, "CREATE ") && !strings.HasPrefix(line, "DROP ") {
			continue
		}
		line = strings.TrimSuffix(line, " WITH (")
		heads = append(heads, strings.TrimSuffix(line, " ("))
	}
	return heads
}
