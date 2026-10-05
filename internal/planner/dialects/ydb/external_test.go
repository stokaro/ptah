package ydb_test

import (
	"errors"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/planner/dialects/ydb"
	"ptah.run/migration/schemadiff/difftypes"
)

// The objects the external plans below change: an object storage source, a
// PostgreSQL source whose password a secret holds, and an external table over
// the first.
var (
	plannedBucket = schemamodel.ExternalDataSource{Name: "bucket", Schema: "ext", SourceType: "ObjectStorage",
		Location: "https://s3.example.test/b/", AuthMethod: "NONE"}
	plannedWarehouse = schemamodel.ExternalDataSource{Name: "warehouse", Schema: "ext", SourceType: "PostgreSQL",
		Location: "pg:5432", AuthMethod: "BASIC",
		Options: map[string]string{"DATABASE_NAME": "app", "LOGIN": "reader", "PASSWORD_SECRET_PATH": "ext/pw"}}
	plannedEvents = schemamodel.ExternalTable{Name: "events", Schema: "ext", DataSource: "ext/bucket", Location: "e/",
		Columns: []schemamodel.ExternalColumn{{Name: "id", Type: "Int64", NotNull: true}},
		Options: map[string]string{"FORMAT": "json_each_row"}}
)

// externalPlanCaps is 26.2 with external data sources turned on, and with
// CREATE OR REPLACE of them when replace is set.
func externalPlanCaps(replace bool) capability.Capabilities {
	return capability.YDB262().With(capability.ExternalDataSources, true).With(capability.ExternalObjectReplace, replace)
}

// movedBucket is plannedBucket at another location.
func movedBucket() schemamodel.ExternalDataSource {
	moved := plannedBucket
	moved.Location = "https://s3.example.test/other/"
	return moved
}

// TestGenerateMigrationAST_External_Order pins where external objects go in a
// YDB plan: removed external tables and then removed data sources early,
// before any table is created; created data sources after the secrets they
// name, external tables after their sources, and both before the views, which
// may read an external table.
func TestGenerateMigrationAST_External_Order(t *testing.T) {
	c := qt.New(t)
	stale := schemamodel.ExternalTable{Name: "stale", DataSource: "old", Location: "x/",
		Columns: []schemamodel.ExternalColumn{{Name: "id", Type: "Int64"}}}
	diff := &difftypes.SchemaDiff{
		ExternalTablesRemoved:      difftypes.ExternalTableChanges{stale},
		ExternalDataSourcesRemoved: difftypes.ExternalDataSourceChanges{{Name: "old"}},
		SecretsAdded:               difftypes.SecretChanges{{Name: "pw", Schema: "ext", ValueEnv: "PTAH_SECRET_PW"}},
		ExternalDataSourcesAdded:   difftypes.ExternalDataSourceChanges{plannedBucket, plannedWarehouse},
		ExternalTablesAdded:        difftypes.ExternalTableChanges{plannedEvents},
		DeclaredExternalTables:     []schemamodel.ExternalTable{plannedEvents},
		ViewsAdded:                 difftypes.ViewChanges{{Name: "v", Body: "SELECT id FROM `ext/events`"}},
	}

	got := render(c, externalPlanCaps(false), diff)

	c.Assert(got, qt.Equals, "DROP EXTERNAL TABLE `stale`;\n"+
		"DROP EXTERNAL DATA SOURCE `old`;\n"+
		"CREATE SECRET `ext/pw` WITH (value = $PTAH_SECRET_PW);\n"+
		"CREATE EXTERNAL DATA SOURCE `ext/bucket` WITH (\n    SOURCE_TYPE = 'ObjectStorage',\n"+
		"    LOCATION = 'https://s3.example.test/b/',\n    AUTH_METHOD = 'NONE'\n);\n"+
		"CREATE EXTERNAL DATA SOURCE `ext/warehouse` WITH (\n    SOURCE_TYPE = 'PostgreSQL',\n"+
		"    LOCATION = 'pg:5432',\n    AUTH_METHOD = 'BASIC',\n    DATABASE_NAME = 'app',\n    LOGIN = 'reader',\n"+
		"    PASSWORD_SECRET_PATH = 'ext/pw'\n);\n"+
		"CREATE EXTERNAL TABLE `ext/events` (\n    `id` Int64 NOT NULL\n) WITH (\n    DATA_SOURCE = 'ext/bucket',\n"+
		"    LOCATION = 'e/',\n    FORMAT = 'json_each_row'\n);\n"+
		"CREATE VIEW `v` WITH (security_invoker = TRUE) AS\nSELECT id FROM `ext/events`\n;\n")
}

// TestGenerateMigrationAST_External_Replacement pins how a changed object is
// replaced. With CREATE OR REPLACE the source and the table are each replaced
// in one statement and the table over the source stays. Without it, the
// source is dropped and created again, and so is every declared external
// table over it, which YDB will not keep while its source is dropped. A
// source whose type changes takes its tables along either way.
func TestGenerateMigrationAST_External_Replacement(t *testing.T) {
	retyped := plannedBucket
	retyped.SourceType = "Ydb"
	changedEvents := plannedEvents
	changedEvents.Location = "f/"
	tests := []struct {
		name    string
		replace bool
		diff    *difftypes.SchemaDiff
		want    []string
	}{
		{
			name: "a source moved, replaced in place", replace: true,
			diff: &difftypes.SchemaDiff{
				ExternalDataSourcesChanged: []difftypes.ExternalDataSourceChange{{Declared: movedBucket(), Current: plannedBucket}},
				DeclaredExternalTables:     []schemamodel.ExternalTable{plannedEvents},
			},
			want: []string{"CREATE OR REPLACE EXTERNAL DATA SOURCE `ext/bucket`"},
		},
		{
			name: "a source moved, dropped and created again with its table", replace: false,
			diff: &difftypes.SchemaDiff{
				ExternalDataSourcesChanged: []difftypes.ExternalDataSourceChange{{Declared: movedBucket(), Current: plannedBucket}},
				DeclaredExternalTables:     []schemamodel.ExternalTable{plannedEvents},
			},
			want: []string{"DROP EXTERNAL TABLE `ext/events`;", "DROP EXTERNAL DATA SOURCE `ext/bucket`;",
				"CREATE EXTERNAL DATA SOURCE `ext/bucket`", "CREATE EXTERNAL TABLE `ext/events`"},
		},
		{
			name: "a source given another type takes its table along", replace: true,
			diff: &difftypes.SchemaDiff{
				ExternalDataSourcesChanged: []difftypes.ExternalDataSourceChange{{Declared: retyped, Current: plannedBucket}},
				DeclaredExternalTables:     []schemamodel.ExternalTable{plannedEvents},
			},
			want: []string{"DROP EXTERNAL TABLE `ext/events`;", "CREATE OR REPLACE EXTERNAL DATA SOURCE `ext/bucket`",
				"CREATE EXTERNAL TABLE `ext/events`"},
		},
		{
			name: "a table changed, replaced in place", replace: true,
			diff: &difftypes.SchemaDiff{
				ExternalTablesChanged:  []difftypes.ExternalTableChange{{Declared: changedEvents, Current: plannedEvents}},
				DeclaredExternalTables: []schemamodel.ExternalTable{changedEvents},
			},
			want: []string{"CREATE OR REPLACE EXTERNAL TABLE `ext/events`"},
		},
		{
			name: "a table changed, dropped and created again", replace: false,
			diff: &difftypes.SchemaDiff{
				ExternalTablesChanged:  []difftypes.ExternalTableChange{{Declared: changedEvents, Current: plannedEvents}},
				DeclaredExternalTables: []schemamodel.ExternalTable{changedEvents},
			},
			want: []string{"DROP EXTERNAL TABLE `ext/events`;", "CREATE EXTERNAL TABLE `ext/events`"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got := render(c, externalPlanCaps(test.replace), test.diff)
			c.Assert(statementHeads(got), qt.DeepEquals, test.want)
		})
	}
}

// TestGenerateMigrationAST_External_FailurePath refuses an external object
// change on a line without the key, and a secret path on a line that reads it
// as a name, before any node is returned.
func TestGenerateMigrationAST_External_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		caps    capability.Capabilities
		diff    *difftypes.SchemaDiff
		key     capability.Capability
		wantErr string
	}{
		{name: "a data source with the flag off", caps: capability.YDB262(),
			diff: &difftypes.SchemaDiff{ExternalDataSourcesAdded: difftypes.ExternalDataSourceChanges{plannedBucket}},
			key:  capability.ExternalDataSources,
			wantErr: "external data source ext.bucket, which requires target capability external_data_sources, " +
				"unavailable on this ydb target"},
		{name: "a removal with the flag off", caps: capability.YDB251(),
			diff:    &difftypes.SchemaDiff{ExternalTablesRemoved: difftypes.ExternalTableChanges{plannedEvents}},
			key:     capability.ExternalDataSources,
			wantErr: "DROP EXTERNAL TABLE ext.events, which requires target capability external_data_sources, .*"},
		{name: "a declared table over a source the plan drops", caps: externalPlanCaps(false),
			diff: &difftypes.SchemaDiff{
				ExternalDataSourcesRemoved: difftypes.ExternalDataSourceChanges{plannedBucket},
				DeclaredExternalTables:     []schemamodel.ExternalTable{plannedEvents},
			},
			key: "external table ext.events",
			wantErr: "external table ext.events: it reads data source ext.bucket, which the plan drops; declare the " +
				"data source or move the table to one the plan keeps"},
		{name: "a secret path on 25.1", caps: capability.YDB251().With(capability.ExternalDataSources, true),
			diff: &difftypes.SchemaDiff{ExternalDataSourcesAdded: difftypes.ExternalDataSourceChanges{plannedWarehouse}},
			key:  capability.ExternalDataSourceSecretPaths,
			wantErr: "external data source ext.warehouse option PASSWORD_SECRET_PATH, which requires target capability " +
				"external_data_source_secret_paths, .*"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			nodes, err := ydb.NewWithCapabilities(test.caps).GenerateMigrationAST(test.diff)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			refusal, ok := errors.AsType[*ptaherr.CapabilityError](err)
			c.Assert(ok, qt.IsTrue)
			c.Assert(refusal.Feature, qt.Equals, string(test.key))
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
