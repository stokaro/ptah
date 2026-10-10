package safety_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbast"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbexternal"
	"ptah.run/migration/safety"
	"ptah.run/migration/schemadiff/difftypes"
)

var (
	safetySource = ydbexternal.DataSource{SourceType: "ObjectStorage", AuthMethod: "NONE"}
	safetyTable  = ydbexternal.Table{DataSource: "s3", Location: "e/", Columns: []ydbexternal.Column{{Name: "id", Type: "Int64"}}}
)

// TestClassify_ExternalObjects judges what a YDB external object statement
// loses, through the effect its owner gives it: none of them loses data YDB
// stores, so a drop, a replacement and a recreation warn and a creation is
// safe.
func TestClassify_ExternalObjects(t *testing.T) {
	tests := []struct {
		name         string
		payload      ast.ExtensionPayload
		wantSeverity safety.Severity
		wantReason   string
	}{
		{name: "a data source dropped", payload: &ydbast.ExternalDataSource{Operation: ydbast.ExternalDrop, Schema: "ext", Name: "s3"},
			wantSeverity: safety.Warning,
			wantReason:   "DROP EXTERNAL DATA SOURCE removes what YDB reads another system through; no data YDB stores is lost"},
		{name: "an external table dropped", payload: &ydbast.ExternalTable{Operation: ydbast.ExternalDrop, Schema: "ext", Name: "events"},
			wantSeverity: safety.Warning, wantReason: "DROP EXTERNAL TABLE removes the columns YDB reads files through; the files stay"},
		{name: "a data source replaced", payload: &ydbast.ExternalDataSource{Operation: ydbast.ExternalReplace, Schema: "ext", Name: "s3", Spec: safetySource},
			wantSeverity: safety.Warning, wantReason: "CREATE OR REPLACE changes what queries reading the external object read"},
		{name: "a data source recreated", payload: &ydbast.ExternalDataSource{Operation: ydbast.ExternalRecreate, Schema: "ext", Name: "s3", Spec: safetySource},
			wantSeverity: safety.Warning, wantReason: "CREATE OR REPLACE changes what queries reading the external object read"},
		{name: "an external table replaced", payload: &ydbast.ExternalTable{Operation: ydbast.ExternalReplace, Schema: "ext", Name: "events", Spec: safetyTable},
			wantSeverity: safety.Warning, wantReason: "CREATE OR REPLACE changes what queries reading the external object read"},
		{name: "a data source created", payload: &ydbast.ExternalDataSource{Operation: ydbast.ExternalCreate, Schema: "ext", Name: "s3", Spec: safetySource},
			wantSeverity: safety.Safe, wantReason: "CREATE EXTERNAL DATA SOURCE adds a data source"},
		{name: "an external table created", payload: &ydbast.ExternalTable{Operation: ydbast.ExternalCreate, Schema: "ext", Name: "events", Spec: safetyTable},
			wantSeverity: safety.Safe, wantReason: "CREATE EXTERNAL TABLE adds an external table"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			assessments := safety.Assess([]ast.Node{&ast.ExtensionStatement{Payload: test.payload}})
			c.Assert(assessments, qt.HasLen, 1)
			c.Assert(assessments[0].Severity, qt.Equals, test.wantSeverity)
			c.Assert(assessments[0].Reason, qt.Equals, test.wantReason)
		})
	}
}

// The same statements read as text, as a migration file a person wrote
// carries them, warn the same way.
func TestAssessSQL_ExternalObjects(t *testing.T) {
	tests := []struct {
		statement string
		want      string
	}{
		{statement: "DROP EXTERNAL DATA SOURCE `ext/s3`",
			want: "DROP EXTERNAL DATA SOURCE removes what YDB reads another system through; no data YDB stores is lost"},
		{statement: "drop external table `ext/events`",
			want: "DROP EXTERNAL TABLE removes the columns YDB reads files through; the files stay"},
		{statement: "CREATE OR REPLACE EXTERNAL TABLE `ext/events` (id Int64) WITH (DATA_SOURCE = 's3', LOCATION = 'e/')",
			want: "CREATE OR REPLACE changes what queries reading the external object read"},
	}
	for _, test := range tests {
		t.Run(test.statement, func(t *testing.T) {
			c := qt.New(t)
			got := safety.AssessSQL(test.statement)
			c.Assert(got.Severity, qt.Equals, safety.Warning)
			c.Assert(got.Reason, qt.Equals, test.want)
		})
	}
}

// A diff that drops or replaces an external object warns, and one that creates
// one is safe.
func TestClassifySchemaDiff_ExternalObjects(t *testing.T) {
	observedSource, desiredSource := &ydbexternal.ObservedSource{Spec: safetySource}, &ydbexternal.DesiredSource{Spec: safetySource}
	observedTable, desiredTable := &ydbexternal.ObservedTable{Spec: safetyTable}, &ydbexternal.DesiredTable{Spec: safetyTable}
	source := func(change *ydbdiff.ExternalDataSource) schemaext.ChangeRecord {
		return schemaext.ChangeRecord{Subject: ydbexternal.SourceRef("", "s3"), Value: change}
	}
	table := func(change *ydbdiff.ExternalTable) schemaext.ChangeRecord {
		return schemaext.ChangeRecord{Subject: ydbexternal.TableRef("", "events"), Value: change}
	}
	tests := []struct {
		name    string
		changes []schemaext.ChangeRecord
		want    safety.Severity
	}{
		{name: "a data source dropped", changes: []schemaext.ChangeRecord{source(&ydbdiff.ExternalDataSource{Before: observedSource})}, want: safety.Warning},
		{name: "a data source replaced", changes: []schemaext.ChangeRecord{source(&ydbdiff.ExternalDataSource{Before: observedSource, After: desiredSource})},
			want: safety.Warning},
		{name: "an external table dropped", changes: []schemaext.ChangeRecord{table(&ydbdiff.ExternalTable{Before: observedTable})}, want: safety.Warning},
		{name: "an external table replaced", changes: []schemaext.ChangeRecord{table(&ydbdiff.ExternalTable{Before: observedTable, After: desiredTable})},
			want: safety.Warning},
		{name: "both created", changes: []schemaext.ChangeRecord{source(&ydbdiff.ExternalDataSource{After: desiredSource}),
			table(&ydbdiff.ExternalTable{After: desiredTable})}, want: safety.Safe},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(safety.Highest(safety.ClassifySchemaDiff(&difftypes.SchemaDiff{FeatureChanges: test.changes})), qt.Equals, test.want)
		})
	}
}
