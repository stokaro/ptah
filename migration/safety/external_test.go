package safety_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/safety"
	"ptah.run/migration/schemadiff/difftypes"
)

// TestClassify_ExternalObjects judges what a YDB external object statement
// loses: none of them loses data YDB stores, so a drop and a replacement warn
// and a creation is safe.
func TestClassify_ExternalObjects(t *testing.T) {
	tests := []struct {
		name         string
		node         ast.Node
		wantSeverity safety.Severity
		wantReason   string
	}{
		{name: "a data source dropped", node: ast.NewDropExternalDataSource("ext.s3"), wantSeverity: safety.Warning,
			wantReason: "DROP EXTERNAL DATA SOURCE removes what YDB reads another system through; no data YDB stores is lost"},
		{name: "an external table dropped", node: ast.NewDropExternalTable("ext.events"), wantSeverity: safety.Warning,
			wantReason: "DROP EXTERNAL TABLE removes the columns YDB reads files through; the files stay"},
		{name: "a data source replaced", node: &ast.CreateExternalDataSourceNode{Name: "ext.s3", Replace: true},
			wantSeverity: safety.Warning,
			wantReason:   "CREATE OR REPLACE changes what queries reading the external object read"},
		{name: "an external table replaced", node: &ast.CreateExternalTableNode{Name: "ext.events", Replace: true},
			wantSeverity: safety.Warning,
			wantReason:   "CREATE OR REPLACE changes what queries reading the external object read"},
		{name: "a data source created", node: &ast.CreateExternalDataSourceNode{Name: "ext.s3"},
			wantSeverity: safety.Safe, wantReason: "does not remove data or tighten constraints"},
		{name: "an external table created", node: &ast.CreateExternalTableNode{Name: "ext.events"},
			wantSeverity: safety.Safe, wantReason: "does not remove data or tighten constraints"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			assessments := safety.Assess([]ast.Node{test.node})
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
	tests := []struct {
		name string
		diff *difftypes.SchemaDiff
		want safety.Severity
	}{
		{name: "a data source dropped", diff: &difftypes.SchemaDiff{
			ExternalDataSourcesRemoved: difftypes.ExternalDataSourceChanges{{Name: "s3"}}}, want: safety.Warning},
		{name: "a data source replaced", diff: &difftypes.SchemaDiff{ExternalDataSourcesChanged: []difftypes.ExternalDataSourceChange{
			{Declared: schemamodel.ExternalDataSource{Name: "s3"}}}}, want: safety.Warning},
		{name: "an external table dropped", diff: &difftypes.SchemaDiff{
			ExternalTablesRemoved: difftypes.ExternalTableChanges{{Name: "events"}}}, want: safety.Warning},
		{name: "an external table replaced", diff: &difftypes.SchemaDiff{ExternalTablesChanged: []difftypes.ExternalTableChange{
			{Declared: schemamodel.ExternalTable{Name: "events"}}}}, want: safety.Warning},
		{name: "both created", diff: &difftypes.SchemaDiff{
			ExternalDataSourcesAdded: difftypes.ExternalDataSourceChanges{{Name: "s3"}},
			ExternalTablesAdded:      difftypes.ExternalTableChanges{{Name: "events"}}}, want: safety.Safe},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(safety.Highest(safety.ClassifySchemaDiff(test.diff)), qt.Equals, test.want)
		})
	}
}
