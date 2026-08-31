package atlashclrender_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/internal/atlashclrender"
)

// exportMetadataDatabase is one table and one column, with the export-only
// attributes the caller names.
func exportMetadataDatabase(table schemamodel.Table, field schemamodel.Field) *schemamodel.Database {
	table.StructName = "U"
	table.Name = "users"
	field.StructName = "U"
	field.Name = "email_addr"
	field.Type = "TEXT"
	return &schemamodel.Database{
		Schemas: []schemamodel.Schema{{Name: "public"}},
		Tables:  []schemamodel.Table{table},
		Fields:  []schemamodel.Field{field},
	}
}

// TestRender_ExportMetadataIsReportedAsAnExportLoss is what protects an API
// contract from the exporter, and it is the same mechanism the dialect scope
// beside it relies on.
//
// HCL has no spelling for any of these attributes, so the loss is real. It was
// tolerable for a scope only because a scope is reported; this was not reported
// at all. Measured on the baseline: `ptah schema export --to hcl
// --cleanup-go-annotations` wrote HCL without the metadata, deleted the Go
// annotations holding it, said `Cleaned 1 file(s), removed 3 annotation
// line(s)` and exited 0 — after which the published OpenAPI schema had gone
// from `AccountDoc` with a read-only `emailDoc` to `users` with a writable
// `email_addr`, and the intent existed nowhere (stokaro/ptah#2607).
func TestRender_ExportMetadataIsReportedAsAnExportLoss(t *testing.T) {
	tests := []struct {
		name string
		db   *schemamodel.Database
		want []atlashclrender.Diagnostic
	}{
		{
			name: "a table api name",
			db: exportMetadataDatabase(
				schemamodel.Table{APIName: "Account"},
				schemamodel.Field{},
			),
			want: []atlashclrender.Diagnostic{{
				Severity: atlashclrender.SeverityWarning,
				Path:     "table.users",
				Message:  `export metadata api_name="Account" is not represented in HCL`,
			}},
		},
		{
			name: "a column api type",
			db: exportMetadataDatabase(
				schemamodel.Table{},
				schemamodel.Field{APIType: "TEXT"},
			),
			want: []atlashclrender.Diagnostic{{
				Severity: atlashclrender.SeverityWarning,
				Path:     "column.users.email_addr",
				Message:  `export metadata api_type="TEXT" is not represented in HCL`,
			}},
		},
		{
			name: "a column exposure",
			db: exportMetadataDatabase(
				schemamodel.Table{},
				schemamodel.Field{APIExpose: "read"},
			),
			want: []atlashclrender.Diagnostic{{
				Severity: atlashclrender.SeverityWarning,
				Path:     "column.users.email_addr",
				Message:  `export metadata api_expose="read" is not represented in HCL`,
			}},
		},
		{
			name: "a per-target name",
			db: exportMetadataDatabase(
				schemamodel.Table{},
				schemamodel.Field{APINames: schemamodel.TargetNames{GraphQL: "emailNode"}},
			),
			want: []atlashclrender.Diagnostic{{
				Severity: atlashclrender.SeverityWarning,
				Path:     "column.users.email_addr",
				Message:  `export metadata graphql_name="emailNode" is not represented in HCL`,
			}},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			rendered, err := atlashclrender.Render(test.db)

			c.Assert(err, qt.IsNil)
			c.Assert(rendered.Diagnostics, qt.DeepEquals, test.want)
		})
	}
}

// TestRender_ASchemaWithoutExportMetadataReportsNothing is the control.
//
// Without it, a report that fired on every table would satisfy the table above
// and would refuse every cleanup in the product — the feature is only useful
// while a schema that loses nothing still cleans.
func TestRender_ASchemaWithoutExportMetadataReportsNothing(t *testing.T) {
	c := qt.New(t)

	rendered, err := atlashclrender.Render(
		exportMetadataDatabase(schemamodel.Table{}, schemamodel.Field{}))

	c.Assert(err, qt.IsNil)
	c.Assert(rendered.Diagnostics, qt.HasLen, 0)
}
