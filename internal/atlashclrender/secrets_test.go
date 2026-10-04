package atlashclrender_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/atlashclrender"
)

// TestRender_ReportsTheSecretsItLeavesOut names each YDB secret the document
// leaves out as a loss, which is what stops an annotation cleanup from
// deleting the only place it is declared; a database holding none is the
// control.
func TestRender_ReportsTheSecretsItLeavesOut(t *testing.T) {
	tests := []struct {
		name    string
		secrets []schemamodel.Secret
		want    []atlashclrender.Diagnostic
	}{
		{name: "two secrets", secrets: []schemamodel.Secret{
			{Name: "pg_password", ValueEnv: "PTAH_SECRET_PG"}, {Name: "s3", Schema: "ext", ValueEnv: "PTAH_SECRET_S3"},
		}, want: []atlashclrender.Diagnostic{
			{Severity: atlashclrender.SeverityWarning, Path: "secret.pg_password",
				Message: "secret pg_password is not represented in HCL"},
			{Severity: atlashclrender.SeverityWarning, Path: "secret.ext.s3",
				Message: "secret ext.s3 is not represented in HCL"},
		}},
		{name: "none"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db := &schemamodel.Database{
				Tables:  []schemamodel.Table{{Name: "events", StructName: "Events"}},
				Fields:  []schemamodel.Field{{Name: "id", StructName: "Events", Type: "Int64", Primary: true}},
				Secrets: test.secrets,
			}
			result, err := atlashclrender.RenderForDialect(db, platform.YDB)
			c.Assert(err, qt.IsNil)
			c.Assert(result.Diagnostics, qt.DeepEquals, test.want)
			c.Assert(string(result.Data), qt.Not(qt.Contains), "PTAH_SECRET_")
		})
	}
}
