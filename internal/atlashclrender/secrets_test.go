package atlashclrender_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/platform"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbsecret"
	"ptah.run/internal/atlashclrender"
)

// TestRender_ReportsTheSecretsItLeavesOut names each YDB secret the document
// leaves out as a loss, which is what stops an annotation cleanup from
// deleting the only place it is declared, and writes no variable name; a
// database holding none is the control.
func TestRender_ReportsTheSecretsItLeavesOut(t *testing.T) {
	tests := []struct {
		name    string
		secrets []schemaext.Object
		want    []atlashclrender.Diagnostic
	}{
		{name: "two secrets", secrets: []schemaext.Object{
			ydbsecret.DesiredObject("", "pg_password", "", "PTAH_SECRET_PG"), ydbsecret.DesiredObject("ext", "s3", "", "PTAH_SECRET_S3"),
		}, want: []atlashclrender.Diagnostic{
			{Severity: atlashclrender.SeverityWarning, Path: `features["ptah.run/ydb/secret"][""][""][""]["pg_password"][""]`,
				Message: "feature object ptah.run/ydb/secret pg_password of kind ptah.run/ydb/secret is not represented in HCL"},
			{Severity: atlashclrender.SeverityWarning, Path: `features["ptah.run/ydb/secret"][""]["ext"][""]["s3"][""]`,
				Message: "feature object ptah.run/ydb/secret ext.s3 of kind ptah.run/ydb/secret is not represented in HCL"},
		}},
		{name: "none"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db := &schemamodel.Database{
				Tables:         []schemamodel.Table{{Name: "events", StructName: "Events"}},
				Fields:         []schemamodel.Field{{Name: "id", StructName: "Events", Type: "Int64", Primary: true}},
				FeatureObjects: must.Must(schemaext.NewObjects(test.secrets...)),
			}
			result, err := atlashclrender.RenderForDialect(db, platform.YDB)
			c.Assert(err, qt.IsNil)
			c.Assert(result.Diagnostics, qt.DeepEquals, test.want)
			c.Assert(string(result.Data), qt.Not(qt.Contains), "PTAH_SECRET_")
		})
	}
}
