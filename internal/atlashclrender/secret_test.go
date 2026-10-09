package atlashclrender_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/platform"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbsecret"
	"ptah.run/dialect/ydb/ydbstreaming"
	"ptah.run/internal/atlashclrender"
)

// TestRender_ReportsTheSecretsItCannotRecord names each secret the document
// leaves out: one the source declares, and one the source leaves unmanaged,
// which an HCL document has no directive for. A source that describes every
// secret and declares none is the control.
func TestRender_ReportsTheSecretsItCannotRecord(t *testing.T) {
	unmanaged := []schemaext.SubjectCoverage{{Kind: ydbsecret.Kind, Subject: ydbsecret.Ref("ext", "pg.pw"),
		Knowledge: schemaext.Knowledge{State: schemaext.Uninspected, Reason: ydbsecret.UnsupportedReason}}}
	tests := []struct {
		name     string
		declared []schemaext.Object
		limits   []schemaext.SubjectCoverage
		want     []atlashclrender.Diagnostic
	}{
		{name: "a declared secret", declared: []schemaext.Object{ydbsecret.DesiredObject("ext", "pg.pw", "", "PTAH_SECRET_PW")},
			want: []atlashclrender.Diagnostic{{Severity: atlashclrender.SeverityWarning,
				Path:    `features["ptah.run/ydb/secret"][""]["ext"][""]["pg.pw"][""]`,
				Message: "feature object ptah.run/ydb/secret ext.pg.pw of kind ptah.run/ydb/secret is not represented in HCL"}}},
		{name: "an unmanaged secret", limits: unmanaged,
			want: []atlashclrender.Diagnostic{{Severity: atlashclrender.SeverityWarning,
				Path: `features["ptah.run/ydb/secret"][""]["ext"][""]["pg.pw"][""]`,
				Message: "feature object ptah.run/ydb/secret ext.pg.pw of kind ptah.run/ydb/secret is not described " +
					"(target capability secrets is unavailable, so Ptah leaves the secret unmanaged), and HCL cannot record that"}}},
		{name: "a declared secret the source also limits", declared: []schemaext.Object{ydbsecret.DesiredObject("ext", "pg.pw", "", "PTAH_SECRET_PW")},
			limits: unmanaged,
			want: []atlashclrender.Diagnostic{{Severity: atlashclrender.SeverityWarning,
				Path:    `features["ptah.run/ydb/secret"][""]["ext"][""]["pg.pw"][""]`,
				Message: "feature object ptah.run/ydb/secret ext.pg.pw of kind ptah.run/ydb/secret is not represented in HCL"}}},
		{name: "no secret"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db := &schemamodel.Database{
				FeatureObjects:  must.Must(schemaext.NewObjects(test.declared...)),
				FeatureCoverage: must.Must(ydbsecret.Coverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, test.limits)),
			}

			result, err := atlashclrender.RenderForDialect(db, platform.YDB)

			c.Assert(err, qt.IsNil)
			c.Assert(result.Diagnostics, qt.DeepEquals, test.want)
		})
	}
}

// TestRender_LeavesOtherKindsLimitsAlone writes a document from a read that
// records a streaming query it could not read, and reports nothing for it: the
// warning about a limit HCL cannot record is the secret owner's.
func TestRender_LeavesOtherKindsLimitsAlone(t *testing.T) {
	c := qt.New(t)
	limits := []schemaext.SubjectCoverage{{Kind: ydbstreaming.Kind, Subject: ydbstreaming.Ref("", "copy"),
		Knowledge: schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "the read could not decode it"}}}
	db := &schemamodel.Database{FeatureCoverage: must.Must(ydbstreaming.Coverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, limits))}

	result, err := atlashclrender.RenderForDialect(db, platform.YDB)

	c.Assert(err, qt.IsNil)
	c.Assert(result.Diagnostics, qt.HasLen, 0)
}
