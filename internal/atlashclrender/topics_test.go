package atlashclrender_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/platform"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbtopic"
	"ptah.run/internal/atlashclrender"
)

// TestRender_ReportsTheTopicsItCannotRecord names each topic the document
// leaves out, since Atlas HCL has no block for one: a topic the source
// declares, and one a read lists and does not describe, which HCL has no
// directive for. A source that describes every topic and declares none is
// the control.
func TestRender_ReportsTheTopicsItCannotRecord(t *testing.T) {
	unread := []schemaext.SubjectCoverage{{Kind: ydbtopic.Kind, Subject: ydbtopic.Ref("app", "legacy"),
		Knowledge: schemaext.Knowledge{State: schemaext.Uninspected, Reason: ydbtopic.QueueGroupReason}}}
	tests := []struct {
		name     string
		declared []schemaext.Object
		limits   []schemaext.SubjectCoverage
		want     []atlashclrender.Diagnostic
	}{
		{name: "a declared topic", declared: []schemaext.Object{ydbtopic.DesiredObject("app", "events", "", ydbtopic.Spec{})},
			want: []atlashclrender.Diagnostic{{Severity: atlashclrender.SeverityWarning,
				Path:    `features["ptah.run/ydb/topic"][""]["app"][""]["events"][""]`,
				Message: "feature object ptah.run/ydb/topic app.events of kind ptah.run/ydb/topic is not represented in HCL"}}},
		{name: "an unread topic", limits: unread,
			want: []atlashclrender.Diagnostic{{Severity: atlashclrender.SeverityWarning,
				Path: `features["ptah.run/ydb/topic"][""]["app"][""]["legacy"][""]`,
				Message: "feature object ptah.run/ydb/topic app.legacy of kind ptah.run/ydb/topic is not described " +
					"(a persistent queue group, which Ptah does not read), and HCL cannot record that"}}},
		{name: "no topic"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db := &schemamodel.Database{
				FeatureObjects:  must.Must(schemaext.NewObjects(test.declared...)),
				FeatureCoverage: must.Must(ydbtopic.Coverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, test.limits)),
			}

			result, err := atlashclrender.RenderForDialect(db, platform.YDB)

			c.Assert(err, qt.IsNil)
			c.Assert(result.Diagnostics, qt.DeepEquals, test.want)
		})
	}
}
