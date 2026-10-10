package atlashclrender_test

import (
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbexternal"
	"ptah.run/internal/atlashclrender"
)

// TestRender_ReportsTheExternalObjectsItLeavesOut names each YDB external data
// source and external table the document leaves out as a feature object it
// cannot represent, and each one the source records as not described.
func TestRender_ReportsTheExternalObjectsItLeavesOut(t *testing.T) {
	c := qt.New(t)
	source := ydbexternal.SourceRef("ext", "s3")
	table := ydbexternal.TableRef("", "files")
	unread := ydbexternal.TableRef("", "unread")
	coverage := must.Must(ydbexternal.TableCoverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete},
		[]schemaext.SubjectCoverage{{Kind: ydbexternal.TableKind, Subject: unread,
			Knowledge: schemaext.Knowledge{State: schemaext.Uninspected, Reason: "the source leaves this object unmanaged"}}}))
	db := &schemamodel.Database{
		Tables: []schemamodel.Table{{Name: "events", StructName: "Events"}},
		Fields: []schemamodel.Field{{Name: "id", StructName: "Events", Type: "Int64", Primary: true}},
		FeatureObjects: must.Must(schemaext.NewObjects(
			ydbexternal.DesiredSourceObject("ext", "s3", "", ydbexternal.DataSource{SourceType: "ObjectStorage", AuthMethod: "NONE"}),
			ydbexternal.DesiredTableObject("", "files", "", ydbexternal.Table{DataSource: "ext/s3", Location: "f/",
				Columns: []ydbexternal.Column{{Name: "id", Type: "Int64"}}}))),
		FeatureCoverage: coverage,
	}

	result, err := atlashclrender.RenderForDialect(db, platform.YDB)

	c.Assert(err, qt.IsNil)
	c.Assert(result.Diagnostics, qt.DeepEquals, []atlashclrender.Diagnostic{
		{Severity: atlashclrender.SeverityWarning, Path: featurePath(source),
			Message: fmt.Sprintf("feature object %s of kind %s is not represented in HCL", source, source.Kind)},
		{Severity: atlashclrender.SeverityWarning, Path: featurePath(table),
			Message: fmt.Sprintf("feature object %s of kind %s is not represented in HCL", table, table.Kind)},
		{Severity: atlashclrender.SeverityWarning, Path: featurePath(unread),
			Message: fmt.Sprintf("feature object %s of kind %s is not described (the source leaves this object unmanaged), and HCL cannot record that",
				unread, unread.Kind)},
	})
	c.Assert(string(result.Data), qt.Not(qt.Contains), "ObjectStorage")
}

// featurePath is the path a feature object's loss is reported at.
func featurePath(ref objectidentity.ID) string {
	return fmt.Sprintf("features[%q][%q][%q][%q][%q][%q]", ref.Kind, ref.Catalog.Source, ref.Schema.Source, ref.Parent.Source, ref.Name.Source, ref.Signature)
}
