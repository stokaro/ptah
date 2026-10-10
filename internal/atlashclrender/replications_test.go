package atlashclrender_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/platform"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbreplication"
	"ptah.run/internal/atlashclrender"
)

// A document leaves a YDB async replication and a transfer out, since Atlas
// HCL has no block for either, and reports each as a loss. It makes no claim
// about either namespace, so applying it back plans no `DROP ASYNC
// REPLICATION ... CASCADE`.
func TestRenderForDialect_Replications(t *testing.T) {
	c := qt.New(t)
	db := &schemamodel.Database{FeatureObjects: must.Must(schemaext.NewObjects(
		ydbreplication.DesiredReplicationObject("dr", "mirror", "", ydbreplication.ReplicationSpec{
			Connection: ydbreplication.Connection{ConnectionString: "grpc://h:2136/?database=/prod"},
			Items:      []ydbreplication.Item{{Source: "t", Target: "r"}}}),
		ydbreplication.DesiredTransferObject("", "ingest", "", ydbreplication.TransferSpec{Source: "tp", Target: "t",
			Lambda: "($m) -> { return []; }"}),
	))}

	result, err := atlashclrender.RenderForDialect(db, platform.YDB)

	c.Assert(err, qt.IsNil)
	c.Assert(result.Diagnostics, qt.DeepEquals, []atlashclrender.Diagnostic{
		{Severity: atlashclrender.SeverityWarning, Path: `features["ptah.run/ydb/async-replication"][""]["dr"][""]["mirror"][""]`,
			Message: "feature object ptah.run/ydb/async-replication dr.mirror of kind ptah.run/ydb/async-replication is not represented in HCL"},
		{Severity: atlashclrender.SeverityWarning, Path: `features["ptah.run/ydb/transfer"][""][""][""]["ingest"][""]`,
			Message: "feature object ptah.run/ydb/transfer ingest of kind ptah.run/ydb/transfer is not represented in HCL"},
	})
	c.Assert(result.NotDescribed.Objects, qt.HasLen, 0)
}
