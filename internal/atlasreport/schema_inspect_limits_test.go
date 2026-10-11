package atlasreport_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/coverage"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/internal/atlasreport"
)

// TestSchemaInspectHCLKeepsAnOwnersLimitInTheHeader writes a read's record of
// a YDB changefeed it did not describe into the header, on the native and the
// Atlas-compatible surface, through the runtime's coverage vocabulary, and
// leaves the replica table the owner consults in process out.
func TestSchemaInspectHCLKeepsAnOwnersLimitInTheHeader(t *testing.T) {
	for _, omit := range []bool{false, true} {
		t.Run(map[bool]string{false: "native", true: "atlas"}[omit], func(t *testing.T) {
			c := qt.New(t)
			db := &schemamodel.Database{
				Tables: []schemamodel.Table{{Name: "t", StructName: "T"}},
				Fields: []schemamodel.Field{{Name: "id", StructName: "T", Type: "Int64", Primary: true}},
				NotDescribed: coverage.Set{}.With(
					coverage.Object{Kind: ydbschema.CoverageChangefeed, Name: "app.t", Reason: coverage.Unsupported, Provenance: coverage.Observed},
					coverage.Object{Kind: ydbschema.CoverageReplicaTable, Name: "replica.t", Reason: coverage.Unsupported, Provenance: coverage.Observed},
				),
			}
			var diagnostics strings.Builder
			report := newInspectReport(c, db, &catalog.Database{}, catalog.ServerInfo{Dialect: "ydb"}, &diagnostics,
				atlasreport.SchemaInspectReportOptions{OmitAtlasRefusedBlocks: omit})

			document, err := report.MarshalHCL()

			c.Assert(err, qt.IsNil)
			c.Assert(document, qt.Contains, `// ptah:not-described changefeed reason=unsupported provenance=observed "app.t"`+"\n")
			c.Assert(document, qt.Not(qt.Contains), "replica_table")
		})
	}
}
