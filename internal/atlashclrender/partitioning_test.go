package atlashclrender_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/atlashclrender"
)

// HCL has no spelling for a YDB table's settings, so the render names every
// table whose settings it leaves out, with the settings: a cleanup of the Go
// annotations the document was written from refuses rather than loses them.
func TestRender_TablePartitioningIsALoss(t *testing.T) {
	c := qt.New(t)
	db := &schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "Events", Name: "events", Schema: "app", PrimaryKey: []string{"id"},
				YDBPartitioning: &ast.YDBTablePartitioningSpec{MinPartitions: 4, KeyBloomFilter: new(true),
					PartitionAtKeys: [][]string{{"10"}, {"20"}}}},
			{StructName: "Plain", Name: "plain", Schema: "app", PrimaryKey: []string{"id"}},
		},
		Fields: []schemamodel.Field{
			{StructName: "Events", Name: "id", Type: "Uint64", Primary: true},
			{StructName: "Plain", Name: "id", Type: "Uint64", Primary: true},
		},
	}

	result, err := atlashclrender.RenderInspectedForAtlasCLI(db, platform.YDB, "")

	c.Assert(err, qt.IsNil)
	c.Assert(string(result.Data), qt.Not(qt.Contains), "PARTITION")
	c.Assert(result.Diagnostics, qt.DeepEquals, []atlashclrender.Diagnostic{{
		Severity: atlashclrender.SeverityWarning,
		Path:     "table.app.events",
		Message: "YDB table settings (AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 4, KEY_BLOOM_FILTER = ENABLED, " +
			"PARTITION_AT_KEYS = (10, 20)) are not represented in HCL",
	}})
}
