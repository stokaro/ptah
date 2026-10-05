package lint_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/renderer"
	"ptah.run/core/schemamodel"
	ydbplanner "ptah.run/internal/planner/dialects/ydb"
	"ptah.run/migration/schemadiff/difftypes"
)

// A change of a table's or an index's partitioning Ptah plans names every
// setting the change resets, so YD105 and YD118, which report a statement that
// does not, stay quiet over Ptah's own plans. Each row turns splitting on, by
// size or by load, which is what resets the minimum and the size.
func TestYDBRules_LeavePtahsPartitioningPlans(t *testing.T) {
	tests := []struct {
		name    string
		desired *ast.YDBTablePartitioningSpec
		current *ast.YDBTablePartitioningSpec
	}{
		{name: "splitting by load turned on",
			desired: &ast.YDBTablePartitioningSpec{PartitionSizeMB: 100, ByLoad: new(true), MinPartitions: 6},
			current: &ast.YDBTablePartitioningSpec{PartitionSizeMB: 100, MinPartitions: 6}},
		{name: "splitting by size turned back on",
			desired: &ast.YDBTablePartitioningSpec{PartitionSizeMB: 100, MinPartitions: 6},
			current: &ast.YDBTablePartitioningSpec{BySize: new(false), MinPartitions: 6}},
		{name: "splitting by size turned on over no held size",
			desired: &ast.YDBTablePartitioningSpec{BySize: new(true)},
			current: &ast.YDBTablePartitioningSpec{BySize: new(false), MinPartitions: 6}},
		{name: "settings declared back to the defaults",
			desired: &ast.YDBTablePartitioningSpec{BySize: new(true), PartitionSizeMB: 2048, ByLoad: new(false), MinPartitions: 1},
			current: &ast.YDBTablePartitioningSpec{PartitionSizeMB: 100, ByLoad: new(true), MinPartitions: 6}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			caps := capability.YDB262()
			diff := &difftypes.SchemaDiff{
				TablesModified: []difftypes.TableDiff{{
					TableName:             "t",
					YDBPartitioningChange: &difftypes.YDBTablePartitioningChange{Desired: test.desired, Current: test.current},
				}},
				IndexPartitioningChanged: []difftypes.IndexPartitioningChange{{
					TableName: "t", Name: "t_v",
					Partitioning: &ast.IndexPartitioningSpec{PartitionSizeMB: 100, ByLoad: new(true)},
					Previous:     &ast.IndexPartitioningSpec{BySize: new(false)},
				}},
				DeclaredTables: []schemamodel.Table{{Name: "t"}},
			}
			nodes, err := ydbplanner.NewWithCapabilities(caps).GenerateMigrationAST(diff)
			c.Assert(err, qt.IsNil)
			sql, err := renderer.RenderSQLWithCapabilities("ydb", caps, nodes...)
			c.Assert(err, qt.IsNil)
			c.Assert(strings.Count(sql, " SET ("), qt.Equals, 2, qt.Commentf("the plan:\n%s", sql))

			sites := ydbSites(ydbLint(c, map[string]string{"0001_t.up.sql": sql}, ""))

			c.Assert(sites, qt.HasLen, 0)
		})
	}
}
