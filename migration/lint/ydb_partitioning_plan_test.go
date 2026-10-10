package lint_test

import (
	"context"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/engine/builtin"
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
		desired *ydbschema.TablePartitioning
		current *ydbschema.TablePartitioning
	}{
		{name: "splitting by load turned on",
			desired: &ydbschema.TablePartitioning{PartitionSizeMB: 100, ByLoad: new(true), MinPartitions: 6},
			current: &ydbschema.TablePartitioning{PartitionSizeMB: 100, MinPartitions: 6}},
		{name: "splitting by size turned back on",
			desired: &ydbschema.TablePartitioning{PartitionSizeMB: 100, MinPartitions: 6},
			current: &ydbschema.TablePartitioning{BySize: new(false), MinPartitions: 6}},
		{name: "splitting by size turned on over no held size",
			desired: &ydbschema.TablePartitioning{BySize: new(true)},
			current: &ydbschema.TablePartitioning{BySize: new(false), MinPartitions: 6}},
		{name: "settings declared back to the defaults",
			desired: &ydbschema.TablePartitioning{BySize: new(true), PartitionSizeMB: 2048, ByLoad: new(false), MinPartitions: 1},
			current: &ydbschema.TablePartitioning{PartitionSizeMB: 100, ByLoad: new(true), MinPartitions: 6}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			caps := capability.YDB262()
			diff := &difftypes.SchemaDiff{
				TablesModified: []difftypes.TableDiff{{
					TableName: "t",
					Desired: schemacapture.TableDeclaration{Table: schemamodel.Table{StructName: "T", Name: "t"},
						Fields: []schemamodel.Field{{StructName: "T", Name: "id", Type: "BIGINT", Primary: true}}},
					FeatureChanges: []schemaext.ChangeRecord{{
						Subject: objectidentity.NewBuilder(identifier.ForDialect("ydb")).TableParts("", "t"),
						Value: &ydbdiff.TablePartitioning{
							Before: &ydbschema.ObservedTablePartitioning{TablePartitioning: *test.current},
							After:  &ydbschema.DesiredTablePartitioning{TablePartitioning: *test.desired},
						},
					}, {
						Subject: objectidentity.NewBuilder(identifier.ForDialect("ydb")).IndexParts("", "t", "t_v"),
						Value: &ydbdiff.IndexPartitioning{
							Before: &ydbschema.ObservedIndexPartitioning{IndexPartitioning: ydbschema.IndexPartitioning{BySize: new(false)}},
							After:  &ydbschema.DesiredIndexPartitioning{IndexPartitioning: ydbschema.IndexPartitioning{PartitionSizeMB: 100, ByLoad: new(true)}},
						},
					}},
				}},
				DeclaredTables: []schemamodel.Table{{Name: "t"}},
			}
			nodes, err := ydbplanner.NewWithCapabilities(caps).GenerateMigrationAST(
				context.Background(), must.Must(builtin.New()),
				diff,
			)
			c.Assert(err, qt.IsNil)
			sql, err := builtin.RenderSQLWithCapabilities("ydb", caps, nodes...)
			c.Assert(err, qt.IsNil)
			c.Assert(strings.Count(sql, " SET ("), qt.Equals, 2, qt.Commentf("the plan:\n%s", sql))

			sites := ydbSites(ydbLint(c, map[string]string{"0001_t.up.sql": sql}, ""))

			c.Assert(sites, qt.HasLen, 0)
		})
	}
}
