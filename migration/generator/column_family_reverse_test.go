package generator_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/migration/generator"
	"ptah.run/migration/schemadiff/difftypes"
)

// A rollback moves each column back to the family it left and gives back each
// setting the forward change wrote, in place. A family the forward change
// added and a storage pool it named where the table had none are not stated
// by the earlier state, so they stay, the family emptied of its columns: YQL
// drops no family and removes no pool, and refusing them would refuse the
// whole rollback. The current side reads as the YDB reader gives it, each
// setting at the value the table holds. The forward diff is left as it was.
func TestPlanBidirectionalSchemaDiff_ColumnFamiliesRollBack(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{TablesModified: []difftypes.TableDiff{{
		TableName: "items",
		YDBColumnFamiliesChange: &difftypes.YDBColumnFamiliesChange{
			Desired: []ast.YDBColumnFamilySpec{
				{Name: "cold", Data: "hdd", Compression: "lz4", Columns: []string{"body"}},
				{Name: "default", Data: "ssd", Compression: "lz4"},
				{Name: "warm"},
			},
			Current: []ast.YDBColumnFamilySpec{
				{Name: "default", Compression: "off"},
				{Name: "warm", Compression: "off", Columns: []string{"body"}},
			},
		},
	}}}

	plan, err := generator.PlanBidirectionalSchemaDiff(t.Context(),
		generator.BidirectionalSchemaPlanOptions{Runtime: must.Must(builtin.New()), Diff: diff,
			DesiredSchema: &schemamodel.Database{},
			CurrentSchema: &catalog.Database{},
			Dialect:       platform.YDB,
			Capabilities:  capability.YDB262(),
		})
	c.Assert(err, qt.IsNil)
	forward, err := builtin.RenderSQLWithCapabilities(platform.YDB, capability.YDB262(), plan.Forward.Nodes...)
	c.Assert(err, qt.IsNil)
	reverse, err := builtin.RenderSQLWithCapabilities(platform.YDB, capability.YDB262(), plan.Reverse.Nodes...)

	c.Assert(err, qt.IsNil)
	c.Assert(forward, qt.Equals, "ALTER TABLE `items` ADD FAMILY `cold` (DATA = 'hdd', COMPRESSION = 'lz4'), "+
		"ALTER FAMILY `default` SET DATA 'ssd', ALTER FAMILY `default` SET COMPRESSION 'lz4', "+
		"ALTER COLUMN `body` SET FAMILY `cold`;\n")
	c.Assert(reverse, qt.Equals, "ALTER TABLE `items` ALTER FAMILY `default` SET COMPRESSION 'off', "+
		"ALTER COLUMN `body` SET FAMILY `warm`;\n")
	c.Assert(diff.TablesModified[0].YDBColumnFamiliesChange.Current, qt.DeepEquals, []ast.YDBColumnFamilySpec{
		{Name: "default", Compression: "off"},
		{Name: "warm", Compression: "off", Columns: []string{"body"}},
	}, qt.Commentf("the reversal must not write through to the forward diff"))
}
