package generator_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/difftypes"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/migration/generator"
)

// A down migration restores the default collation the table held. The reverse
// builder is a struct literal, so a collation change it leaves out rolls back
// to nothing.
func TestPlanBidirectionalSchemaDiff_TableCollationRollsBack(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{TablesModified: []difftypes.TableDiff{{
		TableName:       "t",
		CollationChange: &difftypes.CollationChange{Current: "utf8mb4_bin", Desired: "utf8mb4_general_ci"},
	}}}
	current := &catalog.Database{Tables: []catalog.Table{{
		Name: "t", Type: "BASE TABLE", Collate: "utf8mb4_bin",
		Columns: []catalog.Column{{Name: "id", DataType: "int", IsNullable: "NO"}},
	}}}
	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "T", Name: "t", Collate: "utf8mb4_general_ci"}},
		Fields: []schemamodel.Field{{StructName: "T", Name: "id", Type: "int", Primary: true}},
	}

	plan, err := generator.PlanBidirectionalSchemaDiff(c.Context(), generator.BidirectionalSchemaPlanOptions{
		Runtime: must.Must(builtin.New()), Diff: diff, DesiredSchema: desired, CurrentSchema: current,
		Dialect: platform.MySQL, Capabilities: capability.MySQL84(),
	})
	c.Assert(err, qt.IsNil)
	forward, err := builtin.RenderSQL(platform.MySQL, plan.Forward.Nodes...)
	c.Assert(err, qt.IsNil)
	reverse, err := builtin.RenderSQL(platform.MySQL, plan.Reverse.Nodes...)
	c.Assert(err, qt.IsNil)

	c.Assert(forward, qt.Contains, "ALTER TABLE `t` COLLATE utf8mb4_general_ci;")
	c.Assert(reverse, qt.Contains, "ALTER TABLE `t` COLLATE utf8mb4_bin;")
	c.Assert(plan.Reverse.Diff.TablesModified[0].CollationChange, qt.DeepEquals,
		&difftypes.CollationChange{Current: "utf8mb4_general_ci", Desired: "utf8mb4_bin"})
	c.Assert(diff.TablesModified[0].CollationChange, qt.DeepEquals,
		&difftypes.CollationChange{Current: "utf8mb4_bin", Desired: "utf8mb4_general_ci"})
}
