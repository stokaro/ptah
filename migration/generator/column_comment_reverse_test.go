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

// A down migration restores the prior column comment even when the comment
// is the only change to the column.
func TestPlanBidirectionalSchemaDiff_ColumnCommentRollsBack(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{TablesModified: []difftypes.TableDiff{{
		TableName: "t",
		ColumnsModified: []difftypes.ColumnDiff{{
			ColumnName:    "a",
			CommentChange: &difftypes.CommentChange{Current: "old", Desired: "new"},
		}},
	}}}
	current := &catalog.Database{Tables: []catalog.Table{{
		Name: "t", Type: "BASE TABLE",
		Columns: []catalog.Column{{Name: "a", DataType: "integer", IsNullable: "YES", Comment: "old"}},
	}}}
	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "T", Name: "t"}},
		Fields: []schemamodel.Field{{StructName: "T", Name: "a", Type: "integer", Nullable: true, Comment: "new"}},
	}
	runtime := must.Must(builtin.New())

	plan, err := generator.PlanBidirectionalSchemaDiff(c.Context(), generator.BidirectionalSchemaPlanOptions{
		Runtime: runtime, Diff: diff, DesiredSchema: desired, CurrentSchema: current,
		Dialect: platform.Postgres, Capabilities: capability.Postgres18(),
	})
	c.Assert(err, qt.IsNil)
	forward, err := builtin.RenderSQL(platform.Postgres, plan.Forward.Nodes...)
	c.Assert(err, qt.IsNil)
	reverse, err := builtin.RenderSQL(platform.Postgres, plan.Reverse.Nodes...)
	c.Assert(err, qt.IsNil)

	c.Assert(forward, qt.Contains, `COMMENT ON COLUMN "t"."a" IS 'new';`)
	c.Assert(reverse, qt.Contains, `COMMENT ON COLUMN "t"."a" IS 'old';`)
	c.Assert(diff.TablesModified[0].ColumnsModified[0].CommentChange,
		qt.DeepEquals, &difftypes.CommentChange{Current: "old", Desired: "new"})
}
