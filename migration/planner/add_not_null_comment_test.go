package planner_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/difftypes"
	"ptah.run/core/planext"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/migration/planner"
)

// The comment reaches a plan however its statements are grouped. The catalog's shape
// merges what it can into one ALTER TABLE, and the comment stays ahead of that statement,
// so the statement is the one it was.
func TestGenerateSchemaDiffAST_AddNotNullComment_HappyPath(t *testing.T) {
	const comment = `-- POSTGRES: ADD COLUMN "c" fails if "t" holds any row; the column is NOT NULL and declares no default to fill it with.` + "\n"
	tests := []struct {
		name  string
		shape planext.StatementShape
		want  string
	}{
		{
			name:  "as planned",
			shape: planext.StatementShapePlanned,
			want:  comment + `ALTER TABLE "t" ADD COLUMN "c" text NOT NULL;` + "\n\n" + `ALTER TABLE "t" DROP COLUMN "old";` + "\n\n",
		},
		{
			name:  "in the catalog's shape",
			shape: planext.StatementShapeCatalog,
			want:  comment + `ALTER TABLE "t" DROP COLUMN "old", ADD COLUMN "c" text NOT NULL;` + "\n\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			caps := capability.Postgres18()
			diff := &difftypes.SchemaDiff{DeclaredTables: []schemamodel.Table{{Name: "t"}}, TablesModified: []difftypes.TableDiff{{
				TableName:      "t",
				ColumnsAdded:   []schemamodel.Field{{Name: "c", Type: "text"}},
				ColumnsRemoved: []schemamodel.Field{{Name: "old", Type: "text", Nullable: true}},
			}}}

			nodes, err := planner.GenerateSchemaDiffASTWithOptions(
				context.Background(), must.Must(builtin.New()), diff, platform.Postgres,
				planext.Options{Capabilities: caps, Statements: planext.StatementStyleExact, Shape: test.shape},
			)

			c.Assert(err, qt.IsNil)
			c.Assert(must.Must(builtin.RenderSQLWithCapabilities(platform.Postgres, caps, nodes...)), qt.Equals, test.want)
		})
	}
}
