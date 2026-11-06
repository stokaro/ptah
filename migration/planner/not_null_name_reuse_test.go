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

// renamedNotNullColumn is the diff between a table and one whose column full_name
// was renamed to display_name: RENAME COLUMN leaves the NOT NULL's name as it was,
// so the added column declares the name the removed one holds.
func renamedNotNullColumn() *difftypes.SchemaDiff {
	return &difftypes.SchemaDiff{DeclaredTables: []schemamodel.Table{{Name: "t"}}, TablesModified: []difftypes.TableDiff{{
		TableName: "t",
		ColumnsAdded: []schemamodel.Field{
			{Name: "display_name", Type: "text", NotNullConstraintName: "t_full_name_not_null"},
		},
		ColumnsRemoved: []schemamodel.Field{
			{Name: "full_name", Type: "text", NotNullConstraintName: "t_full_name_not_null"},
		},
	}}}
}

// A plan frees the name for the statement that takes it, and keeps its own order:
// the column leaves last. The catalog shape merges the drop and the addition into
// one statement, which PostgreSQL 18.6 runs with no rename, because it runs a
// statement's drops first. Measured on it: `constraint "t_full_name_not_null" for
// relation "t" already exists` for the first plan without the rename.
func TestGenerateSchemaDiffAST_ANotNullNameStillHeld_HappyPath(t *testing.T) {
	tests := []struct {
		name  string
		shape planext.StatementShape
		want  string
	}{
		{
			name:  "as planned",
			shape: planext.StatementShapePlanned,
			want: `ALTER TABLE "t" RENAME CONSTRAINT "t_full_name_not_null" TO "t_full_name_not_null__ptah_old";` + "\n\n" +
				`-- POSTGRES: ADD COLUMN "display_name" fails if "t" holds any row; the column is NOT NULL and declares no default to fill it with.` + "\n" +
				`ALTER TABLE "t" ADD COLUMN "display_name" text CONSTRAINT "t_full_name_not_null" NOT NULL;` + "\n\n" +
				`ALTER TABLE "t" DROP COLUMN "full_name";` + "\n\n",
		},
		{
			name:  "in the catalog's shape",
			shape: planext.StatementShapeCatalog,
			want: `-- POSTGRES: ADD COLUMN "display_name" fails if "t" holds any row; the column is NOT NULL and declares no default to fill it with.` + "\n" +
				`ALTER TABLE "t" DROP COLUMN "full_name", ADD COLUMN "display_name" text CONSTRAINT "t_full_name_not_null" NOT NULL;` + "\n\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			caps := capability.Postgres18()

			nodes, err := planner.GenerateSchemaDiffASTWithOptions(
				context.Background(), must.Must(builtin.New()), renamedNotNullColumn(), platform.Postgres,
				planext.Options{Capabilities: caps, Statements: planext.StatementStyleExact, Shape: test.shape},
			)

			c.Assert(err, qt.IsNil)
			c.Assert(must.Must(builtin.RenderSQLWithCapabilities(platform.Postgres, caps, nodes...)), qt.Equals, test.want)
		})
	}
}

// A target that keeps no NOT NULL name refuses the declared one, as it does with
// nothing holding the name, rather than printing a rename no such target can run.
func TestGenerateSchemaDiffAST_ANotNullNameOnATargetThatKeepsNone_FailurePath(t *testing.T) {
	c := qt.New(t)
	caps := capability.Postgres17()

	nodes, err := planner.GenerateSchemaDiffASTWithOptions(
		context.Background(), must.Must(builtin.New()), renamedNotNullColumn(), platform.Postgres,
		planext.Options{Capabilities: caps, Statements: planext.StatementStyleExact},
	)
	c.Assert(err, qt.IsNil)

	got, err := builtin.RenderSQLWithCapabilities(platform.Postgres, caps, nodes...)

	c.Assert(err, qt.ErrorMatches, `(?s).*this target does not keep a NOT NULL constraint name \(t_full_name_not_null\).*`)
	c.Assert(got, qt.Equals, "")
}
