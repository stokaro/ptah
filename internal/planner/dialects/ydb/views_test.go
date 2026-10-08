package ydb_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/internal/planner/dialects/ydb"
	"ptah.run/migration/schemadiff/difftypes"
)

// TestGenerateMigrationAST_Views_HappyPath pins where views go in a YDB plan.
// Every view the plan removes or replaces is dropped first, before any table
// is touched, a view before the view it reads; every view it adds or replaces
// is created last, after the tables are created, changed and dropped, a view
// after the view it reads. YDB has no CREATE OR REPLACE VIEW on any line, so
// a changed view is dropped and created again.
//
// YDB forces neither order on a drop -- it records no dependency on a view or
// on the table a view reads -- and checks a new view's query against the
// schema: measured on 25.1.4.7 and 26.2.1.14, a CREATE VIEW over a table that
// does not exist yet answers `Cannot find table`.
func TestGenerateMigrationAST_Views_HappyPath(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{
		TablesAdded: difftypes.TableChanges{{
			Name:   "app.tags",
			Table:  schemamodel.Table{StructName: "T", Name: "tags", Schema: "app"},
			Fields: []schemamodel.Field{{StructName: "T", Name: "id", Type: "BIGINT", Primary: true}},
		}},
		TablesRemoved: difftypes.TableRemovals{{Name: "legacy"}},
		ViewsAdded: difftypes.ViewChanges{
			// Declared before the view it reads, so only the order the plan
			// computes puts it after.
			{Name: "app.tag_count_doubled", Body: "SELECT n * 2 AS n FROM `app/tag_count`"},
			{Name: "app.tag_count", Body: "SELECT COUNT(*) AS n FROM `app/tags`"},
		},
		ViewsRemoved: difftypes.ViewChanges{
			{Name: "legacy_ids", Body: "SELECT id FROM legacy"},
			{Name: "legacy_ids_again", Body: "SELECT id FROM legacy_ids"},
		},
		ViewsModified: []difftypes.ViewDiff{{
			ViewName:     "active",
			Changes:      map[string]string{"body": "SELECT id FROM legacy -> SELECT id FROM `app/tags`"},
			Desired:      schemamodel.View{Name: "active", Body: "SELECT id FROM `app/tags`"},
			PreviousBody: "SELECT id FROM legacy",
		}},
	}

	got := render(c, capability.YDB251(), diff)

	c.Assert(got, qt.Equals, "DROP VIEW `active`;\n"+
		"DROP VIEW `legacy_ids_again`;\n"+
		"DROP VIEW `legacy_ids`;\n"+
		"CREATE TABLE `app/tags` (\n"+
		"    `id` Int64 NOT NULL,\n"+
		"    PRIMARY KEY (`id`)\n"+
		");\n"+
		"DROP TABLE `legacy`;\n"+
		"CREATE VIEW `app/tag_count` WITH (security_invoker = TRUE) AS\n"+
		"SELECT COUNT(*) AS n FROM `app/tags`\n"+
		";\n"+
		"CREATE VIEW `app/tag_count_doubled` WITH (security_invoker = TRUE) AS\n"+
		"SELECT n * 2 AS n FROM `app/tag_count`\n"+
		";\n"+
		"CREATE VIEW `active` WITH (security_invoker = TRUE) AS\n"+
		"SELECT id FROM `app/tags`\n"+
		";\n")
}

// A target with [capability.CreateOrReplaceView] replaces a changed view in
// place: the plan drops nothing and asks the renderer for the replacement.
// No YDB line has the key, which is why this is decided on the plan's nodes
// rather than on rendered text.
func TestGenerateMigrationAST_ReplacesAViewInPlaceWhereTheTargetCan(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{
		ViewsModified: []difftypes.ViewDiff{{
			ViewName:     "active",
			Changes:      map[string]string{"body": "SELECT 1 AS a -> SELECT 2 AS a"},
			Desired:      schemamodel.View{Name: "active", Body: "SELECT 2 AS a"},
			PreviousBody: "SELECT 1 AS a",
		}},
	}

	nodes, err := ydb.NewWithCapabilities(capability.YDB262().With(capability.CreateOrReplaceView, true)).
		GenerateMigrationAST(
			context.Background(), must.Must(builtin.New()),
			diff,
		)

	c.Assert(err, qt.IsNil)
	c.Assert(nodes, qt.DeepEquals, []ast.Node{ast.NewCreateView("active").SetBody("SELECT 2 AS a").SetReplace()})
}
