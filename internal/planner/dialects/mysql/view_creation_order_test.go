package mysql_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/difftypes"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/planner/dialects/mysql"
)

// TestPlanner_GenerateMigrationAST_CreatesAViewAfterTheViewsItReads holds a
// view to after every view of the plan its body reads, whichever comes first
// in the diff. The servers this planner writes for check a view's query when it
// is created, so the other order stops the plan.
func TestPlanner_GenerateMigrationAST_CreatesAViewAfterTheViewsItReads(t *testing.T) {
	tests := []struct {
		name    string
		diff    *difftypes.SchemaDiff
		earlier string
		later   string
	}{
		{
			name: "an added view reading a view added after it",
			diff: &difftypes.SchemaDiff{ViewsAdded: difftypes.ViewChanges{
				{Name: "a_v", Body: "SELECT n FROM b_v"},
				{Name: "b_v", Body: "SELECT n FROM t"},
			}},
			earlier: "VIEW b_v AS",
			later:   "VIEW a_v AS",
		},
		{
			name: "an added view reading a column a replaced view adds",
			diff: &difftypes.SchemaDiff{
				ViewsAdded: difftypes.ViewChanges{{Name: "a_v", Body: "SELECT m FROM b_v"}},
				ViewsModified: []difftypes.ViewDiff{{
					ViewName: "b_v",
					Changes:  map[string]string{"body": "SELECT n FROM t -> SELECT n, n AS m FROM t"},
					Desired:  schemamodel.View{Name: "b_v", Body: "SELECT n, n AS m FROM t"},
				}},
			},
			earlier: "VIEW b_v AS",
			later:   "VIEW a_v AS",
		},
		{
			name: "a declared dependency the body does not show",
			diff: &difftypes.SchemaDiff{ViewsAdded: difftypes.ViewChanges{
				{Name: "a_v", Body: "SELECT 1 AS n", DependsOn: []string{"b_v"}},
				{Name: "b_v", Body: "SELECT n FROM t"},
			}},
			earlier: "VIEW b_v AS",
			later:   "VIEW a_v AS",
		},
		{
			name: "a name after a double-quoted string with an escaped quote",
			diff: &difftypes.SchemaDiff{ViewsAdded: difftypes.ViewChanges{
				{Name: "a_v", Body: `SELECT "x\"y" AS s, n FROM b_v`},
				{Name: "b_v", Body: "SELECT n FROM t"},
			}},
			earlier: "VIEW b_v AS",
			later:   "VIEW a_v AS",
		},
		{
			// Each alias names the other view, and neither view reads it.
			name: "views whose column aliases name each other keep the diff's order",
			diff: &difftypes.SchemaDiff{ViewsAdded: difftypes.ViewChanges{
				{Name: "a_v", Body: "SELECT n AS b_v FROM t"},
				{Name: "b_v", Body: "SELECT n AS a_v FROM t"},
			}},
			earlier: "VIEW a_v AS",
			later:   "VIEW b_v AS",
		},
		{
			name: "views whose common table expressions name each other keep the diff's order",
			diff: &difftypes.SchemaDiff{ViewsAdded: difftypes.ViewChanges{
				{Name: "a_v", Body: "WITH b_v AS (SELECT n FROM t) SELECT n FROM b_v"},
				{Name: "b_v", Body: "WITH a_v AS (SELECT n FROM t) SELECT n FROM a_v"},
			}},
			earlier: "VIEW a_v AS",
			later:   "VIEW b_v AS",
		},
		{
			// a_v reads b_v, which exists already, so a_v can be created
			// first; b_v then reads the a_v that exists.
			name: "a ring through a replaced view keeps the diff's order",
			diff: &difftypes.SchemaDiff{
				ViewsAdded: difftypes.ViewChanges{{Name: "a_v", Body: "SELECT n FROM b_v"}},
				ViewsModified: []difftypes.ViewDiff{{
					ViewName: "b_v",
					Changes:  map[string]string{"body": "SELECT n FROM t -> SELECT n FROM a_v"},
					Desired:  schemamodel.View{Name: "b_v", Body: "SELECT n FROM a_v"},
				}},
			},
			earlier: "VIEW a_v AS",
			later:   "VIEW b_v AS",
		},
	}
	for _, dialect := range mysqlFamilyDialects {
		for _, test := range tests {
			t.Run(dialect+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)

				sql := renderMySQLFamily(c, dialect, test.diff, &schemamodel.Database{})

				assertContainsBefore(c, sql, test.earlier, test.later)
			})
		}
	}
}

// TestPlanner_GenerateMigrationAST_ViewCycle_FailurePath refuses views the plan
// adds that read one another in a ring, naming them, since no order creates
// them.
func TestPlanner_GenerateMigrationAST_ViewCycle_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		views   difftypes.ViewChanges
		wantErr string
	}{
		{
			name: "two views",
			views: difftypes.ViewChanges{
				{Name: "a_v", Body: "SELECT n FROM b_v"},
				{Name: "b_v", Body: "SELECT n FROM a_v"},
			},
			wantErr: `the views a_v, b_v read one another in a cycle, so none of them can be created first: ` +
				`a_v reads b_v reads a_v`,
		},
		{
			name: "three views behind one that reads them",
			views: difftypes.ViewChanges{
				{Name: "head_v", Body: "SELECT n FROM c_v"},
				{Name: "c_v", Body: "SELECT n FROM d_v"},
				{Name: "d_v", Body: "SELECT n FROM e_v"},
				{Name: "e_v", Body: "SELECT n FROM c_v"},
			},
			wantErr: `the views c_v, d_v, e_v read one another in a cycle, so none of them can be created first: ` +
				`c_v reads d_v reads e_v reads c_v`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			nodes, err := mysql.New().GenerateMigrationAST(c.Context(), builtintest.Runtime(),
				withDeclaredObjects(&difftypes.SchemaDiff{ViewsAdded: test.views}, &schemamodel.Database{}))

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(nodes, qt.IsNil)
		})
	}
}
