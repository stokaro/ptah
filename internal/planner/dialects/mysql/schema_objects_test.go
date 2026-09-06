package mysql_test

import (
	"fmt"
	"regexp"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/planner/dialects/mysql"
	migrationplanner "ptah.run/migration/planner"
	"ptah.run/migration/schemadiff/difftypes"
)

func TestPlanner_GenerateMigrationAST_ViewsAndTriggersModified(t *testing.T) {
	c := qt.New(t)
	planner := mysql.New()

	desired := &schemamodel.Database{
		Views: []schemamodel.View{{
			Name: "active_users",
			Body: "SELECT id FROM users WHERE deleted_at IS NULL",
		}},
		Triggers: []schemamodel.Trigger{{
			Name:   "set_updated_at",
			Table:  "users",
			Timing: "BEFORE",
			Event:  "UPDATE",
			Body:   "SET NEW.updated_at = NOW()",
		}},
	}
	diff := &difftypes.SchemaDiff{
		ViewsModified: []difftypes.ViewDiff{{ViewName: "active_users",
			Desired: schemamodel.View{Name: "active_users",
				Body: "SELECT id FROM users WHERE deleted_at IS NULL"}, Changes: map[string]string{"body": "old -> new"}}},
		TriggersModified: []difftypes.TriggerDiff{{
			TriggerName: "set_updated_at", TableName: "users",
			Changes: map[string]string{"body": "old -> new"},
			Desired: desired.Triggers[0],
		}},
	}

	nodes, err := planner.GenerateMigrationAST(withDeclaredObjects(diff, desired))
	c.Assert(err, qt.IsNil)
	sql, err := renderer.RenderSQL("mysql", nodes...)
	c.Assert(err, qt.IsNil)
	sql = legacyRenderedSQL(sql)
	c.Assert(sql, qt.Contains, "CREATE OR REPLACE VIEW active_users")
	c.Assert(sql, qt.Contains, "DROP TRIGGER IF EXISTS set_updated_at;")
	c.Assert(sql, qt.Contains, "CREATE TRIGGER set_updated_at BEFORE UPDATE ON users FOR EACH ROW SET NEW.updated_at = NOW();")
}

// TestPlanner_GenerateMigrationAST_RejectsUniqueIncludeColumns states the
// constraint in the DIFF alone.
//
// It used to state it in both the diff and the declaration, which made either
// half of the refusal sufficient and neither of them measured. The declaration
// is empty here, so this asserts the record-driven half and nothing else
// (stokaro/ptah#2315).
func TestPlanner_GenerateMigrationAST_RejectsUniqueIncludeColumns(t *testing.T) {
	c := qt.New(t)
	planner := mysql.New()

	diff := &difftypes.SchemaDiff{
		ConstraintsAdded: []difftypes.ConstraintAdditionInfo{{
			Name:           "users_email_key",
			TableName:      "users",
			Type:           "UNIQUE",
			Columns:        []string{"email"},
			IncludeColumns: []string{"updated_at"},
		}},
	}

	_, err := planner.GenerateMigrationAST(diff)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, "MySQL-family does not support PostgreSQL INCLUDE columns on UNIQUE constraints.*")
}

// TestPlanner_GenerateMigrationAST_ACoveringUniqueThePlanDoesNotTouch is the
// control that makes the test above measure something.
//
// The declaration holds the same constraint and the diff does not, so a
// refusal here would be about a document rather than about this plan -- and a
// plan that does nothing would refuse to do it. That fact is real and wider
// than one family: `ptah schema render --dialect mysql` prints the constraint
// without its INCLUDE and exits 0, on five dialects. stokaro/ptah#2538 is where
// it is answered, at the point a declaration meets a target.
//
// Without this control, deleting the refusal outright would leave the test
// above passing on a planner that had stopped refusing anything.
func TestPlanner_GenerateMigrationAST_ACoveringUniqueThePlanDoesNotTouch(t *testing.T) {
	c := qt.New(t)

	nodes, err := mysql.New().GenerateMigrationAST(&difftypes.SchemaDiff{})

	c.Assert(err, qt.IsNil)
	c.Assert(nodes, qt.HasLen, 0)
}

func TestPlanner_GenerateSchemaDiffSQLStatements_CompoundTriggerBody(t *testing.T) {
	c := qt.New(t)

	desired := &schemamodel.Database{
		Triggers: []schemamodel.Trigger{{
			Name:   "set_updated_at",
			Table:  "users",
			Timing: "BEFORE",
			Event:  "UPDATE",
			Body:   "BEGIN SET NEW.updated_at = NOW(); SET NEW.name = TRIM(NEW.name); END",
		}},
	}
	diff := &difftypes.SchemaDiff{
		TriggersModified: []difftypes.TriggerDiff{{
			TriggerName: "set_updated_at",
			TableName:   "users",
			Changes:     map[string]string{"body": "old -> new"},
			Desired:     desired.Triggers[0],
		}},
	}

	statements, err := migrationplanner.GenerateSchemaDiffSQLStatements(diff, "mysql")
	c.Assert(err, qt.IsNil)
	for i, statement := range statements {
		statements[i] = legacyRenderedSQL(statement)
	}

	c.Assert(statements, qt.HasLen, 2)
	c.Assert(statements[0], qt.Equals, "DROP TRIGGER IF EXISTS set_updated_at")
	c.Assert(statements[1], qt.Contains, "CREATE TRIGGER set_updated_at BEFORE UPDATE ON users FOR EACH ROW BEGIN")
	c.Assert(statements[1], qt.Contains, "SET NEW.updated_at = NOW();")
	c.Assert(statements[1], qt.Contains, "SET NEW.name = TRIM(NEW.name);")
	c.Assert(statements[1], qt.Contains, "END")
}

func TestPlanner_GenerateMigrationAST_RejectsMaterializedViews(t *testing.T) {
	c := qt.New(t)
	planner := mysql.New()

	diff := &difftypes.SchemaDiff{
		MaterializedViewsAdded: difftypes.MaterializedViewChanges{{Name: "user_stats", Body: "SELECT id, COUNT(*) FROM users GROUP BY id"}},
	}
	desired := &schemamodel.Database{
		MaterializedViews: []schemamodel.MaterializedView{{
			Name: "user_stats",
			Body: "SELECT id, COUNT(*) FROM users GROUP BY id",
		}},
	}

	nodes, err := planner.GenerateMigrationAST(withDeclaredObjects(diff, desired))
	c.Assert(nodes, qt.IsNil)
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, "materialized views are not supported by MySQL or MariaDB.*")
}

// TestPlanner_GenerateMigrationAST_RoutesEveryRoleChangeToItsStatement pins
// what each of the three role categories becomes.
//
// It used to assert a refusal for all three, because nothing read a role back.
// The read half exists now (stokaro/ptah#1762), so an addition and a removal
// render, and only a MODIFICATION still refuses -- a MySQL-family role has no
// attribute to alter, so a change to one is a change to something the object
// does not have.
func TestPlanner_GenerateMigrationAST_RoutesEveryRoleChangeToItsStatement(t *testing.T) {
	planner := mysql.New()

	tests := []struct {
		name      string
		diff      *difftypes.SchemaDiff
		wantNode  string
		wantSQL   string
		wantRefus string
	}{
		{
			name:     "added role",
			diff:     &difftypes.SchemaDiff{RolesAdded: difftypes.RoleChanges{{Name: "app_role"}}},
			wantNode: "*ast.CreateRoleNode",
			wantSQL:  "CREATE ROLE IF NOT EXISTS `app_role`;",
		},
		{
			name: "modified role",
			diff: &difftypes.SchemaDiff{RolesModified: []difftypes.RoleDiff{{
				RoleName: "app_role",
				Changes:  map[string]string{"login": "false -> true"},
			}}},
			wantNode:  "*ast.AlterRoleNode",
			wantRefus: "(?s).*app_role.*an altered attribute.*",
		},
		{
			name:     "removed role",
			diff:     &difftypes.SchemaDiff{RolesRemoved: difftypes.RoleChanges{{Name: "app_role"}}},
			wantNode: "*ast.DropRoleNode",
			wantSQL:  "DROP ROLE IF EXISTS `app_role`;",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			// The addition is planned from the declaration, so the desired
			// schema has to hold what the diff names or the phase contributes
			// nothing (stokaro/ptah#1762).
			nodes, err := planner.GenerateMigrationAST(test.diff)
			c.Assert(err, qt.IsNil)
			c.Assert(nodes, qt.HasLen, 1)
			c.Check(fmt.Sprintf("%T", nodes[0]), qt.Equals, test.wantNode)

			for _, dialect := range []string{"mysql", "mariadb"} {
				t.Run(dialect, func(t *testing.T) {
					c := qt.New(t)
					sql, err := renderer.RenderSQL(dialect, nodes...)
					c.Assert(err == nil, qt.Equals, test.wantRefus == "",
						qt.Commentf("err: %v", err))
					c.Check(renderedOrRefusal(sql, err), qt.Matches,
						expectedRoleAnswer(test.wantSQL, test.wantRefus))
				})
			}
		})
	}
}

// renderedOrRefusal returns whatever the render produced: its SQL, or the
// message it refused with.
func renderedOrRefusal(sql string, err error) string {
	if err != nil {
		return err.Error()
	}
	return strings.TrimSpace(sql)
}

// expectedRoleAnswer turns the row's expectation into one pattern, so the
// assertion above needs no branch of its own.
func expectedRoleAnswer(wantSQL, wantRefusal string) string {
	if wantRefusal != "" {
		return wantRefusal
	}
	return regexp.QuoteMeta(wantSQL)
}
