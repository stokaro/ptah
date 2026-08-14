package mysql_test

import (
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/ptaherr"
	"go.5x5.cz/ptah/core/renderer"
	"go.5x5.cz/ptah/internal/planner/dialects/mysql"
	migrationplanner "go.5x5.cz/ptah/migration/planner"
	difftypes "go.5x5.cz/ptah/migration/schemadiff/types"
)

func TestPlanner_GenerateMigrationAST_ViewsAndTriggersModified(t *testing.T) {
	c := qt.New(t)
	planner := mysql.New()

	generated := &goschema.Database{
		Views: []goschema.View{{
			Name: "active_users",
			Body: "SELECT id FROM users WHERE deleted_at IS NULL",
		}},
		Triggers: []goschema.Trigger{{
			Name:   "set_updated_at",
			Table:  "users",
			Timing: "BEFORE",
			Event:  "UPDATE",
			Body:   "SET NEW.updated_at = NOW()",
		}},
	}
	diff := &difftypes.SchemaDiff{
		ViewsModified:    []difftypes.ViewDiff{{ViewName: "active_users", Changes: map[string]string{"body": "old -> new"}}},
		TriggersModified: []difftypes.TriggerDiff{{TriggerName: "set_updated_at", TableName: "users", Changes: map[string]string{"body": "old -> new"}}},
	}

	nodes, err := planner.GenerateMigrationASTChecked(diff, generated)
	c.Assert(err, qt.IsNil)
	sql, err := renderer.RenderSQL("mysql", nodes...)
	c.Assert(err, qt.IsNil)
	sql = legacyRenderedSQL(sql)
	c.Assert(sql, qt.Contains, "CREATE OR REPLACE VIEW active_users")
	c.Assert(sql, qt.Contains, "DROP TRIGGER IF EXISTS set_updated_at;")
	c.Assert(sql, qt.Contains, "CREATE TRIGGER set_updated_at BEFORE UPDATE ON users FOR EACH ROW SET NEW.updated_at = NOW();")
}

func TestPlanner_GenerateMigrationASTChecked_RejectsUniqueIncludeColumns(t *testing.T) {
	c := qt.New(t)
	planner := mysql.New()

	diff := &difftypes.SchemaDiff{
		ConstraintsAdded: []string{"users_email_key"},
		ConstraintsAddedWithTables: []difftypes.ConstraintAdditionInfo{{
			Name:           "users_email_key",
			TableName:      "users",
			Type:           "UNIQUE",
			Columns:        []string{"email"},
			IncludeColumns: []string{"updated_at"},
		}},
	}
	generated := &goschema.Database{
		Constraints: []goschema.Constraint{{
			StructName:     "User",
			Name:           "users_email_key",
			Type:           "UNIQUE",
			Table:          "users",
			Columns:        []string{"email"},
			IncludeColumns: []string{"updated_at"},
		}},
	}

	_, err := planner.GenerateMigrationASTChecked(diff, generated)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, "MySQL-family does not support PostgreSQL INCLUDE columns on UNIQUE constraints.*")
}

func TestPlanner_GenerateSchemaDiffSQLStatements_CompoundTriggerBody(t *testing.T) {
	c := qt.New(t)

	generated := &goschema.Database{
		Triggers: []goschema.Trigger{{
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
		}},
	}

	statements, err := migrationplanner.GenerateSchemaDiffSQLStatements(diff, generated, "mysql")
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
		MaterializedViewsAdded: []string{"user_stats"},
	}
	generated := &goschema.Database{
		MaterializedViews: []goschema.MaterializedView{{
			Name: "user_stats",
			Body: "SELECT id, COUNT(*) FROM users GROUP BY id",
		}},
	}

	nodes, err := planner.GenerateMigrationASTChecked(diff, generated)
	c.Assert(nodes, qt.IsNil)
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, "materialized views are not supported by MySQL or MariaDB.*")
}

func TestPlanner_GenerateMigrationAST_RoutesEveryRoleChangeToARefusal(t *testing.T) {
	planner := mysql.New()

	tests := []struct {
		name     string
		diff     *difftypes.SchemaDiff
		wantNode string
		wantOp   string
	}{
		{
			name:     "added role",
			diff:     &difftypes.SchemaDiff{RolesAdded: []string{"app_role"}},
			wantNode: "*ast.CreateRoleNode",
			wantOp:   "CREATE ROLE",
		},
		{
			name: "modified role",
			diff: &difftypes.SchemaDiff{RolesModified: []difftypes.RoleDiff{{
				RoleName: "app_role",
				Changes:  map[string]string{"login": "false -> true"},
			}}},
			wantNode: "*ast.AlterRoleNode",
			wantOp:   "ALTER ROLE",
		},
		{
			name:     "removed role",
			diff:     &difftypes.SchemaDiff{RolesRemoved: []string{"app_role"}},
			wantNode: "*ast.DropRoleNode",
			wantOp:   "DROP ROLE",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			nodes, err := planner.GenerateMigrationASTChecked(test.diff, &goschema.Database{})
			c.Assert(err, qt.IsNil)
			c.Assert(nodes, qt.HasLen, 1)
			c.Check(fmt.Sprintf("%T", nodes[0]), qt.Equals, test.wantNode)

			for _, dialect := range []string{"mysql", "mariadb"} {
				t.Run(dialect, func(t *testing.T) {
					c := qt.New(t)

					sql, err := renderer.RenderSQL(dialect, nodes...)
					c.Check(sql, qt.Equals, "")
					c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
					c.Check(err, qt.ErrorMatches,
						".*"+dialect+": "+test.wantOp+" app_role: Ptah does not read or compare MySQL-family role state.*")
				})
			}
		})
	}
}
