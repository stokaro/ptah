package mysql_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/core/renderer"
	"github.com/stokaro/ptah/migration/planner/dialects/mysql"
	difftypes "github.com/stokaro/ptah/migration/schemadiff/types"
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

	nodes := planner.GenerateMigrationAST(diff, generated)
	sql, err := renderer.RenderSQL("mysql", nodes...)
	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, "CREATE OR REPLACE VIEW active_users")
	c.Assert(sql, qt.Contains, "DROP TRIGGER IF EXISTS set_updated_at;")
	c.Assert(sql, qt.Contains, "CREATE TRIGGER set_updated_at BEFORE UPDATE ON users FOR EACH ROW SET NEW.updated_at = NOW();")
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

	c.Assert(func() {
		planner.GenerateMigrationAST(diff, generated)
	}, qt.PanicMatches, "materialized views are not supported by MySQL or MariaDB.*")
}
