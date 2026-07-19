package mysql_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/core/renderer"
	migrationplanner "github.com/stokaro/ptah/migration/planner"
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
	sql = legacyRenderedSQL(sql)
	c.Assert(sql, qt.Contains, "CREATE OR REPLACE VIEW active_users")
	c.Assert(sql, qt.Contains, "DROP TRIGGER IF EXISTS set_updated_at;")
	c.Assert(sql, qt.Contains, "CREATE TRIGGER set_updated_at BEFORE UPDATE ON users FOR EACH ROW SET NEW.updated_at = NOW();")
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
	c.Assert(err, qt.ErrorMatches, "materialized views are not supported by MySQL or MariaDB.*")

	c.Assert(func() {
		_ = planner.GenerateMigrationAST(diff, generated)
	}, qt.Not(qt.PanicMatches), ".*")
}
