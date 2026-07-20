package postgres_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/core/renderer"
	"github.com/stokaro/ptah/internal/planner/dialects/postgres"
	difftypes "github.com/stokaro/ptah/migration/schemadiff/types"
)

func TestPlanner_GenerateMigrationAST_SchemaObjectsModified(t *testing.T) {
	c := qt.New(t)
	planner := postgres.New()

	generated := &goschema.Database{
		Views: []goschema.View{{
			Name: "active_users",
			Body: "SELECT id FROM users WHERE deleted_at IS NULL",
		}},
		MaterializedViews: []goschema.MaterializedView{{
			Name: "user_stats",
			Body: "SELECT id, COUNT(*) FROM users GROUP BY id",
		}},
		Triggers: []goschema.Trigger{{
			Name:   "set_updated_at",
			Table:  "users",
			Timing: "BEFORE",
			Event:  "UPDATE",
			Body:   "NEW.updated_at = NOW(); RETURN NEW;",
		}},
	}
	diff := &difftypes.SchemaDiff{
		ViewsModified:             []difftypes.ViewDiff{{ViewName: "active_users", Changes: map[string]string{"body": "old -> new"}}},
		MaterializedViewsModified: []difftypes.MaterializedViewDiff{{ViewName: "user_stats", Changes: map[string]string{"body": "old -> new"}}},
		TriggersModified:          []difftypes.TriggerDiff{{TriggerName: "set_updated_at", TableName: "users", Changes: map[string]string{"body": "old -> new"}}},
	}

	nodes := planner.GenerateMigrationAST(diff, generated)
	sql, err := renderer.RenderSQL("postgres", nodes...)
	c.Assert(err, qt.IsNil)
	sql = legacyRenderedSQL(sql)
	c.Assert(sql, qt.Contains, "CREATE OR REPLACE VIEW active_users")
	c.Assert(sql, qt.Contains, "DROP MATERIALIZED VIEW IF EXISTS user_stats CASCADE;")
	c.Assert(sql, qt.Contains, "CREATE MATERIALIZED VIEW user_stats AS")
	c.Assert(sql, qt.Contains, "CREATE OR REPLACE TRIGGER set_updated_at BEFORE UPDATE ON users FOR EACH ROW EXECUTE FUNCTION ptah_trigger_users_set_updated_at();")
}

func TestPlanner_GenerateMigrationAST_DuplicateTriggerNamesUseDistinctFunctions(t *testing.T) {
	c := qt.New(t)
	planner := postgres.New()

	generated := &goschema.Database{
		Triggers: []goschema.Trigger{
			{
				Name:   "set_updated_at",
				Table:  "users",
				Timing: "BEFORE",
				Event:  "UPDATE",
				Body:   "NEW.updated_at = NOW(); RETURN NEW;",
			},
			{
				Name:   "set_updated_at",
				Table:  "posts",
				Timing: "BEFORE",
				Event:  "UPDATE",
				Body:   "NEW.updated_at = clock_timestamp(); RETURN NEW;",
			},
		},
	}
	diff := &difftypes.SchemaDiff{
		TriggersAdded: []difftypes.TriggerRef{
			{TriggerName: "set_updated_at", TableName: "users"},
			{TriggerName: "set_updated_at", TableName: "posts"},
		},
	}

	nodes := planner.GenerateMigrationAST(diff, generated)
	sql, err := renderer.RenderSQL("postgres", nodes...)
	c.Assert(err, qt.IsNil)
	sql = legacyRenderedSQL(sql)
	c.Assert(sql, qt.Contains, "CREATE OR REPLACE FUNCTION ptah_trigger_users_set_updated_at()")
	c.Assert(sql, qt.Contains, "CREATE OR REPLACE FUNCTION ptah_trigger_posts_set_updated_at()")
	c.Assert(sql, qt.Contains, "CREATE TRIGGER set_updated_at BEFORE UPDATE ON users FOR EACH ROW EXECUTE FUNCTION ptah_trigger_users_set_updated_at();")
	c.Assert(sql, qt.Contains, "CREATE TRIGGER set_updated_at BEFORE UPDATE ON posts FOR EACH ROW EXECUTE FUNCTION ptah_trigger_posts_set_updated_at();")
}

func TestPlanner_GenerateMigrationAST_MaterializedViewRefreshStrategyDoesNotAutoRefresh(t *testing.T) {
	c := qt.New(t)
	planner := postgres.New()

	generated := &goschema.Database{
		MaterializedViews: []goschema.MaterializedView{{
			Name:            "user_stats",
			Body:            "SELECT id, COUNT(*) FROM users GROUP BY id",
			RefreshStrategy: "concurrently",
		}},
	}
	diff := &difftypes.SchemaDiff{
		MaterializedViewsAdded: []string{"user_stats"},
	}

	nodes := planner.GenerateMigrationAST(diff, generated)
	sql, err := renderer.RenderSQL("postgres", nodes...)
	c.Assert(err, qt.IsNil)
	sql = legacyRenderedSQL(sql)
	c.Assert(sql, qt.Contains, "CREATE MATERIALIZED VIEW user_stats AS")
	c.Assert(sql, qt.Not(qt.Contains), "REFRESH MATERIALIZED VIEW CONCURRENTLY")
}

func TestPlanner_GenerateMigrationAST_OrdersFunctionsByDependencies(t *testing.T) {
	c := qt.New(t)
	planner := postgres.New()

	generated := &goschema.Database{
		Functions: []goschema.Function{
			{
				Name:       "a_child",
				Parameters: "",
				Returns:    "INTEGER",
				Language:   "sql",
				Body:       "SELECT z_parent()",
			},
			{
				Name:       "z_parent",
				Parameters: "",
				Returns:    "INTEGER",
				Language:   "sql",
				Body:       "SELECT 1",
			},
		},
		FunctionDependencies: map[string][]string{
			"a_child": {"z_parent"},
		},
	}
	diff := &difftypes.SchemaDiff{
		FunctionsAdded: []string{"a_child", "z_parent"},
	}

	nodes := planner.GenerateMigrationAST(diff, generated)
	sql, err := renderer.RenderSQL("postgres", nodes...)
	c.Assert(err, qt.IsNil)
	sql = legacyRenderedSQL(sql)

	assertBefore(t, sql, "CREATE OR REPLACE FUNCTION z_parent()", "CREATE OR REPLACE FUNCTION a_child()")
}

func TestPlanner_GenerateMigrationAST_OrdersViewLikeObjectsByDependencies(t *testing.T) {
	c := qt.New(t)
	planner := postgres.New()

	generated := &goschema.Database{
		Views: []goschema.View{{
			Name: "a_report",
			Body: "SELECT id FROM z_base",
		}},
		MaterializedViews: []goschema.MaterializedView{{
			Name: "z_base",
			Body: "SELECT id FROM users",
		}},
	}
	diff := &difftypes.SchemaDiff{
		ViewsAdded:             []string{"a_report"},
		MaterializedViewsAdded: []string{"z_base"},
	}

	nodes := planner.GenerateMigrationAST(diff, generated)
	sql, err := renderer.RenderSQL("postgres", nodes...)
	c.Assert(err, qt.IsNil)
	sql = legacyRenderedSQL(sql)

	assertBefore(t, sql, "CREATE MATERIALIZED VIEW z_base AS", "CREATE VIEW a_report AS")
}

func TestPlanner_GenerateMigrationAST_ModifiesRLSPolicies(t *testing.T) {
	c := qt.New(t)
	planner := postgres.New()

	generated := &goschema.Database{
		RLSPolicies: []goschema.RLSPolicy{{
			Name:            "tenant_isolation",
			Table:           "accounts",
			PolicyFor:       "SELECT",
			ToRoles:         "app_user",
			UsingExpression: "tenant_id = current_setting('app.tenant_id')::uuid",
		}},
	}
	diff := &difftypes.SchemaDiff{
		RLSPoliciesModified: []difftypes.RLSPolicyDiff{{
			PolicyName: "tenant_isolation",
			TableName:  "accounts",
			Changes:    map[string]string{"using_expression": "old -> new"},
		}},
	}

	nodes := planner.GenerateMigrationAST(diff, generated)
	sql, err := renderer.RenderSQL("postgres", nodes...)
	c.Assert(err, qt.IsNil)
	sql = legacyRenderedSQL(sql)

	c.Assert(sql, qt.Contains, "DROP POLICY IF EXISTS tenant_isolation ON accounts;")
	c.Assert(sql, qt.Contains, "CREATE POLICY tenant_isolation ON accounts FOR SELECT TO app_user")
}

func assertBefore(t *testing.T, sql, earlier, later string) {
	t.Helper()
	c := qt.New(t)
	earlierIndex := strings.Index(sql, earlier)
	laterIndex := strings.Index(sql, later)
	c.Assert(earlierIndex, qt.Not(qt.Equals), -1, qt.Commentf("missing %q in:\n%s", earlier, sql))
	c.Assert(laterIndex, qt.Not(qt.Equals), -1, qt.Commentf("missing %q in:\n%s", later, sql))
	c.Assert(earlierIndex < laterIndex, qt.IsTrue, qt.Commentf("expected %q before %q in:\n%s", earlier, later, sql))
}
