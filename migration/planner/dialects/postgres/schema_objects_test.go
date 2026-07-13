package postgres_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/core/renderer"
	"github.com/stokaro/ptah/migration/planner/dialects/postgres"
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
	c.Assert(sql, qt.Contains, "CREATE OR REPLACE FUNCTION ptah_trigger_users_set_updated_at()")
	c.Assert(sql, qt.Contains, "CREATE OR REPLACE FUNCTION ptah_trigger_posts_set_updated_at()")
	c.Assert(sql, qt.Contains, "CREATE TRIGGER set_updated_at BEFORE UPDATE ON users FOR EACH ROW EXECUTE FUNCTION ptah_trigger_users_set_updated_at();")
	c.Assert(sql, qt.Contains, "CREATE TRIGGER set_updated_at BEFORE UPDATE ON posts FOR EACH ROW EXECUTE FUNCTION ptah_trigger_posts_set_updated_at();")
}
