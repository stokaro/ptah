package postgres_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/ptaherr"
	"go.5x5.cz/ptah/core/renderer"
	"go.5x5.cz/ptah/internal/planner/dialects/postgres"
	difftypes "go.5x5.cz/ptah/migration/schemadiff/types"
)

// TestPlanner_GenerateMigrationAST_SchemaObjectsModified pins the forward plan
// for the diff an embedder builds by hand: a modified view, materialized view
// and trigger, with no PreviousBody on the view.
//
// The missing prior body is the point. That is what every ViewDiff built outside
// the comparator carries, and this test says what the planner does with it
// rather than being handed the field that would decide it. A fixture that grew a
// PreviousBody to keep this assertion green would have moved the input to meet
// the behavior and pinned nothing.
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
		ViewsModified: []difftypes.ViewDiff{{
			ViewName: "active_users",
			Changes:  map[string]string{"body": "old -> new"},
		}},
		MaterializedViewsModified: []difftypes.MaterializedViewDiff{{ViewName: "user_stats", Changes: map[string]string{"body": "old -> new"}}},
		TriggersModified:          []difftypes.TriggerDiff{{TriggerName: "set_updated_at", TableName: "users", Changes: map[string]string{"body": "old -> new"}}},
	}

	nodes, err := planner.GenerateMigrationASTChecked(diff, generated)
	c.Assert(err, qt.IsNil)
	sql, err := renderer.RenderSQL("postgres", nodes...)
	c.Assert(err, qt.IsNil)
	sql = legacyRenderedSQL(sql)
	c.Assert(sql, qt.Contains, "CREATE OR REPLACE VIEW active_users")
	c.Assert(sql, qt.Not(qt.Contains), "DROP VIEW")
	c.Assert(sql, qt.Contains, "DROP MATERIALIZED VIEW IF EXISTS user_stats CASCADE;")
	c.Assert(sql, qt.Contains, "CREATE MATERIALIZED VIEW user_stats AS")
	c.Assert(sql, qt.Contains, "CREATE OR REPLACE TRIGGER set_updated_at BEFORE UPDATE ON users FOR EACH ROW EXECUTE FUNCTION ptah_trigger_users_set_updated_at();")
}

// TestPlanner_GenerateMigrationAST_ModifiedViewDropsWhenReplaceWouldBeRefused
// pins the branch PostgreSQL refuses. Measured on PostgreSQL 17.10 against a
// view over (id bigint, email text, age integer): appending a trailing column
// is accepted, while dropping the appended column, renaming a column and
// changing a column's type each fail with "cannot drop columns from view",
// "cannot change name of view column" and "cannot change data type of view
// column". A plan that renders the replace for those shapes is refused at
// execution time, which on a rollback means the operator finds out during the
// incident.
//
// A swapped relation belongs here too. The select list alone answered "appends
// only" for it, because a plain reference reduces to the bare column name, and
// PostgreSQL then refused the statement: the type of a projected column is fixed
// by the relation it reads.
//
// Every row is decided by the change itself, so the direction being planned does
// not enter into it; the shapes that direction DOES decide are
// TestPlanner_GenerateMigrationAST_UndecidableViewBodyFollowsTheDirection.
func TestPlanner_GenerateMigrationAST_ModifiedViewDropsWhenReplaceWouldBeRefused(t *testing.T) {
	cases := []struct {
		name         string
		previousBody string
		nextBody     string
		wantReplace  bool
	}{
		{
			name:         "append a trailing column",
			previousBody: "SELECT id, email FROM users",
			nextBody:     "SELECT id, email, age FROM users",
			wantReplace:  true,
		},
		{
			name:         "drop the appended column",
			previousBody: "SELECT id, email, age FROM users",
			nextBody:     "SELECT id, email FROM users",
			wantReplace:  false,
		},
		{
			name:         "rename a column",
			previousBody: "SELECT id, email FROM users",
			nextBody:     "SELECT id AS uid, email FROM users",
			wantReplace:  false,
		},
		{
			name:         "change a column type",
			previousBody: "SELECT id, email FROM users",
			nextBody:     "SELECT id::text AS id, email FROM users",
			wantReplace:  false,
		},
		{
			name:         "swap the relation under an unchanged column",
			previousBody: "SELECT id FROM other_users",
			nextBody:     "SELECT id, email FROM users",
			wantReplace:  false,
		},
		{
			name:         "join in a second relation",
			previousBody: "SELECT id, email FROM users",
			nextBody:     "SELECT id, email FROM users JOIN accounts ON accounts.user_id = users.id",
			wantReplace:  false,
		},
		{
			name:         "change only the predicate",
			previousBody: "SELECT id, email FROM users WHERE id > 0",
			nextBody:     "SELECT id, email FROM users WHERE id > 10",
			wantReplace:  true,
		},
		{
			name:         "change only the ordering",
			previousBody: "SELECT id, email FROM users ORDER BY id",
			nextBody:     "SELECT id, email FROM users ORDER BY email",
			wantReplace:  true,
		},
		{
			name:         "read the catalog spelling of the same projection",
			previousBody: "SELECT users.id, users.email FROM users WHERE id > 0",
			nextBody:     "SELECT id, email FROM users WHERE id > 10",
			wantReplace:  true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			planner := postgres.New()

			generated := &goschema.Database{
				Views: []goschema.View{{Name: "active_users", Body: tc.nextBody}},
			}
			diff := &difftypes.SchemaDiff{
				ViewsModified: []difftypes.ViewDiff{{
					ViewName:     "active_users",
					Changes:      map[string]string{"body": "old -> new"},
					PreviousBody: tc.previousBody,
				}},
			}

			nodes, err := planner.GenerateMigrationASTChecked(diff, generated)
			c.Assert(err, qt.IsNil)
			sql, err := renderer.RenderSQL("postgres", nodes...)
			c.Assert(err, qt.IsNil)
			sql = legacyRenderedSQL(sql)

			c.Assert(strings.Contains(sql, "CREATE OR REPLACE VIEW active_users"), qt.Equals, tc.wantReplace,
				qt.Commentf("rendered:\n%s", sql))
			c.Assert(strings.Contains(sql, "DROP VIEW IF EXISTS active_users CASCADE;"), qt.Equals, !tc.wantReplace,
				qt.Commentf("rendered:\n%s", sql))
			c.Assert(sql, qt.Contains, tc.nextBody)
		})
	}
}

// TestPlanner_GenerateMigrationAST_UndecidableViewBodyFollowsTheDirection pins
// the shapes the mechanical test cannot decide, where the two directions want
// opposite answers.
//
// Forward, the replace is worth attempting: a body this parser cannot read is
// most often a predicate-only edit to a WITH, star or set-operation view, where
// the column list never moves and PostgreSQL accepts the replace, and where the
// drop would take dependent objects the plan never rebuilt. If the replace turns
// out to be illegal the engine says so and the migration stops having destroyed
// nothing. A rollback cannot be answered that way -- it is already running
// during the incident -- so it takes the statement that always applies.
func TestPlanner_GenerateMigrationAST_UndecidableViewBodyFollowsTheDirection(t *testing.T) {
	cases := []struct {
		name         string
		previousBody string
		nextBody     string
		rollback     bool
		wantReplace  bool
	}{
		{
			name:         "prior body unknown, forward",
			previousBody: "",
			nextBody:     "SELECT id, email FROM users",
			wantReplace:  true,
		},
		{
			name:         "prior body unknown, rollback",
			previousBody: "",
			nextBody:     "SELECT id, email FROM users",
			rollback:     true,
			wantReplace:  false,
		},
		{
			name:         "star projection hides the column list, forward",
			previousBody: "SELECT * FROM users",
			nextBody:     "SELECT * FROM users WHERE id > 10",
			wantReplace:  true,
		},
		{
			name:         "star projection hides the column list, rollback",
			previousBody: "SELECT * FROM users",
			nextBody:     "SELECT * FROM users WHERE id > 10",
			rollback:     true,
			wantReplace:  false,
		},
		{
			name:         "with prefix, forward",
			previousBody: "WITH src AS (SELECT id, email FROM users) SELECT id, email FROM src",
			nextBody:     "WITH src AS (SELECT id, email FROM users WHERE id > 10) SELECT id, email FROM src",
			wantReplace:  true,
		},
		{
			name:         "with prefix, rollback",
			previousBody: "WITH src AS (SELECT id, email FROM users) SELECT id, email FROM src",
			nextBody:     "WITH src AS (SELECT id, email FROM users WHERE id > 10) SELECT id, email FROM src",
			rollback:     true,
			wantReplace:  false,
		},
		{
			name:         "set operation, forward",
			previousBody: "SELECT id FROM users UNION SELECT id FROM archived_users",
			nextBody:     "SELECT id FROM users WHERE id > 1 UNION SELECT id FROM archived_users",
			wantReplace:  true,
		},
		{
			name:         "set operation, rollback",
			previousBody: "SELECT id FROM users UNION SELECT id FROM archived_users",
			nextBody:     "SELECT id FROM users WHERE id > 1 UNION SELECT id FROM archived_users",
			rollback:     true,
			wantReplace:  false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			planner := postgres.New()

			generated := &goschema.Database{
				Views: []goschema.View{{Name: "active_users", Body: tc.nextBody}},
			}
			diff := &difftypes.SchemaDiff{
				ViewsModified: []difftypes.ViewDiff{{
					ViewName:     "active_users",
					Changes:      map[string]string{"body": "old -> new"},
					PreviousBody: tc.previousBody,
					Rollback:     tc.rollback,
				}},
			}

			nodes, err := planner.GenerateMigrationASTChecked(diff, generated)
			c.Assert(err, qt.IsNil)
			sql, err := renderer.RenderSQL("postgres", nodes...)
			c.Assert(err, qt.IsNil)
			sql = legacyRenderedSQL(sql)

			c.Assert(strings.Contains(sql, "CREATE OR REPLACE VIEW active_users"), qt.Equals, tc.wantReplace,
				qt.Commentf("rendered:\n%s", sql))
			c.Assert(strings.Contains(sql, "DROP VIEW IF EXISTS active_users CASCADE;"), qt.Equals, !tc.wantReplace,
				qt.Commentf("rendered:\n%s", sql))
			c.Assert(sql, qt.Contains, tc.nextBody)
		})
	}
}

// TestPlanner_GenerateMigrationAST_ModifiedViewsDropInDependencyOrder pins the
// ordering the drop path needs. DROP VIEW ... CASCADE takes dependent views with
// it, so every drop is emitted before any create and the creates follow
// dependency order -- a view that reads another must be recreated after it.
func TestPlanner_GenerateMigrationAST_ModifiedViewsDropInDependencyOrder(t *testing.T) {
	c := qt.New(t)
	planner := postgres.New()

	generated := &goschema.Database{
		Views: []goschema.View{
			{Name: "a_report", Body: "SELECT id FROM z_base"},
			{Name: "z_base", Body: "SELECT id FROM users"},
		},
	}
	diff := &difftypes.SchemaDiff{
		ViewsModified: []difftypes.ViewDiff{
			{ViewName: "a_report", Changes: map[string]string{"body": "old -> new"}, Rollback: true},
			{ViewName: "z_base", Changes: map[string]string{"body": "old -> new"}, Rollback: true},
		},
	}

	nodes, err := planner.GenerateMigrationASTChecked(diff, generated)
	c.Assert(err, qt.IsNil)
	sql, err := renderer.RenderSQL("postgres", nodes...)
	c.Assert(err, qt.IsNil)
	sql = legacyRenderedSQL(sql)

	assertBefore(t, sql, "DROP VIEW IF EXISTS a_report CASCADE;", "CREATE VIEW z_base AS")
	assertBefore(t, sql, "DROP VIEW IF EXISTS z_base CASCADE;", "CREATE VIEW z_base AS")
	assertBefore(t, sql, "CREATE VIEW z_base AS", "CREATE VIEW a_report AS")
}

// TestPlanner_GenerateMigrationAST_DroppedViewRebuildsDeclaredDependents pins
// what the drop path owes the schema it was generated from.
//
// DROP VIEW ... CASCADE does not stop at the view named. It takes every view and
// materialized view that reads it, transitively, and none of those are mentioned
// anywhere else in the plan -- so a migration that only dropped and recreated the
// named view applied cleanly and left the database short of objects the same
// schema declares. Re-planning did not repair it either: the next plan created
// the dependent and dropped it again in the same file.
func TestPlanner_GenerateMigrationAST_DroppedViewRebuildsDeclaredDependents(t *testing.T) {
	c := qt.New(t)
	planner := postgres.New()

	generated := &goschema.Database{
		Views: []goschema.View{
			{Name: "base_view", Body: "SELECT id, email FROM users"},
			{Name: "mid_view", Body: "SELECT id FROM base_view"},
			{Name: "leaf_view", Body: "SELECT id FROM mid_view"},
			{Name: "unrelated_view", Body: "SELECT id FROM accounts"},
		},
		MaterializedViews: []goschema.MaterializedView{
			{Name: "mid_stats", Body: "SELECT count(*) AS total FROM base_view", RefreshStrategy: "manual"},
		},
	}
	diff := &difftypes.SchemaDiff{
		ViewsModified: []difftypes.ViewDiff{{
			ViewName: "base_view",
			Changes:  map[string]string{"body": "old -> new"},
			// The column list shrank, so the replace is refused and the drop is
			// the only statement that applies -- in both directions.
			PreviousBody: "SELECT id, email, age FROM users",
		}},
	}

	nodes, err := planner.GenerateMigrationASTChecked(diff, generated)
	c.Assert(err, qt.IsNil)
	sql, err := renderer.RenderSQL("postgres", nodes...)
	c.Assert(err, qt.IsNil)
	sql = legacyRenderedSQL(sql)

	c.Assert(sql, qt.Contains, "DROP VIEW IF EXISTS base_view CASCADE;", qt.Commentf("rendered:\n%s", sql))
	c.Assert(sql, qt.Contains, "CREATE VIEW base_view AS", qt.Commentf("rendered:\n%s", sql))
	c.Assert(sql, qt.Contains, "CREATE OR REPLACE VIEW mid_view AS", qt.Commentf("rendered:\n%s", sql))
	c.Assert(sql, qt.Contains, "CREATE OR REPLACE VIEW leaf_view AS", qt.Commentf("rendered:\n%s", sql))
	c.Assert(sql, qt.Contains, "CREATE MATERIALIZED VIEW mid_stats AS", qt.Commentf("rendered:\n%s", sql))

	// A view the CASCADE cannot reach has no business in this plan.
	c.Assert(sql, qt.Not(qt.Contains), "unrelated_view", qt.Commentf("rendered:\n%s", sql))

	// And the rebuild has to follow the read order, or the same CASCADE takes it
	// straight back out.
	assertBefore(t, sql, "CREATE VIEW base_view AS", "CREATE OR REPLACE VIEW mid_view AS")
	assertBefore(t, sql, "CREATE OR REPLACE VIEW mid_view AS", "CREATE OR REPLACE VIEW leaf_view AS")
}

// TestPlanner_GenerateMigrationAST_CascadeRebuildReadsCodeNotText pins the
// other side of the rebuild set: what the CASCADE cannot reach must stay out of
// the plan even when the dropped view's name appears in the body as text.
//
// The rebuild list exists to put back what DROP VIEW ... CASCADE takes, so every
// name on it is answered with a statement: a materialized view is dropped and
// recreated, a view is re-asserted. Reading the name out of a string literal or
// a comment therefore does not merely add a redundant statement -- it issues
// DROP MATERIALIZED VIEW ... CASCADE against an object no part of the migration
// touched. Measured on PostgreSQL 17.10, that took a hand-made dependent view, a
// unique index on the materialized view, and the SELECT granted on it; none of
// the three is declared, so nothing put them back.
func TestPlanner_GenerateMigrationAST_CascadeRebuildReadsCodeNotText(t *testing.T) {
	c := qt.New(t)
	planner := postgres.New()

	generated := &goschema.Database{
		Views: []goschema.View{
			{Name: "base_view", Body: "SELECT id, email FROM users"},
			{Name: "reader_view", Body: "SELECT id FROM base_view"},
			{Name: "label_view", Body: "SELECT id, 'base_view' AS label FROM accounts"},
			{Name: "note_view", Body: "SELECT id FROM accounts -- was base_view once\n"},
		},
		MaterializedViews: []goschema.MaterializedView{
			{Name: "label_stats", Body: "SELECT 'base_view' AS label, count(*) AS total FROM accounts", RefreshStrategy: "manual"},
			{Name: "reader_stats", Body: "SELECT count(*) AS total FROM base_view", RefreshStrategy: "manual"},
		},
	}
	diff := &difftypes.SchemaDiff{
		ViewsModified: []difftypes.ViewDiff{{
			ViewName: "base_view",
			Changes:  map[string]string{"body": "old -> new"},
			// The column list shrank, so the drop is the only statement that
			// applies and the rebuild list is what decides who else is touched.
			PreviousBody: "SELECT id, email, age FROM users",
		}},
	}

	nodes, err := planner.GenerateMigrationASTChecked(diff, generated)
	c.Assert(err, qt.IsNil)
	sql, err := renderer.RenderSQL("postgres", nodes...)
	c.Assert(err, qt.IsNil)
	sql = legacyRenderedSQL(sql)

	// The genuine dependents are still rebuilt.
	c.Assert(sql, qt.Contains, "DROP VIEW IF EXISTS base_view CASCADE;", qt.Commentf("rendered:\n%s", sql))
	c.Assert(sql, qt.Contains, "CREATE OR REPLACE VIEW reader_view AS", qt.Commentf("rendered:\n%s", sql))
	c.Assert(sql, qt.Contains, "CREATE MATERIALIZED VIEW reader_stats AS", qt.Commentf("rendered:\n%s", sql))

	// The objects that only spell the name are not named by any statement.
	c.Assert(sql, qt.Not(qt.Contains), "label_view", qt.Commentf("rendered:\n%s", sql))
	c.Assert(sql, qt.Not(qt.Contains), "note_view", qt.Commentf("rendered:\n%s", sql))
	c.Assert(sql, qt.Not(qt.Contains), "label_stats", qt.Commentf("rendered:\n%s", sql))
	c.Assert(sql, qt.Not(qt.Contains), "DROP MATERIALIZED VIEW IF EXISTS label_stats", qt.Commentf("rendered:\n%s", sql))
}

// TestPlanner_GenerateMigrationAST_QuotedRelationCaseIsNotFolded pins the
// identity of a quoted relation.
//
// PostgreSQL folds an unquoted identifier to lower case and keeps a quoted one
// exactly, so "Foo" and "foo" are two different relations and a view moved
// between them takes its column types from a different place. Folding the whole
// FROM text to lower case answered "the relations did not change" and produced
// CREATE OR REPLACE VIEW, which PostgreSQL 17.10 refused with `cannot change
// data type of view column "id" from bigint to text` -- rc=3 under
// psql -v ON_ERROR_STOP=1, in the down direction, which is the exact failure
// this change exists to prevent.
func TestPlanner_GenerateMigrationAST_QuotedRelationCaseIsNotFolded(t *testing.T) {
	cases := []struct {
		name         string
		previousBody string
		nextBody     string
		wantReplace  bool
	}{
		{
			name:         "quoted relations differing only in case are different relations",
			previousBody: `SELECT id FROM "Foo"`,
			nextBody:     `SELECT id FROM "foo"`,
			wantReplace:  false,
		},
		{
			name:         "the same quoted relation still appends",
			previousBody: `SELECT id FROM "Foo"`,
			nextBody:     `SELECT id, email FROM "Foo"`,
			wantReplace:  true,
		},
		{
			name:         "the same quoted relation with only a predicate change still replaces",
			previousBody: `SELECT id FROM "Foo" WHERE id > 0`,
			nextBody:     `SELECT id FROM "Foo" WHERE id > 10`,
			wantReplace:  true,
		},
		{
			name:         "case still folds outside quotes",
			previousBody: "SELECT id FROM foo",
			nextBody:     "select id FROM FOO",
			wantReplace:  true,
		},
		{
			// A literal decides no column's type, so its case still folds -- and
			// the quotation mark inside it must not be read as opening an
			// identifier, which would carry the rest of the body through
			// unfolded.
			name:         "a quotation mark inside a literal does not open an identifier",
			previousBody: `SELECT 'a"b' AS label FROM foo`,
			nextBody:     `SELECT 'A"B' AS label, id FROM FOO`,
			wantReplace:  true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			planner := postgres.New()

			generated := &goschema.Database{
				Views: []goschema.View{{Name: "probe_view", Body: tc.nextBody}},
			}
			diff := &difftypes.SchemaDiff{
				ViewsModified: []difftypes.ViewDiff{{
					ViewName:     "probe_view",
					Changes:      map[string]string{"body": "old -> new"},
					PreviousBody: tc.previousBody,
					Rollback:     true,
				}},
			}

			nodes, err := planner.GenerateMigrationASTChecked(diff, generated)
			c.Assert(err, qt.IsNil)
			sql, err := renderer.RenderSQL("postgres", nodes...)
			c.Assert(err, qt.IsNil)
			sql = legacyRenderedSQL(sql)

			c.Assert(strings.Contains(sql, "CREATE OR REPLACE VIEW probe_view"), qt.Equals, tc.wantReplace,
				qt.Commentf("rendered:\n%s", sql))
			c.Assert(strings.Contains(sql, "DROP VIEW IF EXISTS probe_view CASCADE;"), qt.Equals, !tc.wantReplace,
				qt.Commentf("rendered:\n%s", sql))
		})
	}
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

	nodes, err := planner.GenerateMigrationASTChecked(diff, generated)
	c.Assert(err, qt.IsNil)
	sql, err := renderer.RenderSQL("postgres", nodes...)
	c.Assert(err, qt.IsNil)
	sql = legacyRenderedSQL(sql)
	c.Assert(sql, qt.Contains, "CREATE OR REPLACE FUNCTION ptah_trigger_users_set_updated_at()")
	c.Assert(sql, qt.Contains, "CREATE OR REPLACE FUNCTION ptah_trigger_posts_set_updated_at()")
	c.Assert(sql, qt.Contains, "CREATE TRIGGER set_updated_at BEFORE UPDATE ON users FOR EACH ROW EXECUTE FUNCTION ptah_trigger_users_set_updated_at();")
	c.Assert(sql, qt.Contains, "CREATE TRIGGER set_updated_at BEFORE UPDATE ON posts FOR EACH ROW EXECUTE FUNCTION ptah_trigger_posts_set_updated_at();")
}

// TestPlanner_GenerateMigrationAST_MaterializedViewRefreshStrategyDoesNotAutoRefresh
// keeps the property its name states -- planning a materialized view emits no
// REFRESH -- on the one strategy a target can carry.
//
// It used to state that property with `concurrently`, which is the declaration
// stokaro/ptah#1523 reports as silently lost: the plan rendered a create that
// said nothing about the policy and exited 0. That half of the case is now the
// refusal below.
func TestPlanner_GenerateMigrationAST_MaterializedViewRefreshStrategyDoesNotAutoRefresh(t *testing.T) {
	c := qt.New(t)
	planner := postgres.New()

	generated := &goschema.Database{
		MaterializedViews: []goschema.MaterializedView{{
			Name:            "user_stats",
			Body:            "SELECT id, COUNT(*) FROM users GROUP BY id",
			RefreshStrategy: "manual",
		}},
	}
	diff := &difftypes.SchemaDiff{
		MaterializedViewsAdded: []string{"user_stats"},
	}

	nodes, err := planner.GenerateMigrationASTChecked(diff, generated)
	c.Assert(err, qt.IsNil)
	sql, err := renderer.RenderSQL("postgres", nodes...)
	c.Assert(err, qt.IsNil)
	sql = legacyRenderedSQL(sql)
	c.Assert(sql, qt.Contains, "CREATE MATERIALIZED VIEW user_stats AS")
	c.Assert(sql, qt.Not(qt.Contains), "REFRESH MATERIALIZED VIEW")
}

// TestPlanner_GenerateMigrationAST_MaterializedViewRefreshStrategyRefused pins
// the planned half of the refusal: the node the planner builds carries the
// declared strategy, so rendering the plan refuses it exactly as rendering the
// declaration does, and no statement is produced for an operator to apply.
func TestPlanner_GenerateMigrationAST_MaterializedViewRefreshStrategyRefused(t *testing.T) {
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

	nodes, err := planner.GenerateMigrationASTChecked(diff, generated)
	c.Assert(err, qt.IsNil)
	sql, err := renderer.RenderSQL("postgres", nodes...)
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err.Error(), qt.Contains, `refresh_strategy "concurrently"`)
	c.Assert(sql, qt.Equals, "")
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

	nodes, err := planner.GenerateMigrationASTChecked(diff, generated)
	c.Assert(err, qt.IsNil)
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

	nodes, err := planner.GenerateMigrationASTChecked(diff, generated)
	c.Assert(err, qt.IsNil)
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

	nodes, err := planner.GenerateMigrationASTChecked(diff, generated)
	c.Assert(err, qt.IsNil)
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
