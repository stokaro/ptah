package generator

// White-box testing required: the reverse plan is built by the unexported
// reverseSchemaDiffWithSchema and rendered by the unexported
// generateDownMigrationSQL. Asserting on the reversed SchemaDiff alone would
// only restate the swap; these tests need the rendered down SQL, which is not
// reachable through the exported GenerateMigration API without writing
// migration files to disk and reading them back.

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/migration/schemadiff"
	"go.5x5.cz/ptah/migration/schemadiff/types"
)

// indexOfFragment reports where a fragment starts, so an ordering assertion can
// be written as a comparison rather than as control flow in a test body.
func indexOfFragment(sql, fragment string) int {
	return strings.Index(sql, fragment)
}

// viewLikeTable is the table both the view-like objects and the trigger hang
// off. It exists in the database before and after every case below, so a
// DROP TABLE ... CASCADE can never be what removes the view, the materialized
// view or the trigger: the down migration has to name them itself.
func viewLikeTable() goschema.Table {
	return goschema.Table{StructName: "RevViewUser", Name: "rev_view_users"}
}

func viewLikeFields() []goschema.Field {
	return []goschema.Field{
		{StructName: "RevViewUser", Name: "id", Type: "BIGINT", Primary: true},
		{StructName: "RevViewUser", Name: "email", Type: "TEXT"},
	}
}

func viewLikeDBWithTableOnly() *dbschematypes.DBSchema {
	return &dbschematypes.DBSchema{
		Tables: []dbschematypes.DBTable{
			{
				Name: "rev_view_users",
				Type: "TABLE",
				Columns: []dbschematypes.DBColumn{
					{Name: "id", DataType: "bigint", IsNullable: "NO", IsPrimaryKey: true, OrdinalPosition: 1},
					{Name: "email", DataType: "text", IsNullable: "NO", OrdinalPosition: 2},
				},
			},
		},
	}
}

func viewLikeGoSchemaWithObjects(viewBody, matViewBody, triggerBody string) *goschema.Database {
	schema := &goschema.Database{
		Tables: []goschema.Table{viewLikeTable()},
		Fields: viewLikeFields(),
		Views: []goschema.View{
			{StructName: "RevActiveUsers", Name: "rev_active_users", Body: viewBody},
		},
		MaterializedViews: []goschema.MaterializedView{
			{
				StructName:      "RevUserStats",
				Name:            "rev_user_stats",
				Body:            matViewBody,
				RefreshStrategy: "manual",
			},
		},
		Triggers: []goschema.Trigger{
			{
				StructName: "RevViewUser",
				Name:       "rev_touch",
				Table:      "rev_view_users",
				Timing:     "BEFORE",
				Event:      "UPDATE",
				ForEach:    "ROW",
				Body:       triggerBody,
			},
		},
	}
	goschema.Finalize(schema)
	return schema
}

func viewLikeDBWithObjects(viewBody, matViewBody, triggerBody string) *dbschematypes.DBSchema {
	db := viewLikeDBWithTableOnly()
	db.Views = []dbschematypes.DBView{
		{Name: "rev_active_users", Body: viewBody},
	}
	db.MatViews = []dbschematypes.DBMatView{
		{Name: "rev_user_stats", Body: matViewBody, RefreshStrategy: "manual"},
	}
	db.Triggers = []dbschematypes.DBTrigger{
		{
			Name:    "rev_touch",
			Table:   "rev_view_users",
			Timing:  "BEFORE",
			Event:   "UPDATE",
			ForEach: "ROW",
			Body:    triggerBody,
		},
	}
	return db
}

const (
	revViewBody    = "SELECT id FROM rev_view_users"
	revMatViewBody = "SELECT count(*) FROM rev_view_users"
	revTriggerBody = "RETURN NEW;"
)

// TestGenerateDownMigrationSQL_DropsViewLikeObjectsCreatedByUp pins the first
// half of issue #1287: an up migration that creates a view, a materialized view
// and a trigger on an already-existing table must roll back by dropping all
// three. Before the reverse plan carried these categories, the down file was
// "-- No rollback operations needed" and every object survived the rollback.
func TestGenerateDownMigrationSQL_DropsViewLikeObjectsCreatedByUp(t *testing.T) {
	c := qt.New(t)

	schema := viewLikeGoSchemaWithObjects(revViewBody, revMatViewBody, revTriggerBody)
	db := viewLikeDBWithTableOnly()
	upDiff := schemadiff.CompareWithDialect(schema, db, "postgres")

	c.Assert(upDiff.ViewsAdded, qt.DeepEquals, []string{"rev_active_users"})
	c.Assert(upDiff.MaterializedViewsAdded, qt.DeepEquals, []string{"rev_user_stats"})
	c.Assert(upDiff.TriggersAdded, qt.DeepEquals, []types.TriggerRef{
		{TriggerName: "rev_touch", TableName: "rev_view_users"},
	})

	downSQL, err := generateDownMigrationSQL(upDiff, schema, db, "postgres")
	c.Assert(err, qt.IsNil)
	downSQL = legacyRenderedSQL(downSQL)

	wants := []struct {
		name     string
		fragment string
	}{
		{name: "drops the view", fragment: "DROP VIEW IF EXISTS rev_active_users CASCADE;"},
		{name: "drops the materialized view", fragment: "DROP MATERIALIZED VIEW IF EXISTS rev_user_stats CASCADE;"},
		{name: "drops the trigger", fragment: "DROP TRIGGER IF EXISTS rev_touch ON rev_view_users CASCADE;"},
		{name: "drops the trigger function", fragment: "DROP FUNCTION IF EXISTS ptah_trigger_rev_view_users_rev_touch();"},
	}

	for _, want := range wants {
		t.Run(want.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(downSQL, qt.Contains, want.fragment)
		})
	}

	// The table pre-existed the up migration, so the rollback must not drop it.
	c.Assert(downSQL, qt.Not(qt.Contains), "DROP TABLE")
}

// TestGenerateDownMigrationSQL_RestoresViewLikeObjectsDroppedByUp pins the
// other direction: an up migration that drops a view, a materialized view and a
// trigger present only in the database must roll back by recreating all three
// from the pre-change database schema.
func TestGenerateDownMigrationSQL_RestoresViewLikeObjectsDroppedByUp(t *testing.T) {
	c := qt.New(t)

	schema := &goschema.Database{
		Tables: []goschema.Table{viewLikeTable()},
		Fields: viewLikeFields(),
	}
	goschema.Finalize(schema)
	db := viewLikeDBWithObjects(revViewBody, revMatViewBody, revTriggerBody)

	upDiff := schemadiff.CompareWithDialect(schema, db, "postgres")

	c.Assert(upDiff.ViewsRemoved, qt.DeepEquals, []string{"rev_active_users"})
	c.Assert(upDiff.MaterializedViewsRemoved, qt.DeepEquals, []string{"rev_user_stats"})
	c.Assert(upDiff.TriggersRemoved, qt.DeepEquals, []types.TriggerRef{
		{TriggerName: "rev_touch", TableName: "rev_view_users"},
	})

	downSQL, err := generateDownMigrationSQL(upDiff, schema, db, "postgres")
	c.Assert(err, qt.IsNil)
	downSQL = legacyRenderedSQL(downSQL)

	wants := []struct {
		name     string
		fragment string
	}{
		{name: "recreates the view", fragment: "CREATE VIEW rev_active_users AS"},
		{name: "recreates the view body", fragment: revViewBody},
		{name: "recreates the materialized view", fragment: "CREATE MATERIALIZED VIEW rev_user_stats AS"},
		{name: "recreates the trigger function", fragment: "CREATE OR REPLACE FUNCTION ptah_trigger_rev_view_users_rev_touch()"},
		{name: "recreates the trigger", fragment: "CREATE TRIGGER rev_touch BEFORE UPDATE ON rev_view_users"},
	}

	for _, want := range wants {
		t.Run(want.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(downSQL, qt.Contains, want.fragment)
		})
	}
}

// TestGenerateDownMigrationSQL_ModifiedViewRollbackDropsInsteadOfReplacing is
// the trap issue #1287 names. The up migration appends a column to the view;
// the rollback has to take it away again, and PostgreSQL refuses
// "CREATE OR REPLACE VIEW" that drops a column ("cannot drop columns from
// view", measured on 17.10). The down plan therefore has to drop and recreate
// the view, and it has to do so from inside the modify step: done from outside,
// the reverse plan comes out create-then-drop and leaves no view at all.
func TestGenerateDownMigrationSQL_ModifiedViewRollbackDropsInsteadOfReplacing(t *testing.T) {
	c := qt.New(t)

	const oldBody = "SELECT id FROM rev_view_users"
	const newBody = "SELECT id, email FROM rev_view_users"

	schema := viewLikeGoSchemaWithObjects(newBody, revMatViewBody, revTriggerBody)
	db := viewLikeDBWithObjects(oldBody, revMatViewBody, revTriggerBody)

	upDiff := schemadiff.CompareWithDialect(schema, db, "postgres")
	c.Assert(upDiff.ViewsModified, qt.HasLen, 1)
	c.Assert(upDiff.ViewsModified[0].ViewName, qt.Equals, "rev_active_users")

	upSQL, err := generateUpMigrationSQL(upDiff, schema, "postgres")
	c.Assert(err, qt.IsNil)
	// Appending a trailing column is the one shape PostgreSQL does accept, so
	// the up direction keeps the dependency-preserving replace.
	c.Assert(legacyRenderedSQL(upSQL), qt.Contains, "CREATE OR REPLACE VIEW rev_active_users")

	downSQL, err := generateDownMigrationSQL(upDiff, schema, db, "postgres")
	c.Assert(err, qt.IsNil)
	downSQL = legacyRenderedSQL(downSQL)

	wants := []struct {
		name     string
		fragment string
	}{
		{name: "drops the view first", fragment: "DROP VIEW IF EXISTS rev_active_users CASCADE;"},
		{name: "recreates it with the prior body", fragment: "CREATE VIEW rev_active_users AS"},
		{name: "restores the prior column list", fragment: oldBody},
	}
	for _, want := range wants {
		t.Run(want.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(downSQL, qt.Contains, want.fragment)
		})
	}

	t.Run("never emits the refused replace", func(t *testing.T) {
		c := qt.New(t)
		c.Assert(downSQL, qt.Not(qt.Contains), "CREATE OR REPLACE VIEW rev_active_users")
	})
	t.Run("drops before it recreates", func(t *testing.T) {
		c := qt.New(t)
		c.Assert(indexOfFragment(downSQL, "DROP VIEW IF EXISTS rev_active_users") <
			indexOfFragment(downSQL, "CREATE VIEW rev_active_users"), qt.IsTrue,
			qt.Commentf("down SQL:\n%s", downSQL))
	})
	t.Run("does not carry the post-up body", func(t *testing.T) {
		c := qt.New(t)
		c.Assert(downSQL, qt.Not(qt.Contains), newBody)
	})
}

// TestGenerateDownMigrationSQL_ModifiedViewKeepsLegalReplace is the other half
// of the same rule. A predicate-only change leaves the column list untouched,
// PostgreSQL accepts the replace, and the replace is worth keeping: the drop
// carries CASCADE and would take dependent objects with it.
func TestGenerateDownMigrationSQL_ModifiedViewKeepsLegalReplace(t *testing.T) {
	c := qt.New(t)

	const oldBody = "SELECT id FROM rev_view_users WHERE id > 0"
	const newBody = "SELECT id FROM rev_view_users WHERE id > 10"

	schema := viewLikeGoSchemaWithObjects(newBody, revMatViewBody, revTriggerBody)
	db := viewLikeDBWithObjects(oldBody, revMatViewBody, revTriggerBody)

	upDiff := schemadiff.CompareWithDialect(schema, db, "postgres")
	c.Assert(upDiff.ViewsModified, qt.HasLen, 1)

	downSQL, err := generateDownMigrationSQL(upDiff, schema, db, "postgres")
	c.Assert(err, qt.IsNil)
	downSQL = legacyRenderedSQL(downSQL)

	t.Run("replaces in place", func(t *testing.T) {
		c := qt.New(t)
		c.Assert(downSQL, qt.Contains, "CREATE OR REPLACE VIEW rev_active_users")
	})
	t.Run("restores the prior predicate", func(t *testing.T) {
		c := qt.New(t)
		c.Assert(downSQL, qt.Contains, oldBody)
	})
	t.Run("keeps dependents by not dropping", func(t *testing.T) {
		c := qt.New(t)
		c.Assert(downSQL, qt.Not(qt.Contains), "DROP VIEW")
	})
}

// TestGenerateDownMigrationSQL_ModifiedMatViewAndTriggerRollback covers the two
// remaining modified categories. A materialized view has no in-place replace at
// all, so both directions drop and recreate it; a trigger does, and PostgreSQL
// accepts CREATE OR REPLACE TRIGGER even for a timing change (measured on
// 17.10), so the trigger keeps the replace.
func TestGenerateDownMigrationSQL_ModifiedMatViewAndTriggerRollback(t *testing.T) {
	c := qt.New(t)

	const oldMatView = "SELECT count(*) FROM rev_view_users"
	const newMatView = "SELECT count(id) FROM rev_view_users"
	const oldTrigger = "RETURN NEW;"
	const newTrigger = "RETURN OLD;"

	schema := viewLikeGoSchemaWithObjects(revViewBody, newMatView, newTrigger)
	db := viewLikeDBWithObjects(revViewBody, oldMatView, oldTrigger)

	upDiff := schemadiff.CompareWithDialect(schema, db, "postgres")
	c.Assert(upDiff.MaterializedViewsModified, qt.HasLen, 1)
	c.Assert(upDiff.TriggersModified, qt.HasLen, 1)

	downSQL, err := generateDownMigrationSQL(upDiff, schema, db, "postgres")
	c.Assert(err, qt.IsNil)
	downSQL = legacyRenderedSQL(downSQL)

	wants := []struct {
		name     string
		fragment string
	}{
		{name: "drops the materialized view", fragment: "DROP MATERIALIZED VIEW IF EXISTS rev_user_stats CASCADE;"},
		{name: "recreates the materialized view", fragment: "CREATE MATERIALIZED VIEW rev_user_stats AS"},
		{name: "restores the prior materialized body", fragment: oldMatView},
		{name: "replaces the trigger function", fragment: "CREATE OR REPLACE FUNCTION ptah_trigger_rev_view_users_rev_touch()"},
		{name: "restores the prior trigger body", fragment: oldTrigger},
	}
	for _, want := range wants {
		t.Run(want.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(downSQL, qt.Contains, want.fragment)
		})
	}
}

// TestReverseSchemaDiff_ReversesViewLikeChangeDescriptions checks the recorded
// change descriptions read truthfully in the down direction. The planner does
// not consume this map, but the diff is serialized into reports, so an
// unreversed "old -> new" would describe the up migration on a down plan.
func TestReverseSchemaDiff_ReversesViewLikeChangeDescriptions(t *testing.T) {
	input := &types.SchemaDiff{
		ViewsModified: []types.ViewDiff{
			{ViewName: "v", Changes: map[string]string{"body": "OLD -> NEW"}},
		},
		MaterializedViewsModified: []types.MaterializedViewDiff{
			{ViewName: "mv", Changes: map[string]string{"body": "OLD -> NEW"}},
		},
		TriggersModified: []types.TriggerDiff{
			{TriggerName: "trg", TableName: "t", Changes: map[string]string{"timing": "BEFORE -> AFTER"}},
		},
	}

	result := reverseSchemaDiff(input)

	t.Run("view identity is preserved and the change is flipped", func(t *testing.T) {
		c := qt.New(t)
		c.Assert(result.ViewsModified, qt.HasLen, 1)
		c.Assert(result.ViewsModified[0].ViewName, qt.Equals, "v")
		c.Assert(result.ViewsModified[0].Changes["body"], qt.Equals, "NEW -> OLD")
	})

	t.Run("materialized view identity is preserved and the change is flipped", func(t *testing.T) {
		c := qt.New(t)
		c.Assert(result.MaterializedViewsModified, qt.HasLen, 1)
		c.Assert(result.MaterializedViewsModified[0].ViewName, qt.Equals, "mv")
		c.Assert(result.MaterializedViewsModified[0].Changes["body"], qt.Equals, "NEW -> OLD")
	})

	t.Run("trigger identity including its table is preserved", func(t *testing.T) {
		c := qt.New(t)
		c.Assert(result.TriggersModified, qt.HasLen, 1)
		c.Assert(result.TriggersModified[0].TriggerName, qt.Equals, "trg")
		c.Assert(result.TriggersModified[0].TableName, qt.Equals, "t")
		c.Assert(result.TriggersModified[0].Changes["timing"], qt.Equals, "AFTER -> BEFORE")
	})
}
