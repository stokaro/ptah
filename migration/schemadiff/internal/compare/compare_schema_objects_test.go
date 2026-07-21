package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/goschema"
	dbschematypes "github.com/stokaro/ptah/dbschema/types"
	"github.com/stokaro/ptah/migration/schemadiff/internal/compare"
	difftypes "github.com/stokaro/ptah/migration/schemadiff/types"
)

func TestViews_DetectsBodyChange(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{}

	compare.Views(&goschema.Database{
		Views: []goschema.View{{Name: "active_users", Body: "SELECT id FROM users WHERE deleted_at IS NULL"}},
	}, &dbschematypes.DBSchema{
		Views: []dbschematypes.DBView{{Name: "active_users", Body: "SELECT id FROM users WHERE deleted_at IS NULL AND enabled = true"}},
	}, diff)

	c.Assert(diff.ViewsModified, qt.HasLen, 1)
	c.Assert(diff.ViewsModified[0].Changes["body"], qt.Not(qt.Equals), "")
}

func TestViews_IgnoresDatabaseOnlyQualification(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{}

	compare.Views(&goschema.Database{
		Views: []goschema.View{{Name: "active_users", Body: "SELECT id FROM users WHERE deleted_at IS NULL"}},
	}, &dbschematypes.DBSchema{
		Views: []dbschematypes.DBView{{Name: "active_users", Body: "SELECT users.id FROM users WHERE users.deleted_at IS NULL"}},
	}, diff)

	c.Assert(diff.ViewsModified, qt.HasLen, 0)
}

func TestViews_IgnoresMySQLCanonicalViewBody(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{}

	compare.Views(&goschema.Database{
		Views: []goschema.View{{Name: "live_products", Body: "SELECT id, name FROM products WHERE archived = false"}},
	}, &dbschematypes.DBSchema{
		Views: []dbschematypes.DBView{{
			Name: "live_products",
			Body: "select `ptah_issue_502`.`products`.`id` AS `id`,`ptah_issue_502`.`products`.`name` AS `name` " +
				"from `ptah_issue_502`.`products` where (`ptah_issue_502`.`products`.`archived` = false)",
		}},
	}, diff)

	c.Assert(diff.ViewsModified, qt.HasLen, 0)
}

func TestViews_DetectsMySQLCanonicalPredicateChange(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{}

	compare.Views(&goschema.Database{
		Views: []goschema.View{{Name: "live_products", Body: "SELECT id, name FROM products WHERE archived = false"}},
	}, &dbschematypes.DBSchema{
		Views: []dbschematypes.DBView{{
			Name: "live_products",
			Body: "select `ptah_issue_502`.`products`.`id` AS `id`,`ptah_issue_502`.`products`.`name` AS `name` " +
				"from `ptah_issue_502`.`products` where (`ptah_issue_502`.`products`.`archived` = true)",
		}},
	}, diff)

	c.Assert(diff.ViewsModified, qt.HasLen, 1)
	c.Assert(diff.ViewsModified[0].Changes["body"], qt.Not(qt.Equals), "")
}

func TestViews_DetectsExplicitQualifierChange(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{}

	compare.Views(&goschema.Database{
		Views: []goschema.View{{Name: "active_users", Body: "SELECT users.id FROM users JOIN posts ON posts.user_id = users.id"}},
	}, &dbschematypes.DBSchema{
		Views: []dbschematypes.DBView{{Name: "active_users", Body: "SELECT posts.id FROM users JOIN posts ON posts.user_id = users.id"}},
	}, diff)

	c.Assert(diff.ViewsModified, qt.HasLen, 1)
	c.Assert(diff.ViewsModified[0].Changes["body"], qt.Not(qt.Equals), "")
}

func TestMaterializedViews_DetectsBodyChange(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{}

	compare.MaterializedViews(&goschema.Database{
		MaterializedViews: []goschema.MaterializedView{{
			Name:            "user_stats",
			Body:            "SELECT id, COUNT(*) FROM users GROUP BY id",
			RefreshStrategy: "concurrently",
		}},
	}, &dbschematypes.DBSchema{
		MatViews: []dbschematypes.DBMatView{{
			Name:            "user_stats",
			Body:            "SELECT id, COUNT(*) FROM users WHERE enabled GROUP BY id",
			RefreshStrategy: "manual",
		}},
	}, diff)

	c.Assert(diff.MaterializedViewsModified, qt.HasLen, 1)
	c.Assert(diff.MaterializedViewsModified[0].Changes["body"], qt.Not(qt.Equals), "")
	c.Assert(diff.MaterializedViewsModified[0].Changes["refresh_strategy"], qt.Equals, "")
}

func TestMaterializedViews_IgnoresRefreshStrategyDrift(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{}

	compare.MaterializedViews(&goschema.Database{
		MaterializedViews: []goschema.MaterializedView{{
			Name:            "user_stats",
			Body:            "SELECT id, COUNT(*) FROM users GROUP BY id",
			RefreshStrategy: "concurrently",
		}},
	}, &dbschematypes.DBSchema{
		MatViews: []dbschematypes.DBMatView{{
			Name:            "user_stats",
			Body:            "SELECT id, COUNT(*) FROM users GROUP BY id",
			RefreshStrategy: "manual",
		}},
	}, diff)

	c.Assert(diff.MaterializedViewsModified, qt.HasLen, 0)
}

func TestMaterializedViews_IgnoresPostgreSQLDefaultAggregateAlias(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{}

	compare.MaterializedViews(&goschema.Database{
		MaterializedViews: []goschema.MaterializedView{{
			Name: "user_stats",
			Body: "SELECT id, COUNT(*) FROM users GROUP BY id",
		}},
	}, &dbschematypes.DBSchema{
		MatViews: []dbschematypes.DBMatView{{
			Name: "user_stats",
			Body: "SELECT id,\n    count(*) AS count\n   FROM users\n  GROUP BY id;",
		}},
	}, diff)

	c.Assert(diff.MaterializedViewsModified, qt.HasLen, 0)
}

func TestTriggers_KeyedByTableAndDetectsBodyChange(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{}

	compare.Triggers(&goschema.Database{
		Triggers: []goschema.Trigger{{
			Name:    "set_updated_at",
			Table:   "users",
			Timing:  "BEFORE",
			Event:   "UPDATE",
			ForEach: "ROW",
			Body:    "NEW.updated_at = NOW(); RETURN NEW;",
		}},
	}, &dbschematypes.DBSchema{
		Triggers: []dbschematypes.DBTrigger{{
			Name:    "set_updated_at",
			Table:   "users",
			Timing:  "BEFORE",
			Event:   "UPDATE",
			ForEach: "ROW",
			Body:    "BEGIN NEW.updated_at = clock_timestamp(); RETURN NEW; END;",
		}},
	}, diff)

	c.Assert(diff.TriggersModified, qt.HasLen, 1)
	c.Assert(diff.TriggersModified[0].TableName, qt.Equals, "users")
	c.Assert(diff.TriggersModified[0].Changes["body"], qt.Not(qt.Equals), "")
}

func TestTriggers_DetectsNewOldQualifierChange(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{}

	compare.Triggers(&goschema.Database{
		Triggers: []goschema.Trigger{{
			Name:    "set_updated_at",
			Table:   "users",
			Timing:  "BEFORE",
			Event:   "UPDATE",
			ForEach: "ROW",
			Body:    "NEW.updated_at = NOW(); RETURN NEW;",
		}},
	}, &dbschematypes.DBSchema{
		Triggers: []dbschematypes.DBTrigger{{
			Name:    "set_updated_at",
			Table:   "users",
			Timing:  "BEFORE",
			Event:   "UPDATE",
			ForEach: "ROW",
			Body:    "BEGIN OLD.updated_at = NOW(); RETURN OLD; END;",
		}},
	}, diff)

	c.Assert(diff.TriggersModified, qt.HasLen, 1)
	c.Assert(diff.TriggersModified[0].Changes["body"], qt.Not(qt.Equals), "")
}
