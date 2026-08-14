package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/migration/schemadiff/internal/compare"
	difftypes "go.5x5.cz/ptah/migration/schemadiff/types"
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

func TestViews_MatchesGeneratedQualifiedNameToDatabaseSchema(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{}

	compare.Views(&goschema.Database{
		Views: []goschema.View{{Name: "tenant.active_users", Body: "SELECT id FROM tenant.users WHERE deleted_at IS NULL"}},
	}, &dbschematypes.DBSchema{
		Views: []dbschematypes.DBView{{
			Name:   "active_users",
			Schema: "tenant",
			Body:   "SELECT id FROM tenant.users WHERE deleted_at IS NULL",
		}},
	}, diff)

	c.Assert(diff.HasChanges(), qt.IsFalse, qt.Commentf("diff: %#v", diff))
}

func TestViews_PreservesLiteralDotAndQualifiedIdentities(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{}

	compare.Views(&goschema.Database{
		Views: []goschema.View{
			{Name: `"tenant.data"`, Body: "SELECT 'literal'"},
			{Name: "tenant.data", Body: "SELECT 'qualified'"},
		},
	}, &dbschematypes.DBSchema{
		Views: []dbschematypes.DBView{
			{Name: "tenant.data", Body: "SELECT 'literal'"},
			{Name: "data", Schema: "tenant", Body: "SELECT 'qualified'"},
		},
	}, diff)

	c.Assert(diff.HasChanges(), qt.IsFalse, qt.Commentf("diff: %#v", diff))
}

func TestViews_DetectsAmbiguousDatabaseSchemasForUnqualifiedGeneratedView(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{}

	compare.Views(&goschema.Database{
		Views: []goschema.View{{Name: "active_users", Body: "SELECT id FROM users"}},
	}, &dbschematypes.DBSchema{
		Views: []dbschematypes.DBView{
			{Name: "active_users", Schema: "tenant", Body: "SELECT id FROM tenant.users"},
			{Name: "active_users", Schema: "other", Body: "SELECT id FROM other.users"},
		},
	}, diff)

	c.Assert(diff.ViewsAdded, qt.DeepEquals, []string{"active_users"})
	c.Assert(diff.ViewsRemoved, qt.DeepEquals, []string{"other.active_users", "tenant.active_users"})
	c.Assert(diff.ViewsModified, qt.HasLen, 0)
}

func TestViews_IgnoresMySQLCanonicalViewBody(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{}

	compare.Views(&goschema.Database{
		Views: []goschema.View{{Name: "live_products", Body: "SELECT id, name FROM products WHERE archived = false"}},
	}, &dbschematypes.DBSchema{
		Views: []dbschematypes.DBView{{
			Name:   "live_products",
			Schema: "ptah_issue_502",
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

// TestMaterializedViews_IgnoresCatalogAddedSchemaQualifier pins the readback
// normalization a materialized view gets against the one an ordinary view
// already got.
//
// A server records the definition it resolved rather than the text the author
// wrote: measured on PostgreSQL 18.4 an authored `FROM users` comes back from
// pg_get_viewdef as `FROM analytics.users`, and measured on ClickHouse 26.7.3.19
// system.tables.as_select answers `FROM mvqual.users` for the same authored
// body. The plain view beside it is the control -- it round-tripped before this
// change and must keep doing so.
func TestMaterializedViews_IgnoresCatalogAddedSchemaQualifier(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{}

	compare.MaterializedViews(&goschema.Database{
		MaterializedViews: []goschema.MaterializedView{{
			Name: "analytics.user_counts",
			Body: "SELECT count(*) AS c FROM users",
		}},
		Views: []goschema.View{{
			Name: "analytics.user_counts_plain",
			Body: "SELECT count(*) AS c FROM users",
		}},
	}, &dbschematypes.DBSchema{
		MatViews: []dbschematypes.DBMatView{{
			Name:   "user_counts",
			Schema: "analytics",
			Body:   "SELECT count(*) AS c FROM analytics.users",
		}},
		Views: []dbschematypes.DBView{{
			Name:   "user_counts_plain",
			Schema: "analytics",
			Body:   "SELECT count(*) AS c FROM analytics.users",
		}},
	}, diff)

	c.Assert(diff.MaterializedViewsModified, qt.HasLen, 0)
	c.Assert(diff.MaterializedViewsAdded, qt.HasLen, 0)
	c.Assert(diff.MaterializedViewsRemoved, qt.HasLen, 0)
}

// TestMaterializedViews_DetectsAuthoredSchemaQualifierChange is the inverse
// control for the normalization above: a qualifier the author wrote is part of
// the declaration, so a body naming a different schema is still a change. A
// normalization that stripped every qualifier rather than only the one the
// object's own schema adds would report no change here.
func TestMaterializedViews_DetectsAuthoredSchemaQualifierChange(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{}

	compare.MaterializedViews(&goschema.Database{
		MaterializedViews: []goschema.MaterializedView{{
			Name: "analytics.user_counts",
			Body: "SELECT count(*) AS c FROM archive.users",
		}},
	}, &dbschematypes.DBSchema{
		MatViews: []dbschematypes.DBMatView{{
			Name:   "user_counts",
			Schema: "analytics",
			Body:   "SELECT count(*) AS c FROM analytics.users",
		}},
	}, diff)

	c.Assert(diff.MaterializedViewsModified, qt.HasLen, 1)
	c.Assert(diff.MaterializedViewsModified[0].Changes["body"], qt.Not(qt.Equals), "")
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
