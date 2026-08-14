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

// TestMaterializedViews_IgnoresCatalogQualifierWhenTheBodyAliasesItsSource pins
// the same readback normalization for a body that prefixes its columns.
//
// Measured on ClickHouse 26.7.3.19, a body authored as
// `SELECT u.id AS id FROM users AS u` comes back from system.tables.as_select as
// `SELECT u.id AS id FROM <database>.users AS u`: the alias survives untouched
// and only the relation gains the database. Reading `u.` as an authored schema
// refused the qualifier-stripping comparison and reported a body change, and on
// ClickHouse that plan is a DROP VIEW and a CREATE, which destroys the rows the
// materialized view had accumulated.
//
// The plain view beside it carries the same body, because the guard is shared:
// the two kinds must answer alike.
func TestMaterializedViews_IgnoresCatalogQualifierWhenTheBodyAliasesItsSource(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{}

	compare.MaterializedViews(&goschema.Database{
		MaterializedViews: []goschema.MaterializedView{{
			Name: "analytics.user_ids",
			Body: "SELECT u.id AS id FROM users AS u",
		}},
		Views: []goschema.View{{
			Name: "analytics.user_ids_plain",
			Body: "SELECT u.id AS id FROM users AS u",
		}},
	}, &dbschematypes.DBSchema{
		MatViews: []dbschematypes.DBMatView{{
			Name:   "user_ids",
			Schema: "analytics",
			Body:   "SELECT u.id AS id FROM analytics.users AS u",
		}},
		Views: []dbschematypes.DBView{{
			Name:   "user_ids_plain",
			Schema: "analytics",
			Body:   "SELECT u.id AS id FROM analytics.users AS u",
		}},
	}, diff)

	c.Assert(diff.MaterializedViewsModified, qt.HasLen, 0)
	c.Assert(diff.MaterializedViewsAdded, qt.HasLen, 0)
	c.Assert(diff.MaterializedViewsRemoved, qt.HasLen, 0)
}

// TestViews_IgnoresCatalogQualifierWhenTheBodyAliasesItsSource is the plain-view
// half of the same guard, driven through the view comparator itself rather than
// as a bystander of the materialized-view one.
func TestViews_IgnoresCatalogQualifierWhenTheBodyAliasesItsSource(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{}

	compare.ViewsWithDialect(&goschema.Database{
		Views: []goschema.View{{
			Name: "analytics.user_ids_plain",
			Body: "SELECT u.id AS id FROM users AS u",
		}},
	}, &dbschematypes.DBSchema{
		Views: []dbschematypes.DBView{{
			Name:   "user_ids_plain",
			Schema: "analytics",
			Body:   "SELECT u.id AS id FROM analytics.users AS u",
		}},
	}, diff, "clickhouse")

	c.Assert(diff.ViewsModified, qt.HasLen, 0)
	c.Assert(diff.ViewsAdded, qt.HasLen, 0)
	c.Assert(diff.ViewsRemoved, qt.HasLen, 0)
}

// TestMaterializedViews_DetectsAColumnQualifierChange is the inverse control for
// the alias normalization above.
//
// Only the qualifiers the declaration itself uses survive the readback strip. A
// body that reads a column off a different relation is a different body, and a
// normalization that dropped every column prefix instead of only the ones the
// catalog added would report these two as equal.
func TestMaterializedViews_DetectsAColumnQualifierChange(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{}

	compare.MaterializedViews(&goschema.Database{
		MaterializedViews: []goschema.MaterializedView{{
			Name: "analytics.joined_ids",
			Body: "SELECT a.id AS id FROM users AS a, accounts AS b",
		}},
	}, &dbschematypes.DBSchema{
		MatViews: []dbschematypes.DBMatView{{
			Name:   "joined_ids",
			Schema: "analytics",
			Body:   "SELECT b.id AS id FROM analytics.users AS a, analytics.accounts AS b",
		}},
	}, diff)

	c.Assert(diff.MaterializedViewsModified, qt.HasLen, 1)
	c.Assert(diff.MaterializedViewsModified[0].Changes["body"], qt.Not(qt.Equals), "")
}

// TestMaterializedViews_IgnoresCatalogQualifierWhenTheAliasIsTheSchemaName is
// the collision the authored-qualifier set alone cannot survive.
//
// A relation alias may be spelled the same as the database it lives in, and the
// catalog adds that same word in front of the relation. Removing every
// occurrence of the schema name took the alias prefix off the readback while the
// declaration kept it, so an unedited object read as a body change -- and on
// ClickHouse a materialized-view body change is a drop and a create.
//
// The strip is therefore confined to the positions a catalog qualifies:
// in front of a relation, and in front of the table half of a three-part column
// reference.
func TestMaterializedViews_IgnoresCatalogQualifierWhenTheAliasIsTheSchemaName(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{}

	compare.MaterializedViews(&goschema.Database{
		MaterializedViews: []goschema.MaterializedView{{
			Name: "analytics.user_ids",
			Body: "SELECT analytics.id AS id FROM users AS analytics",
		}},
		Views: []goschema.View{{
			Name: "analytics.user_ids_plain",
			Body: "SELECT analytics.id AS id FROM users AS analytics",
		}},
	}, &dbschematypes.DBSchema{
		MatViews: []dbschematypes.DBMatView{{
			Name:   "user_ids",
			Schema: "analytics",
			Body:   "SELECT analytics.id AS id FROM analytics.users AS analytics",
		}},
		Views: []dbschematypes.DBView{{
			Name:   "user_ids_plain",
			Schema: "analytics",
			Body:   "SELECT analytics.id AS id FROM analytics.users AS analytics",
		}},
	}, diff)

	c.Assert(diff.MaterializedViewsModified, qt.HasLen, 0)
	c.Assert(diff.MaterializedViewsAdded, qt.HasLen, 0)
	c.Assert(diff.MaterializedViewsRemoved, qt.HasLen, 0)
}

// TestMaterializedViews_MatchesUnqualifiedNameToTheOnlyDatabaseSchema pins that
// the two view kinds agree about what an unqualified declaration names.
//
// A catalog reports every object with a schema, and a declaration ordinarily
// carries none. Matching only on the qualified form reported the unchanged
// object as both added and removed, which the planner answers with a CREATE
// before the removal -- refused by the server, because the name is still taken.
// The plain view beside it has matched a bare name against a uniquely-named
// database view since #1276 and reported nothing; the materialized view now does
// the same.
func TestMaterializedViews_MatchesUnqualifiedNameToTheOnlyDatabaseSchema(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{}

	compare.MaterializedViews(&goschema.Database{
		MaterializedViews: []goschema.MaterializedView{{
			Name: "user_stats",
			Body: "SELECT count() AS c FROM users",
		}},
		Views: []goschema.View{{
			Name: "active_users",
			Body: "SELECT id FROM users",
		}},
	}, &dbschematypes.DBSchema{
		MatViews: []dbschematypes.DBMatView{{
			Name:   "user_stats",
			Schema: "ptah_test",
			Body:   "SELECT count() AS c FROM ptah_test.users",
		}},
		Views: []dbschematypes.DBView{{
			Name:   "active_users",
			Schema: "ptah_test",
			Body:   "SELECT id FROM ptah_test.users",
		}},
	}, diff)

	c.Assert(diff.MaterializedViewsAdded, qt.HasLen, 0)
	c.Assert(diff.MaterializedViewsRemoved, qt.HasLen, 0)
	c.Assert(diff.MaterializedViewsModified, qt.HasLen, 0)
}

// TestMaterializedViews_DetectsAmbiguousDatabaseSchemasForUnqualifiedName is the
// inverse control: two schemas holding the same name is not a match to guess at,
// so the declaration stays an addition and both live objects stay removals. This
// is the same answer TestViews_DetectsAmbiguousDatabaseSchemasForUnqualifiedGeneratedView
// pins for the other kind.
func TestMaterializedViews_DetectsAmbiguousDatabaseSchemasForUnqualifiedName(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{}

	compare.MaterializedViews(&goschema.Database{
		MaterializedViews: []goschema.MaterializedView{{
			Name: "user_stats",
			Body: "SELECT count() AS c FROM users",
		}},
	}, &dbschematypes.DBSchema{
		MatViews: []dbschematypes.DBMatView{
			{Name: "user_stats", Schema: "tenant", Body: "SELECT count() AS c FROM tenant.users"},
			{Name: "user_stats", Schema: "other", Body: "SELECT count() AS c FROM other.users"},
		},
	}, diff)

	c.Assert(diff.MaterializedViewsAdded, qt.DeepEquals, []string{"user_stats"})
	c.Assert(diff.MaterializedViewsRemoved, qt.DeepEquals, []string{"other.user_stats", "tenant.user_stats"})
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
