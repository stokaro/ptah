//go:build integration

package clickhouse_test

import (
	"database/sql"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	clickhousedb "go.5x5.cz/ptah/internal/dbschema/clickhouse"
	"go.5x5.cz/ptah/internal/sqlident"
	"go.5x5.cz/ptah/migration/planner"
	"go.5x5.cz/ptah/migration/schemadiff"
)

func readClickHouseMaterializedViews(
	t *testing.T,
	db *sql.DB,
	database string,
) *dbschematypes.DBSchema {
	t.Helper()
	c := qt.New(t)

	schema, err := clickhousedb.NewClickHouseReader(db, database).ReadSchema()
	c.Assert(err, qt.IsNil)
	return &dbschematypes.DBSchema{MatViews: schema.MatViews}
}

// readClickHouseViewLikes reads both view kinds and nothing else, so a
// comparison against a desired schema declaring only views is not also a
// comparison about the source table.
func readClickHouseViewLikes(
	t *testing.T,
	db *sql.DB,
	database string,
) *dbschematypes.DBSchema {
	t.Helper()
	c := qt.New(t)

	schema, err := clickhousedb.NewClickHouseReader(db, database).ReadSchema()
	c.Assert(err, qt.IsNil)
	return &dbschematypes.DBSchema{Views: schema.Views, MatViews: schema.MatViews}
}

// TestMaterializedViewLifecycleRoundTripsLive is the acceptance for #1462.
//
// It declares a materialized view, plans it, executes the plan against a live
// ClickHouse, reads the catalog back and compares the read against the same
// desired state, which must produce no change at all. Only then does it change
// the query, and the second comparison must produce exactly one.
//
// A rendered statement is not an applied one, so every statement here is
// executed and every claim about the server is read back from system.tables.
func TestMaterializedViewLifecycleRoundTripsLive(t *testing.T) {
	c := qt.New(t)
	db, database := openLiveClickHouseRealmDatabase(t, "PTAH_CLICKHOUSE_REALM_TEST_URL")
	sourceTable := sqlident.Qualified(platform.ClickHouse, database, "users")
	viewName := database + ".user_counts"
	// A plain view stands beside the materialized one for the whole test. The
	// two engines differ by one word in system.tables, so without a plain view
	// present a read that selected the wrong engine would return the same set
	// and look correct.
	plainView := sqlident.Qualified(platform.ClickHouse, database, "active_users")
	executeClickHouseViewPlan(t, db, []string{
		"CREATE TABLE " + sourceTable + " (id UInt64, active Bool) ENGINE = MergeTree ORDER BY id",
		"CREATE VIEW " + plainView + " AS SELECT id FROM " + sourceTable,
	})

	created := &goschema.Database{MaterializedViews: []goschema.MaterializedView{{
		StructName: "UserCounts",
		Name:       viewName,
		Body:       "SELECT count() AS c FROM " + sourceTable + " WHERE active = true",
	}}}
	creationDiff := schemadiff.CompareWithDialect(
		created,
		readClickHouseMaterializedViews(t, db, database),
		platform.ClickHouse,
	)
	c.Assert(creationDiff.MaterializedViewsAdded, qt.DeepEquals, []string{viewName})
	createStatements, err := planner.GenerateSchemaDiffSQLStatements(
		creationDiff,
		created,
		platform.ClickHouse,
	)
	c.Assert(err, qt.IsNil)
	c.Assert(createStatements, qt.HasLen, 1)
	c.Assert(createStatements[0], qt.Contains, "CREATE MATERIALIZED VIEW ")
	c.Assert(createStatements[0], qt.Contains, "ENGINE = MergeTree ORDER BY tuple()")
	executeClickHouseViewPlan(t, db, createStatements)

	createdReadback := readClickHouseMaterializedViews(t, db, database)
	c.Assert(createdReadback.MatViews, qt.HasLen, 1)
	c.Assert(createdReadback.MatViews[0].Name, qt.Equals, "user_counts")
	c.Assert(createdReadback.MatViews[0].Body, qt.Contains, "users")
	c.Assert(
		schemadiff.CompareWithDialect(created, createdReadback, platform.ClickHouse).HasChanges(),
		qt.IsFalse,
	)

	// The storage table ClickHouse creates for the view is a real MergeTree
	// table in the same database. It must not arrive as a table the desired
	// schema never declared, or the very next plan would drop the view's own
	// storage.
	fullReadback, err := clickhousedb.NewClickHouseReader(db, database).ReadSchema()
	c.Assert(err, qt.IsNil)
	c.Assert(fullReadback.Tables, qt.HasLen, 1)
	c.Assert(fullReadback.Tables[0].Name, qt.Equals, "users")

	changed := &goschema.Database{MaterializedViews: []goschema.MaterializedView{{
		StructName: "UserCounts",
		Name:       viewName,
		Body:       "SELECT count() AS c FROM " + sourceTable + " WHERE active = false",
	}}}
	changeDiff := schemadiff.CompareWithDialect(changed, createdReadback, platform.ClickHouse)
	c.Assert(changeDiff.MaterializedViewsModified, qt.HasLen, 1)
	c.Assert(changeDiff.MaterializedViewsAdded, qt.HasLen, 0)
	c.Assert(changeDiff.MaterializedViewsRemoved, qt.HasLen, 0)
	changeStatements, err := planner.GenerateSchemaDiffSQLStatements(
		changeDiff,
		changed,
		platform.ClickHouse,
	)
	c.Assert(err, qt.IsNil)
	c.Assert(changeStatements, qt.HasLen, 2)
	c.Assert(changeStatements[0], qt.Contains, "DROP VIEW IF EXISTS ")
	c.Assert(changeStatements[1], qt.Contains, "CREATE MATERIALIZED VIEW ")
	executeClickHouseViewPlan(t, db, changeStatements)

	changedReadback := readClickHouseMaterializedViews(t, db, database)
	c.Assert(
		schemadiff.CompareWithDialect(changed, changedReadback, platform.ClickHouse).HasChanges(),
		qt.IsFalse,
	)

	removalDiff := schemadiff.CompareWithDialect(
		&goschema.Database{},
		changedReadback,
		platform.ClickHouse,
	)
	c.Assert(removalDiff.MaterializedViewsRemoved, qt.DeepEquals, []string{viewName})
	dropStatements, err := planner.GenerateSchemaDiffSQLStatements(
		removalDiff,
		&goschema.Database{},
		platform.ClickHouse,
	)
	c.Assert(err, qt.IsNil)
	c.Assert(dropStatements, qt.HasLen, 1)
	c.Assert(dropStatements[0], qt.Contains, "DROP VIEW IF EXISTS ")
	executeClickHouseViewPlan(t, db, dropStatements)

	removedReadback := readClickHouseMaterializedViews(t, db, database)
	c.Assert(removedReadback.MatViews, qt.HasLen, 0)
	c.Assert(
		schemadiff.CompareWithDialect(
			&goschema.Database{},
			removedReadback,
			platform.ClickHouse,
		).HasChanges(),
		qt.IsFalse,
	)

	// The drop takes the storage with it: nothing is left behind for the table
	// read to report either.
	emptyReadback, err := clickhousedb.NewClickHouseReader(db, database).ReadSchema()
	c.Assert(err, qt.IsNil)
	c.Assert(emptyReadback.Tables, qt.HasLen, 1)
	c.Assert(emptyReadback.Tables[0].Name, qt.Equals, "users")
}

// TestMaterializedViewUnqualifiedBodyRoundTripsLive is the acceptance for a
// declaration that names its source without a database qualifier.
//
// Every other live test here writes the source table fully qualified, which is
// the one spelling that cannot see this: ClickHouse resolves the query at CREATE
// time and records what it resolved, so an authored `FROM users` comes back from
// system.tables.as_select as `FROM <db>.users`. Measured on server 26.7.3.19:
//
//	CREATE MATERIALIZED VIEW user_counts ... AS SELECT count() AS c FROM users
//	-> as_select = SELECT count() AS c FROM mvqual.users
//
// Nothing about the declaration changed, so the comparison after the apply must
// report no change. Reporting one would plan a DROP and a CREATE, and on
// ClickHouse the drop takes the inner storage and every accumulated row with it.
//
// The plain view beside it is the control: that half normalized the readback
// already, so a run where only the materialized half moves is the whole finding.
func TestMaterializedViewUnqualifiedBodyRoundTripsLive(t *testing.T) {
	c := qt.New(t)
	db, database := openLiveClickHouseRealmDatabase(t, "PTAH_CLICKHOUSE_REALM_TEST_URL")
	sourceTable := sqlident.Qualified(platform.ClickHouse, database, "users")
	executeClickHouseViewPlan(t, db, []string{
		"CREATE TABLE " + sourceTable + " (id UInt64, active Bool) ENGINE = MergeTree ORDER BY id",
	})

	// Unqualified on purpose: the connection's database is the realm database,
	// so this is what an author writes and what the server has to resolve.
	declared := &goschema.Database{
		MaterializedViews: []goschema.MaterializedView{{
			StructName: "UserCounts",
			Name:       database + ".user_counts",
			Body:       "SELECT count() AS c FROM users",
		}},
		Views: []goschema.View{{
			StructName: "ActiveUsers",
			Name:       database + ".active_users",
			Body:       "SELECT id FROM users",
		}},
	}
	creationDiff := schemadiff.CompareWithDialect(
		declared,
		readClickHouseViewLikes(t, db, database),
		platform.ClickHouse,
	)
	createStatements, err := planner.GenerateSchemaDiffSQLStatements(
		creationDiff,
		declared,
		platform.ClickHouse,
	)
	c.Assert(err, qt.IsNil)
	c.Assert(createStatements, qt.HasLen, 2)
	executeClickHouseViewPlan(t, db, createStatements)

	// The readback is the qualified spelling the server resolved, for both
	// kinds, which is what makes the comparison below a real question.
	readback := readClickHouseViewLikes(t, db, database)
	c.Assert(readback.MatViews, qt.HasLen, 1)
	c.Assert(readback.MatViews[0].Body, qt.Contains, database+".users")
	c.Assert(readback.Views, qt.HasLen, 1)
	c.Assert(readback.Views[0].Body, qt.Contains, database+".users")

	settledDiff := schemadiff.CompareWithDialect(declared, readback, platform.ClickHouse)
	c.Assert(settledDiff.MaterializedViewsModified, qt.HasLen, 0)
	c.Assert(settledDiff.ViewsModified, qt.HasLen, 0)
	c.Assert(settledDiff.HasChanges(), qt.IsFalse, qt.Commentf("settled diff: %+v", settledDiff))

	// A body that really did change is still a change: the normalization removes
	// the qualifier the server added, not the difference the author made.
	changed := &goschema.Database{
		MaterializedViews: []goschema.MaterializedView{{
			StructName: "UserCounts",
			Name:       database + ".user_counts",
			Body:       "SELECT count() AS c FROM users WHERE active = true",
		}},
		Views: declared.Views,
	}
	changeDiff := schemadiff.CompareWithDialect(changed, readback, platform.ClickHouse)
	c.Assert(changeDiff.MaterializedViewsModified, qt.HasLen, 1)
	c.Assert(changeDiff.ViewsModified, qt.HasLen, 0)
}

// TestMaterializedViewAliasedBodyRoundTripsLive is the acceptance for a
// declaration that gives its source an alias.
//
// It is the same readback one spelling further along, and the spelling that
// separates "the declaration qualifies a relation" from "the declaration
// prefixes a column". Measured on server 26.7.3.19, both view kinds:
//
//	CREATE MATERIALIZED VIEW user_ids ... AS SELECT u.id AS id FROM users AS u
//	-> as_select = SELECT u.id AS id FROM <db>.users AS u
//
// The alias survives untouched and only the relation gained the database. A
// comparison that read `u.` as an authored schema qualifier refused to remove
// the one the server added, reported a body change on a declaration nobody
// edited, and the ClickHouse planner answers that with a DROP VIEW and a CREATE,
// which destroys every row the materialized view had accumulated.
//
// The plain view beside it carries the same body: the guard is shared between
// the two kinds, so both had the defect and both have to answer alike now.
func TestMaterializedViewAliasedBodyRoundTripsLive(t *testing.T) {
	c := qt.New(t)
	db, database := openLiveClickHouseRealmDatabase(t, "PTAH_CLICKHOUSE_REALM_TEST_URL")
	sourceTable := sqlident.Qualified(platform.ClickHouse, database, "users")
	executeClickHouseViewPlan(t, db, []string{
		"CREATE TABLE " + sourceTable + " (id UInt64, active Bool) ENGINE = MergeTree ORDER BY id",
	})

	declared := &goschema.Database{
		MaterializedViews: []goschema.MaterializedView{{
			StructName: "UserIDs",
			Name:       database + ".user_ids",
			Body:       "SELECT u.id AS id FROM users AS u",
		}},
		Views: []goschema.View{{
			StructName: "UserIDsPlain",
			Name:       database + ".user_ids_plain",
			Body:       "SELECT u.id AS id FROM users AS u",
		}},
	}
	creationDiff := schemadiff.CompareWithDialect(
		declared,
		readClickHouseViewLikes(t, db, database),
		platform.ClickHouse,
	)
	createStatements, err := planner.GenerateSchemaDiffSQLStatements(
		creationDiff,
		declared,
		platform.ClickHouse,
	)
	c.Assert(err, qt.IsNil)
	c.Assert(createStatements, qt.HasLen, 2)
	executeClickHouseViewPlan(t, db, createStatements)

	// The readback keeps the alias and adds the database, for both kinds, which
	// is what makes the comparison below a real question rather than a string
	// equality.
	readback := readClickHouseViewLikes(t, db, database)
	c.Assert(readback.MatViews, qt.HasLen, 1)
	c.Assert(readback.MatViews[0].Body, qt.Contains, "u.id")
	c.Assert(readback.MatViews[0].Body, qt.Contains, database+".users")
	c.Assert(readback.Views, qt.HasLen, 1)
	c.Assert(readback.Views[0].Body, qt.Contains, "u.id")
	c.Assert(readback.Views[0].Body, qt.Contains, database+".users")

	settledDiff := schemadiff.CompareWithDialect(declared, readback, platform.ClickHouse)
	c.Assert(settledDiff.MaterializedViewsModified, qt.HasLen, 0)
	c.Assert(settledDiff.ViewsModified, qt.HasLen, 0)
	c.Assert(settledDiff.HasChanges(), qt.IsFalse, qt.Commentf("settled diff: %+v", settledDiff))

	// A body that really did change is still a change.
	changed := &goschema.Database{
		MaterializedViews: []goschema.MaterializedView{{
			StructName: "UserIDs",
			Name:       database + ".user_ids",
			Body:       "SELECT u.id AS id FROM users AS u WHERE u.active = true",
		}},
		Views: declared.Views,
	}
	changeDiff := schemadiff.CompareWithDialect(changed, readback, platform.ClickHouse)
	c.Assert(changeDiff.MaterializedViewsModified, qt.HasLen, 1)
	c.Assert(changeDiff.ViewsModified, qt.HasLen, 0)
}

// TestMaterializedViewAliasNamedLikeTheDatabaseRoundTripsLive is the collision
// between the two normalizations: an alias spelled exactly like the database.
//
// The catalog adds the database in front of the relation, and the declaration
// already uses that same word as a column prefix. A normalization that removed
// every occurrence of the schema name took the prefix off the readback while the
// declaration kept it, and reported a body change on an object nobody edited --
// which on ClickHouse is planned as a drop and a create.
//
// The alias here is the realm database's own name, so the two really are the
// same word on a live server rather than in a fixture.
func TestMaterializedViewAliasNamedLikeTheDatabaseRoundTripsLive(t *testing.T) {
	c := qt.New(t)
	db, database := openLiveClickHouseRealmDatabase(t, "PTAH_CLICKHOUSE_REALM_TEST_URL")
	sourceTable := sqlident.Qualified(platform.ClickHouse, database, "users")
	executeClickHouseViewPlan(t, db, []string{
		"CREATE TABLE " + sourceTable + " (id UInt64) ENGINE = MergeTree ORDER BY id",
	})

	body := "SELECT " + database + ".id AS id FROM users AS " + database
	declared := &goschema.Database{
		MaterializedViews: []goschema.MaterializedView{{
			StructName: "UserIDs",
			Name:       database + ".user_ids",
			Body:       body,
		}},
		Views: []goschema.View{{
			StructName: "UserIDsPlain",
			Name:       database + ".user_ids_plain",
			Body:       body,
		}},
	}
	creationDiff := schemadiff.CompareWithDialect(
		declared,
		readClickHouseViewLikes(t, db, database),
		platform.ClickHouse,
	)
	createStatements, err := planner.GenerateSchemaDiffSQLStatements(
		creationDiff,
		declared,
		platform.ClickHouse,
	)
	c.Assert(err, qt.IsNil)
	c.Assert(createStatements, qt.HasLen, 2)
	executeClickHouseViewPlan(t, db, createStatements)

	// The readback carries the database twice over: once as the alias the
	// declaration wrote, once as the qualifier the server resolved.
	readback := readClickHouseViewLikes(t, db, database)
	c.Assert(readback.MatViews, qt.HasLen, 1)
	c.Assert(readback.MatViews[0].Body, qt.Contains, database+".id")
	c.Assert(readback.MatViews[0].Body, qt.Contains, database+".users")

	settledDiff := schemadiff.CompareWithDialect(declared, readback, platform.ClickHouse)
	c.Assert(settledDiff.MaterializedViewsModified, qt.HasLen, 0)
	c.Assert(settledDiff.ViewsModified, qt.HasLen, 0)
	c.Assert(settledDiff.HasChanges(), qt.IsFalse, qt.Commentf("settled diff: %+v", settledDiff))
}

// TestMaterializedViewUnqualifiedNameRoundTripsLive is the acceptance for a
// declaration that names the object itself without a database.
//
// Every other live test here writes the object's name qualified, and that is the
// one spelling this cannot see. ClickHouse reports every object with the current
// database as its schema and its connection leaves the default identifier schema
// empty, so a declaration of "user_stats" and a readback of
// "<database>.user_stats" have to be recognized as one object. Matching only the
// qualified form reported the unchanged object as both added and removed, the
// planner emitted the create before the removal, and the server refused it:
//
//	Code: 57. DB::Exception: Table <db>.user_stats already exists.
//	(TABLE_ALREADY_EXISTS)
//
// The plain view standing beside it is the control: it has matched bare names
// against a uniquely-named database view since #1276, so a run where only the
// materialized half moves is the whole finding.
//
// The second apply is the point of the test: a plan produced from a settled
// database must be empty, and an empty plan is the only one that can be executed
// twice.
func TestMaterializedViewUnqualifiedNameRoundTripsLive(t *testing.T) {
	c := qt.New(t)
	db, database := openLiveClickHouseRealmDatabase(t, "PTAH_CLICKHOUSE_REALM_TEST_URL")
	sourceTable := sqlident.Qualified(platform.ClickHouse, database, "users")
	executeClickHouseViewPlan(t, db, []string{
		"CREATE TABLE " + sourceTable + " (id UInt64) ENGINE = MergeTree ORDER BY id",
	})

	// Unqualified on both halves: the object's own name as well as its source.
	declared := &goschema.Database{
		MaterializedViews: []goschema.MaterializedView{{
			StructName: "UserStats",
			Name:       "user_stats",
			Body:       "SELECT count() AS c FROM users",
		}},
		Views: []goschema.View{{
			StructName: "ActiveUsers",
			Name:       "active_users",
			Body:       "SELECT id FROM users",
		}},
	}
	creationDiff := schemadiff.CompareWithDialect(
		declared,
		readClickHouseViewLikes(t, db, database),
		platform.ClickHouse,
	)
	createStatements, err := planner.GenerateSchemaDiffSQLStatements(
		creationDiff,
		declared,
		platform.ClickHouse,
	)
	c.Assert(err, qt.IsNil)
	c.Assert(createStatements, qt.HasLen, 2)
	executeClickHouseViewPlan(t, db, createStatements)

	readback := readClickHouseViewLikes(t, db, database)
	c.Assert(readback.MatViews, qt.HasLen, 1)
	c.Assert(readback.MatViews[0].Schema, qt.Equals, database)
	c.Assert(readback.Views, qt.HasLen, 1)
	c.Assert(readback.Views[0].Schema, qt.Equals, database)

	settledDiff := schemadiff.CompareWithDialect(declared, readback, platform.ClickHouse)
	c.Assert(settledDiff.MaterializedViewsAdded, qt.HasLen, 0)
	c.Assert(settledDiff.MaterializedViewsRemoved, qt.HasLen, 0)
	c.Assert(settledDiff.HasChanges(), qt.IsFalse, qt.Commentf("settled diff: %+v", settledDiff))

	// The second apply: an empty plan, executed.
	settledStatements, err := planner.GenerateSchemaDiffSQLStatements(
		settledDiff,
		declared,
		platform.ClickHouse,
	)
	c.Assert(err, qt.IsNil)
	c.Assert(settledStatements, qt.HasLen, 0)
	executeClickHouseViewPlan(t, db, settledStatements)
}

// TestDropAllTablesResetIsReplayableLive is the acceptance for the reset
// contract every caller of DropAllTables depends on.
//
// The shadow replay in migration/generator, the dev-database cleanup in
// internal/atlasschema and the integration harness all reset with this method
// and then replay DDL into the same database. Before this change the ClickHouse
// writer dropped base tables only, and measured on server 26.7.3.19 the replay
// of the very migration that had just been reset failed on both view kinds:
//
//	CREATE VIEW <db>.plain_v               -> code: 57, Table <db>.plain_v already exists
//	CREATE MATERIALIZED VIEW <db>.stored_v -> code: 57, Table <db>.stored_v already exists
//
// A plain view stands beside the materialized one because those two are the
// separate halves of the same defect: a fix that removed only materialized
// views would leave the plain view standing and the replay still failing.
//
// The materialized view's ".inner_id.<uuid>" storage is not dropped as a table
// of its own -- DROP TABLE on it succeeds while leaving the view behind,
// pointing at nothing -- so the check that the database is empty afterwards is
// also the check that the storage left with its owner rather than separately.
func TestDropAllTablesResetIsReplayableLive(t *testing.T) {
	c := qt.New(t)
	db, database := openLiveClickHouseRealmDatabase(t, "PTAH_CLICKHOUSE_REALM_TEST_URL")
	sourceTable := sqlident.Qualified(platform.ClickHouse, database, "users")
	plainView := sqlident.Qualified(platform.ClickHouse, database, "active_users")
	storedView := sqlident.Qualified(platform.ClickHouse, database, "user_counts")
	migration := []string{
		"CREATE TABLE " + sourceTable + " (id UInt64) ENGINE = MergeTree ORDER BY id",
		"CREATE VIEW " + plainView + " AS SELECT id FROM " + sourceTable,
		"CREATE MATERIALIZED VIEW " + storedView +
			" ENGINE = MergeTree ORDER BY tuple() AS SELECT count() AS c FROM " + sourceTable,
	}
	executeClickHouseViewPlan(t, db, migration)
	executeClickHouseViewPlan(t, db, []string{"INSERT INTO " + sourceTable + " VALUES (1)"})

	err := clickhousedb.NewClickHouseWriter(db, database).DropAllTables(t.Context())
	c.Assert(err, qt.IsNil)

	var remaining uint64
	err = db.QueryRowContext(
		t.Context(),
		"SELECT count() FROM system.tables WHERE database = ? AND is_temporary = 0",
		database,
	).Scan(&remaining)
	c.Assert(err, qt.IsNil)
	c.Assert(remaining, qt.Equals, uint64(0))

	// The reset is only a reset if the same migration runs again on top of it.
	executeClickHouseViewPlan(t, db, migration)

	readback, err := clickhousedb.NewClickHouseReader(db, database).ReadSchema()
	c.Assert(err, qt.IsNil)
	c.Assert(readback.Tables, qt.HasLen, 1)
	c.Assert(readback.Tables[0].Name, qt.Equals, "users")
	c.Assert(readback.Views, qt.HasLen, 1)
	c.Assert(readback.Views[0].Name, qt.Equals, "active_users")
	c.Assert(readback.MatViews, qt.HasLen, 1)
	c.Assert(readback.MatViews[0].Name, qt.Equals, "user_counts")
}

// TestMaterializedViewStoresRatherThanRecomputesLive is the measurement behind
// capability.MaterializedViews for this preset.
//
// A plain view and a materialized view are built over the same query. Both
// report 1 after an INSERT, which alone proves nothing -- MySQL's parsed-and-
// dropped MATERIALIZED does the same. The source rows are then removed: the
// plain view falls back to 0 because it recomputes, and the materialized view
// keeps 1 because its result is stored, which is what the key claims.
func TestMaterializedViewStoresRatherThanRecomputesLive(t *testing.T) {
	c := qt.New(t)
	db, database := openLiveClickHouseRealmDatabase(t, "PTAH_CLICKHOUSE_REALM_TEST_URL")
	sourceTable := sqlident.Qualified(platform.ClickHouse, database, "users")
	plainView := sqlident.Qualified(platform.ClickHouse, database, "plain_counts")
	storedView := sqlident.Qualified(platform.ClickHouse, database, "stored_counts")

	setup := []string{
		"CREATE TABLE " + sourceTable + " (id UInt64) ENGINE = MergeTree ORDER BY id",
		"CREATE VIEW " + plainView + " AS SELECT count() AS c FROM " + sourceTable,
		"CREATE MATERIALIZED VIEW " + storedView +
			" ENGINE = MergeTree ORDER BY tuple() AS SELECT count() AS c FROM " + sourceTable,
		"INSERT INTO " + sourceTable + " VALUES (1)",
	}
	executeClickHouseViewPlan(t, db, setup)

	readSum := func(view string) uint64 {
		var total uint64
		err := db.QueryRowContext(t.Context(), "SELECT sum(c) FROM "+view).Scan(&total)
		c.Assert(err, qt.IsNil)
		return total
	}

	c.Assert(readSum(plainView), qt.Equals, uint64(1))
	c.Assert(readSum(storedView), qt.Equals, uint64(1))

	executeClickHouseViewPlan(t, db, []string{"TRUNCATE TABLE " + sourceTable})

	c.Assert(readSum(plainView), qt.Equals, uint64(0))
	c.Assert(readSum(storedView), qt.Equals, uint64(1))
}

// TestViewKindChangeAppliesLive is the acceptance for a name that changes kind
// without changing its name.
//
// Both view comparators are independent, so this arrives as an addition of one
// kind next to a removal of the other, and ClickHouse resolves both against one
// namespace: a plan that emitted the create before the removal is refused with
// "Code: 57 ... Table ... already exists. (TABLE_ALREADY_EXISTS)". A rendered
// statement is not an applied one, so the plan is executed here and the catalog
// is read back.
//
// Both directions run, because the two are separate branches of the planner and
// a fix that reordered only one of them would leave the other unapplyable.
func TestViewKindChangeAppliesLive(t *testing.T) {
	c := qt.New(t)
	db, database := openLiveClickHouseRealmDatabase(t, "PTAH_CLICKHOUSE_REALM_TEST_URL")
	sourceTable := sqlident.Qualified(platform.ClickHouse, database, "users")
	viewName := database + ".user_counts"
	body := "SELECT count() AS c FROM " + sourceTable
	executeClickHouseViewPlan(t, db, []string{
		"CREATE TABLE " + sourceTable + " (id UInt64) ENGINE = MergeTree ORDER BY id",
		"CREATE MATERIALIZED VIEW " +
			sqlident.Qualified(platform.ClickHouse, database, "user_counts") +
			" ENGINE = MergeTree ORDER BY tuple() AS " + body,
	})

	asPlainView := &goschema.Database{Views: []goschema.View{{
		StructName: "UserCounts",
		Name:       viewName,
		Body:       body,
	}}}
	toPlainDiff := schemadiff.CompareWithDialect(
		asPlainView,
		readClickHouseViewLikes(t, db, database),
		platform.ClickHouse,
	)
	c.Assert(toPlainDiff.ViewsAdded, qt.DeepEquals, []string{viewName})
	c.Assert(toPlainDiff.MaterializedViewsRemoved, qt.DeepEquals, []string{viewName})
	toPlainStatements, err := planner.GenerateSchemaDiffSQLStatements(
		toPlainDiff,
		asPlainView,
		platform.ClickHouse,
	)
	c.Assert(err, qt.IsNil)
	c.Assert(toPlainStatements, qt.HasLen, 2)
	// Executed before the shape is inspected, so the server is the one that
	// judges the order: a plan that creates first is refused outright.
	executeClickHouseViewPlan(t, db, toPlainStatements)
	c.Assert(toPlainStatements[0], qt.Contains, "DROP VIEW IF EXISTS ")
	c.Assert(toPlainStatements[1], qt.Contains, "CREATE VIEW ")

	plainReadback := readClickHouseViewLikes(t, db, database)
	c.Assert(plainReadback.Views, qt.HasLen, 1)
	c.Assert(plainReadback.MatViews, qt.HasLen, 0)
	c.Assert(
		schemadiff.CompareWithDialect(asPlainView, plainReadback, platform.ClickHouse).HasChanges(),
		qt.IsFalse,
	)

	asMaterialized := &goschema.Database{MaterializedViews: []goschema.MaterializedView{{
		StructName: "UserCounts",
		Name:       viewName,
		Body:       body,
	}}}
	toMaterializedDiff := schemadiff.CompareWithDialect(
		asMaterialized,
		plainReadback,
		platform.ClickHouse,
	)
	c.Assert(toMaterializedDiff.MaterializedViewsAdded, qt.DeepEquals, []string{viewName})
	c.Assert(toMaterializedDiff.ViewsRemoved, qt.DeepEquals, []string{viewName})
	toMaterializedStatements, err := planner.GenerateSchemaDiffSQLStatements(
		toMaterializedDiff,
		asMaterialized,
		platform.ClickHouse,
	)
	c.Assert(err, qt.IsNil)
	c.Assert(toMaterializedStatements, qt.HasLen, 2)
	executeClickHouseViewPlan(t, db, toMaterializedStatements)
	c.Assert(toMaterializedStatements[0], qt.Contains, "DROP VIEW IF EXISTS ")
	c.Assert(toMaterializedStatements[1], qt.Contains, "CREATE MATERIALIZED VIEW ")

	materializedReadback := readClickHouseViewLikes(t, db, database)
	c.Assert(materializedReadback.Views, qt.HasLen, 0)
	c.Assert(materializedReadback.MatViews, qt.HasLen, 1)
	c.Assert(
		schemadiff.CompareWithDialect(
			asMaterialized,
			materializedReadback,
			platform.ClickHouse,
		).HasChanges(),
		qt.IsFalse,
	)
}
