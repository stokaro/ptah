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
	schema, err := clickhousedb.NewClickHouseReader(db, database).ReadSchema()
	qt.Assert(t, err, qt.IsNil)
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
	schema, err := clickhousedb.NewClickHouseReader(db, database).ReadSchema()
	qt.Assert(t, err, qt.IsNil)
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
		qt.Assert(t, err, qt.IsNil)
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
