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
