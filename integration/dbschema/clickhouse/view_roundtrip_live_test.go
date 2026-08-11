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

func readClickHouseViews(t *testing.T, db *sql.DB, database string) *dbschematypes.DBSchema {
	t.Helper()
	schema, err := clickhousedb.NewClickHouseReader(db, database).ReadSchema()
	qt.Assert(t, err, qt.IsNil)
	return &dbschematypes.DBSchema{Views: schema.Views}
}

func executeClickHouseViewPlan(t *testing.T, db *sql.DB, statements []string) {
	t.Helper()
	for _, statement := range statements {
		_, err := db.ExecContext(t.Context(), statement)
		qt.Assert(t, err, qt.IsNil, qt.Commentf("execute ClickHouse view plan: %s", statement))
	}
}

func TestViewLifecycleRoundTripsLive(t *testing.T) {
	c := qt.New(t)
	db, database := openLiveClickHouseRealmDatabase(t, "PTAH_CLICKHOUSE_REALM_TEST_URL")
	sourceTable := sqlident.Qualified(platform.ClickHouse, database, "users")
	viewName := database + ".active_users"
	_, err := db.ExecContext(
		t.Context(),
		"CREATE TABLE "+sourceTable+" (id UInt64, active Bool) ENGINE = MergeTree ORDER BY id",
	)
	c.Assert(err, qt.IsNil)

	created := &goschema.Database{Views: []goschema.View{{
		StructName: "ActiveUsers",
		Name:       viewName,
		Body:       "SELECT id FROM " + sourceTable + " WHERE active = true",
	}}}
	creationDiff := schemadiff.CompareWithDialect(
		created,
		readClickHouseViews(t, db, database),
		platform.ClickHouse,
	)
	c.Assert(creationDiff.ViewsAdded, qt.DeepEquals, []string{viewName})
	createStatements, err := planner.GenerateSchemaDiffSQLStatements(
		creationDiff,
		created,
		platform.ClickHouse,
	)
	c.Assert(err, qt.IsNil)
	c.Assert(createStatements, qt.HasLen, 1)
	c.Assert(createStatements[0], qt.Contains, "CREATE VIEW ")
	executeClickHouseViewPlan(t, db, createStatements)

	createdReadback := readClickHouseViews(t, db, database)
	c.Assert(createdReadback.Views, qt.HasLen, 1)
	c.Assert(createdReadback.Views[0].Body, qt.Contains, "users")
	c.Assert(
		schemadiff.CompareWithDialect(created, createdReadback, platform.ClickHouse).HasChanges(),
		qt.IsFalse,
	)

	replaced := &goschema.Database{Views: []goschema.View{{
		StructName: "ActiveUsers",
		Name:       viewName,
		Body:       "SELECT id FROM " + sourceTable + " WHERE active = false",
	}}}
	replacementDiff := schemadiff.CompareWithDialect(
		replaced,
		createdReadback,
		platform.ClickHouse,
	)
	c.Assert(replacementDiff.ViewsModified, qt.HasLen, 1)
	replaceStatements, err := planner.GenerateSchemaDiffSQLStatements(
		replacementDiff,
		replaced,
		platform.ClickHouse,
	)
	c.Assert(err, qt.IsNil)
	c.Assert(replaceStatements, qt.HasLen, 1)
	c.Assert(replaceStatements[0], qt.Contains, "CREATE OR REPLACE VIEW ")
	executeClickHouseViewPlan(t, db, replaceStatements)

	replacedReadback := readClickHouseViews(t, db, database)
	c.Assert(
		schemadiff.CompareWithDialect(replaced, replacedReadback, platform.ClickHouse).HasChanges(),
		qt.IsFalse,
	)

	removalDiff := schemadiff.CompareWithDialect(
		&goschema.Database{},
		replacedReadback,
		platform.ClickHouse,
	)
	c.Assert(removalDiff.ViewsRemoved, qt.DeepEquals, []string{viewName})
	dropStatements, err := planner.GenerateSchemaDiffSQLStatements(
		removalDiff,
		&goschema.Database{},
		platform.ClickHouse,
	)
	c.Assert(err, qt.IsNil)
	c.Assert(dropStatements, qt.HasLen, 1)
	c.Assert(dropStatements[0], qt.Contains, "DROP VIEW IF EXISTS ")
	executeClickHouseViewPlan(t, db, dropStatements)

	removedReadback := readClickHouseViews(t, db, database)
	c.Assert(removedReadback.Views, qt.HasLen, 0)
	c.Assert(
		schemadiff.CompareWithDialect(&goschema.Database{}, removedReadback, platform.ClickHouse).HasChanges(),
		qt.IsFalse,
	)
}
