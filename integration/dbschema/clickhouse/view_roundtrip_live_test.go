//go:build integration

package clickhouse_test

import (
	"database/sql"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	clickhousedb "ptah.run/internal/dbschema/clickhouse"
	"ptah.run/internal/sqlident"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

func readClickHouseViews(t *testing.T, db *sql.DB, database string) *catalog.Database {
	c := qt.New(t)
	t.Helper()
	schema, err := clickhousedb.NewClickHouseReader(db, database).ReadSchemaContext(t.Context())
	c.Assert(err, qt.IsNil)
	return &catalog.Database{Views: schema.Views}
}

func executeClickHouseViewPlan(t *testing.T, db *sql.DB, statements []string) {
	c := qt.New(t)
	t.Helper()
	for _, statement := range statements {
		_, err := db.ExecContext(t.Context(), statement)
		c.Assert(err, qt.IsNil, qt.Commentf("execute ClickHouse view plan: %s", statement))
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

	created := &schemamodel.Database{Views: []schemamodel.View{{
		StructName: "ActiveUsers",
		Name:       viewName,
		Body:       "SELECT id FROM " + sourceTable + " WHERE active = true",
	}}}
	creationDiff := schemadiff.CompareWithDialect(
		created,
		readClickHouseViews(t, db, database),
		platform.ClickHouse,
	)
	c.Assert(creationDiff.ViewsAdded.Names(), qt.DeepEquals, []string{viewName})
	createStatements, err := planner.GenerateSchemaDiffSQLStatements(
		creationDiff,

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

	replaced := &schemamodel.Database{Views: []schemamodel.View{{
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
		&schemamodel.Database{},
		replacedReadback,
		platform.ClickHouse,
	)
	c.Assert(removalDiff.ViewsRemoved.Names(), qt.DeepEquals, []string{viewName})
	dropStatements, err := planner.GenerateSchemaDiffSQLStatements(
		removalDiff,

		platform.ClickHouse,
	)

	c.Assert(err, qt.IsNil)
	c.Assert(dropStatements, qt.HasLen, 1)
	c.Assert(dropStatements[0], qt.Contains, "DROP VIEW IF EXISTS ")
	executeClickHouseViewPlan(t, db, dropStatements)

	removedReadback := readClickHouseViews(t, db, database)
	c.Assert(removedReadback.Views, qt.HasLen, 0)
	c.Assert(
		schemadiff.CompareWithDialect(&schemamodel.Database{}, removedReadback, platform.ClickHouse).HasChanges(),
		qt.IsFalse,
	)
}
