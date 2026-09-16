//go:build integration

package migrator_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/sqlutil"
	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
	"ptah.run/internal/ddltx"
	"ptah.run/migration/migrator"
)

// The versioned migration path carries a Spanner arm in several places --
// the revision table's timestamp type, the metadata schema lookup, the
// revision DDL branch the PostgreSQL family shares -- and every one of them
// was asserted as a string produced by a zero-value Migrator. A string
// assertion cannot say whether PGAdapter parses what the arm produced, nor
// whether a revision row written through it reads back, so the whole
// versioned path was declared for this target and never run against it.
//
// These tests run it. They live here rather than beside the migrator because
// only a server decides them: the revision DDL has to survive PGAdapter's
// parser, the revision row has to survive an INSERT and a SELECT through a
// TIMESTAMPTZ column Spanner spells its own way, and the rollback has to
// remove a table on an endpoint that refuses DDL inside the transaction the
// migrator opens around it.

// spannerObjectNames names the two tables one run creates: the revision table
// the migrator keeps its bookkeeping in, and the table the migration applies.
// The suffix keeps concurrent runs off each other's objects, since the
// emulator serves one database and a fixed name would make a second run
// collide rather than fail.
func spannerObjectNames() (revisionTable, subjectTable string) {
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	return "ptah_sp_revisions_" + suffix, "ptah_sp_items_" + suffix
}

// TestSpannerVersionedMigrationPathLive_HappyPath walks the path end to end:
// initialize the revision metadata, apply one migration, read the revision
// back through the migrator's own reader, and roll it off again.
//
// Each step asserts against the catalog as well as against the migrator, so a
// step that reported success without reaching the server is caught. That is
// the failure this engine is exposed to: Spanner refuses DDL inside an
// explicit transaction, so the migrator applies a body through a transaction
// object whose Commit and Rollback do nothing, and a body that never ran would
// still see a clean commit.
func TestSpannerVersionedMigrationPathLive_HappyPath(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.Spanner)
	c := qt.New(t)
	ctx := t.Context()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	revisionTable, subjectTable := spannerObjectNames()
	defer dropSpannerTables(c, conn, subjectTable, revisionTable)

	migration := migrator.CreateMigrationFromSQL(
		1,
		"create items",
		`CREATE TABLE "`+subjectTable+`" ("id" bigint PRIMARY KEY, "name" varchar(200) NOT NULL)`,
		`DROP TABLE "`+subjectTable+`"`,
	)
	m := migrator.NewMigrator(conn, migrator.NewRegisteredMigrationProvider(migration)).
		WithMigrationsTable("", revisionTable)

	// 1. Initialize. The DDL this writes is the one arm nothing had put through
	// PGAdapter: Spanner's PostgreSQL interface has no `timestamp`, so the
	// revision table's own CREATE is refused unless the column is spelled
	// TIMESTAMPTZ.
	c.Assert(m.Initialize(ctx), qt.IsNil)
	c.Assert(spannerTableExists(c, conn, revisionTable), qt.IsTrue)

	initialVersion, err := m.GetCurrentVersion(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(initialVersion, qt.Equals, int64(0))

	// 2. Apply one migration. The body reaches the server outside a
	// transaction, which is what this endpoint accepts.
	c.Assert(m.MigrateUp(ctx), qt.IsNil)
	c.Assert(spannerTableExists(c, conn, subjectTable), qt.IsTrue)

	// 3. Read the revision back. The row went in through a TIMESTAMPTZ column
	// and a `$1` placeholder rewrite, and comes back through the migrator's own
	// reader rather than through a query this test wrote, so the assertion
	// covers the write and the read the product performs.
	revisions, err := m.GetRevisions(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(revisions, qt.HasLen, 1)
	c.Assert(revisions[0].Version, qt.Equals, int64(1))
	c.Assert(revisions[0].Description, qt.Equals, "create items")
	c.Assert(revisions[0].Dirty, qt.IsFalse)
	c.Assert(revisions[0].AppliedAt.IsZero(), qt.IsFalse)

	appliedVersion, err := m.GetCurrentVersion(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(appliedVersion, qt.Equals, int64(1))

	// 4. Roll it back. The down body and the revision delete travel the same
	// no-op transaction, so both have to have reached the server on their own.
	c.Assert(m.MigrateDown(ctx), qt.IsNil)
	c.Assert(spannerTableExists(c, conn, subjectTable), qt.IsFalse)

	remaining, err := m.GetRevisions(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(remaining, qt.HasLen, 0)

	finalVersion, err := m.GetCurrentVersion(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(finalVersion, qt.Equals, int64(0))
}

// TestSpannerVersionedMigrationPathLive_FailurePath drives a migration whose
// body the server refuses, and asserts what the revision table holds
// afterwards.
//
// The capability preset answers false for DDLInsideTransaction, which routes
// the apply through a transaction object whose Commit and Rollback do nothing.
// Only a server says what that leaves behind, and what it says is below: a body
// that fails partway keeps every statement that already ran, and the revision
// row is the dirty record an operator has to act on. The survival assertion is
// read out of internal/ddltx rather than written as a literal, so the class and
// the server cannot disagree without this test saying so.
func TestSpannerVersionedMigrationPathLive_FailurePath(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.Spanner)
	c := qt.New(t)
	ctx := t.Context()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	revisionTable, subjectTable := spannerObjectNames()
	defer dropSpannerTables(c, conn, subjectTable, revisionTable)

	// The second statement names a type Spanner's PostgreSQL interface does not
	// have, so the server refuses it after the first statement has been taken.
	migration := migrator.CreateMigrationFromSQL(
		1,
		"create items then fail",
		`CREATE TABLE "`+subjectTable+`" ("id" bigint PRIMARY KEY);`+"\n"+
			`ALTER TABLE "`+subjectTable+`" ADD COLUMN "amount" money;`,
		`DROP TABLE "`+subjectTable+`"`,
	)
	m := migrator.NewMigrator(conn, migrator.NewRegisteredMigrationProvider(migration)).
		WithMigrationsTable("", revisionTable)

	err = m.MigrateUp(ctx)
	c.Assert(err, qt.IsNotNil)

	// The statement that ran before the refusal is still there. That is the
	// measurement: the migrator's apply path rolled back, and on this endpoint
	// the rollback undid nothing, because the DDL was never inside a
	// transaction to begin with.
	c.Assert(
		spannerTableExists(c, conn, subjectTable),
		qt.Equals,
		ddltx.AllStatementsDurable(ddltx.ClassOf(conn.Info().Dialect)),
	)

	revisions, err := m.GetRevisions(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(revisions, qt.HasLen, 1)
	c.Assert(revisions[0].Version, qt.Equals, int64(1))
	c.Assert(revisions[0].Dirty, qt.IsTrue)
	c.Assert(revisions[0].Error, qt.Not(qt.Equals), "")
	c.Assert(revisions[0].ErrorStatement, qt.Contains, "money")

	// The revision says nothing is applied, and the failed run is not a version
	// the database may be resumed past.
	currentVersion, err := m.GetCurrentVersion(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(currentVersion, qt.Equals, int64(0))
}

// TestSpannerMigratorKeepsEveryStatementItRanLive pins what a failed body
// leaves behind on Spanner, which is the DDL transaction class internal/ddltx
// assigns.
//
// The second statement fails. The table the first one created is still there
// afterwards, and the revision row says one statement of two was applied, so a
// retry resumes at the statement that failed rather than at the body's first
// one. That is the NoTransaction contract: the preset refuses DDL inside an
// explicit transaction, so the migrator applies the body through a transaction
// whose Commit and Rollback do nothing and each statement commits as it runs.
// Under the Transactional contract the CREATE would have rolled back with the
// body and the prefix would be zero (stokaro/ptah#3320).
func TestSpannerMigratorKeepsEveryStatementItRanLive(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.Spanner)
	c := qt.New(t)
	ctx := t.Context()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	revisionTable, subjectTable := spannerObjectNames()
	defer dropSpannerTables(c, conn, subjectTable, revisionTable)

	migration := migrator.CreateMigrationFromSQL(
		1,
		"durable prefix",
		`CREATE TABLE "`+subjectTable+`" ("id" bigint PRIMARY KEY);`+"\n"+
			`ALTER TABLE "`+subjectTable+`" ADD COLUMN "amount" money;`,
		`DROP TABLE "`+subjectTable+`"`,
	)
	m := migrator.NewMigrator(conn, migrator.NewRegisteredMigrationProvider(migration)).
		WithMigrationsTable("", revisionTable)

	err = m.MigrateUp(ctx)

	c.Assert(err, qt.ErrorMatches, `(?s).*Type <money> is not supported.*`)
	// The class is read from the package rather than restated, so a class that
	// stopped matching this endpoint fails here with the server's own answer.
	class := ddltx.ClassOf(conn.Info().Dialect)
	c.Assert(spannerTableExists(c, conn, subjectTable), qt.Equals, ddltx.AllStatementsDurable(class))

	revisions, err := m.GetRevisions(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(revisions, qt.HasLen, 1)
	c.Assert(revisions[0].Version, qt.Equals, int64(1))
	c.Assert(revisions[0].Applied, qt.Equals, 1)
	c.Assert(revisions[0].Total, qt.Equals, 2)
	c.Assert(revisions[0].Dirty, qt.IsTrue)
}

// spannerTableExists asks the catalog whether one table is there, through the
// SQL-standard view Spanner's PostgreSQL interface publishes.
func spannerTableExists(c *qt.C, conn *dbschema.DatabaseConnection, table string) bool {
	c.Helper()

	query := sqlutil.Rebind(conn.Info().Dialect, `
SELECT COUNT(*)
FROM information_schema.tables
WHERE table_schema = 'public' AND table_name = ?`)
	var count int64
	err := conn.QueryRowContext(context.Background(), query, table).Scan(&count)
	c.Assert(err, qt.IsNil)
	return count > 0
}

// dropSpannerTables removes what a run created. Failures are ignored: a
// refused run leaves some of these tables uncreated, and a cleanup that
// asserted would replace an already-reported failure with a less informative
// one.
func dropSpannerTables(c *qt.C, conn *dbschema.DatabaseConnection, tables ...string) {
	c.Helper()

	for _, table := range tables {
		_, _ = conn.ExecContext(context.Background(), `DROP TABLE IF EXISTS "`+table+`"`)
	}
}
