//go:build integration

package gonative_test

import (
	"database/sql"
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/migration/migrator"
)

// A non-transactional rollback that fails partway is the shape
// stokaro/ptah#995 is about: one down statement commits, the next fails, and
// the schema is left half-reverted. The unit tests cover it on SQLite; this
// covers the four steps the issue asks for — fail, inspect, resume, final
// schema state — on a live PostgreSQL, where DDL really is transactional and
// the `no_transaction` directive is the only reason a committed statement
// survives the failure.
const (
	directionalRepairKeep         = "ptah_issue995_keep"
	directionalRepairDropped      = "ptah_issue995_dropped"
	directionalRepairMissing      = "ptah_issue995_missing"
	directionalRepairTracker      = "schema_migrations_issue_995"
	directionalRepairAtlasTracker = "atlas_schema_revisions_issue_995"
	directionalRepairVersion      = int64(1785756995)
	directionalRepairResumeAt     = 2
)

func TestPostgreSQLDirectionalRepairResumesPartialRollbackIntegration(t *testing.T) {
	dsn := skipIfNoPostgreSQL(t)
	c := qt.New(t)
	ctx := t.Context()

	db := openDirectionalRepairDB(c, dsn)
	dropDirectionalRepairObjects(c, db)
	c.Cleanup(func() { dropDirectionalRepairObjects(c, db) })

	conn, err := dbschema.ConnectToDatabase(ctx, dsn)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(conn.Close(), qt.IsNil) })
	mig := directionalRepairMigrator(conn)

	c.Assert(mig.MigrateUp(ctx), qt.IsNil)
	c.Assert(directionalRepairTableExists(c, db, directionalRepairKeep), qt.IsTrue)
	c.Assert(directionalRepairTableExists(c, db, directionalRepairDropped), qt.IsTrue)

	// Fail: the first down statement commits, the second names a table that
	// does not exist.
	c.Assert(mig.MigrateDownTo(ctx, 0), qt.IsNotNil)

	// Inspect: the committed statement is gone, the row records the direction
	// and how far the rollback got. applied=1 is what makes the resume start at
	// the right place rather than replaying a DROP that would now fail.
	c.Assert(directionalRepairTableExists(c, db, directionalRepairDropped), qt.IsFalse)
	c.Assert(directionalRepairTableExists(c, db, directionalRepairKeep), qt.IsTrue)
	state, applied, total := directionalRepairRevision(c, db)
	c.Assert(state, qt.Equals, "failed:down")
	c.Assert(applied, qt.Equals, 1)
	c.Assert(total, qt.Equals, 2)

	// The operator creates what the down expected to find.
	_, err = db.ExecContext(ctx, "CREATE TABLE "+directionalRepairMissing+" (id integer)")
	c.Assert(err, qt.IsNil)

	// Resume: only the remaining statement runs. Replaying the first would fail
	// on a table already dropped, so a clean exit is itself the evidence that
	// it was skipped.
	c.Assert(mig.RepairMigration(ctx, migrator.RepairMigrationOptions{
		Version:    directionalRepairVersion,
		ResumeFrom: directionalRepairResumeAt,
	}), qt.IsNil)

	// Final schema state: the rollback finished, and its revision is gone
	// rather than left reading as applied over a reverted schema.
	c.Assert(directionalRepairTableExists(c, db, directionalRepairMissing), qt.IsFalse)
	c.Assert(directionalRepairTableExists(c, db, directionalRepairKeep), qt.IsTrue)
	c.Assert(directionalRepairRevisionCount(c, db), qt.Equals, 0)
}

func TestPostgreSQLAtlasDirectionalRepairResumesPartialRollbackIntegration(t *testing.T) {
	dsn := skipIfNoPostgreSQL(t)
	c := qt.New(t)
	ctx := t.Context()

	db := openDirectionalRepairDB(c, dsn)
	dropDirectionalRepairObjects(c, db)
	c.Cleanup(func() { dropDirectionalRepairObjects(c, db) })

	conn, err := dbschema.ConnectToDatabase(ctx, dsn)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(conn.Close(), qt.IsNil) })
	mig := directionalRepairMigrator(conn).
		WithMigrationsTable("", directionalRepairAtlasTracker).
		WithRevisionTableFormat(migrator.RevisionTableFormatAtlas)

	c.Assert(mig.MigrateUp(ctx), qt.IsNil)
	c.Assert(mig.MigrateDownTo(ctx, 0), qt.IsNotNil)
	c.Assert(directionalRepairTableExists(c, db, directionalRepairDropped), qt.IsFalse)
	c.Assert(directionalRepairTableExists(c, db, directionalRepairKeep), qt.IsTrue)

	var operatorVersion string
	var applied, total int
	err = db.QueryRow(
		"SELECT operator_version, applied, total FROM "+directionalRepairAtlasTracker+" WHERE version = $1",
		fmt.Sprint(directionalRepairVersion),
	).Scan(&operatorVersion, &applied, &total)
	c.Assert(err, qt.IsNil)
	c.Assert(operatorVersion, qt.Equals, "Ptah/down")
	c.Assert(applied, qt.Equals, 1)
	c.Assert(total, qt.Equals, 2)

	status, err := mig.GetMigrationStatus(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(status.DirtyRevision, qt.IsNotNil)
	c.Assert(status.DirtyRevision.Direction, qt.Equals, migrator.MigrationDirectionDown)

	_, err = db.ExecContext(ctx, "CREATE TABLE "+directionalRepairMissing+" (id integer)")
	c.Assert(err, qt.IsNil)
	c.Assert(mig.RepairMigration(ctx, migrator.RepairMigrationOptions{
		Version:    directionalRepairVersion,
		ResumeFrom: directionalRepairResumeAt,
	}), qt.IsNil)

	c.Assert(directionalRepairTableExists(c, db, directionalRepairMissing), qt.IsFalse)
	c.Assert(directionalRepairTableExists(c, db, directionalRepairKeep), qt.IsTrue)
	var revisions int
	c.Assert(db.QueryRow("SELECT COUNT(*) FROM "+directionalRepairAtlasTracker).Scan(&revisions), qt.IsNil)
	c.Assert(revisions, qt.Equals, 0)
}

func directionalRepairMigrator(conn *dbschema.DatabaseConnection) *migrator.Migrator {
	up := fmt.Sprintf(
		"CREATE TABLE %s (id integer);\nCREATE TABLE %s (id integer);",
		directionalRepairKeep, directionalRepairDropped,
	)
	down := fmt.Sprintf(
		"-- +ptah no_transaction\nDROP TABLE %s;\nDROP TABLE %s;",
		directionalRepairDropped, directionalRepairMissing,
	)
	migration := migrator.CreateMigrationFromSQL(directionalRepairVersion, "issue 995 rollback", up, down)
	return migrator.NewMigrator(conn, migrator.NewRegisteredMigrationProvider(migration)).
		WithMigrationsTable("", directionalRepairTracker)
}

func openDirectionalRepairDB(c *qt.C, dsn string) *sql.DB {
	c.Helper()
	db, err := sql.Open("pgx", dsn)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(db.Close(), qt.IsNil) })
	return db
}

func dropDirectionalRepairObjects(c *qt.C, db *sql.DB) {
	c.Helper()
	for _, statement := range []string{
		"DROP TABLE IF EXISTS " + directionalRepairKeep,
		"DROP TABLE IF EXISTS " + directionalRepairDropped,
		"DROP TABLE IF EXISTS " + directionalRepairMissing,
		"DROP TABLE IF EXISTS " + directionalRepairTracker,
		"DROP TABLE IF EXISTS " + directionalRepairAtlasTracker,
	} {
		_, err := db.Exec(statement)
		c.Assert(err, qt.IsNil)
	}
}

func directionalRepairTableExists(c *qt.C, db *sql.DB, name string) bool {
	c.Helper()
	var count int
	err := db.QueryRow(
		"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = $1",
		name,
	).Scan(&count)
	c.Assert(err, qt.IsNil)
	return count > 0
}

func directionalRepairRevision(c *qt.C, db *sql.DB) (state string, applied, total int) {
	c.Helper()
	err := db.QueryRow(
		"SELECT state, applied, total FROM "+directionalRepairTracker+" WHERE version = $1",
		directionalRepairVersion,
	).Scan(&state, &applied, &total)
	c.Assert(err, qt.IsNil)
	return state, applied, total
}

func directionalRepairRevisionCount(c *qt.C, db *sql.DB) int {
	c.Helper()
	var count int
	c.Assert(db.QueryRow("SELECT COUNT(*) FROM "+directionalRepairTracker).Scan(&count), qt.IsNil)
	return count
}
