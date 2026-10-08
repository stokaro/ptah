//go:build integration

package gonative_test

import (
	"database/sql"
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
	"ptah.run/migration/migrator"
)

const indexConstraintSchema = "ptah_index_constraint_attachment"

func TestPostgreSQLIndexConstraintAttachment(t *testing.T) {
	tests := []struct {
		name       string
		constraint string
		directive  string
		concurrent string
	}{
		{name: "transactional unique", constraint: "UNIQUE"},
		{name: "transactional primary key", constraint: "PRIMARY KEY"},
		{name: "concurrent unique", constraint: "UNIQUE", directive: "-- +ptah no_transaction\n", concurrent: "CONCURRENTLY "},
		{name: "concurrent primary key", constraint: "PRIMARY KEY", directive: "-- +ptah no_transaction\n", concurrent: "CONCURRENTLY "},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db, conn := indexConstraintConnections(c)
			up := fmt.Sprintf("%sCREATE UNIQUE INDEX %sIF NOT EXISTS temporary_key ON %s.members (id);\n"+
				"ALTER TABLE %s.members ADD CONSTRAINT members_key %s USING INDEX temporary_key;",
				test.directive, test.concurrent, indexConstraintSchema, indexConstraintSchema, test.constraint)
			mig := indexConstraintMigrator(conn, up)

			c.Assert(mig.MigrateUp(c.Context()), qt.IsNil)
			assertIndexConstraintApplied(c, db, mig)
			c.Assert(mig.RepairMigration(c.Context(), migrator.RepairMigrationOptions{Version: 1, Force: true}), qt.IsNil)
			assertIndexConstraintApplied(c, db, mig)
		})
	}
}

func TestPostgreSQLIndexConstraintAttachmentRetry(t *testing.T) {
	c := qt.New(t)
	db, conn := indexConstraintConnections(c)
	up := fmt.Sprintf("-- +ptah no_transaction\nSET search_path = %s;\n"+
		"CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS temporary_key ON members (id);\n"+
		"ALTER TABLE members ADD COLUMN note text, ADD CONSTRAINT members_key UNIQUE USING INDEX temporary_key;\n"+
		"INSERT INTO marker VALUES (1);", indexConstraintSchema)
	mig := indexConstraintMigrator(conn, up)
	c.Assert(mig.MigrateUp(c.Context()), qt.ErrorMatches, `(?s).*relation "marker" does not exist.*`)
	status, err := mig.GetMigrationStatus(c.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(status.DirtyRevision.Applied, qt.Equals, 3)
	_, err = db.ExecContext(c.Context(), "CREATE TABLE "+indexConstraintSchema+".marker (id integer)")
	c.Assert(err, qt.IsNil)

	c.Assert(mig.MigrateUpWithOptions(c.Context(), migrator.MigrateUpOptions{AllowDirty: true}), qt.IsNil)
	assertIndexConstraintApplied(c, db, mig)
}

func TestPostgreSQLIndexConstraintAttachmentRefusesDroppedConstraint(t *testing.T) {
	c := qt.New(t)
	db, conn := indexConstraintConnections(c)
	up := fmt.Sprintf("-- +ptah no_transaction\n"+
		"CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS temporary_key ON %s.members (id);\n"+
		"ALTER TABLE %s.members ADD CONSTRAINT members_key UNIQUE USING INDEX temporary_key;\n"+
		"ALTER TABLE %s.members DROP CONSTRAINT members_key;", indexConstraintSchema, indexConstraintSchema, indexConstraintSchema)
	mig := indexConstraintMigrator(conn, up)
	c.Assert(mig.MigrateUp(c.Context()), qt.ErrorMatches, `(?s).*failed to verify migration.*`)
	c.Assert(mig.RepairMigration(c.Context(), migrator.RepairMigrationOptions{Version: 1}), qt.ErrorMatches, `(?s).*cannot be repaired.*`)
	_, err := db.ExecContext(c.Context(), "INSERT INTO "+indexConstraintSchema+".members VALUES (1)")
	c.Assert(err, qt.IsNil)
	status, err := mig.GetMigrationStatus(c.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(status.DirtyRevision, qt.IsNotNil)
	c.Assert(status.AppliedMigrations, qt.HasLen, 0)
}

func indexConstraintConnections(c *qt.C) (*sql.DB, *dbschema.DatabaseConnection) {
	c.Helper()
	dsn := dbtarget.URL(c, dbtarget.PostgreSQL)
	db, err := sql.Open("pgx", dsn)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(db.Close(), qt.IsNil) })
	cleanup := func() {
		_, err := db.Exec("DROP SCHEMA IF EXISTS " + indexConstraintSchema + " CASCADE")
		c.Check(err, qt.IsNil)
	}
	cleanup()
	c.Cleanup(cleanup)
	_, err = db.ExecContext(c.Context(), "CREATE SCHEMA "+indexConstraintSchema)
	c.Assert(err, qt.IsNil)
	_, err = db.ExecContext(c.Context(), "CREATE TABLE "+indexConstraintSchema+".members (id integer NOT NULL)")
	c.Assert(err, qt.IsNil)
	_, err = db.ExecContext(c.Context(), "INSERT INTO "+indexConstraintSchema+".members VALUES (1)")
	c.Assert(err, qt.IsNil)
	conn, err := dbschema.ConnectToDatabase(c.Context(), dsn)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(conn.Close(), qt.IsNil) })
	return db, conn
}

func indexConstraintMigrator(conn *dbschema.DatabaseConnection, up string) *migrator.Migrator {
	migration := migrator.CreateMigrationFromSQL(1, "attach an index to a constraint", up, "SELECT 1;")
	return migrator.NewMigrator(conn, migrator.NewRegisteredMigrationProvider(migration)).
		WithMigrationsTable(indexConstraintSchema, "revisions")
}

func assertIndexConstraintApplied(c *qt.C, db *sql.DB, mig *migrator.Migrator) {
	c.Helper()
	var name string
	var valid, ready bool
	err := db.QueryRowContext(c.Context(), `SELECT i.relname, x.indisvalid, x.indisready
		FROM pg_constraint k JOIN pg_class i ON i.oid = k.conindid
		JOIN pg_index x ON x.indexrelid = i.oid
		WHERE k.conrelid = '`+indexConstraintSchema+`.members'::regclass AND k.conname = 'members_key'`).Scan(&name, &valid, &ready)
	c.Assert(err, qt.IsNil)
	c.Assert(name, qt.Equals, "members_key")
	c.Assert(valid, qt.IsTrue)
	c.Assert(ready, qt.IsTrue)
	_, err = db.ExecContext(c.Context(), "INSERT INTO "+indexConstraintSchema+".members VALUES (1)")
	c.Assert(err, qt.ErrorMatches, `(?s).*duplicate key value violates unique constraint "members_key".*`)
	status, err := mig.GetMigrationStatus(c.Context())
	c.Assert(err, qt.IsNil)
	c.Assert(status.DirtyRevision, qt.IsNil)
	c.Assert(status.AppliedMigrations, qt.DeepEquals, []int64{1})
}
