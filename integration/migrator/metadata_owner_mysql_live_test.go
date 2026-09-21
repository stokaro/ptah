//go:build integration

package migrator_test

import (
	"context"
	"testing"
	"testing/fstest"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
	"ptah.run/internal/sqlident"
	"ptah.run/migration/migrator"
)

// The MySQL family is outside the ownership refusal, and this is the reason
// rather than an assumption.
//
// There is no table owner to compare against, and the escalation the refusal
// exists to stop does not happen there: a trigger runs as its definer, so a
// metadata table somebody else prepared gains them a hook on every migration
// and not the migration role's privileges. `CURRENT_USER` inside the body is
// the definer, which is what a privilege check reads, while `USER` reports the
// account that connected.
//
// Adding a MySQL arm to the refusal would have to start by measuring that the
// engine answers differently (stokaro/ptah#3474).
func TestMySQLTriggerOnAMetadataTableRunsAsItsDefinerLive(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		admin   dbtarget.Engine
	}{
		{name: "mysql", dialect: platform.MySQL, admin: dbtarget.MySQLAdmin},
		{name: "mariadb", dialect: platform.MariaDB, admin: dbtarget.MariaDBAdmin},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			conn := newDefinerFixture(t, test.dialect, test.admin)

			mig, err := migrator.NewFSMigrator(conn, definerFixtureMigrations())
			c.Assert(err, qt.IsNil)
			c.Assert(mig.MigrateUp(t.Context()), qt.IsNil)

			// Adopted rather than refused, and the body observed the definer.
			notes := definerObservations(c, conn)
			c.Assert(notes, qt.Not(qt.HasLen), 0)
			for _, note := range notes {
				c.Assert(note, qt.Contains, "CURRENT_USER=ptah_definer@%")
			}
		})
	}
}

func definerFixtureMigrations() fstest.MapFS {
	return fstest.MapFS{
		"0000000001_widgets.up.sql": &fstest.MapFile{
			Data: []byte("CREATE TABLE ptah_definer_widgets (id INT PRIMARY KEY);\n"),
		},
		"0000000001_widgets.down.sql": &fstest.MapFile{
			Data: []byte("DROP TABLE ptah_definer_widgets;\n"),
		},
	}
}

// newDefinerFixture prepares the log table and a trigger whose definer is
// another account, which is the shape the refusal would have to answer if the
// MySQL family needed one.
//
// The trigger's definer is set at creation rather than by connecting as that
// account, because what the measurement turns on is the definer recorded with
// the trigger and not who typed the statement.
func newDefinerFixture(t *testing.T, dialect string, admin dbtarget.Engine) *dbschema.DatabaseConnection {
	t.Helper()
	c := qt.New(t)
	target := mySQLFamilyScratchDatabaseURL(t, dialect, admin, "ptah_definer")
	conn, err := dbschema.ConnectToDatabase(t.Context(), target)
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() { dbschema.CloseAndWarn(conn) })

	for _, statement := range []string{
		"CREATE USER IF NOT EXISTS 'ptah_definer'@'%' IDENTIFIED BY 'ptah_password'",
		// The definer's own privileges are what the server checks inside the
		// body, so an account that really pre-created this table is the shape
		// to measure. Without the grant the trigger is denied and the
		// measurement would be about a missing privilege instead.
		"GRANT ALL ON " + sqlident.Quote(dialect, conn.Info().Schema) + ".* TO 'ptah_definer'@'%'",
		"CREATE TABLE ptah_definer_evidence (note VARCHAR(200))",
		definerLogTableDDL,
		"CREATE DEFINER='ptah_definer'@'%' TRIGGER ptah_definer_trg" +
			" BEFORE INSERT ON schema_migrations_log FOR EACH ROW" +
			" INSERT INTO ptah_definer_evidence (note)" +
			" VALUES (CONCAT('CURRENT_USER=', CURRENT_USER(), ' USER=', USER()))",
	} {
		_, execErr := conn.ExecContext(t.Context(), statement)
		c.Assert(execErr, qt.IsNil, qt.Commentf("statement: %s", statement))
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), definerCleanupTimeout)
		defer cancel()
		_, _ = conn.ExecContext(ctx, "DROP USER IF EXISTS 'ptah_definer'@'%'")
	})
	return conn
}

// definerCleanupTimeout bounds the cleanup, whose context is created fresh
// because t.Context is canceled before cleanup runs.
const definerCleanupTimeout = 30 * time.Second

// definerLogTableDDL is the log table's own shape, so the table is adopted
// rather than refused for being the wrong table.
const definerLogTableDDL = "CREATE TABLE schema_migrations_log (" +
	"run_id varchar(64) NOT NULL, seq bigint NOT NULL, operation varchar(32) NOT NULL," +
	" version bigint NOT NULL, state varchar(32) NOT NULL, actor varchar(256) DEFAULT NULL," +
	" actor_source varchar(32) NOT NULL, checksum varchar(64) DEFAULT NULL," +
	" logged_at timestamp NOT NULL, error text, PRIMARY KEY (run_id, seq)) ENGINE=InnoDB"

func definerObservations(c *qt.C, conn *dbschema.DatabaseConnection) []string {
	c.Helper()
	rows, err := conn.QueryContext(context.Background(), "SELECT note FROM ptah_definer_evidence")
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(rows.Close(), qt.IsNil) }()
	notes := []string{}
	for rows.Next() {
		var note string
		c.Assert(rows.Scan(&note), qt.IsNil)
		notes = append(notes, note)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return notes
}
