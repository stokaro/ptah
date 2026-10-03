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
	"ptah.run/migration/migrator"
)

// On MySQL and MariaDB a DDL statement waits for a metadata lock, and every
// later read and write of the table queues behind it. lock_timeout has to
// bound that wait, which innodb_lock_wait_timeout does not: that variable
// bounds row-lock waits only (stokaro/ptah#4012).
//
// A second session holds the metadata lock with an open transaction that read
// the table, for longer than the test runs. Each migration has to fail with
// ERROR 1205 inside its bound and leave the table as it was. Without the bound
// the migration waits until the context below gives up, which fails the
// elapsed-time assertion instead of hanging the run.

// metadataLockWaiters are statements that wait for a metadata lock, each
// written as the migration body that runs it.
var metadataLockWaiters = []struct {
	name string
	body string
}{
	{
		// The table carries a trigger, which keeps every migration that
		// touches it out of a transaction file.
		name: "ALTER TABLE",
		body: "-- +ptah no_transaction\n-- +ptah lock_timeout=1s\n" +
			"ALTER TABLE items ADD COLUMN blocked_value VARCHAR(10);\n",
	},
	{
		// The trigger swap a plan writes runs outside a transaction, and its
		// LOCK TABLES is the statement that waits (stokaro/ptah#4014).
		name: "LOCK TABLES of a trigger swap",
		body: "-- +ptah no_transaction\n-- +ptah lock_timeout=1s\n" +
			"LOCK TABLES `items` WRITE;\n" +
			"DROP TRIGGER IF EXISTS `items_audit`;\n" +
			"CREATE TRIGGER `items_audit` BEFORE INSERT ON `items` FOR EACH ROW SET NEW.note = 'new';\n" +
			"UNLOCK TABLES;\n",
	},
}

// itemsState answers the table's columns and the body of its trigger, which
// is what a migration that timed out must leave alone.
func itemsState(c *qt.C, conn *dbschema.DatabaseConnection) (columns, trigger string) {
	c.Helper()
	err := conn.QueryRowContext(c.Context(), `
		SELECT
			(SELECT GROUP_CONCAT(COLUMN_NAME ORDER BY ORDINAL_POSITION) FROM information_schema.COLUMNS
			 WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'items'),
			(SELECT ACTION_STATEMENT FROM information_schema.TRIGGERS
			 WHERE TRIGGER_SCHEMA = DATABASE() AND TRIGGER_NAME = 'items_audit')`,
	).Scan(&columns, &trigger)
	c.Assert(err, qt.IsNil)
	return columns, trigger
}

// migrateWhileItemsIsRead runs the migration while a session of holder has an
// open transaction that read items, and answers how long it took and how it
// ended. The 30-second context is the backstop for a wait nothing bounds.
func migrateWhileItemsIsRead(
	c *qt.C,
	holder *dbschema.DatabaseConnection,
	mig *migrator.Migrator,
) (elapsed time.Duration, migrateErr error) {
	c.Helper()
	err := holder.WithSession(c.Context(), func(session *dbschema.DatabaseConnection) error {
		if _, err := session.ExecContext(c.Context(), "START TRANSACTION"); err != nil {
			return err
		}
		if _, err := session.ExecContext(c.Context(), "SELECT COUNT(*) FROM items"); err != nil {
			return err
		}
		ctx, cancel := context.WithTimeout(c.Context(), 30*time.Second)
		defer cancel()
		start := time.Now()
		migrateErr = mig.MigrateUp(ctx)
		elapsed = time.Since(start)
		_, err := session.ExecContext(c.Context(), "ROLLBACK")
		return err
	})
	c.Assert(err, qt.IsNil)
	return elapsed, migrateErr
}

func TestMigrateUpBoundsAMetadataLockWaitLive(t *testing.T) {
	engines := []struct {
		dialect string
		engine  dbtarget.Engine
	}{
		{dialect: platform.MySQL, engine: dbtarget.MySQLAdmin},
		{dialect: platform.MariaDB, engine: dbtarget.MariaDBAdmin},
	}
	for _, engine := range engines {
		for _, waiter := range metadataLockWaiters {
			t.Run(engine.dialect+"/"+waiter.name, func(t *testing.T) {
				c := qt.New(t)
				dbURL := mySQLFamilyScratchDatabaseURL(t, engine.dialect, engine.engine, "ptah_mdl_timeout")
				conn, err := dbschema.ConnectToDatabase(c.Context(), dbURL)
				c.Assert(err, qt.IsNil)
				defer dbschema.CloseAndWarn(conn)
				_, err = conn.ExecContext(c.Context(), "CREATE TABLE items (id INT PRIMARY KEY, note VARCHAR(10))")
				c.Assert(err, qt.IsNil)
				_, err = conn.ExecContext(c.Context(),
					"CREATE TRIGGER items_audit BEFORE INSERT ON items FOR EACH ROW SET NEW.note = 'old'")
				c.Assert(err, qt.IsNil)
				holder, err := dbschema.ConnectToDatabase(c.Context(), dbURL)
				c.Assert(err, qt.IsNil)
				defer dbschema.CloseAndWarn(holder)
				mig, err := migrator.NewFSMigrator(conn, fstest.MapFS{
					"0000000001_wait.up.sql":   {Data: []byte(waiter.body)},
					"0000000001_wait.down.sql": {Data: []byte("SELECT 1;\n")},
				})
				c.Assert(err, qt.IsNil)

				elapsed, migrateErr := migrateWhileItemsIsRead(c, holder, mig)
				columns, trigger := itemsState(c, conn)

				c.Assert(migrateErr, qt.ErrorMatches, `(?s).*Error 1205.*Lock wait timeout exceeded.*`)
				c.Assert(elapsed < 10*time.Second, qt.IsTrue, qt.Commentf("the migration waited %s", elapsed))
				c.Assert(columns, qt.Equals, "id,note")
				c.Assert(trigger, qt.Equals, "SET NEW.note = 'old'")
			})
		}
	}
}
