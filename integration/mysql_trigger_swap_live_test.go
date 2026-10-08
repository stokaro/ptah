//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/platform"
	"ptah.run/dbschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/dbtarget"
	"ptah.run/internal/schemafile"
	"ptah.run/migration/generator"
	"ptah.run/migration/migrator"
)

// A MySQL-family plan that replaces a trigger runs the replacement as two or
// more statements, and a write that lands between them sees no trigger or both
// of them: on MySQL 8.4.11 a row inserted between DROP and CREATE of a
// same-name trigger got no audit row, and one inserted between CREATE of a
// renamed trigger and DROP of the old one got two. The plan holds those writes
// off with LOCK TABLES (stokaro/ptah#4014, ariga/atlas#3534).
//
// The writer here starts inside the window, from an observer the migrator calls
// after the statement that opens it, and the test asks the server whether the
// writer is waiting. Its row then has to carry exactly one audit row, written
// by the new body.

// triggerSwapTables is the part of the schema the change leaves alone.
const triggerSwapTables = "CREATE TABLE mytable (id int PRIMARY KEY AUTO_INCREMENT, name varchar(100));\n" +
	"CREATE TABLE secondtable (id int PRIMARY KEY AUTO_INCREMENT, tbl_time datetime, NewColumn varchar(100));\n"

// triggerSwapSchema is the schema with one audit trigger named name, writing
// the columns body writes.
func triggerSwapSchema(name, columns, values string) string {
	return triggerSwapTables + fmt.Sprintf(
		"CREATE TRIGGER %s AFTER INSERT ON mytable FOR EACH ROW INSERT INTO secondtable (%s) VALUES (%s);\n",
		name, columns, values,
	)
}

// writeAfterStatement is the migrator observer that starts a write after one
// statement of a migration and reports whether the server held it.
type writeAfterStatement struct {
	// openedBy is the start of the statement the write follows.
	openedBy string
	// insert is the write.
	insert string
	// writerDSN is the driver address of the database the migration changes.
	writerDSN string
	// processes is a connection that can read the writer's process state.
	processes *sql.DB

	held     bool
	writeErr chan error
}

// ObserveStatement starts the writer after the statement openedBy names, and
// waits until the server either holds it on the metadata lock or lets it
// finish.
func (w *writeAfterStatement) ObserveStatement(ctx context.Context, event migrator.StatementEvent) error {
	if w.writeErr != nil || !strings.HasPrefix(withoutCommentLines(event.Statement), w.openedBy) {
		return nil
	}
	w.writeErr = make(chan error, 1)
	// The insert outlives this call: it is still waiting when the migration
	// resumes, which is the point.
	writeCtx := context.WithoutCancel(ctx)
	go func() {
		writer, err := sql.Open("mysql", w.writerDSN)
		if err != nil {
			w.writeErr <- err
			return
		}
		defer writer.Close()
		_, err = writer.ExecContext(writeCtx, w.insert)
		w.writeErr <- err
	}()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		var waiting int
		err := w.processes.QueryRowContext(ctx, `
			SELECT COUNT(*) FROM information_schema.PROCESSLIST
			WHERE STATE = 'Waiting for table metadata lock' AND INFO LIKE 'INSERT INTO mytable%'`,
		).Scan(&waiting)
		if err != nil {
			return err
		}
		if waiting > 0 {
			w.held = true
			return nil
		}
		select {
		case err := <-w.writeErr:
			// The write finished inside the window: nothing held it.
			w.writeErr <- err
			return nil
		case <-time.After(50 * time.Millisecond):
		}
	}
	return fmt.Errorf("the writer neither waited for the metadata lock nor finished")
}

// awaitWrite answers how the writer's insert ended, or fails the test when the
// observer never started it or the insert is still waiting.
func (w *writeAfterStatement) awaitWrite(c *qt.C) error {
	c.Helper()
	c.Assert(w.writeErr, qt.IsNotNil, qt.Commentf("no statement starting with %q ran", w.openedBy))
	select {
	case err := <-w.writeErr:
		return err
	case <-time.After(30 * time.Second):
		c.Fatalf("the write inside the swap is still waiting after the migration finished")
		return nil
	}
}

// withoutCommentLines drops the comment lines a planned statement opens with.
func withoutCommentLines(statement string) string {
	var kept []string
	for line := range strings.SplitSeq(statement, "\n") {
		if !strings.HasPrefix(strings.TrimSpace(line), "--") {
			kept = append(kept, line)
		}
	}
	return strings.TrimSpace(strings.Join(kept, "\n"))
}

// Each query answers one audit column of every audit row, in insertion order.
const (
	auditNewColumn = "SELECT COALESCE(NewColumn, 'NULL') FROM secondtable ORDER BY id"
	auditLabel     = "SELECT COALESCE(label, 'NULL') FROM secondtable ORDER BY id"
)

// auditRows answers the audit column query reads, one value per audit row.
func auditRows(c *qt.C, conn *sql.DB, query string) []string {
	c.Helper()
	rows, err := conn.QueryContext(c.Context(), query)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(rows.Close(), qt.IsNil) }()
	var values []string
	for rows.Next() {
		var value string
		c.Assert(rows.Scan(&value), qt.IsNil)
		values = append(values, value)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return values
}

// TestMigrateUpHoldsWritesWhileAMySQLTriggerIsSwappedLive generates the
// migration that replaces the audit trigger, applies it, and inserts a row from
// a second connection inside the replacement.
//
// MariaDB replaces a same-name trigger with one CREATE OR REPLACE TRIGGER and
// opens no window, so only a rename is run there.
func TestMigrateUpHoldsWritesWhileAMySQLTriggerIsSwappedLive(t *testing.T) {
	tests := []struct {
		name     string
		engine   dbtarget.Engine
		dialect  string
		current  string
		desired  string
		openedBy string
	}{
		{
			name:     "MySQL same-name replacement",
			engine:   dbtarget.MySQLAdmin,
			dialect:  platform.MySQL,
			current:  "audit",
			desired:  "audit",
			openedBy: "DROP TRIGGER",
		},
		{
			name:     "MySQL rename",
			engine:   dbtarget.MySQLAdmin,
			dialect:  platform.MySQL,
			current:  "audit_2025_03_20",
			desired:  "audit_2025_06_12",
			openedBy: "CREATE TRIGGER",
		},
		{
			name:     "MariaDB rename",
			engine:   dbtarget.MariaDBAdmin,
			dialect:  platform.MariaDB,
			current:  "audit_2025_03_20",
			desired:  "audit_2025_06_12",
			openedBy: "CREATE TRIGGER",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			scratch := newMySQLFamilyScratch(c, test.engine)
			name, target := scratch.builtFrom(c, triggerSwapSchema(test.current, "tbl_time", "NOW()"))
			desiredFile := filepath.Join(c.TempDir(), "schema.sql")
			c.Assert(os.WriteFile(desiredFile, []byte(
				triggerSwapSchema(test.desired, "tbl_time, NewColumn", "NOW(), NEW.name"),
			), 0o600), qt.IsNil)
			desired, err := schemafile.LoadPath(desiredFile, schemafile.Options{Dialect: test.dialect})
			c.Assert(err, qt.IsNil)
			conn, err := dbschema.ConnectToDatabase(c.Context(), target)
			c.Assert(err, qt.IsNil)
			defer dbschema.CloseAndWarn(conn)
			dir := c.TempDir()
			_, err = generator.GenerateMigration(c.Context(), generator.GenerateMigrationOptions{
				Runtime:   must.Must(builtin.New()),
				Generated: desired, DBConn: conn, OutputDir: dir, MigrationName: "swap_audit_trigger",
			})
			c.Assert(err, qt.IsNil)
			writerDSN := mySQLDSNForDatabase(c, scratch.adminDSN, name)
			observer := &writeAfterStatement{
				openedBy:  test.openedBy,
				insert:    "INSERT INTO mytable (name) VALUES ('during-swap')",
				writerDSN: writerDSN,
				processes: scratch.admin,
			}
			mig, err := migrator.NewFSMigrator(conn, os.DirFS(dir), migrator.WithStatementObserver(observer))
			c.Assert(err, qt.IsNil)

			c.Assert(mig.WithTransactionMode(migrator.MigrationTxModeNone).MigrateUp(c.Context()), qt.IsNil)
			c.Assert(observer.awaitWrite(c), qt.IsNil)
			reader, err := sql.Open("mysql", writerDSN)
			c.Assert(err, qt.IsNil)
			defer func() { c.Check(reader.Close(), qt.IsNil) }()

			c.Check(auditRows(c, reader, auditNewColumn), qt.DeepEquals, []string{"during-swap"},
				qt.Commentf("one audit row, written by the new trigger"))
			c.Assert(observer.held, qt.IsTrue, qt.Commentf("the write inside the swap was not held"))
		})
	}
}

// TestMigrateUpKeepsWritesWorkingWhenATriggerColumnIsDroppedLive drops a column
// the audit trigger reads, in the migration that points the trigger at another
// column, and inserts a row right after the column is gone.
//
// MySQL and MariaDB accept DROP COLUMN while a trigger reads NEW.<column>, and
// every write to the table then fails with ERROR 1054 until the trigger is
// replaced. So the trigger has to be replaced first.
func TestMigrateUpKeepsWritesWorkingWhenATriggerColumnIsDroppedLive(t *testing.T) {
	tests := []struct {
		name    string
		engine  dbtarget.Engine
		dialect string
	}{
		{name: "MySQL", engine: dbtarget.MySQLAdmin, dialect: platform.MySQL},
		{name: "MariaDB", engine: dbtarget.MariaDBAdmin, dialect: platform.MariaDB},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			scratch := newMySQLFamilyScratch(c, test.engine)
			name, target := scratch.builtFrom(c,
				"CREATE TABLE mytable (id int PRIMARY KEY AUTO_INCREMENT, name varchar(100), legacy varchar(100));\n"+
					"CREATE TABLE secondtable (id int PRIMARY KEY AUTO_INCREMENT, label varchar(100));\n"+
					"CREATE TRIGGER audit AFTER INSERT ON mytable FOR EACH ROW INSERT INTO secondtable (label) VALUES (NEW.legacy);\n",
			)
			desiredFile := filepath.Join(c.TempDir(), "schema.sql")
			c.Assert(os.WriteFile(desiredFile, []byte(
				"CREATE TABLE mytable (id int PRIMARY KEY AUTO_INCREMENT, name varchar(100));\n"+
					"CREATE TABLE secondtable (id int PRIMARY KEY AUTO_INCREMENT, label varchar(100));\n"+
					"CREATE TRIGGER audit AFTER INSERT ON mytable FOR EACH ROW INSERT INTO secondtable (label) VALUES (NEW.name);\n",
			), 0o600), qt.IsNil)
			desired, err := schemafile.LoadPath(desiredFile, schemafile.Options{Dialect: test.dialect})
			c.Assert(err, qt.IsNil)
			conn, err := dbschema.ConnectToDatabase(c.Context(), target)
			c.Assert(err, qt.IsNil)
			defer dbschema.CloseAndWarn(conn)
			dir := c.TempDir()
			_, err = generator.GenerateMigration(c.Context(), generator.GenerateMigrationOptions{
				Runtime:   must.Must(builtin.New()),
				Generated: desired, DBConn: conn, OutputDir: dir, MigrationName: "drop_legacy",
			})
			c.Assert(err, qt.IsNil)
			writerDSN := mySQLDSNForDatabase(c, scratch.adminDSN, name)
			observer := &writeAfterStatement{
				openedBy:  "ALTER TABLE `mytable` DROP COLUMN",
				insert:    "INSERT INTO mytable (name) VALUES ('after-drop')",
				writerDSN: writerDSN,
				processes: scratch.admin,
			}
			mig, err := migrator.NewFSMigrator(conn, os.DirFS(dir), migrator.WithStatementObserver(observer))
			c.Assert(err, qt.IsNil)

			c.Assert(mig.WithTransactionMode(migrator.MigrationTxModeNone).MigrateUp(c.Context()), qt.IsNil)
			writeErr := observer.awaitWrite(c)
			reader, err := sql.Open("mysql", writerDSN)
			c.Assert(err, qt.IsNil)
			defer func() { c.Check(reader.Close(), qt.IsNil) }()

			c.Assert(writeErr, qt.IsNil, qt.Commentf("a write after the column is dropped"))
			c.Assert(auditRows(c, reader, auditLabel), qt.DeepEquals, []string{"after-drop"})
		})
	}
}
