package migratedown_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/cli/migraterepair"
)

// A rollback that fails leaves its revision row dirty and no longer applied, so
// the applied version reads as already rolled back over it. A second rollback
// to that version refuses the row, as `migrations up` does and as a rollback
// with something left to roll back does, and names the command that ends it.

// gearsUp and gearsDown are a migration that cannot be rolled back until the
// table its down file drops second exists. The down file runs in a
// transaction, so the failed rollback commits nothing.
const (
	gearsUp   = "CREATE TABLE gears (id INTEGER PRIMARY KEY);\n"
	gearsDown = "DROP TABLE gears;\nDROP TABLE gear_parts;\n"
)

// widgetsThenGears is a directory whose second migration is the gears one.
var widgetsThenGears = map[string]string{
	"0000000001_init.up.sql":    "CREATE TABLE widgets (id INTEGER PRIMARY KEY);\n",
	"0000000001_init.down.sql":  "DROP TABLE widgets;\n",
	"0000000002_gears.up.sql":   gearsUp,
	"0000000002_gears.down.sql": gearsDown,
}

// onlyGears is a directory whose one migration is the gears one.
var onlyGears = map[string]string{
	"0000000001_gears.up.sql":   gearsUp,
	"0000000001_gears.down.sql": gearsDown,
}

// failedRollbackDB applies files, rolls back to target and watches the
// rollback fail, and returns the directory and the database file.
func failedRollbackDB(c *qt.C, files map[string]string, target string) (dir, dbPath string) {
	c.Helper()
	dir = c.TempDir()
	for name, content := range files {
		c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600), qt.IsNil)
	}
	dbPath = filepath.Join(c.TempDir(), "dirty.db")
	applyWidgets(c, dir, dbPath)
	_, err := runDown("--db-url", "sqlite://"+dbPath, "--migrations-dir", dir, "--target", target, "--confirm")
	c.Assert(err, qt.ErrorMatches, `(?s).*no such table: gear_parts.*`)
	return dir, dbPath
}

// revisionRows reads every revision row back as "version:state applied/total".
func revisionRows(c *qt.C, dbPath string) []string {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	rows, err := conn.QueryContext(context.Background(),
		`SELECT version || ':' || state || ' ' || applied || '/' || total FROM schema_migrations ORDER BY version`)
	c.Assert(err, qt.IsNil)
	defer func() { c.Assert(rows.Close(), qt.IsNil) }()
	var got []string
	for rows.Next() {
		var row string
		c.Assert(rows.Scan(&row), qt.IsNil)
		got = append(got, row)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return got
}

func TestMigrateDownOverADirtyRevision_FailurePath(t *testing.T) {
	// The second rollback asks for the version the failed one was rolling back
	// to. The dirty row is not applied, so the version already reads as that
	// target, and the run would report the rollback done.
	t.Run("a second rollback to the target of the failed one", func(t *testing.T) {
		c := qt.New(t)
		dir, dbPath := failedRollbackDB(c, widgetsThenGears, "1")

		out, err := runDown("--db-url", "sqlite://"+dbPath, "--migrations-dir", dir, "--target", "1", "--confirm")

		c.Assert(err, qt.ErrorMatches,
			`(?s)error running down migrations: migration 2 is dirty: state=failed applied=0/2 .*\n`+
				`No down statement of this rollback committed, so the migration is still applied\. `+
				`Run 'ptah migrations repair --version 2' to record it applied, then roll it back again\.`)
		c.Assert(out, qt.Not(qt.Contains), "already at or below target")
		c.Assert(revisionRows(c, dbPath), qt.DeepEquals, []string{"1:applied 1/1", "2:failed:down 0/2"})
	})

	// The dirty row is the only one: the failed rollback was rolling back the
	// oldest migration, and the version already reads as 0.
	t.Run("a second rollback past the only migration", func(t *testing.T) {
		c := qt.New(t)
		dir, dbPath := failedRollbackDB(c, onlyGears, "0")

		out, err := runDown("--db-url", "sqlite://"+dbPath, "--migrations-dir", dir, "--target", "0", "--confirm")

		c.Assert(err, qt.ErrorMatches, `(?s)error running down migrations: migration 1 is dirty: .*\n`+
			`No down statement of this rollback committed, so the migration is still applied\. `+
			`Run 'ptah migrations repair --version 1' to record it applied, then roll it back again\.`)
		c.Assert(out, qt.Not(qt.Contains), "already at or below target")
		c.Assert(revisionRows(c, dbPath), qt.DeepEquals, []string{"1:failed:down 0/2"})
	})

	// A dry run prints the rollback a real run would make, and a real run
	// refuses the row, so the dry run refuses it too.
	t.Run("a dry run", func(t *testing.T) {
		c := qt.New(t)
		dir, dbPath := failedRollbackDB(c, onlyGears, "0")

		out, err := runDown("--db-url", "sqlite://"+dbPath, "--migrations-dir", dir, "--target", "0", "--dry-run")

		c.Assert(err, qt.ErrorMatches, `(?s)error running down migrations: migration 1 is dirty: .*\n`+
			`No down statement of this rollback committed, .*`)
		c.Assert(out, qt.Not(qt.Contains), "already at or below target")
		c.Assert(revisionRows(c, dbPath), qt.DeepEquals, []string{"1:failed:down 0/2"})
	})
}

// The control: the repair the refusal names ends the refusal, and once the
// cause is fixed the rollback it refused runs to its end.
func TestMigrateDownOverADirtyRevision_HappyPath(t *testing.T) {
	c := qt.New(t)
	dir, dbPath := failedRollbackDB(c, widgetsThenGears, "1")
	createTable(c, dbPath, "gear_parts")

	repairOut, err := runRepair("--db-url", "sqlite://"+dbPath, "--migrations-dir", dir, "--version", "2")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", repairOut))
	c.Assert(revisionRows(c, dbPath), qt.DeepEquals, []string{"1:applied 1/1", "2:applied 1/1"})

	out, err := runDown("--db-url", "sqlite://"+dbPath, "--migrations-dir", dir, "--target", "1", "--confirm")

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(revisionRows(c, dbPath), qt.DeepEquals, []string{"1:applied 1/1"})
	c.Assert(tableCensus(c, dbPath), qt.DeepEquals, []string{"schema_migrations", "schema_migrations_log", "widgets"})
}

func runRepair(args ...string) (string, error) {
	cmd := migraterepair.NewMigrateRepairCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs(args)
	err := cmd.Execute()
	return out.String(), err
}

// createTable creates the table the failing down file drops, which is the
// operator fixing what made the rollback fail.
func createTable(c *qt.C, dbPath, name string) {
	c.Helper()
	execSQLite(c, dbPath, "CREATE TABLE "+name+" (id INTEGER PRIMARY KEY)")
}

func execSQLite(c *qt.C, dbPath, statement string) {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	_, err = conn.ExecContext(context.Background(), statement)
	c.Assert(err, qt.IsNil)
}
