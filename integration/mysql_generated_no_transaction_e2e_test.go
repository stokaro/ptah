//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/devdocker"
	"ptah.run/internal/envbool/envbooltest"
)

// `migrations up` runs a MySQL or MariaDB file inside a transaction unless the
// file opts out, and refuses one whose statements it cannot tie to that
// transaction: a trigger definition, LOCK TABLES, or a statement on a table a
// trigger fires on. `migrations generate` asks the migrator which files it
// would refuse and marks those no_transaction, so every generated file applies
// and rolls back with the default flags (stokaro/ptah#4013).

// generatedTransactionModes are changes whose files the migrator refuses or
// runs in a transaction, and whether the generated pair is marked.
var generatedTransactionModes = []struct {
	name              string
	current, desired  string
	wantNoTransaction bool
}{
	{
		name:              "a renamed trigger",
		current:           triggerSwapSchema("audit_2025_03_20", "tbl_time", "NOW()"),
		desired:           triggerSwapSchema("audit_2025_06_12", "tbl_time, NewColumn", "NOW(), NEW.name"),
		wantNoTransaction: true,
	},
	{
		// No trigger changes; the column is added to a table one fires on.
		name:    "a column on a table with a trigger",
		current: triggerSwapSchema("audit", "tbl_time", "NOW()"),
		desired: strings.Replace(triggerSwapSchema("audit", "tbl_time", "NOW()"),
			"name varchar(100))", "name varchar(100), extra int)", 1),
		wantNoTransaction: true,
	},
	{
		// The down file drops the trigger and the column. Against today's
		// catalog nothing in it is refused, but it runs after the up file,
		// on a table the new trigger fires on.
		name:    "a first trigger and a column on a table",
		current: "CREATE TABLE plain (id int PRIMARY KEY);\nCREATE TABLE log (v int);\n",
		desired: "CREATE TABLE plain (id int PRIMARY KEY, extra int);\nCREATE TABLE log (v int);\n" +
			"CREATE TRIGGER plain_audit AFTER INSERT ON plain FOR EACH ROW INSERT INTO log (v) VALUES (NEW.id);\n",
		wantNoTransaction: true,
	},
	{
		name:              "a column on a table without one",
		current:           "CREATE TABLE plain (id int PRIMARY KEY);\n",
		desired:           "CREATE TABLE plain (id int PRIMARY KEY, extra int);\n",
		wantNoTransaction: false,
	},
}

// generatedPair reads the up and down files of the one migration in dir.
func generatedPair(c *qt.C, dir string) (up, down string) {
	c.Helper()
	ups, err := filepath.Glob(filepath.Join(dir, "*.up.sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(ups, qt.HasLen, 1)
	upBytes, err := os.ReadFile(ups[0])
	c.Assert(err, qt.IsNil)
	downBytes, err := os.ReadFile(strings.TrimSuffix(ups[0], ".up.sql") + ".down.sql")
	c.Assert(err, qt.IsNil)
	return string(upBytes), string(downBytes)
}

// TestMigrationsGenerateWritesFilesTheMigratorRunsE2E generates each change,
// applies it and rolls it back with the default transaction mode.
func TestMigrationsGenerateWritesFilesTheMigratorRunsE2E(t *testing.T) {
	for _, engine := range singleStatementTriggerEngines {
		for _, change := range generatedTransactionModes {
			t.Run(engine.name+"/"+change.name, func(t *testing.T) {
				c := qt.New(t)
				scratch := newMySQLFamilyScratch(c, engine.engine)
				_, target := scratch.builtFrom(c, change.current)
				desired := filepath.Join(c.TempDir(), "schema.sql")
				c.Assert(os.WriteFile(desired, []byte(change.desired), 0o600), qt.IsNil)
				dir := c.TempDir()

				runPtahNative(c, "migrations", "generate", "--db-url", target, "--schema-file", desired,
					"--migrations-dir", dir, "--name", "change")
				up, down := generatedPair(c, dir)
				applied, applyErr := runPtahNativeWithError("migrations", "up", "--db-url", target,
					"--migrations-dir", dir, "--allow-destructive")
				rolledBack, rollbackErr := runPtahNativeWithError("migrations", "down", "--db-url", target,
					"--migrations-dir", dir, "--target", "0", "--confirm")

				c.Assert(strings.Contains(up, "-- +ptah no_transaction\n"), qt.Equals, change.wantNoTransaction)
				c.Assert(strings.Contains(down, "-- +ptah no_transaction\n"), qt.Equals, change.wantNoTransaction)
				c.Assert(up, qt.Contains, "-- +ptah lock_timeout=3s\n")
				c.Assert(applyErr, qt.IsNil, qt.Commentf("%s", applied))
				c.Assert(rollbackErr, qt.IsNil, qt.Commentf("%s", rolledBack))
			})
		}
	}
}

// TestCompatMigrateDiffWritesFilesTheMigratorRunsE2E is the same question on
// the Atlas layout: `ptah-compat migrate diff` tags a file the migrator would
// refuse with `-- atlas:txmode none`, and `migrate apply` runs the directory
// with its default --tx-mode. The dev database is on a second server, declared
// disposable, because the replay creates triggers.
func TestCompatMigrateDiffWritesFilesTheMigratorRunsE2E(t *testing.T) {
	for _, engine := range mysqlDevServerEngines {
		for _, change := range generatedTransactionModes {
			t.Run(engine.name+"/"+change.name, func(t *testing.T) {
				c := qt.New(t)
				envbooltest.Set(devdocker.DisposableServerEnvVar, "1")(c)
				_, target := newMySQLFamilyScratch(c, engine.admin).database(c, "compat_target")
				_, dev := newMySQLFamilyScratch(c, engine.dev).database(c, "compat_dev")
				dir := c.TempDir()
				for _, step := range []struct{ name, schema string }{
					{"base", change.current}, {"change", change.desired},
				} {
					file := filepath.Join(c.TempDir(), step.name+".sql")
					c.Assert(os.WriteFile(file, []byte(step.schema), 0o600), qt.IsNil)
					out, err := runCompatVerb("migrate", "diff", step.name, "--dir", "file://"+dir,
						"--to", "file://"+file, "--dev-url", dev)
					c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
				}
				written := readMigrationNamed(c, dir, "change")

				applied, err := runCompatVerb("migrate", "apply", "--url", target, "--dir", "file://"+dir)

				c.Assert(strings.HasPrefix(written, "-- atlas:txmode none\n"), qt.Equals, change.wantNoTransaction)
				c.Assert(err, qt.IsNil, qt.Commentf("%s", applied))
			})
		}
	}
}
