//go:build integration

package integration_test

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/devdocker"
	"ptah.run/internal/envbool/envbooltest"
)

// The scenarios ariga/atlas#3534 reports, applied with `ptah schema apply` on
// both engines: a renamed trigger whose body writes a new column of another
// table, the same trigger changed in place, and a trigger pointed away from a
// column the change drops. The plan for each holds LOCK TABLES on MySQL, and on
// MariaDB for the rename; this asks the server to run it and reads back what it
// built (stokaro/ptah#4014).

// triggerSwapScenarios are the current and desired schemas of each scenario,
// the trigger the desired schema names, and the query that reads the audit
// column a write fills.
var triggerSwapScenarios = []struct {
	name        string
	current     string
	desired     string
	wantTrigger string
	auditQuery  string
}{
	{
		name:        "rename, writing a new column",
		current:     triggerSwapSchema("audit_2025_03_20", "tbl_time", "NOW()"),
		desired:     triggerSwapSchema("audit_2025_06_12", "tbl_time, NewColumn", "NOW(), NEW.name"),
		wantTrigger: "audit_2025_06_12",
		auditQuery:  auditNewColumn,
	},
	{
		name:        "same name, new body",
		current:     triggerSwapSchema("audit", "tbl_time", "NOW()"),
		desired:     triggerSwapSchema("audit", "tbl_time, NewColumn", "NOW(), NEW.name"),
		wantTrigger: "audit",
		auditQuery:  auditNewColumn,
	},
	{
		name: "the old body reads a dropped column",
		current: "CREATE TABLE mytable (id int PRIMARY KEY AUTO_INCREMENT, name varchar(100), legacy varchar(100));\n" +
			"CREATE TABLE secondtable (id int PRIMARY KEY AUTO_INCREMENT, label varchar(100));\n" +
			"CREATE TRIGGER audit AFTER INSERT ON mytable FOR EACH ROW INSERT INTO secondtable (label) VALUES (NEW.legacy);\n",
		desired: "CREATE TABLE mytable (id int PRIMARY KEY AUTO_INCREMENT, name varchar(100));\n" +
			"CREATE TABLE secondtable (id int PRIMARY KEY AUTO_INCREMENT, label varchar(100));\n" +
			"CREATE TRIGGER audit AFTER INSERT ON mytable FOR EACH ROW INSERT INTO secondtable (label) VALUES (NEW.name);\n",
		wantTrigger: "audit",
		auditQuery:  auditLabel,
	},
}

// TestSchemaApplySwapsMySQLTriggersE2E applies each scenario, then inserts a
// row: the trigger the schema declares is the only one, it writes the audit
// row, and a second plan finds nothing to do.
func TestSchemaApplySwapsMySQLTriggersE2E(t *testing.T) {
	for _, engine := range singleStatementTriggerEngines {
		for _, scenario := range triggerSwapScenarios {
			t.Run(engine.name+"/"+scenario.name, func(t *testing.T) {
				c := qt.New(t)
				scratch := newMySQLFamilyScratch(c, engine.engine)
				name, target := scratch.builtFrom(c, scenario.current)
				file := filepath.Join(c.TempDir(), "schema.sql")
				c.Assert(os.WriteFile(file, []byte(scenario.desired), 0o600), qt.IsNil)

				runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", file, "--auto-approve")
				conn, err := sql.Open("mysql", mySQLDSNForDatabase(c, scratch.adminDSN, name))
				c.Assert(err, qt.IsNil)
				defer func() { c.Check(conn.Close(), qt.IsNil) }()
				_, err = conn.ExecContext(c.Context(), "INSERT INTO mytable (name) VALUES ('after-apply')")
				c.Assert(err, qt.IsNil)
				var triggers string
				c.Assert(conn.QueryRowContext(c.Context(),
					"SELECT GROUP_CONCAT(TRIGGER_NAME) FROM information_schema.TRIGGERS WHERE TRIGGER_SCHEMA = ?", name,
				).Scan(&triggers), qt.IsNil)

				c.Assert(triggers, qt.Equals, scenario.wantTrigger)
				c.Assert(auditRows(c, conn, scenario.auditQuery), qt.DeepEquals, []string{"after-apply"})
				c.Assert(runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", file, "--dry-run"),
					qt.Contains, "Schema is synced")
			})
		}
	}
}

// TestSchemaApplyRehearsesMySQLTriggerBodiesOnTheDevDatabaseE2E applies each
// scenario with --dev-url, so the plan is rehearsed on the dev database before
// it reaches the target. Every new body reads NEW.name, and the rehearsal reads
// that NEW as the trigger's row rather than as a database (stokaro/ptah#4016).
//
// The dev database lives on a second server: the rehearsal empties it
// afterwards, and that cleanup refuses while another database on its server
// holds a trigger, which the target here does.
func TestSchemaApplyRehearsesMySQLTriggerBodiesOnTheDevDatabaseE2E(t *testing.T) {
	for _, engine := range mysqlDevServerEngines {
		for _, scenario := range triggerSwapScenarios {
			t.Run(engine.name+"/"+scenario.name, func(t *testing.T) {
				c := qt.New(t)
				scratch := newMySQLFamilyScratch(c, engine.admin)
				name, target := scratch.builtFrom(c, scenario.current)
				_, dev := newMySQLFamilyScratch(c, engine.dev).database(c, "rehearsal_dev")
				file := filepath.Join(c.TempDir(), "schema.sql")
				c.Assert(os.WriteFile(file, []byte(scenario.desired), 0o600), qt.IsNil)

				out, err := runPtahNativeWithError("schema", "apply", "--db-url", target, "--schema-file", file,
					"--dev-url", dev, "--auto-approve")
				c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
				conn, err := sql.Open("mysql", mySQLDSNForDatabase(c, scratch.adminDSN, name))
				c.Assert(err, qt.IsNil)
				defer func() { c.Check(conn.Close(), qt.IsNil) }()
				_, err = conn.ExecContext(c.Context(), "INSERT INTO mytable (name) VALUES ('after-apply')")
				c.Assert(err, qt.IsNil)

				c.Assert(out, qt.Contains, "Schema apply completed successfully.")
				c.Assert(auditRows(c, conn, scenario.auditQuery), qt.DeepEquals, []string{"after-apply"})
			})
		}
	}
}

// tablesIn counts the tables of database, read through the scratch's
// administrative connection.
func (s mysqlScratch) tablesIn(c *qt.C, database string) int {
	c.Helper()
	var tables int
	c.Assert(s.admin.QueryRowContext(c.Context(),
		"SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = ?", database,
	).Scan(&tables), qt.IsNil)
	return tables
}

// TestMigrateDiffReplaysAMySQLTriggerSwapE2E writes the migration that renames
// the audit trigger with ptah-compat, then diffs the directory against the same
// schema: the replay runs the swap's LOCK TABLES on the dev database, and the
// directory is synced. The dev database is declared disposable because the
// replay creates triggers.
func TestMigrateDiffReplaysAMySQLTriggerSwapE2E(t *testing.T) {
	for _, engine := range singleStatementTriggerEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			envbooltest.Set(devdocker.DisposableServerEnvVar, "1")(c)
			scratch := newMySQLFamilyScratch(c, engine.engine)
			_, dev := scratch.database(c, "swap_dev")
			dir := c.TempDir()
			current := filepath.Join(c.TempDir(), "current.sql")
			desired := filepath.Join(c.TempDir(), "desired.sql")
			c.Assert(os.WriteFile(current, []byte(triggerSwapScenarios[0].current), 0o600), qt.IsNil)
			c.Assert(os.WriteFile(desired, []byte(triggerSwapScenarios[0].desired), 0o600), qt.IsNil)
			for _, step := range []struct{ name, schema string }{{"base", current}, {"rename_audit", desired}} {
				out, err := runCompatVerb("migrate", "diff", step.name, "--dir", "file://"+dir,
					"--to", "file://"+step.schema, "--dev-url", dev)
				c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			}
			swaps, err := filepath.Glob(filepath.Join(dir, "*_rename_audit.sql"))
			c.Assert(err, qt.IsNil)
			c.Assert(swaps, qt.HasLen, 1)
			swap, err := os.ReadFile(swaps[0])
			c.Assert(err, qt.IsNil)

			out, err := runCompatVerb("migrate", "diff", "--dir", "file://"+dir,
				"--to", "file://"+desired, "--dev-url", dev, "--dry-run")

			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			c.Assert(string(swap), qt.Contains, "LOCK TABLES `mytable` WRITE;")
			c.Assert(out, qt.Contains, "The migration directory is synced with the desired state")
		})
	}
}

// TestMigrationsLintCleansTheDevDatabaseAfterAReplayFailsInsideALockE2E replays
// a swap whose new body the server refuses while the replay session holds
// LOCK TABLES. The lint reports the refusal, and the dev database is empty
// afterwards: the cleanup runs on the session that still held the lock.
func TestMigrationsLintCleansTheDevDatabaseAfterAReplayFailsInsideALockE2E(t *testing.T) {
	for _, engine := range singleStatementTriggerEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			envbooltest.Set(devdocker.DisposableServerEnvVar, "1")(c)
			scratch := newMySQLFamilyScratch(c, engine.engine)
			devName, dev := scratch.database(c, "swap_dev")
			dir := c.TempDir()
			files := map[string]string{
				"0000000001_base.up.sql":   triggerSwapSchema("audit", "tbl_time", "NOW()"),
				"0000000001_base.down.sql": "DROP TABLE secondtable;\nDROP TABLE mytable;\n",
				"0000000002_swap.up.sql": "LOCK TABLES `mytable` WRITE;\n" +
					"DROP TRIGGER IF EXISTS `audit`;\n" +
					"CREATE TRIGGER `audit` AFTER INSERT ON `mytable` FOR EACH ROW SET @x = NEW.missing;\n" +
					"UNLOCK TABLES;\n",
				"0000000002_swap.down.sql": "SELECT 1;\n",
			}
			for name, body := range files {
				c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600), qt.IsNil)
			}

			out, err := runPtahNativeWithError("migrations", "lint", "--dir", dir, "--dev-url", dev)

			c.Assert(err, qt.ErrorMatches, `(?s).*Unknown column 'missing' in 'NEW'.*`, qt.Commentf("%s", out))
			c.Assert(scratch.tablesIn(c, devName), qt.Equals, 0)
		})
	}
}
