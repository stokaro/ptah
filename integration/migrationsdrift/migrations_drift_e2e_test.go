//go:build integration

package migrationsdrift_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/atlasurl"
	"ptah.run/internal/cli/migrationsdrift"
	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
	"ptah.run/migration/migrator"
)

// These tests run `ptah migrations drift` against a live server: whether a
// replayed history matches the database is decided by what each server's
// catalog says, and the migration lock the report holds is the server's.

// driftMigrations is a history whose second migration adds an index, so a
// database at version 1 has the index pending and must not read it as drift.
var driftMigrations = map[string]string{
	"0000000001_users.up.sql":         "CREATE TABLE drift_users (id BIGINT PRIMARY KEY, email VARCHAR(255) NOT NULL);\n",
	"0000000001_users.down.sql":       "DROP TABLE drift_users;\n",
	"0000000002_users_email.up.sql":   "CREATE INDEX drift_users_email ON drift_users (email);\n",
	"0000000002_users_email.down.sql": "DROP INDEX drift_users_email;\n",
}

// driftEngine is a server the report runs against, reached with an account
// that may create databases, with the statements that differ between engines.
// Each query reads the connection's own schema.
type driftEngine struct {
	name   string
	admin  dbtarget.Engine
	drop   string
	tables string
	// nickname counts the columns named nickname that drift_users has.
	nickname string
	// ptahTable and atlasTable are the default revision table of each format,
	// quoted and qualified as the engine names it.
	ptahTable, atlasTable string
}

var (
	postgresDrift = driftEngine{
		name: "postgres", admin: dbtarget.PostgreSQL,
		drop:   "DROP DATABASE IF EXISTS %s WITH (FORCE)",
		tables: "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = current_schema()",
		nickname: "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = current_schema() " +
			"AND table_name = 'drift_users' AND column_name = 'nickname'",
		// Atlas keeps its revision table in a schema of its own.
		ptahTable: `"schema_migrations"`, atlasTable: `"atlas_schema_revisions"."atlas_schema_revisions"`,
	}
	mysqlDrift = driftEngine{
		name: "mysql", admin: dbtarget.MySQLAdmin,
		drop:   "DROP DATABASE IF EXISTS %s",
		tables: "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE()",
		nickname: "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = DATABASE() " +
			"AND table_name = 'drift_users' AND column_name = 'nickname'",
		ptahTable: "`schema_migrations`", atlasTable: "`atlas_schema_revisions`",
	}
	driftEngines = []driftEngine{postgresDrift, mysqlDrift}
)

// driftRun is a target database migrated to a version of driftMigrations and
// an empty shadow database, both of the test's own on the engine's server.
type driftRun struct {
	engine              driftEngine
	dir, target, shadow string
	// targetName is the target database's name on the server.
	targetName string
}

func newDriftRun(c *qt.C, engine driftEngine, version string, outOfBand ...string) driftRun {
	c.Helper()
	return newDriftRunApplying(c, engine, []string{"--to-version", version}, outOfBand...)
}

// newDriftRunApplying is newDriftRun with the flags of the `migrations up`
// that migrates the target.
func newDriftRunApplying(c *qt.C, engine driftEngine, upFlags []string, outOfBand ...string) driftRun {
	c.Helper()
	address := dbtarget.URL(c, engine.admin)
	admin, err := dbschema.ConnectToDatabase(c.Context(), address)
	c.Assert(err, qt.IsNil)
	stamp := time.Now().UnixNano()
	names := []string{fmt.Sprintf("ptah_mdrift_target_%d", stamp), fmt.Sprintf("ptah_mdrift_shadow_%d", stamp)}
	urls := make([]string, 0, len(names))
	for _, name := range names {
		_, err = admin.ExecContext(c.Context(), "CREATE DATABASE "+name)
		c.Assert(err, qt.IsNil)
		url, err := atlasurl.WithDatabaseName(address, name)
		c.Assert(err, qt.IsNil)
		urls = append(urls, url)
	}
	c.Cleanup(func() {
		for _, name := range names {
			_, dropErr := admin.ExecContext(context.Background(), fmt.Sprintf(engine.drop, name))
			c.Check(dropErr, qt.IsNil)
		}
		dbschema.CloseAndWarn(admin)
	})
	run := driftRun{engine: engine, dir: c.TempDir(), target: urls[0], shadow: urls[1], targetName: names[0]}
	for name, content := range driftMigrations {
		c.Assert(os.WriteFile(filepath.Join(run.dir, name), []byte(content), 0o600), qt.IsNil)
	}
	applied := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir()},
		append([]string{"migrations", "up", "--db-url", run.target, "--migrations-dir", run.dir}, upFlags...)...)
	c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
	run.exec(c, outOfBand...)
	return run
}

func (r driftRun) exec(c *qt.C, statements ...string) {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), r.target)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	for _, statement := range statements {
		_, err := conn.ExecContext(c.Context(), statement)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
	}
}

func (r driftRun) scalar(c *qt.C, url, query string) int {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), url)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	var value int
	c.Assert(conn.QueryRowContext(c.Context(), query).Scan(&value), qt.IsNil)
	return value
}

// appliedVersion is the highest version the target's revision table records
// as applied.
func (r driftRun) appliedVersion(c *qt.C) int {
	return r.scalar(c, r.target, "SELECT COALESCE(MAX(version), 0) FROM schema_migrations WHERE state = 'applied'")
}

// nicknameColumns counts the columns named nickname that the target's
// drift_users table has: 1 while the out-of-band column is there, 0 after.
func (r driftRun) nicknameColumns(c *qt.C) int {
	return r.scalar(c, r.target, r.engine.nickname)
}

// document is the report with its diff left undecoded: the diff is the
// planner's input, and these tests read what the report says about it.
type document struct {
	migrationsdrift.Document
	Diff json.RawMessage `json:"diff"`
}

func (r driftRun) report(c *qt.C, extra ...string) (document, clirun.Result) {
	c.Helper()
	args := append([]string{
		"migrations", "drift", "--db-url", r.target, "--migrations-dir", r.dir, "--shadow-db", r.shadow, "--json",
	}, extra...)
	result := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir(), Timeout: time.Minute}, args...)
	var got document
	c.Assert(json.Unmarshal([]byte(result.Stdout), &got), qt.IsNil,
		qt.Commentf("stdout:\n%s\nstderr:\n%s", result.Stdout, result.Stderr))
	return got, result
}

// A history in Atlas's revision format is read where Atlas keeps it, a schema
// of its own on PostgreSQL, and the report names that table as the engine
// qualifies it.
func TestMigrationsDriftE2E_HappyPathReadsAnAtlasFormatHistory(t *testing.T) {
	for _, engine := range driftEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			run := newDriftRunApplying(c, engine, []string{"--to-version", "1", "--revision-format", "atlas"})

			got, result := run.report(c, "--revision-format", "atlas")

			c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("%s", result.Stderr))
			c.Assert(got.RevisionTable, qt.Equals, engine.atlasTable)
			c.Assert(got.CurrentVersion, qt.Equals, "1")
			c.Assert(got.Check.Outcome, qt.Equals, migrator.DriftClean)
			c.Assert(got.Warnings, qt.HasLen, 0)
		})
	}
}

// The same history read in Ptah's format records no applied migration, so
// every object reads as drift. Naming --migrations-table and
// --migrations-schema alone would send a person to a table Ptah's format
// cannot read, so the report names the Atlas table that holds the history and
// the flag that reads it.
func TestMigrationsDriftE2E_FailurePathNamesAnAtlasFormatHistory(t *testing.T) {
	for _, engine := range driftEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			run := newDriftRunApplying(c, engine, []string{"--to-version", "1", "--revision-format", "atlas"})

			got, result := run.report(c)

			c.Assert(result.ExitCode, qt.Equals, 1, qt.Commentf("%s", result.Stderr))
			c.Assert(got.RevisionTable, qt.Equals, engine.ptahTable)
			c.Assert(got.Check.Outcome, qt.Equals, migrator.DriftDrifted)
			c.Assert(got.Warnings, qt.DeepEquals, []string{
				"the revision table " + engine.ptahTable + " records no applied migration, or does not exist, " +
					"but the atlas-format revision table " + engine.atlasTable + " records 1 applied migration: " +
					"run the report again with --revision-format atlas; " +
					"the statements below would drop every object the report compares",
			})
		})
	}
}

func TestMigrationsDriftE2E_HappyPath(t *testing.T) {
	for _, engine := range driftEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			run := newDriftRun(c, engine, "1")

			got, result := run.report(c)

			c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("%s", result.Stderr))
			c.Assert(got.Check.Outcome, qt.Equals, migrator.DriftClean)
			c.Assert(got.Check.Replayed, qt.DeepEquals, []int64{1})
			c.Assert(got.CurrentVersion, qt.Equals, "1")
			c.Assert(got.Pending, qt.DeepEquals, []string{"2"})
			c.Assert(run.appliedVersion(c), qt.Equals, 1)
			c.Assert(run.scalar(c, run.shadow, engine.tables), qt.Equals, 0)
		})
	}
}

// The report finds an out-of-band column and plans the statements that remove
// it. The database decides both: the column is in its catalog, and after the
// statements run the next report finds the database matching its history.
func TestMigrationsDriftE2E_FailurePathReportsDrift(t *testing.T) {
	for _, engine := range driftEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			run := newDriftRun(c, engine, "1", "ALTER TABLE drift_users ADD COLUMN nickname VARCHAR(40)")

			got, result := run.report(c)

			c.Assert(result.ExitCode, qt.Equals, 1, qt.Commentf("%s", result.Stderr))
			c.Assert(got.Check.Outcome, qt.Equals, migrator.DriftDrifted)
			c.Assert(got.Check.Findings, qt.DeepEquals, []migrator.DriftFinding{{
				Kind: "extra_column", Object: "drift_users.nickname", Table: "drift_users", Column: "nickname",
				Message: "extra column drift_users.nickname",
			}})
			c.Assert(got.Diff, qt.IsNotNil)
			c.Assert(got.PlanError, qt.Equals, "")
			c.Assert(got.Statements, qt.Not(qt.HasLen), 0)
			c.Assert(run.nicknameColumns(c), qt.Equals, 1)
			c.Assert(run.appliedVersion(c), qt.Equals, 1)
			c.Assert(run.scalar(c, run.shadow, engine.tables), qt.Equals, 0)

			run.exec(c, got.Statements...)
			again, result := run.report(c)

			c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("%s", result.Stderr))
			c.Assert(again.Check.Outcome, qt.Equals, migrator.DriftClean)
			c.Assert(run.nicknameColumns(c), qt.Equals, 0)
		})
	}
}

// The report holds the migration lock while it reads the history: a runner
// holding the lock makes it wait, and --migration-lock-timeout bounds the
// wait. A report under another lock name does not wait, which is what proves
// the name reaches the lock rather than the run failing for another reason,
// and the name the project file sets reaches it the same way.
func TestMigrationsDriftE2E_FailurePathWaitsForTheMigrationLock(t *testing.T) {
	for _, engine := range driftEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			run := newDriftRun(c, engine, "1")
			conn, err := dbschema.ConnectToDatabase(c.Context(), run.target)
			c.Assert(err, qt.IsNil)
			defer dbschema.CloseAndWarn(conn)
			holder, err := migrator.NewFSMigrator(conn, os.DirFS(run.dir))
			c.Assert(err, qt.IsNil)

			project := filepath.Join(c.TempDir(), "ptah.yaml")
			c.Assert(os.WriteFile(project, []byte("migration:\n  migration_lock_name: ptah_drift_project\n"), 0o600), qt.IsNil)

			var waited, separate, configured clirun.Result
			err = holder.HoldMigrationLock(c.Context(), "test", func(context.Context) error {
				_, waited = run.report(c, "--migration-lock-timeout", "1s")
				_, separate = run.report(c, "--migration-lock-timeout", "1s", "--migration-lock-name", "ptah_drift_report")
				_, configured = run.report(c, "--migration-lock-timeout", "1s", "--config", project)
				return nil
			})

			c.Assert(err, qt.IsNil)
			c.Assert(waited.ExitCode, qt.Equals, 2, qt.Commentf("%s", waited.Stderr))
			c.Assert(waited.Stderr, qt.Contains, `timed out acquiring migration lock "ptah_migrate"`)
			c.Assert(separate.ExitCode, qt.Equals, 0, qt.Commentf("%s", separate.Stderr))
			c.Assert(configured.ExitCode, qt.Equals, 0, qt.Commentf("%s", configured.Stderr))
		})
	}
}
