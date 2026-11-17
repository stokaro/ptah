//go:build integration

package migrateup_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/atlasurl"
	"ptah.run/internal/clirun"
	"ptah.run/internal/dblock"
	"ptah.run/internal/dbtarget"
	"ptah.run/migration/migrator"
)

// These tests drive the shipped binary against PostgreSQL. The directory's
// versioned migration creates a table and an audit table; its repeatable
// migration rebuilds a view and appends one audit row. The audit rows and the
// view definition are read back from the server, so "ran" and "did not run"
// are what the database holds, not what a log line says.

const repeatableE2EVersioned = `CREATE TABLE accounts (id INT PRIMARY KEY, name TEXT NOT NULL);
CREATE TABLE repeatable_audit (body TEXT NOT NULL);
`

// repeatableE2EView is the repeatable body, tagged so the audit row says which
// content ran.
func repeatableE2EView(tag, columns string) string {
	return fmt.Sprintf(`CREATE OR REPLACE VIEW account_names AS SELECT %s FROM accounts;
INSERT INTO repeatable_audit (body) VALUES ('%s');
`, columns, tag)
}

// repeatableE2ETarget is a PostgreSQL database of the test's own, a direct
// handle on it, and a migration directory.
type repeatableE2ETarget struct {
	url  string
	conn *dbschema.DatabaseConnection
	dir  string
}

func newRepeatableE2ETarget(c *qt.C) *repeatableE2ETarget {
	c.Helper()
	address := dbtarget.URL(c, dbtarget.PostgreSQL)
	admin, err := dbschema.ConnectToDatabase(c.Context(), address)
	c.Assert(err, qt.IsNil)
	name := fmt.Sprintf("ptah_rt_repeatable_%d", time.Now().UnixNano())
	_, err = admin.ExecContext(c.Context(), "CREATE DATABASE "+name)
	c.Assert(err, qt.IsNil)
	target, err := atlasurl.WithDatabaseName(address, name)
	c.Assert(err, qt.IsNil)
	conn, err := dbschema.ConnectToDatabase(c.Context(), target)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		dbschema.CloseAndWarn(conn)
		_, dropErr := admin.ExecContext(context.Background(), "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
		c.Check(dropErr, qt.IsNil)
		dbschema.CloseAndWarn(admin)
	})
	dir := c.TempDir()
	r := &repeatableE2ETarget{url: target, conn: conn, dir: dir}
	r.write(c, "0000000001_accounts.up.sql", repeatableE2EVersioned)
	r.write(c, "0000000001_accounts.down.sql", "DROP TABLE repeatable_audit;\nDROP TABLE accounts;\n")
	r.write(c, "R__account_names.sql", repeatableE2EView("v1", "name"))
	return r
}

func (r *repeatableE2ETarget) write(c *qt.C, name, body string) {
	c.Helper()
	c.Assert(os.WriteFile(filepath.Join(r.dir, name), []byte(body), 0o600), qt.IsNil)
}

// ptah runs the binary in a directory of its own.
func (r *repeatableE2ETarget) ptah(c *qt.C, args ...string) clirun.Result {
	c.Helper()
	return clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir()}, args...)
}

// up runs `migrations up --json` and reads the run document.
func (r *repeatableE2ETarget) up(c *qt.C, extra ...string) (clirun.Result, migrator.RunResult) {
	c.Helper()
	result := r.ptah(c, append([]string{"migrations", "up", "--db-url", r.url, "--migrations-dir", r.dir, "--json"}, extra...)...)
	var document migrator.RunResult
	c.Assert(json.Unmarshal([]byte(result.Stdout), &document), qt.IsNil,
		qt.Commentf("stdout:\n%s\nstderr:\n%s", result.Stdout, result.Stderr))
	return result, document
}

// status runs `migrations status --json` and reads the document.
func (r *repeatableE2ETarget) status(c *qt.C) migrator.MigrationStatus {
	c.Helper()
	result := r.ptah(c, "migrations", "status", "--db-url", r.url, "--migrations-dir", r.dir, "--json")
	var status migrator.MigrationStatus
	c.Assert(json.Unmarshal([]byte(result.Stdout), &status), qt.IsNil,
		qt.Commentf("stdout:\n%s\nstderr:\n%s", result.Stdout, result.Stderr))
	return status
}

// audit reads the audit rows in the order the server wrote them.
func (r *repeatableE2ETarget) audit(c *qt.C) []string {
	c.Helper()
	rows, err := r.conn.QueryContext(c.Context(), "SELECT body FROM repeatable_audit ORDER BY ctid")
	c.Assert(err, qt.IsNil)
	defer rows.Close()
	bodies := make([]string, 0)
	for rows.Next() {
		var body string
		c.Assert(rows.Scan(&body), qt.IsNil)
		bodies = append(bodies, body)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return bodies
}

// viewColumns reads the columns of the view the repeatable migration builds.
func (r *repeatableE2ETarget) viewColumns(c *qt.C) []string {
	c.Helper()
	rows, err := r.conn.QueryContext(c.Context(),
		`SELECT column_name FROM information_schema.columns WHERE table_name = 'account_names' ORDER BY ordinal_position`)
	c.Assert(err, qt.IsNil)
	defer rows.Close()
	columns := make([]string, 0)
	for rows.Next() {
		var column string
		c.Assert(rows.Scan(&column), qt.IsNil)
		columns = append(columns, column)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return columns
}

// recordTableCount counts the tables named schema_migrations_repeatable.
func (r *repeatableE2ETarget) recordTableCount(c *qt.C) int {
	c.Helper()
	var count int
	c.Assert(r.conn.QueryRowContext(c.Context(),
		`SELECT count(*) FROM pg_tables WHERE tablename = 'schema_migrations_repeatable'`).Scan(&count), qt.IsNil)
	return count
}

func e2eH1(body string) string {
	sum := sha256.Sum256([]byte(body))
	return "h1:" + base64.StdEncoding.EncodeToString(sum[:])
}

func TestRepeatableMigrationsE2E_HappyPath(t *testing.T) {
	t.Run("a versioned table and a repeatable view apply once, and an unchanged rerun runs no SQL", func(t *testing.T) {
		c := qt.New(t)
		r := newRepeatableE2ETarget(c)

		result, document := r.up(c)
		c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("%s", result.Stderr))
		c.Assert(document.Outcome, qt.Equals, migrator.RunOutcomeApplied)
		c.Assert(document.Applied, qt.DeepEquals, []int64{1})
		c.Assert(document.AppliedRepeatables, qt.DeepEquals, []string{"account_names"})
		c.Assert(r.viewColumns(c), qt.DeepEquals, []string{"name"})

		result, document = r.up(c)
		c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("%s", result.Stderr))
		c.Assert(document.Outcome, qt.Equals, migrator.RunOutcomeUpToDate)
		c.Assert(document.PlannedRepeatables, qt.HasLen, 0)
		c.Assert(r.audit(c), qt.DeepEquals, []string{"v1"})
	})

	t.Run("a changed view runs once and then is a no-op", func(t *testing.T) {
		c := qt.New(t)
		r := newRepeatableE2ETarget(c)
		result, _ := r.up(c)
		c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("%s", result.Stderr))
		changed := repeatableE2EView("v2", "name, id")
		r.write(c, "R__account_names.sql", changed)

		result, document := r.up(c)
		c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("%s", result.Stderr))
		c.Assert(document.PlannedRepeatables, qt.DeepEquals, []migrator.RepeatableWork{{
			Identity:        "account_names",
			Path:            "R__account_names.sql",
			Checksum:        e2eH1(changed),
			AppliedChecksum: e2eH1(repeatableE2EView("v1", "name")),
			Reason:          migrator.RepeatableReasonChanged,
		}})
		result, _ = r.up(c)
		c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("%s", result.Stderr))
		c.Assert(r.audit(c), qt.DeepEquals, []string{"v1", "v2"})
		c.Assert(r.viewColumns(c), qt.DeepEquals, []string{"name", "id"})
	})

	t.Run("status and a dry run predict the work an apply does, and the preview writes nothing", func(t *testing.T) {
		c := qt.New(t)
		r := newRepeatableE2ETarget(c)

		status := r.status(c)
		c.Assert(status.HasPendingChanges, qt.IsTrue)
		c.Assert(status.Repeatables, qt.HasLen, 1)
		c.Assert(status.Repeatables[0].State, qt.Equals, migrator.RepeatableStatePending)
		c.Assert(status.Repeatables[0].Checksum, qt.Equals, e2eH1(repeatableE2EView("v1", "name")))

		result, preview := r.up(c, "--dry-run")
		c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("%s", result.Stderr))
		c.Assert(preview.Outcome, qt.Equals, migrator.RunOutcomeDryRun)
		c.Assert(r.recordTableCount(c), qt.Equals, 0)
		c.Assert(userTables(c, r.conn), qt.HasLen, 0)

		result, applied := r.up(c)
		c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("%s", result.Stderr))
		c.Assert(applied.Planned, qt.DeepEquals, preview.Planned)
		c.Assert(applied.PlannedRepeatables, qt.DeepEquals, preview.PlannedRepeatables)
		c.Assert(applied.PlannedRepeatables[0].Checksum, qt.Equals, status.Repeatables[0].Checksum)
	})

	t.Run("two runners over one changed repeatable run it once", func(t *testing.T) {
		c := qt.New(t)
		r := newRepeatableE2ETarget(c)
		result, _ := r.up(c)
		c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("%s", result.Stderr))
		r.write(c, "R__account_names.sql", "SELECT pg_sleep(1);\n"+repeatableE2EView("v2", "name, id"))

		// The test holds the migration lock while both runners start, so both
		// reach it and wait; it is released only once the server shows both
		// waiting, which is what makes the two runs overlap.
		holder, err := dbschema.ConnectToDatabase(c.Context(), r.url)
		c.Assert(err, qt.IsNil)
		c.Cleanup(func() { dbschema.CloseAndWarn(holder) })
		lock, err := dblock.Acquire(c.Context(), holder, "ptah_migrate", time.Second)
		c.Assert(err, qt.IsNil)
		runs := startCompetingRunners(clirun.Build(c, clirun.Ptah), c.TempDir(),
			"migrations", "up", "--db-url", r.url, "--migrations-dir", r.dir, "--migration-lock-timeout", "60s")
		c.Assert(waitForLockWaiters(c, r.conn, 2), qt.IsTrue)
		c.Assert(lock.Release(c.Context()), qt.IsNil)

		outcomes := runs.wait()
		c.Assert(outcomes[0].err, qt.IsNil, qt.Commentf("%s", outcomes[0].output))
		c.Assert(outcomes[1].err, qt.IsNil, qt.Commentf("%s", outcomes[1].output))
		c.Assert(r.audit(c), qt.DeepEquals, []string{"v1", "v2"})
	})

	t.Run("a failed repeatable is retried once it is fixed", func(t *testing.T) {
		c := qt.New(t)
		r := newRepeatableE2ETarget(c)
		r.write(c, "R__account_names.sql", "INSERT INTO repeatable_audit (body) VALUES ('failed');\nSELECT * FROM missing_table;\n")
		result, document := r.up(c)
		c.Assert(result.ExitCode, qt.Not(qt.Equals), 0)
		c.Assert(document.Outcome, qt.Equals, migrator.RunOutcomePartial)

		r.write(c, "R__account_names.sql", repeatableE2EView("v1", "name"))
		result, document = r.up(c, "--allow-dirty")
		c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("%s", result.Stderr))
		c.Assert(document.PlannedRepeatables[0].Reason, qt.Equals, migrator.RepeatableReasonRetry)
		c.Assert(r.audit(c), qt.DeepEquals, []string{"v1"})
		c.Assert(r.status(c).Repeatables[0].State, qt.Equals, migrator.RepeatableStateApplied)
	})

	t.Run("a renamed repeatable runs under its new identity and the old record is reported as orphaned", func(t *testing.T) {
		c := qt.New(t)
		r := newRepeatableE2ETarget(c)
		result, _ := r.up(c)
		c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("%s", result.Stderr))
		c.Assert(os.Rename(filepath.Join(r.dir, "R__account_names.sql"), filepath.Join(r.dir, "R__account_names_v2.sql")), qt.IsNil)

		result, document := r.up(c)
		c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("%s", result.Stderr))
		c.Assert(document.AppliedRepeatables, qt.DeepEquals, []string{"account_names_v2"})
		c.Assert(r.audit(c), qt.DeepEquals, []string{"v1", "v1"})
		records := r.status(c).Repeatables
		c.Assert(records, qt.HasLen, 2)
		c.Assert(records[0].Identity, qt.Equals, "account_names_v2")
		c.Assert(records[0].State, qt.Equals, migrator.RepeatableStateApplied)
		c.Assert(records[1].Identity, qt.Equals, "account_names")
		c.Assert(records[1].State, qt.Equals, migrator.RepeatableStateOrphaned)
		c.Assert(records[1].Orphaned, qt.IsTrue)

		result = r.ptah(c, "migrations", "down", "--db-url", r.url, "--migrations-dir", r.dir, "--confirm")
		c.Assert(result.ExitCode, qt.Not(qt.Equals), 0)
		c.Assert(result.Stderr, qt.Matches,
			`(?s).*cannot roll back: this database records repeatable migrations account_names, account_names_v2,.*`)
		c.Assert(userTables(c, r.conn), qt.Contains, "accounts")
	})

	t.Run("repair removes the row of a deleted file, and the rollback it blocked then runs", func(t *testing.T) {
		c := qt.New(t)
		r := newRepeatableE2ETarget(c)
		result, _ := r.up(c)
		c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("%s", result.Stderr))
		c.Assert(os.Remove(filepath.Join(r.dir, "R__account_names.sql")), qt.IsNil)
		result = r.ptah(c, "migrations", "hash", "--dir", r.dir)
		c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("%s", result.Stderr))

		result = r.ptah(c, "migrations", "repair", "--db-url", r.url, "--migrations-dir", r.dir,
			"--repeatable", "account_names")
		c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("%s", result.Stderr))
		c.Assert(result.Stdout, qt.Equals, "Removed the record of repeatable migration account_names, state orphaned; "+
			"no file of the directory carries it; no migration SQL ran\n")
		c.Assert(r.viewColumns(c), qt.DeepEquals, []string{"name"})
		c.Assert(r.status(c).Repeatables, qt.HasLen, 0)

		_, err := r.conn.ExecContext(c.Context(), "DROP VIEW account_names")
		c.Assert(err, qt.IsNil)
		result = r.ptah(c, "migrations", "down", "--db-url", r.url, "--migrations-dir", r.dir, "--confirm")
		c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("%s", result.Stderr))
		c.Assert(userTables(c, r.conn), qt.Not(qt.Contains), "accounts")
	})
}

func TestRepeatableMigrationsE2E_FailurePath(t *testing.T) {
	t.Run("a body that fails in its transaction leaves no trace and blocks the next run", func(t *testing.T) {
		c := qt.New(t)
		r := newRepeatableE2ETarget(c)
		r.write(c, "R__account_names.sql", "INSERT INTO repeatable_audit (body) VALUES ('failed');\nSELECT * FROM missing_table;\n")

		result, document := r.up(c)
		c.Assert(result.ExitCode, qt.Not(qt.Equals), 0)
		c.Assert(document.Outcome, qt.Equals, migrator.RunOutcomePartial)
		c.Assert(document.Applied, qt.DeepEquals, []int64{1})
		c.Assert(document.Error.Message, qt.Matches, `(?s)repeatable migration account_names failed and took no effect: .*missing_table.*`)
		c.Assert(r.audit(c), qt.HasLen, 0)
		record := r.status(c).Repeatables[0]
		c.Assert(record.State, qt.Equals, migrator.RepeatableStateFailed)
		c.Assert(record.NoTransaction, qt.IsFalse)

		r.write(c, "R__account_names.sql", repeatableE2EView("v1", "name"))
		result, document = r.up(c)
		c.Assert(result.ExitCode, qt.Not(qt.Equals), 0)
		c.Assert(document.Error.Message, qt.Matches, `(?s)repeatable migrations did not finish: account_names .*failed: .*`)
		c.Assert(r.audit(c), qt.HasLen, 0)
	})

	t.Run("a body that fails outside a transaction is reported as possibly partial", func(t *testing.T) {
		c := qt.New(t)
		r := newRepeatableE2ETarget(c)
		r.write(c, "R__account_names.sql",
			"-- +ptah no_transaction\nINSERT INTO repeatable_audit (body) VALUES ('stayed');\nSELECT * FROM missing_table;\n")

		result, document := r.up(c)
		c.Assert(result.ExitCode, qt.Not(qt.Equals), 0)
		c.Assert(document.Outcome, qt.Equals, migrator.RunOutcomeUnknown)
		c.Assert(r.audit(c), qt.DeepEquals, []string{"stayed"})
		record := r.status(c).Repeatables[0]
		c.Assert(record.State, qt.Equals, migrator.RepeatableStateFailed)
		c.Assert(record.NoTransaction, qt.IsTrue)
	})

	t.Run("an edited applied versioned migration is refused after ptah.sum is regenerated", func(t *testing.T) {
		c := qt.New(t)
		r := newRepeatableE2ETarget(c)
		result := r.ptah(c, "migrations", "hash", "--dir", r.dir)
		c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("%s", result.Stderr))
		result, _ = r.up(c)
		c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("%s", result.Stderr))
		r.write(c, "0000000001_accounts.up.sql", repeatableE2EVersioned+"-- edited after it applied\n")
		r.write(c, "R__account_names.sql", repeatableE2EView("v2", "name, id"))
		result = r.ptah(c, "migrations", "hash", "--dir", r.dir)
		c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("%s", result.Stderr))

		result, document := r.up(c)
		c.Assert(result.ExitCode, qt.Not(qt.Equals), 0)
		c.Assert(document.Error.Message, qt.Matches, `(?s).*checksum.*`)
		c.Assert(r.audit(c), qt.DeepEquals, []string{"v1"})
	})

	t.Run("a target below the latest version is refused while repeatable work is pending", func(t *testing.T) {
		c := qt.New(t)
		r := newRepeatableE2ETarget(c)
		r.write(c, "0000000002_more.up.sql", "CREATE TABLE more_accounts (id INT);\n")
		r.write(c, "0000000002_more.down.sql", "DROP TABLE more_accounts;\n")

		result, document := r.up(c, "--to-version", "1")
		c.Assert(result.ExitCode, qt.Not(qt.Equals), 0)
		c.Assert(document.Error.Message, qt.Matches, `cannot run up to target version 1: .*account_names.*`)
		c.Assert(userTables(c, r.conn), qt.Not(qt.Contains), "accounts")
	})

	t.Run("a rollback over an applied repeatable migration is refused", func(t *testing.T) {
		c := qt.New(t)
		r := newRepeatableE2ETarget(c)
		result, _ := r.up(c)
		c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("%s", result.Stderr))

		result = r.ptah(c, "migrations", "down", "--db-url", r.url, "--migrations-dir", r.dir, "--confirm")
		c.Assert(result.ExitCode, qt.Not(qt.Equals), 0)
		c.Assert(result.Stderr, qt.Matches, `(?s).*cannot roll back: this database records repeatable migrations account_names.*`)
		c.Assert(r.viewColumns(c), qt.DeepEquals, []string{"name"})
		c.Assert(r.audit(c), qt.DeepEquals, []string{"v1"})
	})

	t.Run("a rollback after the repeatable file was deleted is refused, and the record stays", func(t *testing.T) {
		c := qt.New(t)
		r := newRepeatableE2ETarget(c)
		result, _ := r.up(c)
		c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("%s", result.Stderr))
		c.Assert(os.Remove(filepath.Join(r.dir, "R__account_names.sql")), qt.IsNil)
		result = r.ptah(c, "migrations", "hash", "--dir", r.dir)
		c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("%s", result.Stderr))

		result = r.ptah(c, "migrations", "down", "--db-url", r.url, "--migrations-dir", r.dir, "--confirm")
		c.Assert(result.ExitCode, qt.Not(qt.Equals), 0)
		c.Assert(result.Stderr, qt.Matches,
			`(?s).*cannot roll back: this database records repeatable migrations account_names, .*"schema_migrations_repeatable".*`)
		c.Assert(r.viewColumns(c), qt.DeepEquals, []string{"name"})
		c.Assert(userTables(c, r.conn), qt.Contains, "accounts")
		records := r.status(c).Repeatables
		c.Assert(records, qt.HasLen, 1)
		c.Assert(records[0].State, qt.Equals, migrator.RepeatableStateOrphaned)
	})

	t.Run("repair keeps the row of a file still in the directory unless forced", func(t *testing.T) {
		c := qt.New(t)
		r := newRepeatableE2ETarget(c)
		result, _ := r.up(c)
		c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("%s", result.Stderr))

		result = r.ptah(c, "migrations", "repair", "--db-url", r.url, "--migrations-dir", r.dir,
			"--repeatable", "account_names")
		c.Assert(result.ExitCode, qt.Not(qt.Equals), 0)
		c.Assert(result.Stderr, qt.Matches, `(?s).*R__account_names.sql carries account_names, .*rerun with --force.*`)
		c.Assert(r.status(c).Repeatables[0].State, qt.Equals, migrator.RepeatableStateApplied)
	})
}

// userTables lists the tables in the public schema, sorted.
func userTables(c *qt.C, conn *dbschema.DatabaseConnection) []string {
	c.Helper()
	rows, err := conn.QueryContext(c.Context(), `SELECT tablename FROM pg_tables WHERE schemaname = 'public' ORDER BY tablename`)
	c.Assert(err, qt.IsNil)
	defer rows.Close()
	tables := make([]string, 0)
	for rows.Next() {
		var table string
		c.Assert(rows.Scan(&table), qt.IsNil)
		tables = append(tables, table)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return tables
}

// runnerOutcome is what one competing process left.
type runnerOutcome struct {
	err    error
	output string
}

// competingRunners is two processes of the same command started together.
type competingRunners struct {
	group    sync.WaitGroup
	outcomes [2]runnerOutcome
}

// startCompetingRunners starts two processes of program with args. Nothing in
// here asserts, because the processes run off the test's goroutine.
func startCompetingRunners(program, dir string, args ...string) *competingRunners {
	runs := &competingRunners{}
	for index := range runs.outcomes {
		runs.group.Go(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			var output bytes.Buffer
			cmd := exec.CommandContext(ctx, program, args...) // #nosec G204 -- the program is the ptah binary this test built
			cmd.Dir = dir
			cmd.Stdout = &output
			cmd.Stderr = &output
			runs.outcomes[index] = runnerOutcome{err: cmd.Run(), output: output.String()}
		})
	}
	return runs
}

// wait returns both outcomes once both processes have ended.
func (r *competingRunners) wait() [2]runnerOutcome {
	r.group.Wait()
	return r.outcomes
}

// waitForLockWaiters reports whether, within half a minute, the server shows
// want sessions of this database asking for the migration lock without holding
// it. The lock is taken with pg_try_advisory_lock asked again until it is
// granted, so a waiter is a session whose last statement was that call and that
// holds no advisory lock.
func waitForLockWaiters(c *qt.C, conn *dbschema.DatabaseConnection, want int) bool {
	c.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		var waiting int
		c.Assert(conn.QueryRowContext(c.Context(), `SELECT count(DISTINCT a.pid) FROM pg_stat_activity a
			WHERE a.datname = current_database() AND a.query LIKE 'SELECT pg_try_advisory_lock(%'
			AND NOT EXISTS (SELECT 1 FROM pg_locks l WHERE l.pid = a.pid AND l.locktype = 'advisory' AND l.granted)`).
			Scan(&waiting), qt.IsNil)
		if waiting >= want {
			return true
		}
		time.Sleep(50 * time.Millisecond)
	}
	return false
}
