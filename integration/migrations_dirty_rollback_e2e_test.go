//go:build integration

package integration_test

import (
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
)

// A rollback that fails leaves its revision row dirty, and the commands that
// meet that row name the one that ends it. `ptah migrations down` refuses a
// second rollback over the row, `ptah migrations status` prints the command,
// and `ptah migrations repair` takes the runner, so a directory written for
// golang-migrate is repaired the way it was rolled back.

// revisionRow is the state and progress the revision row of version records,
// read from the database at url, or the empty string when there is no row.
func revisionRow(c *qt.C, url string, version int64) string {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), url)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	var row string
	c.Assert(conn.QueryRowContext(c.Context(),
		"SELECT COALESCE(max(state || ' ' || applied || '/' || total), '') FROM schema_migrations WHERE version = $1",
		version).Scan(&row), qt.IsNil)
	return row
}

// execOn runs statements on the database at url.
func execOn(c *qt.C, url string, statements ...string) {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), url)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	for _, statement := range statements {
		_, err := conn.ExecContext(c.Context(), statement)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
	}
}

// appliedDirectory writes files, applies them to a scratch database with
// upFlags, and returns the directory and the database.
func appliedDirectory(c *qt.C, binary string, files map[string]string, upFlags ...string) (dir, target string) {
	c.Helper()
	dir = runnerMigrations(c, files)
	target, _ = scratchReplayDatabase(c)
	out, err := runPtahInDir(c.Context(), c.TempDir(), binary,
		append([]string{"migrations", "up", "--migrations-dir", dir, "--db-url", target}, upFlags...)...)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	return dir, target
}

// A rollback whose down file fails inside the migrator's transaction commits
// nothing and leaves the revision dirty. The version then reads as 1, the
// target of the rollback, and a second rollback to it refuses the row rather
// than report the rollback done. The refusal names the repair that ends it.
func TestMigrationsDownRefusesTheRevisionAFailedRollbackLeftE2E(t *testing.T) {
	c := qt.New(t)
	binary := builtPtah(t, c)
	dir, target := appliedDirectory(c, binary,
		ownTransactionDownFiles("DROP TABLE t3;\nINSERT INTO missing_table VALUES (1);\nDROP TABLE t2;\n"))
	out, err := rollBackApplied(c, binary, dir, target, rollback{})
	c.Assert(err, qt.ErrorMatches, "exit status 2", qt.Commentf("%s", out))
	c.Assert(revisionRow(c, target, 2), qt.Equals, "failed:down 0/3")

	out, err = rollBackApplied(c, binary, dir, target, rollback{})

	c.Assert(err, qt.ErrorMatches, "exit status 2", qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "error: error running down migrations: migration 2 is dirty: state=failed applied=0/3")
	c.Assert(out, qt.Contains, "No down statement of this rollback committed, so the migration is still applied. "+
		"Run 'ptah migrations repair --version 2' to record it applied, then roll it back again.")
	c.Assert(out, qt.Not(qt.Contains), "already at or below target")
	c.Assert(revisionRow(c, target, 2), qt.Equals, "failed:down 0/3")
	_, tables := revisionCensus(c, target, 2, "base", "t2", "t3")
	c.Assert(tables, qt.DeepEquals, []string{"base=true", "t2=true", "t3=true"})
}

// completedRollback rolls back, under golang-migrate, a down file that opens
// its own transaction, and makes the checkpoint that records its end fail, so
// the revision stays dirty with every down statement committed. It returns the
// directory and the database.
func completedRollback(c *qt.C, binary string) (dir, target string) {
	c.Helper()
	dir, target = appliedDirectory(c, binary, ownTransactionDownFiles(downOwnTransactionsBody))
	execOn(c, target,
		"CREATE FUNCTION refuse_checkpoint() RETURNS trigger LANGUAGE plpgsql AS $$\n"+
			"BEGIN\n"+
			"  IF NEW.version = 2 AND NEW.state = 'pending:down' AND NEW.applied = NEW.total AND\n"+
			"     OLD.applied <> NEW.total AND COALESCE(NEW.error, '') = '' THEN\n"+
			"    RAISE EXCEPTION 'the checkpoint is refused';\n"+
			"  END IF;\n"+
			"  RETURN NEW;\n"+
			"END $$",
		"CREATE TRIGGER refuse_checkpoint BEFORE UPDATE ON schema_migrations "+
			"FOR EACH ROW EXECUTE FUNCTION refuse_checkpoint()")
	out, err := rollBackApplied(c, binary, dir, target, rollback{flags: []string{"--runner", "golang-migrate"}})
	c.Assert(err, qt.ErrorMatches, "exit status 2", qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "the checkpoint is refused")
	execOn(c, target, "DROP TRIGGER refuse_checkpoint ON schema_migrations")
	c.Assert(revisionRow(c, target, 2), qt.Equals, "failed:down 4/4")
	return dir, target
}

// A rollback under golang-migrate that committed every down statement and
// stopped before removing its revision is finished by a repair that runs
// nothing, once the repair reads the file with its transactions. The runner
// comes from --runner, then PTAH_RUNNER, then `migration.runner` in
// ptah.yaml, and each row names golang-migrate at one of them. The status
// names that repair, with the runner.
func TestMigrationsRepairRunnerFinishesACompletedRollbackE2E(t *testing.T) {
	binary := builtPtah(t, qt.New(t))
	for _, test := range []struct {
		name   string
		config string
		env    []string
		flags  []string
	}{
		{name: "golang-migrate named by the flag", flags: []string{"--runner", "golang-migrate"}},
		{name: "golang-migrate named by PTAH_RUNNER", env: []string{"PTAH_RUNNER=golang-migrate"}},
		{name: "golang-migrate named in ptah.yaml", config: "migration:\n  runner: golang-migrate\n"},
		{
			name:  "the flag over PTAH_RUNNER",
			env:   []string{"PTAH_RUNNER=ptah"},
			flags: []string{"--runner", "golang-migrate"},
		},
		{
			name:   "PTAH_RUNNER over ptah.yaml",
			config: "migration:\n  runner: ptah\n",
			env:    []string{"PTAH_RUNNER=golang-migrate"},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir, target := completedRollback(c, binary)
			status, _ := runPtahInDir(c.Context(), c.TempDir(), binary,
				"migrations", "status", "--migrations-dir", dir, "--db-url", target)
			c.Assert(status, qt.Contains, "Every down statement of this rollback committed and the run stopped "+
				"before removing the revision. Run 'ptah migrations repair --version 2 --runner golang-migrate' "+
				"to finish it.")

			out, err := runConfigured(c, binary, rollback{config: test.config, env: test.env, flags: test.flags},
				"migrations", "repair", "--migrations-dir", dir, "--db-url", target, "--version", "2")

			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			c.Assert(revisionRow(c, target, 2), qt.Equals, "")
			_, tables := revisionCensus(c, target, 2, "base", "t2", "t3")
			c.Assert(tables, qt.DeepEquals, []string{"base=true", "t2=false", "t3=false"})
		})
	}
}

// Under the default runner, and wherever a higher source names the ptah runner
// over a lower one that names golang-migrate, the repair reads the down file
// one statement at a time and refuses its BEGIN. The refusal names the runner,
// and the revision stays.
func TestMigrationsRepairRunnerRefusesTheFileOwnTransactionsE2E(t *testing.T) {
	binary := builtPtah(t, qt.New(t))
	for _, test := range []struct {
		name   string
		config string
		env    []string
		flags  []string
	}{
		{name: "the default runner"},
		{name: "the flag over ptah.yaml", config: "migration:\n  runner: golang-migrate\n", flags: []string{"--runner", "ptah"}},
		{name: "the flag over PTAH_RUNNER", env: []string{"PTAH_RUNNER=golang-migrate"}, flags: []string{"--runner", "ptah"}},
		{
			name:   "PTAH_RUNNER over ptah.yaml",
			config: "migration:\n  runner: golang-migrate\n",
			env:    []string{"PTAH_RUNNER=ptah"},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir, target := completedRollback(c, binary)

			out, err := runConfigured(c, binary, rollback{config: test.config, env: test.env, flags: test.flags},
				"migrations", "repair", "--migrations-dir", dir, "--db-url", target, "--version", "2")

			c.Assert(err, qt.ErrorMatches, "exit status 2", qt.Commentf("%s", out))
			c.Assert(out, qt.Contains, `"BEGIN" controls transaction state`)
			c.Assert(out, qt.Contains, "a directory written for golang-migrate, whose files open their own "+
				"transactions, is repaired with --runner golang-migrate")
			c.Assert(revisionRow(c, target, 2), qt.Equals, "failed:down 4/4")
		})
	}
}

// A rollback under golang-migrate that failed after its own transaction
// committed is not finished by a repair. The status names the two endings,
// and the forced repair it names records the migration applied once the
// operator restored what the rollback reverted.
func TestMigrationsRepairRunnerRecordsARestoredRollbackE2E(t *testing.T) {
	c := qt.New(t)
	binary := builtPtah(t, c)
	dir, target := appliedDirectory(c, binary, ownTransactionDownFiles(
		"BEGIN;\nDROP TABLE t2;\nCOMMIT;\nINSERT INTO missing_table VALUES (1);\nDROP TABLE t3;\n"))
	out, err := rollBackApplied(c, binary, dir, target, rollback{flags: []string{"--runner", "golang-migrate"}})
	c.Assert(err, qt.ErrorMatches, "exit status 2", qt.Commentf("%s", out))
	c.Assert(revisionRow(c, target, 2), qt.Equals, "failed:down 3/5")
	status, _ := runPtahInDir(c.Context(), c.TempDir(), binary,
		"migrations", "status", "--migrations-dir", dir, "--db-url", target)
	c.Assert(status, qt.Contains, "This rollback committed 3 of 5 down statements, in transactions its file "+
		"opens itself, and a resume would run the rest of the file outside them. Finish the rollback by hand")
	c.Assert(status, qt.Contains, "run 'ptah migrations repair --version 2 --force --runner golang-migrate' "+
		"to record the migration applied.")
	execOn(c, target, "CREATE TABLE t2 (id int)")

	out, err = runConfigured(c, binary, rollback{flags: []string{"--runner", "golang-migrate"}},
		"migrations", "repair", "--migrations-dir", dir, "--db-url", target, "--version", "2", "--force")

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(revisionRow(c, target, 2), qt.Equals, "applied 2/2")
}

// A runner Ptah does not model is refused before the directory is read and
// the database is opened, whichever source names it.
func TestMigrationsRepairRunnerRefusesAnUnknownRunnerE2E(t *testing.T) {
	binary := builtPtah(t, qt.New(t))
	for _, test := range []struct {
		name   string
		config string
		env    []string
		flags  []string
	}{
		{name: "named by the flag", flags: []string{"--runner", "flyway"}},
		{name: "named by PTAH_RUNNER", env: []string{"PTAH_RUNNER=flyway"}},
		{name: "named in ptah.yaml", config: "migration:\n  runner: flyway\n"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			out, err := runConfigured(c, binary, rollback{config: test.config, env: test.env, flags: test.flags},
				"migrations", "repair", "--migrations-dir", filepath.Join(c.TempDir(), "missing"),
				"--db-url", unreachableDatabaseURL, "--version", "2")

			c.Assert(err, qt.ErrorMatches, "exit status 2", qt.Commentf("%s", out))
			c.Assert(out, qt.Equals, "error: invalid runner \"flyway\": expected ptah or golang-migrate\n")
		})
	}
}
