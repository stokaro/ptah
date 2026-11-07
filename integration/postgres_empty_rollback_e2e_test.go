//go:build integration

package integration_test

import (
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
)

// A Liquibase changeset with no rollback SQL is imported as a down file with no
// statements. Rolling back through it would run nothing and move the version, and
// the next apply of the changeset would fail on the object it left. These tests
// import the changelog through the shipped binary, apply it to PostgreSQL and read
// the result back from the database.

const (
	// emptyRollbackChangelog is the issue's changelog: the last changeset has no
	// --rollback line.
	emptyRollbackChangelog = `--liquibase formatted sql

--changeset team:1
CREATE TABLE public.subscriptions (id bigint PRIMARY KEY, plan text NOT NULL);
--rollback DROP TABLE public.subscriptions;

--changeset team:2
CREATE TABLE public.invoices (id bigint PRIMARY KEY);
--rollback DROP TABLE public.invoices;

--changeset team:3
CREATE TABLE public.receipts (id bigint PRIMARY KEY);
--rollback DROP TABLE public.receipts;

--changeset team:4
CREATE INDEX subscriptions_plan_idx ON public.subscriptions (plan);
`
	emptyRollbackDownFile = "0000000004_team_4.down.sql"
	emptyRollbackRefusal  = `migration 4 has a down file with no statements (` + emptyRollbackDownFile + `), ` +
		`so a rollback would change nothing and still move the version; ` +
		"write the rollback, or state that it has none with `-- +ptah empty_rollback` at the top of the down file"
)

// emptyRollbackRun is a converted directory and the database it applied to.
type emptyRollbackRun struct {
	dir      string
	database string
}

func (r emptyRollbackRun) ptah(c *qt.C, args ...string) clirun.Result {
	c.Helper()
	return clirun.Run(c, clirun.Ptah, clirun.Options{Dir: r.dir}, args...)
}

func (r emptyRollbackRun) down(c *qt.C, flags ...string) clirun.Result {
	c.Helper()
	return r.ptah(c, append([]string{"migrations", "down", "--db-url", r.database, "--migrations-dir", "migrations", "--confirm"}, flags...)...)
}

func (r emptyRollbackRun) up(c *qt.C) clirun.Result {
	c.Helper()
	return r.ptah(c, "migrations", "up", "--db-url", r.database, "--migrations-dir", "migrations", "--verify-sum")
}

// writeDown replaces a down file and seals the directory again.
func (r emptyRollbackRun) writeDown(c *qt.C, name, content string) {
	c.Helper()
	c.Assert(os.WriteFile(filepath.Join(r.dir, "migrations", name), []byte(content), 0o600), qt.IsNil)
	sealed := r.ptah(c, "migrations", "hash", "--dir", "migrations")
	c.Assert(sealed.ExitCode, qt.Equals, 0, emptyRollbackComment(sealed))
}

func emptyRollbackComment(got clirun.Result) qt.Comment {
	return qt.Commentf("exit %d\nstdout:\n%s\nstderr:\n%s", got.ExitCode, got.Stdout, got.Stderr)
}

// importedHistory imports changelog, applies it to a fresh database and returns
// both.
func importedHistory(c *qt.C, changelog string) emptyRollbackRun {
	c.Helper()
	run := emptyRollbackRun{dir: c.TempDir(), database: newEmptyReadScopeDatabase(c, "rollback")}
	source := filepath.Join(run.dir, "source")
	c.Assert(os.MkdirAll(source, 0o750), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(source, "changelog.sql"), []byte(changelog), 0o600), qt.IsNil)
	imported := run.ptah(c, "migrations", "import", "--from", "liquibase", "--source-dir", "source", "--migrations-dir", "migrations")
	c.Assert(imported.ExitCode, qt.Equals, 0, emptyRollbackComment(imported))
	applied := run.up(c)
	c.Assert(applied.ExitCode, qt.Equals, 0, emptyRollbackComment(applied))
	return run
}

// emptyRollbackCount answers a count from the database itself.
func emptyRollbackCount(c *qt.C, database, query string) int {
	c.Helper()
	db, err := sql.Open("pgx", postgresFamilyDriverURL(c, database))
	c.Assert(err, qt.IsNil)
	defer db.Close()
	var count int
	c.Assert(db.QueryRowContext(c.Context(), query).Scan(&count), qt.IsNil)
	return count
}

func planIndexCount(c *qt.C, database string) int {
	c.Helper()
	return emptyRollbackCount(c, database, "SELECT count(*) FROM pg_indexes WHERE indexname = 'subscriptions_plan_idx'")
}

func applicationTableCount(c *qt.C, database string) int {
	c.Helper()
	return emptyRollbackCount(c, database,
		"SELECT count(*) FROM pg_tables WHERE schemaname = 'public' AND tablename IN ('subscriptions', 'invoices', 'receipts')")
}

// TestARollbackThroughAnImportedChangesetWithNoRollbackIsRefusedE2E is the issue.
// Every way of asking is refused with the same reason and changes nothing: the index
// and the tables stay, the version stays, so the next apply has nothing to do
// instead of failing on the index. A plan reaching further down is refused whole,
// so the real rollbacks below the empty file do not run either.
func TestARollbackThroughAnImportedChangesetWithNoRollbackIsRefusedE2E(t *testing.T) {
	tests := []struct {
		name  string
		flags []string
	}{
		{name: "the one migration", flags: []string{"--target", "3"}},
		{name: "a dry run", flags: []string{"--target", "3", "--dry-run"}},
		{name: "a plan reaching further down", flags: []string{"--target", "0"}},
	}
	c := qt.New(t)
	run := importedHistory(c, emptyRollbackChangelog)

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := run.down(c, test.flags...)

			c.Assert(got.ExitCode, qt.Equals, 2, emptyRollbackComment(got))
			c.Assert(got.Stderr, qt.Contains, emptyRollbackRefusal)
			c.Assert(got.Stdout, qt.Not(qt.Contains), "completed successfully")
			c.Assert(planIndexCount(c, run.database), qt.Equals, 1)
			c.Assert(applicationTableCount(c, run.database), qt.Equals, 3)
			next := run.up(c)
			c.Assert(next.ExitCode, qt.Equals, 0, emptyRollbackComment(next))
			c.Assert(next.Stdout, qt.Contains, "Database is already up to date")
		})
	}
}

// TestARollbackOnAShadowDatabaseMeetsTheSameRuleE2E covers the verification that
// runs ahead of the target. The refusal comes from the shadow database's replay of
// the same plan, so the target is not reached and the shadow database is handed back
// empty.
func TestARollbackOnAShadowDatabaseMeetsTheSameRuleE2E(t *testing.T) {
	c := qt.New(t)
	run := importedHistory(c, emptyRollbackChangelog)
	shadow := newEmptyReadScopeDatabase(c, "rollback_shadow")

	got := run.down(c, "--target", "3", "--shadow-db", shadow)

	c.Assert(got.ExitCode, qt.Equals, 2, emptyRollbackComment(got))
	c.Assert(got.Stderr, qt.Contains, "rollback verification failed")
	c.Assert(got.Stderr, qt.Contains, emptyRollbackRefusal)
	c.Assert(planIndexCount(c, run.database), qt.Equals, 1)
	c.Assert(applicationTableCount(c, shadow), qt.Equals, 0)
}

// TestWritingTheRollbackLiftsTheRefusalE2E is the first way out the refusal names.
func TestWritingTheRollbackLiftsTheRefusalE2E(t *testing.T) {
	c := qt.New(t)
	run := importedHistory(c, emptyRollbackChangelog)
	run.writeDown(c, emptyRollbackDownFile, "DROP INDEX public.subscriptions_plan_idx;\n")

	got := run.down(c, "--target", "3", "--verify-sum")

	c.Assert(got.ExitCode, qt.Equals, 0, emptyRollbackComment(got))
	c.Assert(planIndexCount(c, run.database), qt.Equals, 0)
	next := run.up(c)
	c.Assert(next.ExitCode, qt.Equals, 0, emptyRollbackComment(next))
	c.Assert(planIndexCount(c, run.database), qt.Equals, 1)
}

// TestStatingThatARollbackHasNoneMovesTheVersionE2E is the second way out, and its
// cost. The rollback runs nothing, so the index stays and the version moves. The up
// file runs again on the next apply, so it fails when it cannot run twice, as the
// directive's documentation says, and passes when it can.
func TestStatingThatARollbackHasNoneMovesTheVersionE2E(t *testing.T) {
	const statement = "-- +ptah empty_rollback\n"

	t.Run("an up file that cannot run twice", func(t *testing.T) {
		c := qt.New(t)
		run := importedHistory(c, emptyRollbackChangelog)
		run.writeDown(c, emptyRollbackDownFile, statement)

		got := run.down(c, "--target", "3", "--verify-sum")

		c.Assert(got.ExitCode, qt.Equals, 0, emptyRollbackComment(got))
		c.Assert(got.Stdout, qt.Contains, "Database is now at version: 3")
		c.Assert(planIndexCount(c, run.database), qt.Equals, 1)
		next := run.up(c)
		c.Assert(next.ExitCode, qt.Equals, 2, emptyRollbackComment(next))
		c.Assert(next.Stderr, qt.Contains, "42P07")
	})

	t.Run("an up file that can", func(t *testing.T) {
		c := qt.New(t)
		run := importedHistory(c, strings.Replace(emptyRollbackChangelog, "CREATE INDEX subscriptions_plan_idx",
			"CREATE INDEX IF NOT EXISTS subscriptions_plan_idx", 1))
		run.writeDown(c, emptyRollbackDownFile, statement)

		got := run.down(c, "--target", "3", "--verify-sum")

		c.Assert(got.ExitCode, qt.Equals, 0, emptyRollbackComment(got))
		next := run.up(c)
		c.Assert(next.ExitCode, qt.Equals, 0, emptyRollbackComment(next))
		c.Assert(next.Stdout, qt.Contains, "Database is now at version: 4")
	})
}

// TestADirectiveBesideStatementsIsRefusedE2E covers a down file that says both.
func TestADirectiveBesideStatementsIsRefusedE2E(t *testing.T) {
	c := qt.New(t)
	run := importedHistory(c, emptyRollbackChangelog)
	run.writeDown(c, emptyRollbackDownFile, "-- +ptah empty_rollback\nDROP INDEX public.subscriptions_plan_idx;\n")

	got := run.down(c, "--target", "3", "--verify-sum")

	c.Assert(got.ExitCode, qt.Equals, 2, emptyRollbackComment(got))
	c.Assert(got.Stderr, qt.Contains, "holds 1 statement(s); remove the directive or the statements")
	c.Assert(planIndexCount(c, run.database), qt.Equals, 1)
}

// TestAChangesetThatStatesItHasNoRollbackRollsBackE2E covers Liquibase's own keyword,
// which the import carries into the down file: no edit, and no refusal.
func TestAChangesetThatStatesItHasNoRollbackRollsBackE2E(t *testing.T) {
	tests := []struct {
		name    string
		keyword string
	}{
		{name: "empty", keyword: "--rollback empty\n"},
		{name: "not required", keyword: "--rollback not required\n"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			run := importedHistory(c, emptyRollbackChangelog+test.keyword)

			got := run.down(c, "--target", "3", "--verify-sum")

			c.Assert(got.ExitCode, qt.Equals, 0, emptyRollbackComment(got))
			c.Assert(got.Stdout, qt.Contains, "Database is now at version: 3")
			c.Assert(planIndexCount(c, run.database), qt.Equals, 1)
		})
	}
}

// TestAGeneratedMigrationOnAHistoryWithNoRollbackIsVerifiedE2E is the shadow replay
// the issue asks about. A generate with --replay replays the history up on the dev
// database, and --shadow-db replays it up and takes the new migration down and up
// again, so neither reaches the old empty file. Afterwards the rule holds at the old
// file, and the generated rollback above it runs.
func TestAGeneratedMigrationOnAHistoryWithNoRollbackIsVerifiedE2E(t *testing.T) {
	const declared = `CREATE TABLE public.subscriptions (id bigint PRIMARY KEY, plan text NOT NULL, extra text);
CREATE TABLE public.invoices (id bigint PRIMARY KEY);
CREATE TABLE public.receipts (id bigint PRIMARY KEY);
CREATE INDEX subscriptions_plan_idx ON public.subscriptions (plan);
`
	c := qt.New(t)
	run := importedHistory(c, emptyRollbackChangelog)
	dev, shadow := newEmptyReadScopeDatabase(c, "rollback_dev"), newEmptyReadScopeDatabase(c, "rollback_verify")
	schema := filepath.Join(run.dir, "schema.sql")
	c.Assert(os.WriteFile(schema, []byte(declared), 0o600), qt.IsNil)

	generated := run.ptah(c, "migrations", "generate", "--replay", "--dev-url", dev, "--shadow-db", shadow,
		"--schema-file", "schema.sql", "--migrations-dir", "migrations")

	c.Assert(generated.ExitCode, qt.Equals, 0, emptyRollbackComment(generated))
	sealed := run.ptah(c, "migrations", "hash", "--dir", "migrations")
	c.Assert(sealed.ExitCode, qt.Equals, 0, emptyRollbackComment(sealed))
	applied := run.up(c)
	c.Assert(applied.ExitCode, qt.Equals, 0, emptyRollbackComment(applied))

	above := run.down(c, "--target", "4", "--verify-sum")
	c.Assert(above.ExitCode, qt.Equals, 0, emptyRollbackComment(above))
	c.Assert(above.Stdout, qt.Contains, "Database is now at version: 4")

	through := run.down(c, "--target", "3", "--verify-sum")
	c.Assert(through.ExitCode, qt.Equals, 2, emptyRollbackComment(through))
	c.Assert(through.Stderr, qt.Contains, emptyRollbackRefusal)
}
