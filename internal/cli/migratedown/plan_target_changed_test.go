package migratedown_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"maps"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"sync"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/cli/migratedown"
	"ptah.run/internal/testutils"
	"ptah.run/migration/migrator"
)

// A --plan rollback is derived before the prompt and runs after it, and no
// lock is held while a person reads the plan. So the target can change in
// between, and the statements that run must be the ones the operator was
// shown: the run never derives them again, and it refuses before any of them
// runs when the applied migrations are no longer the ones they were derived
// for.

// refusedChangedTarget is the refusal of a rollback derived at version 2 of
// noDownBodyDir and approved once version 3 was applied.
const refusedChangedTarget = `error running down migrations: the rollback to version 1 was derived for ` +
	`the applied migrations [2], and the migrations applied above version 1 are now [3 2]; ` +
	`nothing was rolled back. Run migrations down --plan again to derive the rollback from the database as it is now`

// answerAfter is the operator's standard input. The first read stands for the
// time the operator spends reading the plan: it runs change, as another
// runner would change the target then, and the operator then types YES.
type answerAfter struct {
	change func()
	once   sync.Once
	answer io.Reader
}

func newAnswerAfter(change func()) *answerAfter {
	return &answerAfter{change: change, answer: strings.NewReader("YES\n")}
}

func (a *answerAfter) Read(p []byte) (int, error) {
	a.once.Do(a.change)
	return a.answer.Read(p)
}

// runPlanDownReading rolls noDownBodyDir back to version 1 with a derived
// plan and no --confirm, reading the answer from in.
func runPlanDownReading(c *qt.C, dir, dbPath string, in io.Reader) (string, error) {
	c.Helper()
	cmd := migratedown.NewMigrateDownCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetIn(in)
	cmd.SetArgs([]string{"--db-url", "sqlite://" + dbPath, "--migrations-dir", dir,
		"--target", "1", "--plan", "--shadow-db", devURL(c)})
	err := cmd.Execute()
	return out.String(), err
}

// applyAThirdMigration is another operator's `migrations up`: it adds
// version 3 to the directory and applies it. It drives the migrator, since a
// second command cannot start its observability inside a running one.
func applyAThirdMigration(c *qt.C, dir, dbPath string) func() {
	return func() {
		c.Assert(os.WriteFile(filepath.Join(dir, "3_child.sql"), []byte(childUpSQL), 0o600), qt.IsNil)
		conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+dbPath)
		c.Assert(err, qt.IsNil)
		defer dbschema.CloseAndWarn(conn)
		up, err := migrator.NewFSMigrator(conn, os.DirFS(dir))
		c.Assert(err, qt.IsNil)
		c.Assert(up.MigrateUp(context.Background()), qt.IsNil)
	}
}

// createATableOutOfBand is another tool writing to the target without a
// migration, so the applied versions stay as they were.
func createATableOutOfBand(c *qt.C, dbPath string) func() {
	return func() {
		conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+dbPath)
		c.Assert(err, qt.IsNil)
		defer dbschema.CloseAndWarn(conn)
		_, err = conn.ExecContext(context.Background(), "CREATE TABLE late_tool (id INTEGER PRIMARY KEY);")
		c.Assert(err, qt.IsNil)
	}
}

// appliedVersions reads every applied version back from the revision table,
// oldest first.
func appliedVersions(c *qt.C, dbPath string) []int64 {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	rows, err := conn.QueryContext(context.Background(),
		"SELECT version FROM schema_migrations WHERE state = 'applied' ORDER BY version")
	c.Assert(err, qt.IsNil)
	defer func() { c.Assert(rows.Close(), qt.IsNil) }()
	var versions []int64
	for rows.Next() {
		var version int64
		c.Assert(rows.Scan(&version), qt.IsNil)
		versions = append(versions, version)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return versions
}

// TestMigrateDownPlanRunsTheShownPlan_HappyPath changes the target while the
// prompt waits, out of band, so the applied migrations stay as they were. The
// run executes the plan it showed, once derived: a rollback derived again
// after the answer would also drop the table that appeared in between, which
// nobody was shown.
func TestMigrateDownPlanRunsTheShownPlan_HappyPath(t *testing.T) {
	c := qt.New(t)
	dir, dbPath := noDownBodyDir(c)

	out, err := runPlanDownReading(c, dir, dbPath, newAnswerAfter(createATableOutOfBand(c, dbPath)))

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Matches, `(?s).*Derived rollback plan: 1 statements\n  DROP TABLE "orphan"[^\n]*\n.*Type 'YES' to confirm: .*`)
	c.Assert(strings.Count(out, "Derived rollback plan"), qt.Equals, 1)
	c.Assert(tableCensus(c, dbPath), qt.DeepEquals, []string{"late_tool", "parent", "schema_migrations", "schema_migrations_log"})
	c.Assert(appliedVersions(c, dbPath), qt.DeepEquals, []int64{1})
}

// TestMigrateDownPlanRefusesAChangedTarget_FailurePath applies version 3
// while the prompt waits. The plan was derived for version 2 alone, so the run
// refuses once it holds the migration lock and reads the applied migrations
// again, before any statement runs: the orphan table, the new child table and
// every revision row are still there, and the refusal names both sets.
func TestMigrateDownPlanRefusesAChangedTarget_FailurePath(t *testing.T) {
	c := qt.New(t)
	dir, dbPath := noDownBodyDir(c)

	out, err := runPlanDownReading(c, dir, dbPath, newAnswerAfter(applyAThirdMigration(c, dir, dbPath)))

	c.Assert(err, qt.ErrorMatches, regexp.QuoteMeta(refusedChangedTarget), qt.Commentf("%s", out))
	_, stale := errors.AsType[*migrator.StaleDerivedRollbackError](err)
	c.Assert(stale, qt.IsTrue)
	c.Assert(out, qt.Contains, `DROP TABLE "orphan"`)
	c.Assert(out, qt.Not(qt.Contains), "missing from the migration provider")
	c.Assert(tableCensus(c, dbPath), qt.DeepEquals, []string{"child", "orphan", "parent", "schema_migrations", "schema_migrations_log"})
	c.Assert(appliedVersions(c, dbPath), qt.DeepEquals, []int64{1, 2, 3})
}

// TestMigrateDownPlanRunsThePreDownHook_HappyPath runs a derived rollback
// with a pre-down hook. The hook runs once the run holds the migration lock
// and before any statement, as for a rollback through the down files.
func TestMigrateDownPlanRunsThePreDownHook_HappyPath(t *testing.T) {
	c := qt.New(t)
	dir, dbPath := noDownBodyDir(c)

	out, err := runPlanDown(c, dir, dbPath, "--pre-down-hook", "echo hook-said-this")

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Matches, `(?s).*Derived rollback plan: 1 statements\n.*hook-said-this.*`)
	c.Assert(tableCensus(c, dbPath), qt.DeepEquals, []string{"parent", "schema_migrations", "schema_migrations_log"})
	c.Assert(appliedVersions(c, dbPath), qt.DeepEquals, []int64{1})
}

// TestMigrateDownPlanRunsThePreDownHook_FailurePath refuses in the hook. The
// derived rollback stops there, so no statement runs and the revisions stay
// at version 2.
func TestMigrateDownPlanRunsThePreDownHook_FailurePath(t *testing.T) {
	c := qt.New(t)
	dir, dbPath := noDownBodyDir(c)

	out, err := runPlanDown(c, dir, dbPath, "--pre-down-hook", testutils.FailingHookCommand("hook-refused", 9))

	c.Assert(err, qt.ErrorMatches, `(?s)error running down migrations: down pre-flight custom command hook failed: exit status 9\n.*hook-refused.*`,
		qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "hook-refused")
	c.Assert(tableCensus(c, dbPath), qt.DeepEquals, []string{"orphan", "parent", "schema_migrations", "schema_migrations_log"})
	c.Assert(appliedVersions(c, dbPath), qt.DeepEquals, []int64{1, 2})
}

// documentFields reads stdout as exactly one JSON document and returns the
// names of its top-level fields, in order.
func documentFields(c *qt.C, stdout string) []string {
	c.Helper()
	var document map[string]json.RawMessage
	c.Assert(json.Unmarshal([]byte(stdout), &document), qt.IsNil, qt.Commentf("stdout:\n%s", stdout))
	return slices.Sorted(maps.Keys(document))
}

// TestMigrateDownPlanJSONRefusedPlan_FailurePath refuses the derived plan
// under --confirm --json. The refusal stops the run before it starts, so the
// document carries the outcome and the error, and no planned versions, no
// status and no migration log.
func TestMigrateDownPlanJSONRefusedPlan_FailurePath(t *testing.T) {
	c := qt.New(t)
	dir, dbPath := planExcludeDir(c, "exclude: [external_audt]\n")
	f := documentRollback{dir: dir, dbURL: "sqlite://" + dbPath}

	stdout, stderr, err := f.down("--target", "1", "--plan", "--shadow-db", devURL(c), "--confirm", "--json")

	c.Assert(err, qt.ErrorMatches, regexp.QuoteMeta(refusedUnmatchedAudit), qt.Commentf("%s", stderr))
	c.Assert(documentFields(c, stdout), qt.DeepEquals, []string{"command", "contract_version", "direction", "error", "outcome"})
	document := decodeRunDocument(c, stdout)
	c.Assert(document.Outcome, qt.Equals, migrator.RunOutcomeFailed)
	c.Assert(document.Error.Message, qt.Equals, err.Error())
	c.Assert(recordedVersion(c, dbPath), qt.Equals, int64(2))
}

// TestMigrateDownPlanJSONPlanned_HappyPath rolls both migrations back with a
// derived plan. The document names them newest first, as for a rollback
// through the down files, on a real run and on a dry run alike.
func TestMigrateDownPlanJSONPlanned_HappyPath(t *testing.T) {
	tests := []struct {
		name        string
		args        []string
		wantOutcome migrator.RunOutcome
		wantVersion int64
	}{
		{name: "a real run", args: []string{"--confirm"}, wantOutcome: migrator.RunOutcomeApplied, wantVersion: 0},
		{name: "a dry run", args: []string{"--dry-run"}, wantOutcome: migrator.RunOutcomeDryRun, wantVersion: 2},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir, dbPath := noDownBodyDir(c)
			f := documentRollback{dir: dir, dbURL: "sqlite://" + dbPath}

			stdout, stderr, err := f.down(append([]string{"--target", "0", "--plan", "--shadow-db", devURL(c), "--json"}, test.args...)...)

			c.Assert(err, qt.IsNil, qt.Commentf("%s", stderr))
			document := decodeRunDocument(c, stdout)
			c.Assert(document.Outcome, qt.Equals, test.wantOutcome)
			c.Assert(document.Planned, qt.DeepEquals, []int64{2, 1})
			c.Assert(document.Status.CurrentVersion, qt.Equals, test.wantVersion)
		})
	}
}
