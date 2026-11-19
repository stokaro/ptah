//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/atlascompat"
	"ptah.run/dbschema"
	"ptah.run/internal/clirun"
	"ptah.run/internal/testutils"
	"ptah.run/migration/migrationfile"
)

// The drift check an atlas.hcl env declares for `migrate apply`, run by the
// shipped ptah-compat against a server that decides. Each test's project holds
// two hashed migrations: 1 creates t and 2 creates u. The target is migrated to
// 1, changed out of band where the test says so, and migrated again, and the
// revision table and the catalog are read back.

// compatDriftEngine is a target, an empty dev database, and the queries that
// read the target back.
type compatDriftEngine struct {
	name   string
	target string
	dev    string
	// revisions reads the applied versions in order, and pendingTable counts
	// the table migration 2 creates.
	revisions    string
	pendingTable string
}

// compatDriftSQLite is a SQLite target at path, which need not exist, with an
// in-memory dev database.
func compatDriftSQLite(path string) compatDriftEngine {
	return compatDriftEngine{
		name:         "SQLite",
		target:       "sqlite://" + filepath.ToSlash(path),
		dev:          "sqlite://file?mode=memory",
		revisions:    "SELECT version FROM atlas_schema_revisions ORDER BY version",
		pendingTable: "SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = 'u'",
	}
}

// compatDriftEngines returns a fresh target and dev database on each engine.
// The PostgreSQL URLs name a database and no schema, so the revision table
// lives in a schema of its own, which the check has to leave out.
func compatDriftEngines(c *qt.C) []compatDriftEngine {
	c.Helper()
	return []compatDriftEngine{
		compatDriftSQLite(filepath.Join(c.TempDir(), "target.db")),
		{
			name:         "PostgreSQL",
			target:       newEmptyReadScopeDatabase(c, "drift_target"),
			dev:          newEmptyReadScopeDatabase(c, "drift_dev"),
			revisions:    "SELECT version FROM atlas_schema_revisions.atlas_schema_revisions ORDER BY version",
			pendingTable: "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'u'",
		},
	}
}

// compatDriftProject writes the project for engine with check as the env's
// drift check, which may be empty, and returns its directory.
func compatDriftProject(c *qt.C, engine compatDriftEngine, check string) string {
	c.Helper()
	dir := c.TempDir()
	migrations := filepath.Join(dir, "migrations")
	c.Assert(os.Mkdir(migrations, 0o750), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(migrations, "1.sql"), []byte("CREATE TABLE t (id integer PRIMARY KEY);\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(migrations, "2.sql"), []byte("CREATE TABLE u (id integer PRIMARY KEY);\n"), 0o600), qt.IsNil)
	sum, err := atlascompat.ComputeSum(os.DirFS(migrations), migrationfile.DirFormatAtlas)
	c.Assert(err, qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(migrations, atlascompat.AtlasSumFileName), sum.Bytes(), 0o600), qt.IsNil)
	project := "env \"local\" {\n" +
		"  url = " + strconv.Quote(engine.target) + "\n" +
		"  dev = " + strconv.Quote(engine.dev) + "\n" +
		"  migration {\n    dir = \"file://migrations\"\n  }\n" +
		check +
		"}\n"
	c.Assert(os.WriteFile(filepath.Join(dir, "atlas.hcl"), []byte(project), 0o600), qt.IsNil)
	return dir
}

// compatDriftApply runs `migrate apply --env local` in dir, with extra after it.
func compatDriftApply(c *qt.C, dir string, env []string, extra ...string) clirun.Result {
	c.Helper()
	return clirun.Run(c, clirun.Compat, clirun.Options{Dir: dir, Env: append(environmentWithoutPtahVariables(), env...)},
		append([]string{"migrate", "apply", "--env", "local"}, extra...)...)
}

// compatDriftExec runs statement on url outside any migration.
func compatDriftExec(c *qt.C, url, statement string) {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), url)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	_, err = conn.ExecContext(c.Context(), statement)
	c.Assert(err, qt.IsNil)
}

// compatDriftRead returns the applied versions of engine's target and the
// number of tables named u in it.
func compatDriftRead(c *qt.C, engine compatDriftEngine) ([]string, int) {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), engine.target)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	rows, err := conn.QueryContext(c.Context(), engine.revisions)
	c.Assert(err, qt.IsNil)
	defer func() { c.Assert(rows.Close(), qt.IsNil) }()
	var versions []string
	for rows.Next() {
		var version string
		c.Assert(rows.Scan(&version), qt.IsNil)
		versions = append(versions, version)
	}
	c.Assert(rows.Err(), qt.IsNil)
	var pending int
	c.Assert(conn.QueryRowContext(c.Context(), engine.pendingTable).Scan(&pending), qt.IsNil)
	return versions, pending
}

const compatDriftBlock = "  check \"migrate_apply\" {\n    drift {}\n  }\n"

// TestCompatMigrateApplyDriftCheckRefusesADriftedTargetE2E holds the declared
// check to its purpose: migration 2 does not run on a target whose schema
// differs from what migration 1 produces. The first apply finds no history
// and skips the check.
func TestCompatMigrateApplyDriftCheckRefusesADriftedTargetE2E(t *testing.T) {
	for _, engine := range compatDriftEngines(qt.New(t)) {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			dir := compatDriftProject(c, engine, compatDriftBlock)
			first := compatDriftApply(c, dir, nil, "1")
			c.Assert(first.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", first.Stdout, first.Stderr))
			compatDriftExec(c, engine.target, "ALTER TABLE t ADD COLUMN extra integer")

			got := compatDriftApply(c, dir, nil)

			c.Assert(first.Stderr, qt.Equals, "Drift check: skipped, because no migration has been applied to compare the database with\n")
			c.Assert(got.ExitCode, qt.Equals, 1, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(got.Stdout, qt.Equals, "")
			c.Assert(got.Stderr, qt.Equals, "Drift check: the database differs from the schema its 1 applied migration produces:\n"+
				"  - extra column t.extra\n"+
				"No pending migration was run. Bring the database back to its history, or record the change in a migration, then run again.\n"+
				"Error: the database schema differs from the schema its 1 applied migration produces, in 1 place, "+
				"so no pending migration was run; first: extra column t.extra\n")
			versions, pending := compatDriftRead(c, engine)
			c.Assert(versions, qt.DeepEquals, []string{"1"})
			c.Assert(pending, qt.Equals, 0)
		})
	}
}

// TestCompatMigrateApplyDriftCheckAppliesToACleanTargetE2E is the control for
// the refusal: a target that matches its history is migrated, and the check
// says it passed.
func TestCompatMigrateApplyDriftCheckAppliesToACleanTargetE2E(t *testing.T) {
	for _, engine := range compatDriftEngines(qt.New(t)) {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			dir := compatDriftProject(c, engine, compatDriftBlock)
			first := compatDriftApply(c, dir, nil, "1")
			c.Assert(first.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", first.Stdout, first.Stderr))

			got := compatDriftApply(c, dir, nil)

			c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(got.Stderr, qt.Equals, "Drift check: the database matches the schema its 1 applied migration produces\n")
			versions, pending := compatDriftRead(c, engine)
			c.Assert(versions, qt.DeepEquals, []string{"1", "2"})
			c.Assert(pending, qt.Equals, 1)
		})
	}
}

// TestCompatMigrateApplyDriftCheckThatContinuesAppliesE2E runs the check with
// on_error = CONTINUE: the drift is reported and migration 2 runs.
func TestCompatMigrateApplyDriftCheckThatContinuesAppliesE2E(t *testing.T) {
	for _, engine := range compatDriftEngines(qt.New(t)) {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			dir := compatDriftProject(c, engine, "  check \"migrate_apply\" {\n    drift {\n      on_error = CONTINUE\n    }\n  }\n")
			first := compatDriftApply(c, dir, nil, "1")
			c.Assert(first.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", first.Stdout, first.Stderr))
			compatDriftExec(c, engine.target, "ALTER TABLE t ADD COLUMN extra integer")

			got := compatDriftApply(c, dir, nil)

			c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(got.Stderr, qt.Equals, "Drift check: the database differs from the schema its 1 applied migration produces:\n"+
				"  - extra column t.extra\n"+
				"The pending migrations run anyway, because the check is set to continue on drift.\n")
			versions, pending := compatDriftRead(c, engine)
			c.Assert(versions, qt.DeepEquals, []string{"1", "2"})
			c.Assert(pending, qt.Equals, 1)
		})
	}
}

// TestCompatMigrateApplyWithoutTheDriftCheckAppliesToADriftedTargetE2E is the
// control for the block: without it, the drifted target is migrated as it is
// without Ptah, and nothing mentions drift.
func TestCompatMigrateApplyWithoutTheDriftCheckAppliesToADriftedTargetE2E(t *testing.T) {
	for _, engine := range compatDriftEngines(qt.New(t)) {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			dir := compatDriftProject(c, engine, "")
			first := compatDriftApply(c, dir, nil, "1")
			c.Assert(first.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", first.Stdout, first.Stderr))
			compatDriftExec(c, engine.target, "ALTER TABLE t ADD COLUMN extra integer")

			got := compatDriftApply(c, dir, nil)

			c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(got.Stderr, qt.Equals, "")
			versions, pending := compatDriftRead(c, engine)
			c.Assert(versions, qt.DeepEquals, []string{"1", "2"})
			c.Assert(pending, qt.Equals, 1)
		})
	}
}

// TestCompatMigrateApplyDriftCheckDivergesFromTheCommunityBinaryE2E records
// the divergence the check is: the pinned community binary ignores the block
// and migrates the drifted target, and ptah-compat refuses. It is a
// capability the community binary lacks, not a parity claim.
func TestCompatMigrateApplyDriftCheckDivergesFromTheCommunityBinaryE2E(t *testing.T) {
	c := qt.New(t)
	reference := atlasReferenceBinary(t)
	engine := compatDriftSQLite(filepath.Join(c.TempDir(), "target.db"))
	dir := compatDriftProject(c, engine, compatDriftBlock)
	first := compatDriftApply(c, dir, nil, "1")
	c.Assert(first.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", first.Stdout, first.Stderr))
	compatDriftExec(c, engine.target, "ALTER TABLE t ADD COLUMN extra integer")
	args := []string{"migrate", "apply", "--env", "local", "--dry-run"}

	communityOut, communityStderr, err := runCLIProcess(c.Context(), dir, reference, args...)
	compat := clirun.Run(c, clirun.Compat, clirun.Options{Dir: dir, Env: environmentWithoutPtahVariables()}, args...)

	c.Assert(exitStatusOf(c, err), qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", communityOut, communityStderr))
	c.Assert(testutils.WithoutAtlasCommunityBuildNotice(communityStderr), qt.Equals, "")
	c.Assert(communityOut, qt.Contains, "-- migrating version 2")
	c.Assert(compat.ExitCode, qt.Equals, 1, qt.Commentf("stdout:\n%s\nstderr:\n%s", compat.Stdout, compat.Stderr))
	c.Assert(compat.Stderr, qt.Contains, "  - extra column t.extra\n")
}

// TestCompatMigrateApplyStrictRefusesTheDriftCheckE2E keeps strict
// compatibility to the pinned binary's capabilities: the block is refused
// before the run connects, by a message naming the capability, and the target
// file is never created.
func TestCompatMigrateApplyStrictRefusesTheDriftCheckE2E(t *testing.T) {
	c := qt.New(t)
	target := filepath.Join(c.TempDir(), "target.db")
	dir := compatDriftProject(c, compatDriftSQLite(target), compatDriftBlock)

	got := compatDriftApply(c, dir, []string{"PTAH_ATLAS_STRICT_COMPAT=1"})

	c.Assert(got.ExitCode, qt.Equals, 1, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
	c.Assert(got.Stdout, qt.Equals, "")
	c.Assert(got.Stderr, qt.Equals, `Error: Atlas Community Edition strict compatibility refuses atlas.hcl check "migrate_apply" drift at atlas.hcl:8: `+
		"the pre-apply drift check is a capability Community Edition lacks\n")
	_, statErr := os.Stat(target)
	c.Assert(statErr, qt.ErrorIs, os.ErrNotExist)
}
