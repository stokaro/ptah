//go:build integration && unix

package integration_test

// Unix only: the pre-migration hooks these runs use to hold a target's run open
// and to change the release under the rollout are POSIX shell scripts.

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlasurl"
	"ptah.run/internal/clirun"
	"ptah.run/internal/migrationrollout"
)

// rolloutWorkspace is one rollout's files: a two-migration release, the SQLite
// databases the targets name, and the definition and state beside them. Every
// target is a file of its own, so each one is an independent database with its
// own revision table, and a target nothing connected to has no file at all.
type rolloutWorkspace struct {
	dir        string
	migrations string
	databases  string
	definition string
	state      string
}

func newRolloutWorkspace(c *qt.C) rolloutWorkspace {
	c.Helper()
	dir := c.TempDir()
	ws := rolloutWorkspace{
		dir:        dir,
		migrations: filepath.Join(dir, "migrations"),
		databases:  filepath.Join(dir, "databases"),
		definition: filepath.Join(dir, "rollout.yaml"),
		state:      filepath.Join(dir, "rollout-state.json"),
	}
	c.Assert(os.MkdirAll(ws.migrations, 0o750), qt.IsNil)
	c.Assert(os.MkdirAll(ws.databases, 0o750), qt.IsNil)
	for name, body := range map[string]string{
		"0000000001_accounts.up.sql":   "CREATE TABLE accounts (id INTEGER PRIMARY KEY);\n",
		"0000000001_accounts.down.sql": "DROP TABLE accounts;\n",
		"0000000002_ledgers.up.sql":    "CREATE TABLE ledgers (id INTEGER PRIMARY KEY, account_id INTEGER NOT NULL);\n",
		"0000000002_ledgers.down.sql":  "DROP TABLE ledgers;\n",
	} {
		c.Assert(os.WriteFile(filepath.Join(ws.migrations, name), []byte(body), 0o600), qt.IsNil)
	}
	return ws
}

// database is the path of a target's SQLite file.
func (ws rolloutWorkspace) database(name string) string {
	return filepath.Join(ws.databases, name+".db")
}

// targets writes an inventory entry for each name, in order.
func (ws rolloutWorkspace) targets(names ...string) string {
	var lines []string
	for _, name := range names {
		lines = append(lines, fmt.Sprintf("  - {name: %s, url: %q}", name, atlasurl.SQLiteURLFromPath(ws.database(name))))
	}
	return "targets:\n" + strings.Join(lines, "\n") + "\n"
}

func (ws rolloutWorkspace) writeDefinition(c *qt.C, definition string) {
	c.Helper()
	c.Assert(os.WriteFile(ws.definition, []byte(definition), 0o600), qt.IsNil)
}

// rollout runs `ptah migrations rollout <verb>` on the workspace, with env
// added to this process's environment.
func (ws rolloutWorkspace) rollout(c *qt.C, env []string, verb string, extra ...string) clirun.Result {
	c.Helper()
	args := append([]string{
		"migrations", "rollout", verb,
		"--definition", ws.definition,
		"--migrations-dir", ws.migrations,
		"--state-file", ws.state,
	}, extra...)
	return clirun.Run(c, clirun.Ptah, clirun.Options{Dir: ws.dir, Env: append(os.Environ(), env...)}, args...)
}

func (ws rolloutWorkspace) readState(c *qt.C) migrationrollout.State {
	c.Helper()
	content, err := os.ReadFile(ws.state)
	c.Assert(err, qt.IsNil)
	var state migrationrollout.State
	c.Assert(json.Unmarshal(content, &state), qt.IsNil)
	return state
}

// rolloutStatuses maps each target of state to its status.
func rolloutStatuses(state migrationrollout.State) map[string]migrationrollout.TargetStatus {
	statuses := make(map[string]migrationrollout.TargetStatus)
	for _, stage := range state.Stages {
		for _, target := range stage.Targets {
			statuses[target.Name] = target.Status
		}
	}
	return statuses
}

// rolloutStageStatuses maps each stage of state to its status.
func rolloutStageStatuses(state migrationrollout.State) map[string]migrationrollout.StageStatus {
	statuses := make(map[string]migrationrollout.StageStatus)
	for _, stage := range state.Stages {
		statuses[stage.Name] = stage.Status
	}
	return statuses
}

// rolloutTarget returns the recorded state of one target.
func rolloutTarget(c *qt.C, state migrationrollout.State, name string) migrationrollout.TargetState {
	c.Helper()
	for _, stage := range state.Stages {
		for _, target := range stage.Targets {
			if target.Name == name {
				return target
			}
		}
	}
	c.Fatalf("the state records no target %q", name)
	return migrationrollout.TargetState{}
}

// applicationTables lists the tables a target's file holds that a migration of
// the release creates, read through the driver rather than through ptah.
func applicationTables(c *qt.C, path string) []string {
	c.Helper()
	db, err := sql.Open("sqlite", path)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(db.Close(), qt.IsNil) }()
	rows, err := db.QueryContext(c.Context(),
		"SELECT name FROM sqlite_master WHERE type = 'table' AND name IN ('accounts', 'ledgers', 'late') ORDER BY name")
	c.Assert(err, qt.IsNil)
	defer func() { _ = rows.Close() }()
	tables := make([]string, 0)
	for rows.Next() {
		var name string
		c.Assert(rows.Scan(&name), qt.IsNil)
		tables = append(tables, name)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return tables
}

func execSQLite(c *qt.C, path, statement string) {
	c.Helper()
	db, err := sql.Open("sqlite", path)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(db.Close(), qt.IsNil) }()
	_, err = db.ExecContext(c.Context(), statement)
	c.Assert(err, qt.IsNil)
}

// ranTargets names the targets a rollout invocation started a run for, read
// from the run's own banner in the relayed output rather than from the state
// the rollout wrote.
func ranTargets(stderr string) []string {
	var names []string
	for line := range strings.Lines(stderr) {
		prefixed, bracketed := strings.CutPrefix(line, "[")
		name, rest, named := strings.Cut(prefixed, "] ")
		if bracketed && named && strings.HasPrefix(rest, "=== MIGRATE UP ===") {
			names = append(names, name)
		}
	}
	slices.Sort(names)
	return names
}

func assertNoDatabase(c *qt.C, path string) {
	c.Helper()
	_, err := os.Stat(path)
	c.Assert(err, qt.ErrorIs, os.ErrNotExist, qt.Commentf("%s was opened, so the rollout reached its target", path))
}

// TestMigrationsRolloutCanaryFailureBlocksLaterStagesE2E drives a rollout whose
// canary fails through the shipped binary, and then the documented recovery.
//
// The canary already holds the table the second migration creates, so its run
// commits the first migration and stops at the second with a dirty revision
// row. Nothing behind the canary may run: their database files must not even
// exist. A resume, even one asked to retry failures, must not run the canary
// again over its dirty row. Once the operator recovers the canary by hand with
// `ptah migrations up --allow-dirty`, a resume reads it back as succeeded
// without running it and rolls the release out to the rest.
func TestMigrationsRolloutCanaryFailureBlocksLaterStagesE2E(t *testing.T) {
	c := qt.New(t)
	ws := newRolloutWorkspace(c)
	ws.writeDefinition(c, ws.targets("canary", "t1", "t2", "t3")+`stages:
  - {name: canary, canary: true, targets: [canary]}
  - {name: wave-1, depends_on: [canary], targets: [t1, t2], concurrency: 2}
  - {name: wave-2, depends_on: [wave-1], targets: [t3]}
`)
	execSQLite(c, ws.database("canary"), "CREATE TABLE ledgers (id INTEGER PRIMARY KEY)")

	first := ws.rollout(c, nil, "run")

	c.Assert(first.ExitCode, qt.Equals, 1, qt.Commentf("stdout:\n%s\nstderr:\n%s", first.Stdout, first.Stderr))
	state := ws.readState(c)
	c.Assert(state.Status, qt.Equals, migrationrollout.RunFailed)
	c.Assert(rolloutStageStatuses(state), qt.DeepEquals, map[string]migrationrollout.StageStatus{
		"canary": migrationrollout.StageFailed,
		"wave-1": migrationrollout.StageBlocked,
		"wave-2": migrationrollout.StageBlocked,
	})
	c.Assert(rolloutStatuses(state), qt.DeepEquals, map[string]migrationrollout.TargetStatus{
		"canary": migrationrollout.TargetFailed,
		"t1":     migrationrollout.TargetBlocked,
		"t2":     migrationrollout.TargetBlocked,
		"t3":     migrationrollout.TargetBlocked,
	})
	c.Assert(rolloutTarget(c, state, "canary").Error.Message, qt.Contains, "table ledgers already exists")
	c.Assert(ranTargets(first.Stderr), qt.DeepEquals, []string{"canary"})
	for _, name := range []string{"t1", "t2", "t3"} {
		assertNoDatabase(c, ws.database(name))
	}

	retried := ws.rollout(c, nil, "resume", "--retry-failed")

	c.Assert(retried.ExitCode, qt.Equals, 1, qt.Commentf("stderr:\n%s", retried.Stderr))
	c.Assert(ranTargets(retried.Stderr), qt.HasLen, 0)
	canary := rolloutTarget(c, ws.readState(c), "canary")
	c.Assert(canary.Status, qt.Equals, migrationrollout.TargetFailed)
	c.Assert(canary.Note, qt.Contains, "holds a dirty row for migration 2, which a rollout never retries")
	assertNoDatabase(c, ws.database("t1"))

	execSQLite(c, ws.database("canary"), "DROP TABLE ledgers")
	recovered := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: ws.dir},
		"migrations", "up", "--allow-dirty",
		"--db-url", atlasurl.SQLiteURLFromPath(ws.database("canary")),
		"--migrations-dir", ws.migrations)
	c.Assert(recovered.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", recovered.Stderr))

	resumed := ws.rollout(c, nil, "resume")

	c.Assert(resumed.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", resumed.Stdout, resumed.Stderr))
	c.Assert(ranTargets(resumed.Stderr), qt.DeepEquals, []string{"t1", "t2", "t3"})
	state = ws.readState(c)
	c.Assert(state.Status, qt.Equals, migrationrollout.RunSucceeded)
	c.Assert(state.Invocations, qt.Equals, 3)
	c.Assert(rolloutTarget(c, state, "canary").Status, qt.Equals, migrationrollout.TargetSucceeded)
	c.Assert(rolloutTarget(c, state, "canary").Attempts, qt.Equals, 1)
	for _, name := range []string{"canary", "t1", "t2", "t3"} {
		c.Assert(applicationTables(c, ws.database(name)), qt.DeepEquals, []string{"accounts", "ledgers"}, qt.Commentf("target %s", name))
	}
}

// concurrencyProbe is a pre-migration hook that counts the target runs holding
// one open at the same moment. Each run leaves a marker for one second and
// appends how many markers it saw; the largest count is the most runs that
// overlapped. The count is taken by the runs themselves, outside the rollout.
const concurrencyProbe = `set -eu
markers="$1"
mine="$markers/running.$$"
: > "$mine"
ls "$markers" | grep -c '^running\.' >> "$markers.counts"
sleep 1
rm -f "$mine"
`

// TestMigrationsRolloutConcurrencyCapE2E measures the stage concurrency from
// inside the target runs. With a cap of two, no more than two runs overlap and
// two do; the uncapped row is the control that proves the probe can see more
// than two when more overlap.
func TestMigrationsRolloutConcurrencyCapE2E(t *testing.T) {
	cases := []struct {
		name        string
		concurrency int
	}{
		{name: "capped at two", concurrency: 2},
		{name: "every member at once", concurrency: 4},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			ws := newRolloutWorkspace(c)
			ws.writeDefinition(c, ws.targets("t1", "t2", "t3", "t4")+
				fmt.Sprintf("stages:\n  - {name: wave, targets: [t1, t2, t3, t4], concurrency: %d}\n", tc.concurrency))
			markers := filepath.Join(ws.dir, "markers")
			c.Assert(os.MkdirAll(markers, 0o750), qt.IsNil)
			probe := filepath.Join(ws.dir, "probe.sh")
			c.Assert(os.WriteFile(probe, []byte(concurrencyProbe), 0o600), qt.IsNil)

			result := ws.rollout(c, []string{fmt.Sprintf("PTAH_PRE_UP_HOOK=sh '%s' '%s'", probe, markers)}, "run")

			c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", result.Stdout, result.Stderr))
			c.Assert(ranTargets(result.Stderr), qt.DeepEquals, []string{"t1", "t2", "t3", "t4"})
			c.Assert(largestCount(c, markers+".counts"), qt.Equals, tc.concurrency)
		})
	}
}

func largestCount(c *qt.C, path string) int {
	c.Helper()
	content, err := os.ReadFile(path)
	c.Assert(err, qt.IsNil)
	largest := 0
	for line := range strings.Lines(string(content)) {
		count, err := strconv.Atoi(strings.TrimSpace(line))
		c.Assert(err, qt.IsNil)
		largest = max(largest, count)
	}
	return largest
}

// releaseMutator is a pre-migration hook that holds a run open for two seconds
// and, the first time it runs, adds a third migration to the directory the
// rollout was started from. A rollout bound to its release must not apply it.
const releaseMutator = `set -eu
release="$1"
if [ ! -e "$release/0000000003_late.up.sql" ]; then
  printf 'CREATE TABLE late (id INTEGER PRIMARY KEY);\n' > "$release/0000000003_late.up.sql"
  printf 'DROP TABLE late;\n' > "$release/0000000003_late.down.sql"
fi
sleep 2
`

// TestMigrationsRolloutMixedOutcomeAndResumeE2E drives a stage that stops on a
// failure while another member is still running, then two resumes.
//
// t2 carries an approval that names only the first migration, so its run is
// refused under its migration lock before it changes anything, while t1 is
// still inside its pre-migration hook. The stage starts nothing further, keeps
// t1's success, skips t3 and t4, and blocks t5. The canary's hook adds a
// migration to the source directory meanwhile, and no target applies it: the
// run is bound to the release it captured. A resume refuses the changed
// directory; with the directory restored, a resume reads every started target
// back and runs nothing, because the only one behind is a failure nobody asked
// to retry. With t2's approval corrected and --retry-failed, a resume runs t2,
// then the skipped and blocked targets, and never the ones already done.
func TestMigrationsRolloutMixedOutcomeAndResumeE2E(t *testing.T) {
	c := qt.New(t)
	ws := newRolloutWorkspace(c)
	approvals := filepath.Join(ws.dir, "approvals")
	c.Assert(os.MkdirAll(approvals, 0o750), qt.IsNil)
	approval := filepath.Join(approvals, "t2.json")
	c.Assert(os.WriteFile(approval, []byte(`{"migrations": [{"version": 1, "version_key": "1"}]}`), 0o600), qt.IsNil)
	inventory := strings.Replace(ws.targets("canary", "t1", "t2", "t3", "t4", "t5"),
		fmt.Sprintf("{name: t2, url: %q}", atlasurl.SQLiteURLFromPath(ws.database("t2"))),
		fmt.Sprintf("{name: t2, url: %q, expect_sequence: approvals/t2.json}", atlasurl.SQLiteURLFromPath(ws.database("t2"))), 1)
	ws.writeDefinition(c, inventory+`stages:
  - {name: canary, canary: true, targets: [canary]}
  - {name: wave-1, depends_on: [canary], targets: [t1, t2, t3, t4], concurrency: 2, on_failure: stop}
  - {name: wave-2, depends_on: [wave-1], targets: [t5]}
`)
	mutator := filepath.Join(ws.dir, "mutate-release.sh")
	c.Assert(os.WriteFile(mutator, []byte(releaseMutator), 0o600), qt.IsNil)

	first := ws.rollout(c, []string{fmt.Sprintf("PTAH_PRE_UP_HOOK=sh '%s' '%s'", mutator, ws.migrations)}, "run")

	c.Assert(first.ExitCode, qt.Equals, 1, qt.Commentf("stdout:\n%s\nstderr:\n%s", first.Stdout, first.Stderr))
	state := ws.readState(c)
	c.Assert(rolloutStatuses(state), qt.DeepEquals, map[string]migrationrollout.TargetStatus{
		"canary": migrationrollout.TargetSucceeded,
		"t1":     migrationrollout.TargetSucceeded,
		"t2":     migrationrollout.TargetFailed,
		"t3":     migrationrollout.TargetSkipped,
		"t4":     migrationrollout.TargetSkipped,
		"t5":     migrationrollout.TargetBlocked,
	})
	c.Assert(rolloutStageStatuses(state), qt.DeepEquals, map[string]migrationrollout.StageStatus{
		"canary": migrationrollout.StageSucceeded,
		"wave-1": migrationrollout.StageFailed,
		"wave-2": migrationrollout.StageBlocked,
	})
	c.Assert(rolloutTarget(c, state, "t2").Error.Message, qt.Contains, "the approved sequence is [1]")
	c.Assert(applicationTables(c, ws.database("canary")), qt.DeepEquals, []string{"accounts", "ledgers"})
	c.Assert(applicationTables(c, ws.database("t1")), qt.DeepEquals, []string{"accounts", "ledgers"})
	c.Assert(applicationTables(c, ws.database("t2")), qt.HasLen, 0)
	for _, name := range []string{"t3", "t4", "t5"} {
		assertNoDatabase(c, ws.database(name))
	}
	_, err := os.Stat(filepath.Join(ws.migrations, "0000000003_late.up.sql"))
	c.Assert(err, qt.IsNil, qt.Commentf("the hook did not change the release under the rollout"))

	changed := ws.rollout(c, nil, "resume")

	c.Assert(changed.ExitCode, qt.Equals, 2)
	c.Assert(changed.Stderr, qt.Contains, "the migration release is not the one this rollout is bound to")
	c.Assert(ranTargets(changed.Stderr), qt.HasLen, 0)

	c.Assert(os.Remove(filepath.Join(ws.migrations, "0000000003_late.up.sql")), qt.IsNil)
	c.Assert(os.Remove(filepath.Join(ws.migrations, "0000000003_late.down.sql")), qt.IsNil)
	unchanged := ws.rollout(c, nil, "resume")

	c.Assert(unchanged.ExitCode, qt.Equals, 1, qt.Commentf("stdout:\n%s\nstderr:\n%s", unchanged.Stdout, unchanged.Stderr))
	c.Assert(ranTargets(unchanged.Stderr), qt.HasLen, 0)
	c.Assert(rolloutStatuses(ws.readState(c))["t2"], qt.Equals, migrationrollout.TargetFailed)
	assertNoDatabase(c, ws.database("t3"))

	c.Assert(os.WriteFile(approval,
		[]byte(`{"migrations": [{"version": 1, "version_key": "1"}, {"version": 2, "version_key": "2"}]}`), 0o600), qt.IsNil)
	retried := ws.rollout(c, nil, "resume", "--retry-failed")

	c.Assert(retried.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", retried.Stdout, retried.Stderr))
	c.Assert(ranTargets(retried.Stderr), qt.DeepEquals, []string{"t2", "t3", "t4", "t5"})
	state = ws.readState(c)
	c.Assert(state.Status, qt.Equals, migrationrollout.RunSucceeded)
	c.Assert(rolloutTarget(c, state, "t1").Attempts, qt.Equals, 1)
	c.Assert(rolloutTarget(c, state, "t2").Attempts, qt.Equals, 2)
	for _, name := range []string{"canary", "t1", "t2", "t3", "t4", "t5"} {
		c.Assert(applicationTables(c, ws.database(name)), qt.DeepEquals, []string{"accounts", "ledgers"}, qt.Commentf("target %s", name))
	}
}
