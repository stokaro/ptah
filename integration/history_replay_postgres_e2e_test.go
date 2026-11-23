//go:build integration

package integration_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/clirun"
)

// A replay of applied history runs each file in a transaction of its own,
// unless a run in tx-mode file refuses the file in one, and then without one.
// It takes no mode from the run: PostgreSQL decides what each history means,
// so only the server shows that the replay reproduced it.
//
// The histories:
//   - setLocal: migration 2 is `SET LOCAL search_path TO s2; CREATE TABLE
//     t2`, applied in the default mode, so t2 is in s2. Replayed outside a
//     transaction, SET LOCAL does nothing and t2 lands in public.
//   - setLocalNone: the same file under `-- atlas:txmode none`, so the
//     directive puts t2 in public, on the target and in the replay alike.
//   - deferred: a foreign key DEFERRABLE INITIALLY DEFERRED, with rows that
//     satisfy it only once both are in, applied in the default mode. Outside
//     a transaction the first row fails the check.
//   - concurrently: CREATE INDEX CONCURRENTLY beside a CREATE TABLE, which
//     only --tx-mode none applies.

var (
	historyReplaySetLocal = map[string]string{
		"0000000001_items.up.sql":   "CREATE SCHEMA s2;\nCREATE TABLE items (id int PRIMARY KEY);\n",
		"0000000001_items.down.sql": "DROP TABLE items;\nDROP SCHEMA s2;\n",
		"0000000002_t2.up.sql":      "SET LOCAL search_path TO s2;\nCREATE TABLE t2 (id int PRIMARY KEY);\n",
		"0000000002_t2.down.sql":    "DROP TABLE s2.t2;\n",
	}
	historyReplaySetLocalNone = map[string]string{
		"0000000001_items.up.sql":   historyReplaySetLocal["0000000001_items.up.sql"],
		"0000000001_items.down.sql": historyReplaySetLocal["0000000001_items.down.sql"],
		"0000000002_t2.up.sql":      "-- atlas:txmode none\n" + historyReplaySetLocal["0000000002_t2.up.sql"],
		"0000000002_t2.down.sql":    "DROP TABLE t2;\n",
	}
	historyReplayDeferred = map[string]string{
		"0000000001_pair.up.sql": "CREATE TABLE a (id int PRIMARY KEY, b_id int NOT NULL);\n" +
			"CREATE TABLE b (id int PRIMARY KEY, a_id int NOT NULL REFERENCES a (id));\n" +
			"ALTER TABLE a ADD CONSTRAINT a_b_fk FOREIGN KEY (b_id) REFERENCES b (id) DEFERRABLE INITIALLY DEFERRED;\n" +
			"INSERT INTO a (id, b_id) VALUES (1, 1);\n" +
			"INSERT INTO b (id, a_id) VALUES (1, 1);\n",
		"0000000001_pair.down.sql": "DROP TABLE b;\nDROP TABLE a;\n",
	}
	historyReplayConcurrently = map[string]string{
		"0000000001_items.up.sql":   "CREATE TABLE items (id int PRIMARY KEY, name text);\nCREATE INDEX CONCURRENTLY items_name ON items (name);\n",
		"0000000001_items.down.sql": "DROP TABLE items;\n",
	}
)

// historyReplayPending is the pending migration each history is given, after
// every applied one.
const (
	historyReplayPendingUp   = "CREATE TABLE pending_note (id int PRIMARY KEY);\n"
	historyReplayPendingDown = "DROP TABLE pending_note;\n"
)

// postgresHistory is a PostgreSQL target that applied a history, with a
// pending migration after it, and an empty shadow database.
type postgresHistory struct {
	root, dir, target, shadow string
}

// newPostgresHistory applies files to a new target with applyArgs, runs each
// statement of outOfBand on it, and adds the pending migration.
func newPostgresHistory(c *qt.C, files map[string]string, applyArgs []string, outOfBand ...string) postgresHistory {
	c.Helper()
	h := postgresHistory{
		root:   c.TempDir(),
		target: newEmptyReadScopeDatabase(c, "replay_target"),
		shadow: newEmptyReadScopeDatabase(c, "replay_shadow"),
	}
	h.dir = filepath.Join(h.root, "migrations")
	c.Assert(os.MkdirAll(h.dir, 0o750), qt.IsNil)
	for name, body := range files {
		c.Assert(os.WriteFile(filepath.Join(h.dir, name), []byte(body), 0o600), qt.IsNil) // #nosec G703 -- the path is c.TempDir() plus a constant name
	}
	applied := h.run(c, nil, append([]string{"migrations", "up", "--db-url", h.target, "--migrations-dir", h.dir}, applyArgs...)...)
	c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("%s%s", applied.Stdout, applied.Stderr))
	for _, statement := range outOfBand {
		compatDriftExec(c, h.target, statement)
	}
	pending := len(files)/2 + 1
	for suffix, body := range map[string]string{
		"up":   historyReplayPendingUp,
		"down": historyReplayPendingDown,
	} {
		name := fmt.Sprintf("%010d_pending_note.%s.sql", pending, suffix)
		c.Assert(os.WriteFile(filepath.Join(h.dir, name), []byte(body), 0o600), qt.IsNil) // #nosec G703 -- the path is c.TempDir() plus a constant name
	}
	return h
}

// run runs ptah in the history's root, in an environment that carries no
// PTAH_ variable but env.
func (h postgresHistory) run(c *qt.C, env []string, args ...string) clirun.Result {
	c.Helper()
	return clirun.Run(c, clirun.Ptah, clirun.Options{Dir: h.root, Env: append(environmentWithoutPtahVariables(), env...)}, args...)
}

// pendingApplied counts the tables named pending_note in the target, which
// the pending migration creates.
func (h postgresHistory) pendingApplied(c *qt.C) int {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), h.target)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	var count int
	c.Assert(conn.QueryRowContext(c.Context(),
		"SELECT count(*) FROM information_schema.tables WHERE table_name = 'pending_note'").Scan(&count), qt.IsNil)
	return count
}

// postgresHistoryCommand is a command that replays the history: the drift
// check of `migrations up`, which applies the pending migration once the
// target is clean, or the report of `migrations drift`, which applies
// nothing.
type postgresHistoryCommand struct {
	args func(h postgresHistory) []string
	env  []string
}

func historyReplayUp(runArgs ...string) postgresHistoryCommand {
	return postgresHistoryCommand{args: func(h postgresHistory) []string {
		return append([]string{"migrations", "up", "--db-url", h.target, "--migrations-dir", h.dir,
			"--check-drift", "--shadow-db", h.shadow}, runArgs...)
	}}
}

func historyReplayDrift(env ...string) postgresHistoryCommand {
	return postgresHistoryCommand{
		args: func(h postgresHistory) []string {
			return []string{"migrations", "drift", "--db-url", h.target, "--migrations-dir", h.dir, "--shadow-db", h.shadow}
		},
		env: env,
	}
}

// TestHistoryReplayPostgresE2E_HappyPath replays histories whose files a
// transaction changes, refuses, or needs, and finds no drift. The pending
// migration runs under the drift check and not under the report.
func TestHistoryReplayPostgresE2E_HappyPath(t *testing.T) {
	tests := []struct {
		name        string
		files       map[string]string
		applyArgs   []string
		command     postgresHistoryCommand
		wantPending int
	}{
		{
			name:        "SET LOCAL, checked by an up run under none",
			files:       historyReplaySetLocal,
			command:     historyReplayUp("--tx-mode", "none"),
			wantPending: 1,
		},
		{
			name:    "SET LOCAL, reported with PTAH_TX_MODE=none",
			files:   historyReplaySetLocal,
			command: historyReplayDrift("PTAH_TX_MODE=none"),
		},
		{
			name:    "SET LOCAL under its directive, reported",
			files:   historyReplaySetLocalNone,
			command: historyReplayDrift(),
		},
		{
			name:        "a deferred foreign key, checked by an up run under none",
			files:       historyReplayDeferred,
			command:     historyReplayUp("--tx-mode", "none"),
			wantPending: 1,
		},
		{
			name:        "CREATE INDEX CONCURRENTLY, checked by an up run",
			files:       historyReplayConcurrently,
			applyArgs:   []string{"--tx-mode", "none"},
			command:     historyReplayUp(),
			wantPending: 1,
		},
		{
			name:      "CREATE INDEX CONCURRENTLY, reported",
			files:     historyReplayConcurrently,
			applyArgs: []string{"--tx-mode", "none"},
			command:   historyReplayDrift(),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			h := newPostgresHistory(c, test.files, test.applyArgs)

			got := h.run(c, test.command.env, test.command.args(h)...)

			c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(h.pendingApplied(c), qt.Equals, test.wantPending)
		})
	}
}

// TestHistoryReplayPostgresE2E_FailurePath adds public.t2 by hand to a target
// whose t2 a SET LOCAL put in s2. The replay puts t2 in s2 too, so public.t2
// is drift, whatever mode the run applies in, and the pending migration does
// not run.
func TestHistoryReplayPostgresE2E_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		command postgresHistoryCommand
	}{
		{name: "checked by an up run under none", command: historyReplayUp("--tx-mode", "none")},
		{name: "reported with PTAH_TX_MODE=none", command: historyReplayDrift("PTAH_TX_MODE=none")},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			h := newPostgresHistory(c, historyReplaySetLocal, nil, "CREATE TABLE public.t2 (id int PRIMARY KEY)")

			got := h.run(c, test.command.env, test.command.args(h)...)

			c.Assert(got.ExitCode, qt.Equals, 1, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(got.Stdout+got.Stderr, qt.Contains, "t2")
			c.Assert(h.pendingApplied(c), qt.Equals, 0)
		})
	}
}

// TestHistoryReplayPostgresCheckpointE2E_HappyPath squashes the CONCURRENTLY
// history, which `migrations checkpoint` replays as the files ran, with no
// mode stated, and the checkpoint it writes holds the index.
func TestHistoryReplayPostgresCheckpointE2E_HappyPath(t *testing.T) {
	c := qt.New(t)
	h := newPostgresHistory(c, historyReplayConcurrently, []string{"--tx-mode", "none"})

	got := h.run(c, nil, "migrations", "checkpoint", "--migrations-dir", h.dir, "--shadow-db", h.shadow, "--dry-run")

	c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
	c.Assert(got.Stdout, qt.Contains, "items_name")
	c.Assert(got.Stdout, qt.Contains, "pending_note")
}
