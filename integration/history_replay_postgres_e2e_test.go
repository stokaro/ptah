//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
)

// A replay of applied history runs each file in a transaction of its own,
// unless a run in tx-mode file refuses the file in one, and then without one.
// It takes no mode from the run: PostgreSQL decides what each history means,
// so only the server shows that the replay reproduced it.
//
// The histories:
//   - setLocal: migration 2 is `SET LOCAL DateStyle = 'ISO, DMY'` and a
//     table t2 whose column due defaults to the literal '01/02/2020', applied
//     in the default mode, so the default is 1 February. Replayed outside a
//     transaction, SET LOCAL does nothing, the server reads the literal month
//     first, and the default is 2 January. SET LOCAL search_path would show
//     the same, but the replay guard refuses it, trusted or not: an
//     unqualified name it sends to pg_catalog escapes the namespace check.
//   - setLocalNone: the same file under `-- atlas:txmode none`, so the
//     directive gives due 2 January, on the target and in the replay alike.
//   - deferred: a foreign key DEFERRABLE INITIALLY DEFERRED, with rows that
//     satisfy it only once both are in, applied in the default mode. Outside
//     a transaction the first row fails the check.
//   - concurrently: CREATE INDEX CONCURRENTLY beside a CREATE TABLE, which
//     only --tx-mode none applies.
//   - enumValue: migration 2 adds an enum value and then uses it, which
//     PostgreSQL refuses in one transaction (SQLSTATE 55P04), so only
//     --tx-mode none applies it. No check before the run can see the use,
//     which is a cast of an expression.
//   - procedureCommit: migration 2 calls a procedure that commits, which
//     PostgreSQL refuses in a transaction block (SQLSTATE 2D000). CockroachDB
//     refuses it with the same code.
//   - sequence: migration 2 inserts a row, whose serial id is 1, and then adds
//     an enum value and uses it, so only --tx-mode none applies it. Migration
//     3 refers to row 1. The rollback of the first attempt keeps the sequence
//     value it took, so the second attempt inserts row 2.

var (
	historyReplaySetLocal = map[string]string{
		"0000000001_items.up.sql":   "CREATE TABLE items (id int PRIMARY KEY);\n",
		"0000000001_items.down.sql": "DROP TABLE items;\n",
		"0000000002_t2.up.sql":      "SET LOCAL DateStyle = 'ISO, DMY';\nCREATE TABLE t2 (id int PRIMARY KEY, due date DEFAULT '01/02/2020');\n",
		"0000000002_t2.down.sql":    "DROP TABLE t2;\n",
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
	historyReplayEnumType = map[string]string{
		"0000000001_mood.up.sql":   "CREATE TYPE mood AS ENUM ('sad');\nCREATE TABLE m (v mood);\n",
		"0000000001_mood.down.sql": "DROP TABLE m;\nDROP TYPE mood;\n",
	}
	historyReplayEnumValue = historyReplayWith(historyReplayEnumType, map[string]string{
		"0000000002_happy.up.sql":   historyReplayEnumValueUp,
		"0000000002_happy.down.sql": "DELETE FROM m;\n",
	})
	historyReplayProcedure = map[string]string{
		"0000000001_pc.up.sql": "CREATE TABLE p (id int);\n" +
			"CREATE PROCEDURE pc() LANGUAGE plpgsql AS $$ BEGIN INSERT INTO p VALUES (1); COMMIT; END $$;\n",
		"0000000001_pc.down.sql": "DROP PROCEDURE pc();\nDROP TABLE p;\n",
	}
	historyReplayProcedureCommit = historyReplayWith(historyReplayProcedure, map[string]string{
		"0000000002_call.up.sql":   historyReplayProcedureCommitUp,
		"0000000002_call.down.sql": "DELETE FROM p;\n",
	})
	historyReplaySequence = map[string]string{
		"0000000001_items.up.sql":   "CREATE TYPE mood AS ENUM ('sad');\nCREATE TABLE items (id serial PRIMARY KEY, v mood);\n",
		"0000000001_items.down.sql": "DROP TABLE items;\nDROP TYPE mood;\n",
		"0000000002_happy.up.sql": "INSERT INTO items (v) VALUES ('sad');\nALTER TYPE mood ADD VALUE 'happy';\n" +
			"INSERT INTO items (v) VALUES (('hap' || 'py')::mood);\n",
		"0000000002_happy.down.sql": "DELETE FROM items;\n",
		"0000000003_refs.up.sql":    "CREATE TABLE refs (item_id int NOT NULL REFERENCES items (id));\nINSERT INTO refs VALUES (1);\n",
		"0000000003_refs.down.sql":  "DROP TABLE refs;\n",
	}
)

// historyReplayWith is a copy of files with the files of next added.
func historyReplayWith(files, next map[string]string) map[string]string {
	merged := maps.Clone(files)
	maps.Copy(merged, next)
	return merged
}

// The migrations a transaction refuses only when the server runs them.
const (
	historyReplayEnumValueUp       = "ALTER TYPE mood ADD VALUE 'happy';\nINSERT INTO m VALUES (('hap' || 'py')::mood);\n"
	historyReplayProcedureCommitUp = "CALL pc();\n"
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

// newPostgresHistory applies files to a new PostgreSQL target with applyArgs,
// runs each statement of outOfBand on it, and adds the pending migration.
func newPostgresHistory(c *qt.C, files map[string]string, applyArgs []string, outOfBand ...string) postgresHistory {
	c.Helper()
	return newEngineHistory(c, dbtarget.PostgreSQL, files, applyArgs, outOfBand...)
}

// newEngineHistory is [newPostgresHistory] on engine, a PostgreSQL-family
// server, whose target and shadow are new databases on it.
func newEngineHistory(
	c *qt.C,
	engine dbtarget.Engine,
	files map[string]string,
	applyArgs []string,
	outOfBand ...string,
) postgresHistory {
	c.Helper()
	h := postgresHistory{
		root:   c.TempDir(),
		target: newHistoryReplayDatabase(c, engine, "target"),
		shadow: newHistoryReplayDatabase(c, engine, "shadow"),
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

// newHistoryReplayDatabase creates a database on engine, dropped when the test
// ends, and returns its URL.
func newHistoryReplayDatabase(c *qt.C, engine dbtarget.Engine, role string) string {
	c.Helper()
	adminURL := dbtarget.URL(c, engine)
	admin, err := sql.Open("pgx", postgresFamilyDriverURL(c, adminURL))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(admin.Close(), qt.IsNil) })
	name := fmt.Sprintf("ptah_replay_%s_%d", role, time.Now().UnixNano())
	createE2EDatabase(c, c.Context(), admin, name)
	c.Cleanup(func() { dropE2EDatabase(c, context.Background(), admin, name) })
	return replaceDatabaseName(c, adminURL, name)
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
	return h.count(c, "SELECT count(*) FROM information_schema.tables WHERE table_name = 'pending_note'")
}

// count runs query, a count, on the target.
func (h postgresHistory) count(c *qt.C, query string) int {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), h.target)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	var count int
	c.Assert(conn.QueryRowContext(c.Context(), query).Scan(&count), qt.IsNil)
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

// historyReplayRollback is the rollback rehearsal of `migrations down
// --shadow-db`, which replays the history before it rolls migration 2 back on
// the target.
func historyReplayRollback(runArgs ...string) postgresHistoryCommand {
	return postgresHistoryCommand{args: func(h postgresHistory) []string {
		return append([]string{"migrations", "down", "--db-url", h.target, "--migrations-dir", h.dir,
			"--shadow-db", h.shadow, "--target", "1", "--confirm"}, runArgs...)
	}}
}

// The procedure histories define a routine, which the replay guard refuses on
// a database the run does not own unless the operator trusts the SQL; these
// are what an operator who wrote the history passes.
const (
	historyReplayTrustFlag = "--trust-replay-sql"
	historyReplayTrustEnv  = "PTAH_TRUST_REPLAY_SQL=1"
)

func historyReplayDrift(env ...string) postgresHistoryCommand {
	return postgresHistoryCommand{
		args: func(h postgresHistory) []string {
			return []string{"migrations", "drift", "--db-url", h.target, "--migrations-dir", h.dir, "--shadow-db", h.shadow}
		},
		env: env,
	}
}

// historyReplayDriftExcluding is the report of `migrations drift` with
// selector left out of the comparison.
func historyReplayDriftExcluding(selector string) postgresHistoryCommand {
	return postgresHistoryCommand{args: func(h postgresHistory) []string {
		return append(historyReplayDrift().args(h), "--exclude", selector)
	}}
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
			name:      "SET LOCAL applied under none, reported with its table left out",
			files:     historyReplaySetLocal,
			applyArgs: []string{"--tx-mode", "none"},
			command:   historyReplayDriftExcluding("public.t2"),
		},
		{
			name:        "SET LOCAL applied under none, checked by an up run with its table left out",
			files:       historyReplaySetLocal,
			applyArgs:   []string{"--tx-mode", "none"},
			command:     historyReplayUp("--drift-exclude", "public.t2"),
			wantPending: 1,
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
		{
			name:        "an enum value added and used, checked by an up run",
			files:       historyReplayEnumValue,
			applyArgs:   []string{"--tx-mode", "none"},
			command:     historyReplayUp(),
			wantPending: 1,
		},
		{
			name:      "an enum value added and used, reported",
			files:     historyReplayEnumValue,
			applyArgs: []string{"--tx-mode", "none"},
			command:   historyReplayDrift(),
		},
		{
			name:      "an enum value added and used, rehearsed before a rollback",
			files:     historyReplayEnumValue,
			applyArgs: []string{"--tx-mode", "none"},
			command:   historyReplayRollback(),
		},
		{
			name:        "a procedure that commits, checked by an up run",
			files:       historyReplayProcedureCommit,
			applyArgs:   []string{"--tx-mode", "none"},
			command:     historyReplayUp(historyReplayTrustFlag),
			wantPending: 1,
		},
		{
			name:      "a procedure that commits, reported",
			files:     historyReplayProcedureCommit,
			applyArgs: []string{"--tx-mode", "none"},
			command:   historyReplayDrift(historyReplayTrustEnv),
		},
		{
			name:      "a procedure that commits, rehearsed before a rollback",
			files:     historyReplayProcedureCommit,
			applyArgs: []string{"--tx-mode", "none"},
			command:   historyReplayRollback(historyReplayTrustFlag),
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

// TestHistoryReplayPostgresE2E_FailurePath finds a default of 2 January on
// t2.due in a target the replay gives 1 February, so t2 is drift and the
// pending migration does not run. A default changed by hand to the one a
// replay outside a transaction would give is drift whatever mode the run
// applies in. A history applied under --tx-mode none gave 2 January, as SET
// LOCAL did nothing outside a transaction, and the replay cannot tell that
// from the file.
func TestHistoryReplayPostgresE2E_FailurePath(t *testing.T) {
	const byHand = "ALTER TABLE t2 ALTER COLUMN due SET DEFAULT '2020-01-02'"
	tests := []struct {
		name      string
		applyArgs []string
		outOfBand []string
		command   postgresHistoryCommand
	}{
		{
			name:      "added by hand, checked by an up run under none",
			outOfBand: []string{byHand},
			command:   historyReplayUp("--tx-mode", "none"),
		},
		{
			name:      "added by hand, reported with PTAH_TX_MODE=none",
			outOfBand: []string{byHand},
			command:   historyReplayDrift("PTAH_TX_MODE=none"),
		},
		{
			name:      "applied under none, reported",
			applyArgs: []string{"--tx-mode", "none"},
			command:   historyReplayDrift(),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			h := newPostgresHistory(c, historyReplaySetLocal, test.applyArgs, test.outOfBand...)

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

// TestHistoryReplayPostgresE2E_PendingFileKeepsItsRefusal applies migration 1
// of a history and leaves the file a transaction refuses pending, after the
// pending note. The drift check replays migration 1 and is clean, and the run
// applies the note and then sends the file in a transaction of its own, as
// the run's mode says. The server refuses it, and nothing of it is applied:
// running a refused file again without a transaction is the replay's rule
// alone.
func TestHistoryReplayPostgresE2E_PendingFileKeepsItsRefusal(t *testing.T) {
	tests := []struct {
		name    string
		applied map[string]string
		pending string
		runArgs []string
		wantErr string
		// changed counts what the pending file changes when it runs.
		changed string
	}{
		{
			name:    "an enum value added and used",
			applied: historyReplayEnumType,
			pending: historyReplayEnumValueUp,
			wantErr: "(SQLSTATE 55P04)",
			changed: "SELECT count(*) FROM pg_enum WHERE enumlabel = 'happy'",
		},
		{
			name:    "a procedure that commits",
			applied: historyReplayProcedure,
			pending: historyReplayProcedureCommitUp,
			runArgs: []string{historyReplayTrustFlag},
			wantErr: "(SQLSTATE 2D000)",
			changed: "SELECT count(*) FROM p",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			h := newPostgresHistory(c, test.applied, []string{"--tx-mode", "none"})
			c.Assert(os.WriteFile(filepath.Join(h.dir, "0000000003_refused.up.sql"), []byte(test.pending), 0o600), qt.IsNil)
			c.Assert(os.WriteFile(filepath.Join(h.dir, "0000000003_refused.down.sql"), []byte("SELECT 1;\n"), 0o600), qt.IsNil)

			got := h.run(c, nil, historyReplayUp(test.runArgs...).args(h)...)

			c.Assert(got.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(got.Stderr, qt.Contains, "failed to apply migration 3")
			c.Assert(got.Stderr, qt.Contains, test.wantErr)
			c.Assert(h.pendingApplied(c), qt.Equals, 1)
			c.Assert(h.count(c, test.changed), qt.Equals, 0)
		})
	}
}

// TestHistoryReplayPostgresFamilyE2E_FailurePathTheReplayCannotRunTwice
// replays histories whose refused file a second attempt cannot reproduce, and
// the replay fails with the server's error rather than reporting drift; the
// pending migration does not run. On PostgreSQL the rollback of the first
// attempt keeps the serial value it took, so migration 3 refers to a row the
// replay does not hold. CockroachDB does not roll schema changes back with a
// transaction, so the refused file is not run a second time.
func TestHistoryReplayPostgresFamilyE2E_FailurePathTheReplayCannotRunTwice(t *testing.T) {
	tests := []struct {
		name    string
		engine  dbtarget.Engine
		files   map[string]string
		command postgresHistoryCommand
		// wantFailed names the migration whose replay fails, and wantErr the
		// server's code. A retry logs the first attempt's error too, so the
		// failure is matched with the replay's own prefix.
		wantFailed string
		wantErr    string
	}{
		{
			name:       "a serial value the rollback keeps, reported",
			engine:     dbtarget.PostgreSQL,
			files:      historyReplaySequence,
			command:    historyReplayDrift(),
			wantFailed: "replay the applied migrations: failed to apply migration 3",
			wantErr:    "(SQLSTATE 23503)",
		},
		{
			name:       "a serial value the rollback keeps, checked by an up run",
			engine:     dbtarget.PostgreSQL,
			files:      historyReplaySequence,
			command:    historyReplayUp(),
			wantFailed: "replay the applied migrations: failed to apply migration 3",
			wantErr:    "(SQLSTATE 23503)",
		},
		{
			name:       "a procedure that commits on CockroachDB, reported",
			engine:     dbtarget.CockroachDB,
			files:      historyReplayProcedureCommit,
			command:    historyReplayDrift(historyReplayTrustEnv),
			wantFailed: "replay the applied migrations: failed to apply migration 2",
			wantErr:    "(SQLSTATE 2D000)",
		},
		{
			name:       "a procedure that commits on CockroachDB, checked by an up run",
			engine:     dbtarget.CockroachDB,
			files:      historyReplayProcedureCommit,
			command:    historyReplayUp(historyReplayTrustFlag),
			wantFailed: "replay the applied migrations: failed to apply migration 2",
			wantErr:    "(SQLSTATE 2D000)",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			h := newEngineHistory(c, test.engine, test.files, []string{"--tx-mode", "none"})

			got := h.run(c, test.command.env, test.command.args(h)...)

			c.Assert(got.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(got.Stdout+got.Stderr, qt.Contains, test.wantFailed)
			c.Assert(got.Stdout+got.Stderr, qt.Contains, test.wantErr)
			c.Assert(h.pendingApplied(c), qt.Equals, 0)
		})
	}
}
