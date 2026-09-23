//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
)

// The online mode is a promise about what an apply does to a live database, so
// what it refuses is measured against one: a blocking index build stops the
// apply and the concurrent form does not (stokaro/ptah#3422).
func TestMigrationsUpOnlineModeRefusesWhatItCannotProveE2E(t *testing.T) {
	tests := []struct {
		name      string
		up        string
		extraArgs []string
		wantExit  int
	}{
		{
			name:      "a blocking index build is refused",
			up:        "CREATE INDEX idx_online_notes_body ON ptah_online_notes (body);\n",
			extraArgs: []string{"--lock-timeout", "3s"},
			wantExit:  2,
		},
		{
			name: "the concurrent form applies",
			up: "-- +ptah no_transaction\n" +
				"CREATE INDEX CONCURRENTLY idx_online_notes_body ON ptah_online_notes (body);\n",
			extraArgs: []string{"--lock-timeout", "3s"},
			wantExit:  0,
		},
	}
	// Both rows share a rollback that the mode proves, so what varies between
	// them is the statement under test and not the file beside it.
	const down = "-- +ptah no_transaction\nDROP INDEX CONCURRENTLY idx_online_notes_body;\n"

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			workDir, scopedURL := newOnlineModeProject(c, t, test.up, down)

			applied := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: workDir},
				append([]string{
					"migrations", "up",
					"--migrations-dir", filepath.Join(workDir, "migrations"),
					"--db-url", scopedURL,
				}, test.extraArgs...)...)

			c.Assert(applied.ExitCode, qt.Equals, test.wantExit,
				qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
		})
	}
}

// The requirement no reading of the SQL can carry: an ALTER that takes ACCESS
// EXCLUSIVE for an instant still queues behind a conflicting lock, and every
// later reader and writer of that table queues behind it. The mode refuses to
// run without a lock timeout rather than calling such a statement online.
//
// The transaction mode does not lift it. `ADD CONSTRAINT ... NOT VALID` takes
// ACCESS EXCLUSIVE in the file the planner marks no_transaction, and
// `--tx-mode none` puts every file outside a transaction, so either one would
// otherwise apply an ALTER that waits with nothing to stop it
// (stokaro/ptah#3501).
func TestMigrationsUpOnlineModeRequiresALockTimeoutE2E(t *testing.T) {
	tests := []struct {
		name      string
		up        string
		down      string
		extraArgs []string
	}{
		{
			name: "a transactional migration",
			up:   "ALTER TABLE ptah_online_notes ADD COLUMN title text;\n",
			down: "ALTER TABLE ptah_online_notes DROP COLUMN title;\n",
		},
		{
			name:      "every migration run outside a transaction",
			up:        "ALTER TABLE ptah_online_notes ADD COLUMN title text;\n",
			down:      "ALTER TABLE ptah_online_notes DROP COLUMN title;\n",
			extraArgs: []string{"--tx-mode", "none"},
		},
		{
			name: "a migration marked no_transaction",
			up:   onlineConstraintUp,
			down: onlineConstraintDown,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			workDir, scopedURL := newOnlineModeProject(c, t, test.up, test.down)

			applied := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: workDir},
				append([]string{
					"migrations", "up",
					"--migrations-dir", filepath.Join(workDir, "migrations"),
					"--db-url", scopedURL,
				}, test.extraArgs...)...)

			c.Assert(applied.ExitCode, qt.Equals, 2,
				qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
			c.Assert(applied.Stdout+applied.Stderr, qt.Contains, "online mode requires a lock timeout")
		})
	}
}

// The control: the same migrations apply once a lock timeout is set, so the
// refusal above is about the timeout and not about the statement. The
// no_transaction row needs the migrator to carry the timeout on the session
// that runs the file: without that, the mode requires a timeout the migrator
// refuses, and the generated constraint form has no invocation that applies it
// (stokaro/ptah#3501).
func TestMigrationsUpOnlineModeAppliesWithALockTimeoutE2E(t *testing.T) {
	tests := []struct {
		name string
		up   string
		down string
	}{
		{
			name: "a transactional migration",
			up:   "ALTER TABLE ptah_online_notes ADD COLUMN title text;\n",
			down: "ALTER TABLE ptah_online_notes DROP COLUMN title;\n",
		},
		{
			name: "a migration marked no_transaction",
			up:   onlineConstraintUp,
			down: onlineConstraintDown,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			workDir, scopedURL := newOnlineModeProject(c, t, test.up, test.down)

			applied := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: workDir},
				"migrations", "up",
				"--migrations-dir", filepath.Join(workDir, "migrations"),
				"--db-url", scopedURL,
				"--lock-timeout", "3s")

			c.Assert(applied.ExitCode, qt.Equals, 0,
				qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
		})
	}
}

// What the timeout is for, measured where it matters: another transaction
// holds a lock the constraint's ALTER conflicts with, and the migration marked
// no_transaction gives up after its lock timeout instead of queueing -- with
// every later reader of the table queued behind it -- for as long as the other
// transaction lasts.
func TestMigrationsUpOnlineModeGivesUpOnItsLockTimeoutE2E(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	workDir, scopedURL := newOnlineModeProject(c, t, onlineConstraintUp, onlineConstraintDown)

	scoped, err := sql.Open("pgx", scopedURL)
	c.Assert(err, qt.IsNil)
	defer scoped.Close()
	holder, err := scoped.BeginTx(ctx, nil)
	c.Assert(err, qt.IsNil)
	defer func() { _ = holder.Rollback() }()
	_, err = holder.ExecContext(ctx, "SELECT count(*) FROM ptah_online_notes")
	c.Assert(err, qt.IsNil)

	start := time.Now()
	applied := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: workDir, Timeout: time.Minute},
		"migrations", "up",
		"--migrations-dir", filepath.Join(workDir, "migrations"),
		"--db-url", scopedURL,
		"--lock-timeout", "1s")
	elapsed := time.Since(start)

	c.Assert(applied.ExitCode, qt.Equals, 2,
		qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
	c.Assert(applied.Stdout+applied.Stderr, qt.Contains, "canceling statement due to lock timeout")
	c.Assert(elapsed < 30*time.Second, qt.IsTrue, qt.Commentf("the run took %s", elapsed))
	c.Assert(holder.Rollback(), qt.IsNil)

	var constraints int
	c.Assert(scoped.QueryRowContext(ctx,
		"SELECT count(*) FROM pg_constraint WHERE conname = 'ptah_online_notes_body_present'",
	).Scan(&constraints), qt.IsNil)
	c.Assert(constraints, qt.Equals, 0)
}

// onlineConstraintUp is the shape `diff.online_alter` generates for a new
// CHECK constraint: added without a scan, then validated under the weaker
// lock, each statement committed on its own.
const onlineConstraintUp = "-- +ptah no_transaction\n" +
	"ALTER TABLE ptah_online_notes ADD CONSTRAINT ptah_online_notes_body_present " +
	"CHECK (body IS NOT NULL) NOT VALID;\n" +
	"ALTER TABLE ptah_online_notes VALIDATE CONSTRAINT ptah_online_notes_body_present;\n"

const onlineConstraintDown = "-- +ptah no_transaction\n" +
	"ALTER TABLE ptah_online_notes DROP CONSTRAINT ptah_online_notes_body_present;\n"

// newOnlineModeProject creates a scoped database with the table the migrations
// below change, and a migrations directory carrying upSQL and a policy that
// selects the mode.
func newOnlineModeProject(c *qt.C, t *testing.T, upSQL, downSQL string) (workDir, scopedURL string) {
	c.Helper()
	ctx := context.Background()
	dbURL := dbtarget.URL(c, dbtarget.PostgreSQL)
	adminDB, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() { adminDB.Close() })
	databaseName := fmt.Sprintf("ptah_online_mode_e2e_%d", time.Now().UnixNano())
	createE2EDatabase(c, ctx, adminDB, databaseName)
	t.Cleanup(func() { dropE2EDatabase(c, context.Background(), adminDB, databaseName) })
	scopedURL = replaceDatabaseName(c, dbURL, databaseName)

	scoped, err := sql.Open("pgx", scopedURL)
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() { scoped.Close() })
	_, err = scoped.ExecContext(ctx, "CREATE TABLE ptah_online_notes (id bigint PRIMARY KEY, body text)")
	c.Assert(err, qt.IsNil)

	workDir = c.TempDir()
	migrations := filepath.Join(workDir, "migrations")
	c.Assert(os.MkdirAll(migrations, 0o750), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(migrations, "0000000001_change.up.sql"),
		[]byte(upSQL), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(migrations, "0000000001_change.down.sql"),
		[]byte(downSQL), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(migrations, ".ptah-lint.yaml"),
		[]byte("dialect: postgres\nonline: require\n"), 0o600), qt.IsNil)
	return workDir, scopedURL
}
