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
			// No lock timeout, because a migration that opted out of the
			// transaction refuses one -- which is why the mode does not
			// require it for a directory made of them.
			name: "the concurrent form applies",
			up: "-- +ptah no_transaction\n" +
				"CREATE INDEX CONCURRENTLY idx_online_notes_body ON ptah_online_notes (body);\n",
			wantExit: 0,
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
func TestMigrationsUpOnlineModeRequiresALockTimeoutE2E(t *testing.T) {
	c := qt.New(t)
	workDir, scopedURL := newOnlineModeProject(c, t,
		"ALTER TABLE ptah_online_notes ADD COLUMN title text;\n",
		"ALTER TABLE ptah_online_notes DROP COLUMN title;\n")

	applied := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: workDir},
		"migrations", "up",
		"--migrations-dir", filepath.Join(workDir, "migrations"),
		"--db-url", scopedURL)

	c.Assert(applied.ExitCode, qt.Not(qt.Equals), 0,
		qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
	c.Assert(applied.Stdout+applied.Stderr, qt.Contains, "online mode requires a lock timeout")
}

// The control: the same migration applies once a lock timeout is set, so the
// refusal above is about the timeout and not about the statement.
func TestMigrationsUpOnlineModeAppliesWithALockTimeoutE2E(t *testing.T) {
	c := qt.New(t)
	workDir, scopedURL := newOnlineModeProject(c, t,
		"ALTER TABLE ptah_online_notes ADD COLUMN title text;\n",
		"ALTER TABLE ptah_online_notes DROP COLUMN title;\n")

	applied := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: workDir},
		"migrations", "up",
		"--migrations-dir", filepath.Join(workDir, "migrations"),
		"--db-url", scopedURL,
		"--lock-timeout", "3s")

	c.Assert(applied.ExitCode, qt.Equals, 0,
		qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
}

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
