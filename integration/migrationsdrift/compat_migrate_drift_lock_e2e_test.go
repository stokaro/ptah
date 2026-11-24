//go:build integration

package migrationsdrift_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/jackc/pgx/v5"

	"ptah.run/internal/clirun"
	pglock "ptah.run/internal/dblockext/postgres"
)

// The report reads the revision table under the migration lock, so a drift
// run waits for an apply that holds the lock its flags name. Each test holds
// an advisory lock on the target database from a session of its own, the lock
// a migrator takes under that name, and runs the report with a one-second
// lock timeout. The server decides whether the run waited.

// holdDriftLock takes the advisory lock a migrator takes under name on the
// database at url, until the test ends.
func holdDriftLock(c *qt.C, url, name string) {
	c.Helper()
	conn, err := pgx.Connect(c.Context(), url)
	c.Assert(err, qt.IsNil)
	_, err = conn.Exec(c.Context(), "SELECT pg_advisory_lock($1)", pglock.Key(name))
	c.Assert(err, qt.IsNil)
	// Closing the session releases a session-level advisory lock.
	c.Cleanup(func() { _ = conn.Close(context.Background()) })
}

// compatDriftLockRun runs `ptah-compat migrate drift` on url with the drift
// verb enabled and env added, and returns what it printed and its status.
func (p compatDriftProject) compatDriftLockRun(c *qt.C, url string, env []string, args ...string) clirun.Result {
	c.Helper()
	return clirun.Run(c, clirun.Compat, clirun.Options{
		Dir:     p.dir,
		Env:     append(append(os.Environ(), "PTAH_ATLAS_MIGRATE_DRIFT=1"), env...),
		Timeout: 2 * time.Minute,
	}, append([]string{"migrate", "drift", "--url", url, "--dev-url", p.shadow, "--dir", "file://migrations",
		"--lock-timeout", "1s"}, args...)...)
}

func TestCompatMigrateDriftE2E_FailurePathWaitsOnTheLockItNames(t *testing.T) {
	tests := []struct {
		name string
		held string
		env  []string
		args []string
		want string
	}{
		{
			name: "--lock-name",
			held: "drift_named",
			args: []string{"--lock-name", "drift_named"},
			want: `timed out acquiring migration lock "drift_named" for postgres after 1s`,
		},
		{
			name: "PTAH_LOCK_NAME",
			held: "drift_named",
			env:  []string{"PTAH_LOCK_NAME=drift_named"},
			want: `timed out acquiring migration lock "drift_named" for postgres after 1s`,
		},
		{
			name: "--lock-timeout on the default lock",
			held: "ptah_migrate",
			want: `timed out acquiring migration lock "ptah_migrate" for postgres after 1s`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			project := newCompatDriftProject(c, postgresDrift)
			url := project.migrated(c)
			holdDriftLock(c, url, test.held)

			got := project.compatDriftLockRun(c, url, test.env, test.args...)

			c.Assert(got.ExitCode, qt.Equals, 1, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(got.Stderr, qt.Contains, test.want)
		})
	}
}

// The control: a lock of another name does not hold the report back, so the
// test above cannot pass on a run that waits on every lock or fails for
// another reason.
func TestCompatMigrateDriftE2E_HappyPathDoesNotWaitOnAnotherName(t *testing.T) {
	c := qt.New(t)
	project := newCompatDriftProject(c, postgresDrift)
	url := project.migrated(c)
	holdDriftLock(c, url, "drift_named")

	got := project.compatDriftLockRun(c, url, nil, "--lock-name", "drift_other")

	c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
	c.Assert(got.Stdout, qt.Contains, "Pending migrations: 0")
}

// The native verb waits on the same lock, so the two surfaces name one lock
// with one name. It exits 2, its status for a report it could not make.
func TestMigrationsDriftE2E_FailurePathWaitsOnTheLockItNames(t *testing.T) {
	c := qt.New(t)
	project := newCompatDriftProject(c, postgresDrift)
	url := project.migrated(c)
	holdDriftLock(c, url, "drift_named")

	got := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir(), Timeout: 2 * time.Minute},
		"migrations", "drift", "--db-url", url, "--shadow-db", project.shadow,
		"--migrations-dir", filepath.Join(project.dir, "migrations"),
		"--migration-lock-name", "drift_named", "--migration-lock-timeout", "1s")

	c.Assert(got.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
	c.Assert(got.Stderr, qt.Contains, `timed out acquiring migration lock "drift_named" for postgres after 1s`)
}
