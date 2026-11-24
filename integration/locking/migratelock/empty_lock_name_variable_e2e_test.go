//go:build integration

package migratelock_test

import (
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/clirun"
	"ptah.run/internal/dblock"
)

// An empty lock name variable is refused rather than read as unset. Read as
// unset, it selects the default lock, which a runner holding the lock the
// variable was meant to name does not hold: the run goes ahead beside it, and
// two runners of one history race the same revision table. Each test holds
// the named lock from a session of its own and reads the target back.

// TestMigrationsUpRefusesAnEmptyLockNameVariableE2E_FailurePath runs `ptah
// migrations up` with PTAH_MIGRATION_LOCK_NAME set but empty while another
// session holds history A's lock. The run refuses before it connects, so the
// schema stays empty. Read as unset, the variable would take the free default
// lock and apply the history.
func TestMigrationsUpRefusesAnEmptyLockNameVariableE2E_FailurePath(t *testing.T) {
	c := qt.New(t)
	target := newLockNameTarget(c)
	holdLock(c, target, lockNameA)

	got := runPtah(c, cleanEnv("PTAH_MIGRATION_LOCK_NAME="),
		target.verb(lockNameHistoryA, "up", "--migration-lock-timeout", heldTimeout)...)

	c.Assert(got.ExitCode, qt.Equals, 2, described(got))
	c.Assert(got.Stderr, qt.Contains, "PTAH_MIGRATION_LOCK_NAME is set but empty")
	c.Assert(target.tablesIn(c, lockNameHistoryA), qt.HasLen, 0)
}

// TestMigrationsUpReadsTheLockNameVariableE2E_FailurePath is the control for
// the refusal: a value in PTAH_MIGRATION_LOCK_NAME selects the lock it names,
// so the run waits on the held lock and gives up when its timeout ends.
func TestMigrationsUpReadsTheLockNameVariableE2E_FailurePath(t *testing.T) {
	c := qt.New(t)
	target := newLockNameTarget(c)
	holdLock(c, target, lockNameA)

	got := runPtah(c, cleanEnv("PTAH_MIGRATION_LOCK_NAME="+lockNameA),
		target.verb(lockNameHistoryA, "up", "--migration-lock-timeout", heldTimeout)...)

	c.Assert(got.ExitCode, qt.Equals, 2, described(got))
	c.Assert(got.Stderr, qt.Contains, `timed out acquiring migration lock "`+lockNameA+`" for postgres after 1s`)
	c.Assert(target.tablesIn(c, lockNameHistoryA), qt.HasLen, 0)
}

// compatHeldLockName is the lock a second session holds in the compat tests,
// and the one PTAH_LOCK_NAME is meant to name.
const compatHeldLockName = "team_lock"

// TestCompatMigrateApplyEmptyLockNameVariableE2E_FailurePath runs `ptah-compat migrate
// apply` under a held lock, with PTAH_LOCK_NAME empty and with the name of the
// held lock. The empty value refuses before the run connects; the name waits
// on the held lock. Neither applies the migration.
func TestCompatMigrateApplyEmptyLockNameVariableE2E_FailurePath(t *testing.T) {
	tests := []struct {
		name     string
		value    string
		wantExit int
		wantErr  string
	}{
		{
			name:     "empty",
			value:    "",
			wantExit: 1,
			wantErr:  "PTAH_LOCK_NAME is set but empty",
		},
		{
			name:     "the held lock",
			value:    compatHeldLockName,
			wantExit: 1,
			wantErr:  `timed out acquiring migration lock "` + compatHeldLockName + `" for postgres after 1s`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target := scratchPostgresDatabase(c)
			dir := writeLockProbeDirectory(c)
			holder, err := dbschema.ConnectToDatabase(c.Context(), target)
			c.Assert(err, qt.IsNil)
			defer dbschema.CloseAndWarn(holder)
			held, err := dblock.Acquire(c.Context(), holder, compatHeldLockName, 5*time.Second)
			c.Assert(err, qt.IsNil)
			defer func() { c.Check(held.Release(c.Context()), qt.IsNil) }()

			got := clirun.Run(c, clirun.Compat, clirun.Options{Dir: c.TempDir(), Env: cleanEnv("PTAH_LOCK_NAME=" + test.value)},
				"migrate", "apply", "--url", target, "--dir", "file://"+dir, "--lock-timeout", heldTimeout)

			c.Assert(got.ExitCode, qt.Equals, test.wantExit, described(got))
			c.Assert(got.Stderr, qt.Contains, test.wantErr)
			c.Assert(lockProbeTableCount(c, target), qt.Equals, 0)
		})
	}
}
