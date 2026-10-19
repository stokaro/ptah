package migrator

import (
	"fmt"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	pglock "ptah.run/internal/dblockext/postgres"
)

func TestIsMigrationLockTimeout(t *testing.T) {
	c := qt.New(t)

	err := fmt.Errorf("wrapped: %w", &MigrationLockTimeoutError{
		Dialect: "postgres",
		Name:    migrationAdvisoryLockName,
		Timeout: 250 * time.Millisecond,
	})

	c.Assert(IsMigrationLockTimeout(err), qt.IsTrue)
	c.Assert(IsMigrationLockTimeout(fmt.Errorf("other error")), qt.IsFalse)
}

// postgresMigrationLockKey is the PostgreSQL advisory lock key a migration run
// takes under name.
func postgresMigrationLockKey(name string) int64 {
	return pglock.Key(normalizeMigrationLockName(name))
}

func TestPostgresMigrationLockKeyStable(t *testing.T) {
	c := qt.New(t)

	c.Assert(postgresMigrationLockKey(""), qt.Equals, int64(2705505214))
	c.Assert(postgresMigrationLockKey(migrationAdvisoryLockName), qt.Equals, int64(2705505214))
	c.Assert(postgresMigrationLockKey("custom-lock"), qt.Not(qt.Equals), int64(2705505214))
	c.Assert(postgresMigrationLockKey(" custom-lock "), qt.Equals, postgresMigrationLockKey("custom-lock"))
}

func TestWithMigrationLockName(t *testing.T) {
	c := qt.New(t)

	base := NewMigrator(nil, nil)
	c.Assert(base.effectiveMigrationLockName(), qt.Equals, migrationAdvisoryLockName)

	custom := base.WithMigrationLockName("custom-lock")
	c.Assert(custom.effectiveMigrationLockName(), qt.Equals, "custom-lock")
	c.Assert(base.effectiveMigrationLockName(), qt.Equals, migrationAdvisoryLockName)

	blank := custom.WithMigrationLockName(" ")
	c.Assert(blank.effectiveMigrationLockName(), qt.Equals, migrationAdvisoryLockName)
}
