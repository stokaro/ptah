package atlas_test

import (
	"bytes"
	"path/filepath"
	"regexp"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/cli/atlas/internal/atlastest"
	"ptah.run/internal/cli/migratedown"
)

// `ptah migrations down --migration-lock-timeout` refuses a dialect with no
// session advisory lock (stokaro/ptah#3417). `ptah-compat migrate down
// --lock-timeout` reaches that same native command through the forwarding
// adapter, and must not inherit the refusal: the compatibility surface answers
// to the Atlas contract, and what the Atlas CLI does with a lock timeout on a
// dialect that cannot lock is not measured in this repository.
//
// The pair is what makes the scope measurable. The compat run proves the
// refusal stops at the boundary; the native run on the same target proves the
// refusal exists, so a compat run that passes because the rule was dropped
// outright is told apart from one that passes because the rule is scoped.

// txtarWithDownSection is one Atlas migration carrying the down body a rollback
// needs, so the compat run reaches the rollback rather than a missing-body
// refusal that would pass whatever the lock decision did.
const txtarWithDownSection = `-- atlas:txtar

-- migration.sql --
CREATE TABLE lock_scope_users (id INTEGER PRIMARY KEY);
-- down.sql --
DROP TABLE lock_scope_users;
`

func TestCompatMigrateDownKeepsLockTimeoutOnUnlockedDialect(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "compat-down.db")
	migrationsDir := filepath.Join(dir, "migrations")
	atlastest.WriteHashedAtlasDir(c, migrationsDir, "20260101000001_init.sql", txtarWithDownSection)

	applied, applyErr := runCompatLockCommand([]string{
		"migrate", "apply",
		"--url", "sqlite://" + dbPath,
		"--dir", "file://" + migrationsDir,
	})
	c.Assert(applyErr, qt.IsNil, qt.Commentf("%s", applied))
	c.Assert(atlastest.SqliteTableCount(c, dbPath, "lock_scope_users"), qt.Equals, 1)

	out, err := runCompatLockCommand([]string{
		"migrate", "down",
		"--url", "sqlite://" + dbPath,
		"--dir", "file://" + migrationsDir,
		"--to-version", "0",
		"--lock-timeout", "10s",
	})

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Not(qt.Contains), "requested the migration advisory lock")
	c.Assert(atlastest.SqliteTableCount(c, dbPath, "lock_scope_users"), qt.Equals, 0)
}

// TestNativeMigrateDownRefusesMigrationLockTimeoutOnUnlockedDialect is the
// control for the scope above, on the command the adapter forwards to.
func TestNativeMigrateDownRefusesMigrationLockTimeoutOnUnlockedDialect(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "native-down.db")
	migrationsDir := filepath.Join(dir, "migrations")
	atlastest.WriteHashedAtlasDir(c, migrationsDir, "20260101000001_init.sql", txtarWithDownSection)

	applied, applyErr := runCompatLockCommand([]string{
		"migrate", "apply",
		"--url", "sqlite://" + dbPath,
		"--dir", "file://" + migrationsDir,
	})
	c.Assert(applyErr, qt.IsNil, qt.Commentf("%s", applied))

	cmd := migratedown.NewMigrateDownCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"--db-url", "sqlite://" + dbPath,
		"--migrations-dir", migrationsDir,
		"--dir-format", "atlas",
		"--revision-format", "atlas",
		"--target", "0",
		"--confirm",
		"--migration-lock-timeout", "10s",
	})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, regexp.QuoteMeta(
		`--migration-lock-timeout requested the migration advisory lock, and dialect "sqlite" has none`)+`.*`,
		qt.Commentf("%s", out.String()))
	c.Assert(atlastest.SqliteTableCount(c, dbPath, "lock_scope_users"), qt.Equals, 1)
}
