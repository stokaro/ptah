package migrateup_test

import (
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

// `--migration-lock-name` selects which advisory lock a run waits for. An empty
// or whitespace-only name would fall back to the default in the migrator, so a
// caller who meant to separate two histories would share a lock without being
// told. A name on a dialect with no advisory lock selects nothing. Both are
// refused before the target is opened, in whichever spelling carried them.

// lockNameCase is one run of `migrations up` against a SQLite file that a
// refusal must leave uncreated. Every row sets the same three inputs, so a row
// that leaves one blank asks nothing through it: an empty PTAH_* variable is
// unset, and a config file without the key carries none.
type lockNameCase struct {
	name   string
	flag   []string
	env    string
	config string
}

// lockNameRun is what one lockNameCase produced.
type lockNameRun struct {
	out    string
	err    error
	dbPath string
}

func (l lockNameCase) run(t *testing.T) lockNameRun {
	t.Helper()
	c := qt.New(t)
	work := t.TempDir()
	dbPath := filepath.Join(work, "target.db")
	configPath := filepath.Join(work, "ptah.yaml")
	c.Assert(os.WriteFile(configPath, []byte("migration:\n"+l.config), 0o600), qt.IsNil)
	t.Setenv("PTAH_MIGRATION_LOCK_NAME", l.env)
	t.Setenv("PTAH_MIGRATION_LOCK_TIMEOUT", "")

	args := append([]string{
		"--config", configPath,
		"--db-url", "sqlite://" + dbPath,
		"--migrations-dir", writeUpMigrations(t),
	}, l.flag...)
	out, err := runUpThroughRoot(args...)
	return lockNameRun{out: out, err: err, dbPath: dbPath}
}

func TestMigrateUpRefusesBlankMigrationLockName_FailurePath(t *testing.T) {
	tests := []struct {
		lockNameCase
		wantErr string
	}{
		{
			lockNameCase: lockNameCase{name: "flag empty", flag: []string{"--migration-lock-name", ""}},
			wantErr:      `--migration-lock-name must name the migration lock: .*`,
		},
		{
			lockNameCase: lockNameCase{name: "flag whitespace", flag: []string{"--migration-lock-name", "  "}},
			wantErr:      `--migration-lock-name must name the migration lock: .*`,
		},
		{
			lockNameCase: lockNameCase{name: "environment whitespace", env: "  "},
			wantErr:      `PTAH_MIGRATION_LOCK_NAME must name the migration lock: .*`,
		},
		{
			lockNameCase: lockNameCase{name: "config empty", config: "  migration_lock_name: \"\"\n"},
			wantErr:      `migration\.migration_lock_name must name the migration lock: .*`,
		},
		{
			lockNameCase: lockNameCase{name: "config whitespace", config: "  migration_lock_name: \"   \"\n"},
			wantErr:      `migration\.migration_lock_name must name the migration lock: .*`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := test.run(t)

			c.Assert(got.err, qt.ErrorMatches, test.wantErr, qt.Commentf("%s", got.out))
			_, statErr := os.Stat(got.dbPath)
			c.Assert(statErr, qt.ErrorIs, fs.ErrNotExist)
		})
	}
}

func TestMigrateUpRefusesMigrationLockNameOnUnlockedDialect_FailurePath(t *testing.T) {
	tests := []struct {
		lockNameCase
		wantSpelling string
	}{
		{
			lockNameCase: lockNameCase{name: "flag", flag: []string{"--migration-lock-name", "svc_a"}},
			wantSpelling: "--migration-lock-name",
		},
		{
			lockNameCase: lockNameCase{name: "environment", env: "svc_a"},
			wantSpelling: "PTAH_MIGRATION_LOCK_NAME",
		},
		{
			lockNameCase: lockNameCase{name: "config", config: "  migration_lock_name: svc_a\n"},
			wantSpelling: "migration.migration_lock_name",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := test.run(t)

			c.Assert(got.err, qt.ErrorMatches, fmtMigrationLockRefusal(test.wantSpelling, "sqlite"), qt.Commentf("%s", got.out))
			_, statErr := os.Stat(got.dbPath)
			c.Assert(statErr, qt.ErrorIs, fs.ErrNotExist)
		})
	}
}

// TestMigrateUpWithoutMigrationLockNameStillApplies is the control: a dialect
// that cannot lock keeps applying when nobody chose a name, and an empty
// PTAH_MIGRATION_LOCK_NAME is no choice.
func TestMigrateUpWithoutMigrationLockNameStillApplies_HappyPath(t *testing.T) {
	c := qt.New(t)

	got := lockNameCase{name: "unset"}.run(t)

	c.Assert(got.err, qt.IsNil, qt.Commentf("%s", got.out))
	c.Assert(got.out, qt.Contains, "Migrations completed successfully")
	c.Assert(got.out, qt.Not(qt.Contains), "Migration lock:")
	c.Assert(queryCurrentVersion(c, got.dbPath), qt.Equals, int64(2))
}

// TestMigrateUpHelpShowsTheMigrationLockNameBinding reads the help the shipped
// tree prints, where the environment binding is installed.
func TestMigrateUpHelpShowsTheMigrationLockNameBinding_HappyPath(t *testing.T) {
	c := qt.New(t)

	out, err := runUpThroughRoot("--help")

	c.Assert(err, qt.IsNil)
	c.Assert(out, qt.Contains, "--migration-lock-name string")
	c.Assert(out, qt.Contains, "[env: PTAH_MIGRATION_LOCK_NAME]")
}
