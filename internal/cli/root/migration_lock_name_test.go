package root_test

import (
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
)

// lockNameVerbs are the native verbs that write migrations or revision state
// under the migration advisory lock. Each one takes the one
// `--migration-lock-name` setting, so one table drives them all through the
// shipped tree, where the PTAH_* binding is installed.
var lockNameVerbs = []struct {
	name string
	args []string
}{
	{name: "up", args: []string{"migrations", "up"}},
	{name: "down", args: []string{"migrations", "down", "--confirm"}},
	{name: "baseline", args: []string{"migrations", "baseline", "--version", "1"}},
	{name: "repair", args: []string{"migrations", "repair", "--version", "1"}},
	{name: "set", args: []string{"migrations", "set", "--version", "1"}},
}

// lockNameRun is one verb run against a SQLite file that did not exist yet.
// dbPath lets a refusal be shown to have left the file uncreated.
type lockNameRun struct {
	output string
	err    error
	dbPath string
}

// runLockNameVerb runs verb against that file.
func runLockNameVerb(t *testing.T, verb []string, extra ...string) lockNameRun {
	t.Helper()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "target.db")
	args := append(slices.Clone(verb),
		"--db-url", "sqlite://"+dbPath,
		"--migrations-dir", dir,
	)
	args = append(args, extra...)
	stdout, stderr, err := executeRootCommand(args...)
	return lockNameRun{output: stdout + stderr, err: err, dbPath: dbPath}
}

func TestMigrationVerbsRefuseABlankMigrationLockName_FailurePath(t *testing.T) {
	for _, verb := range lockNameVerbs {
		t.Run(verb.name, func(t *testing.T) {
			for _, spelling := range []struct {
				name    string
				env     string
				extra   []string
				wantErr string
			}{
				{
					name:    "flag empty",
					extra:   []string{"--migration-lock-name", ""},
					wantErr: `--migration-lock-name must name the migration lock: .*`,
				},
				{
					name:    "flag whitespace",
					extra:   []string{"--migration-lock-name", " \t"},
					wantErr: `--migration-lock-name must name the migration lock: .*`,
				},
				{
					name:    "environment whitespace",
					env:     "   ",
					wantErr: `PTAH_MIGRATION_LOCK_NAME must name the migration lock: .*`,
				},
			} {
				t.Run(spelling.name, func(t *testing.T) {
					c := qt.New(t)
					t.Setenv("PTAH_MIGRATION_LOCK_NAME", spelling.env)

					got := runLockNameVerb(t, verb.args, spelling.extra...)

					c.Assert(got.err, qt.ErrorMatches, spelling.wantErr, qt.Commentf("%s", got.output))
					_, statErr := os.Stat(got.dbPath)
					c.Assert(statErr, qt.ErrorIs, fs.ErrNotExist)
				})
			}
		})
	}
}

func TestMigrationVerbsRefuseAMigrationLockNameOnAnUnlockedDialect_FailurePath(t *testing.T) {
	for _, verb := range lockNameVerbs {
		t.Run(verb.name, func(t *testing.T) {
			for _, spelling := range []struct {
				name  string
				env   string
				extra []string
				want  string
			}{
				{name: "flag", extra: []string{"--migration-lock-name", "svc_a"}, want: "--migration-lock-name"},
				{name: "environment", env: "svc_a", want: "PTAH_MIGRATION_LOCK_NAME"},
			} {
				t.Run(spelling.name, func(t *testing.T) {
					c := qt.New(t)
					t.Setenv("PTAH_MIGRATION_LOCK_NAME", spelling.env)

					got := runLockNameVerb(t, verb.args, spelling.extra...)

					c.Assert(got.err, qt.ErrorMatches, regexp.QuoteMeta(
						spelling.want+` requested the migration advisory lock, and dialect "sqlite" has none: `+
							`only postgres, mysql, mariadb, sqlserver, yugabytedb take a session advisory lock. `+
							`Remove `+spelling.want+` to run without a lock`), qt.Commentf("%s", got.output))
					_, statErr := os.Stat(got.dbPath)
					c.Assert(statErr, qt.ErrorIs, fs.ErrNotExist)
				})
			}
		})
	}
}

// TestMigrationVerbsHelpShowsTheMigrationLockNameBinding_HappyPath reads the
// help each verb prints, where the environment variable is advertised.
func TestMigrationVerbsHelpShowsTheMigrationLockNameBinding_HappyPath(t *testing.T) {
	for _, verb := range lockNameVerbs {
		t.Run(verb.name, func(t *testing.T) {
			c := qt.New(t)

			out := runRootCommandOutput(c, "migrations", verb.args[1], "--help")

			c.Assert(out, qt.Contains, "--migration-lock-name string")
			c.Assert(out, qt.Contains, "[env: PTAH_MIGRATION_LOCK_NAME]")
		})
	}
}
