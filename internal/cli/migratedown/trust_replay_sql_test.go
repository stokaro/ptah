package migratedown_test

import (
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/envbool/envbooltest"
	"ptah.run/internal/replaytrust"
)

// A rollback replays SQL only on a shadow database, to verify the down files
// or to derive --plan, so --trust-replay-sql typed without one would decide
// nothing. It is refused before the target is touched, as an ignored flag is.
func TestMigrateDownTrustReplaySQL_FailurePath(t *testing.T) {
	envbooltest.Unset(replaytrust.EnvVar)(t)
	c := qt.New(t)
	dir, dbPath := twoMigrationDir(c, childDownSQL)

	out, err := runDownThroughRoot("--db-url", "sqlite://"+dbPath, "--migrations-dir", dir, "--target", "1", "--trust-replay-sql", "--confirm")

	c.Assert(err, qt.ErrorMatches, `--trust-replay-sql is used only with --shadow-db: a rollback without a shadow database replays no SQL`,
		qt.Commentf("%s", out))
	c.Assert(downCurrentVersion(c, dbPath), qt.Equals, int64(2))
}

// The controls: the refusal reads what the operator typed. PTAH_TRUST_REPLAY_SQL,
// which a CI job sets for every command it runs, and a typed false leave a
// rollback without a shadow database alone, and typed trust with a shadow
// database trusts the SQL its verification replays.
func TestMigrateDownTrustReplaySQL_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name     string
		variable func(testing.TB)
		args     func(shadowDir string) []string
	}{
		{
			name:     "the variable",
			variable: envbooltest.Set(replaytrust.EnvVar, "1"),
			args:     func(string) []string { return nil },
		},
		{
			name:     "a typed false",
			variable: envbooltest.Unset(replaytrust.EnvVar),
			args:     func(string) []string { return []string{"--trust-replay-sql=false"} },
		},
		{
			name:     "typed trust with a shadow database",
			variable: envbooltest.Unset(replaytrust.EnvVar),
			args: func(shadowDir string) []string {
				return []string{"--trust-replay-sql", "--shadow-db", "sqlite://" + filepath.Join(shadowDir, "shadow.db")}
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			test.variable(t)
			c := qt.New(t)
			dir, dbPath := twoMigrationDir(c, childDownSQL)
			args := append([]string{"--db-url", "sqlite://" + dbPath, "--migrations-dir", dir, "--target", "1", "--confirm"}, test.args(c.TempDir())...)

			out, err := runDownThroughRoot(args...)

			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			c.Assert(downCurrentVersion(c, dbPath), qt.Equals, int64(1))
		})
	}
}
