package atlas_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/atlas"
)

// writeTxModeAllCheckedDir writes a two-migration directory whose second
// migration declares a pre-migration check, hashed because apply verifies
// atlas.sum before planning (stokaro/ptah#970).
func writeTxModeAllCheckedDir(c *qt.C, dir string) string {
	c.Helper()
	migrationsDir := filepath.Join(dir, "migrations")
	c.Assert(os.MkdirAll(migrationsDir, 0o755), qt.IsNil)
	c.Assert(os.WriteFile(
		filepath.Join(migrationsDir, "1_users.sql"),
		[]byte("CREATE TABLE users (id INTEGER PRIMARY KEY);\n"),
		0o600,
	), qt.IsNil)
	c.Assert(os.WriteFile(
		filepath.Join(migrationsDir, "2_checked.sql"),
		[]byte("-- +ptah check name=\"users_empty\" assert=\"SELECT count(*) = 0 FROM users\" on_fail=abort\nDROP TABLE users;\n"),
		0o600,
	), qt.IsNil)
	writeAtlasApplyProjectSum(c, migrationsDir)
	return migrationsDir
}

// tx-mode all refuses a checked directory because a check on the pool
// connection cannot observe the batch's uncommitted state. That rationale is
// about a transaction that actually runs, so since #1005 a dry run — which
// opens no batch transaction at all — is exempt and previews the directory
// under the ordinary deferral rule instead.
//
// Whichever branch answers, the diagnostic must never name --skip-checks:
// `ptah-compat migrate apply` registers no such flag, and Atlas has none there
// either, so suggesting it would send an operator to a flag that does not
// exist.
func TestMigrateApplyTxModeAllChecksHonorDryRun(t *testing.T) {
	tests := []struct {
		name   string
		args   []string
		assert func(c *qt.C, err error, output string)
	}{
		{
			name: "real apply refuses the checked directory",
			args: nil,
			assert: func(c *qt.C, err error, _ string) {
				c.Assert(err, qt.ErrorMatches, `error applying migrations: migration 2 declares pre-migration checks, which cannot run with tx-mode all; use the default per-file transaction mode`)
				c.Assert(err.Error(), qt.Not(qt.Contains), "--skip-checks")
			},
		},
		{
			name: "dry run previews it instead of refusing",
			args: []string{"--dry-run"},
			assert: func(c *qt.C, err error, output string) {
				c.Assert(err, qt.IsNil, qt.Commentf("command output:\n%s", output))
				c.Assert(output, qt.Not(qt.Contains), "cannot run with tx-mode all")
				c.Assert(output, qt.Not(qt.Contains), "--skip-checks")
				// The deferred guard is reported, never dropped in silence.
				c.Assert(output, qt.Contains, "Deferred pre-migration checks for 1 migration (2)")
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := t.TempDir()
			migrationsDir := writeTxModeAllCheckedDir(c, dir)

			cmd := atlas.NewCompatCommand("atlas")
			var output bytes.Buffer
			cmd.SetOut(&output)
			cmd.SetErr(&output)
			cmd.SetArgs(append([]string{
				"migrate", "apply",
				"--url", "sqlite://" + filepath.Join(dir, "tx-mode.db"),
				"--dir", "file://" + migrationsDir,
				"--tx-mode", "all",
			}, test.args...))

			test.assert(c, cmd.Execute(), output.String())
		})
	}
}
