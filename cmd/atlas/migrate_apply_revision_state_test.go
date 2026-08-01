package atlas_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/cmd/atlas"
)

func TestMigrateApplyDryRunTxModeAllDiagnosticDoesNotSuggestUnsupportedFlag(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
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

	cmd := atlas.NewCompatCommand("atlas")
	var output bytes.Buffer
	cmd.SetOut(&output)
	cmd.SetErr(&output)
	cmd.SetArgs([]string{
		"migrate", "apply",
		"--url", "sqlite://" + filepath.Join(dir, "tx-mode.db"),
		"--dir", "file://" + migrationsDir,
		"--dry-run",
		"--tx-mode", "all",
	})

	err := cmd.Execute()
	c.Assert(err, qt.ErrorMatches, `error applying migrations: migration 2 declares pre-migration checks, which cannot run with tx-mode all; use the default per-file transaction mode`)
	c.Assert(err.Error(), qt.Not(qt.Contains), "--skip-checks")
}
