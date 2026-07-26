package migratecheckpoint_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/cmd/migratecheckpoint"
)

func seedMigrations(c *qt.C, dir string) {
	c.Helper()
	writePair := func(name, up, down string) {
		c.Assert(os.WriteFile(filepath.Join(dir, name+".up.sql"), []byte(up), 0o600), qt.IsNil)
		c.Assert(os.WriteFile(filepath.Join(dir, name+".down.sql"), []byte(down), 0o600), qt.IsNil)
	}
	writePair("0000000001_init", "CREATE TABLE users (id INTEGER PRIMARY KEY);\n", "DROP TABLE users;\n")
	writePair("0000000002_email", "ALTER TABLE users ADD COLUMN email TEXT;\n", "ALTER TABLE users DROP COLUMN email;\n")
}

func runCheckpoint(args ...string) (string, error) {
	cmd := migratecheckpoint.NewMigrateCheckpointCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs(args)
	err := cmd.ExecuteContext(context.Background())
	return out.String(), err
}

func TestMigrateCheckpointCommand_WritesCumulativeCheckpoint(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	seedMigrations(c, dir)
	shadow := "sqlite://" + filepath.Join(t.TempDir(), "shadow.db")

	out, err := runCheckpoint("--shadow-db", shadow, "--migrations-dir", dir, "--dialect", "sqlite")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))

	// The checkpoint version is one above the newest migration (2 -> 3), and its
	// up body is the cumulative schema (users carrying the added email column).
	up, err := os.ReadFile(filepath.Join(dir, "0000000003_checkpoint.checkpoint.up.sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(string(up), qt.Contains, "CREATE TABLE")
	c.Assert(string(up), qt.Contains, "email")
	_, err = os.Stat(filepath.Join(dir, "0000000003_checkpoint.checkpoint.down.sql"))
	c.Assert(err, qt.IsNil)

	sum, err := os.ReadFile(filepath.Join(dir, "ptah.sum"))
	c.Assert(err, qt.IsNil)
	c.Assert(string(sum), qt.Contains, "0000000003_checkpoint.checkpoint.up.sql")
}

func TestMigrateCheckpointCommand_DryRunWritesNothing(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	seedMigrations(c, dir)
	shadow := "sqlite://" + filepath.Join(t.TempDir(), "shadow.db")

	out, err := runCheckpoint("--shadow-db", shadow, "--migrations-dir", dir, "--dialect", "sqlite", "--dry-run")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "dry run")
	c.Assert(out, qt.Contains, "CREATE TABLE")

	written, err := filepath.Glob(filepath.Join(dir, "*checkpoint*"))
	c.Assert(err, qt.IsNil)
	c.Assert(written, qt.HasLen, 0)
}

func TestMigrateCheckpointCommand_RequiresShadowDB(t *testing.T) {
	c := qt.New(t)

	out, err := runCheckpoint("--migrations-dir", t.TempDir())
	c.Assert(err, qt.IsNotNil)
	c.Assert(out, qt.Contains, "shadow database URL is required")
}

func TestMigrateCheckpointCommand_RejectsVersionAtOrBelowHistory(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	seedMigrations(c, dir) // versions 1 and 2
	shadow := "sqlite://" + filepath.Join(t.TempDir(), "shadow.db")

	// A version that collides with an existing ordinary migration would poison
	// the directory (mixed checkpoint/non-checkpoint) and must be refused up
	// front, not written and then broken on the next command.
	out, err := runCheckpoint("--shadow-db", shadow, "--migrations-dir", dir, "--dialect", "sqlite", "--version", "2")
	c.Assert(err, qt.IsNotNil)
	c.Assert(out, qt.Contains, "must be above the newest existing migration version 2")

	written, err := filepath.Glob(filepath.Join(dir, "*checkpoint*"))
	c.Assert(err, qt.IsNil)
	c.Assert(written, qt.HasLen, 0)
}

func TestMigrateCheckpointCommand_RejectsNonPtahDirFormat(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	seedMigrations(c, dir)
	shadow := "sqlite://" + filepath.Join(t.TempDir(), "shadow.db")

	// Checkpoint files are ptah-format only; the atlas format would leave a
	// mixed directory with a stale integrity file, so it is refused up front.
	out, err := runCheckpoint("--shadow-db", shadow, "--migrations-dir", dir, "--dialect", "sqlite", "--dir-format", "atlas")
	c.Assert(err, qt.IsNotNil)
	c.Assert(out, qt.Contains, "supports only the")

	written, err := filepath.Glob(filepath.Join(dir, "*checkpoint*"))
	c.Assert(err, qt.IsNil)
	c.Assert(written, qt.HasLen, 0)
}

func TestMigrateCheckpointCommand_RejectsNonPositiveVersion(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	seedMigrations(c, dir)
	shadow := "sqlite://" + filepath.Join(t.TempDir(), "shadow.db")

	out, err := runCheckpoint("--shadow-db", shadow, "--migrations-dir", dir, "--dialect", "sqlite", "--version", "0")
	c.Assert(err, qt.IsNotNil)
	c.Assert(out, qt.Contains, "must be a positive version")
}
