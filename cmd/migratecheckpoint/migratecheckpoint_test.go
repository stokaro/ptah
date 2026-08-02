package migratecheckpoint_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
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

// seedAtlasMigrations fills dir with an Atlas-format history (single files,
// timestamp versions) so the atlas branch is exercised on a directory Atlas
// itself would produce, not on a ptah pair that the Atlas parser merely
// tolerates.
func seedAtlasMigrations(c *qt.C, dir string) {
	c.Helper()
	c.Assert(os.WriteFile(filepath.Join(dir, "20250801000001_init.sql"),
		[]byte("CREATE TABLE users (id INTEGER PRIMARY KEY);\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "20250801000002_email.sql"),
		[]byte("ALTER TABLE users ADD COLUMN email TEXT;\n"), 0o600), qt.IsNil)
}

func TestMigrateCheckpointCommand_RejectsAutoDirFormat(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	seedMigrations(c, dir)
	shadow := "sqlite://" + filepath.Join(t.TempDir(), "shadow.db")

	// "auto" is a read-side probe. Writing under it would have to guess both the
	// file convention and which integrity file to refresh, so it is refused up
	// front rather than leaving a mixed directory with a stale sum.
	out, err := runCheckpoint("--shadow-db", shadow, "--migrations-dir", dir, "--dialect", "sqlite", "--dir-format", "auto")
	c.Assert(err, qt.IsNotNil)
	c.Assert(out, qt.Contains, "cannot write under --dir-format=auto")

	written, err := filepath.Glob(filepath.Join(dir, "*checkpoint*"))
	c.Assert(err, qt.IsNil)
	c.Assert(written, qt.HasLen, 0)
}

func TestMigrateCheckpointCommand_AtlasDirFormatWritesSingleDirectiveFile(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	seedAtlasMigrations(c, dir)
	shadow := "sqlite://" + filepath.Join(t.TempDir(), "shadow.db")

	out, err := runCheckpoint("--shadow-db", shadow, "--migrations-dir", dir, "--dialect", "sqlite",
		"--dir-format", "atlas", "--description", "snapshot")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))

	written, err := filepath.Glob(filepath.Join(dir, "*_snapshot.sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(written, qt.HasLen, 1)

	body, err := os.ReadFile(written[0])
	c.Assert(err, qt.IsNil)
	// The directive is honored only on the first line, so assert the position,
	// not merely that the text occurs somewhere in the file.
	firstLine, _, _ := strings.Cut(string(body), "\n")
	c.Assert(firstLine, qt.Equals, "-- atlas:checkpoint")
	c.Assert(string(body), qt.Contains, "email")

	// Atlas format is up-only and integrity-tracked by atlas.sum. A down file or
	// a ptah.sum here would be a directory neither Atlas nor Ptah reads back the
	// way this checkpoint intends.
	sum, err := os.ReadFile(filepath.Join(dir, "atlas.sum"))
	c.Assert(err, qt.IsNil)
	c.Assert(string(sum), qt.Contains, filepath.Base(written[0]))
	_, err = os.Stat(filepath.Join(dir, "ptah.sum"))
	c.Assert(os.IsNotExist(err), qt.IsTrue)
	downs, err := filepath.Glob(filepath.Join(dir, "*.down.sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(downs, qt.HasLen, 0)
}

func TestMigrateCheckpointCommand_AtlasRejectsPtahWidthVersion(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	seedAtlasMigrations(c, dir)
	shadow := "sqlite://" + filepath.Join(t.TempDir(), "shadow.db")

	// A 10-digit Atlas version is indistinguishable from a ptah file name, so
	// format auto-detection skips the file and the checkpoint silently stops
	// applying. 9999999999 is above the seeded history, so only the width rule
	// can reject it — a shorter or longer value in the same position is accepted.
	out, err := runCheckpoint("--shadow-db", shadow, "--migrations-dir", dir, "--dialect", "sqlite",
		"--dir-format", "atlas", "--version", "9999999999")
	c.Assert(err, qt.IsNotNil)
	c.Assert(out, qt.Contains, "indistinguishable from a ptah migration name")

	written, err := filepath.Glob(filepath.Join(dir, "*checkpoint*"))
	c.Assert(err, qt.IsNil)
	c.Assert(written, qt.HasLen, 0)
}

func TestMigrateCheckpointCommand_AtlasAcceptsVersionAbovePtahMaximum(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	seedAtlasMigrations(c, dir)
	shadow := "sqlite://" + filepath.Join(t.TempDir(), "shadow.db")

	// The ptah 10-digit ceiling must not apply to Atlas names: this version is
	// far above maxMigrationVersion and is exactly the shape Atlas timestamps
	// produce. It is the case that separates a format-aware ceiling from the
	// old unconditional one.
	out, err := runCheckpoint("--shadow-db", shadow, "--migrations-dir", dir, "--dialect", "sqlite",
		"--dir-format", "atlas", "--version", "20260801100335")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))

	_, err = os.Stat(filepath.Join(dir, "20260801100335_checkpoint.sql"))
	c.Assert(err, qt.IsNil)
}

func TestMigrateCheckpointCommand_PtahKeepsTenDigitCeiling(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	seedMigrations(c, dir)
	shadow := "sqlite://" + filepath.Join(t.TempDir(), "shadow.db")

	// Same value the atlas branch accepts, refused here: the ptah file name has
	// no room for it. This pins that widening the ceiling for Atlas did not
	// widen it for ptah.
	out, err := runCheckpoint("--shadow-db", shadow, "--migrations-dir", dir, "--dialect", "sqlite",
		"--version", "20260801100335")
	c.Assert(err, qt.IsNotNil)
	c.Assert(out, qt.Contains, "exceeds the maximum migration version")

	written, err := filepath.Glob(filepath.Join(dir, "*checkpoint*"))
	c.Assert(err, qt.IsNil)
	c.Assert(written, qt.HasLen, 0)
}

func TestMigrateCheckpointCommand_RefusesToAddASecondIntegrityFile(t *testing.T) {
	tests := []struct {
		name       string
		dirFormat  string
		seed       func(*qt.C, string)
		foreignSum string
	}{
		{
			// The case the compat default (atlas) newly makes reachable: an
			// existing ptah directory checkpointed under the atlas convention.
			name:       "atlas checkpoint into a ptah.sum directory",
			dirFormat:  "atlas",
			seed:       seedMigrations,
			foreignSum: "ptah.sum",
		},
		{
			name:       "ptah checkpoint into an atlas.sum directory",
			dirFormat:  "ptah",
			seed:       seedAtlasMigrations,
			foreignSum: "atlas.sum",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			dir := t.TempDir()
			tt.seed(c, dir)
			c.Assert(os.WriteFile(filepath.Join(dir, tt.foreignSum), []byte("h1:seeded\n"), 0o600), qt.IsNil)
			shadow := "sqlite://" + filepath.Join(t.TempDir(), "shadow.db")

			out, err := runCheckpoint("--shadow-db", shadow, "--migrations-dir", dir, "--dialect", "sqlite",
				"--dir-format", tt.dirFormat)
			c.Assert(err, qt.IsNotNil)
			c.Assert(out, qt.Contains, "it would leave both")

			// Assert the protected state: no checkpoint, and above all no second
			// integrity file — a directory carrying both is one `--dir-format
			// auto` read away from failing, long after this command exited.
			written, globErr := filepath.Glob(filepath.Join(dir, "*checkpoint*"))
			c.Assert(globErr, qt.IsNil)
			c.Assert(written, qt.HasLen, 0)
			sums, globErr := filepath.Glob(filepath.Join(dir, "*.sum"))
			c.Assert(globErr, qt.IsNil)
			c.Assert(sums, qt.HasLen, 1)
			// The pre-existing sum is untouched, not rewritten.
			body, readErr := os.ReadFile(filepath.Join(dir, tt.foreignSum))
			c.Assert(readErr, qt.IsNil)
			c.Assert(string(body), qt.Equals, "h1:seeded\n")
		})
	}
}

func TestMigrateCheckpointCommand_AllowsCheckpointBesideItsOwnSum(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	seedAtlasMigrations(c, dir)
	// The directory's OWN integrity file is not a conflict — it is the one the
	// checkpoint refreshes. This is the fixture that separates "a second sum
	// file" from "any sum file at all".
	c.Assert(os.WriteFile(filepath.Join(dir, "atlas.sum"), []byte("h1:stale\n"), 0o600), qt.IsNil)
	shadow := "sqlite://" + filepath.Join(t.TempDir(), "shadow.db")

	out, err := runCheckpoint("--shadow-db", shadow, "--migrations-dir", dir, "--dialect", "sqlite",
		"--dir-format", "atlas")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))

	sum, err := os.ReadFile(filepath.Join(dir, "atlas.sum"))
	c.Assert(err, qt.IsNil)
	c.Assert(string(sum), qt.Not(qt.Equals), "h1:stale\n")
	c.Assert(string(sum), qt.Contains, "_checkpoint.sql")
}

func TestMigrateCheckpointCommand_AtlasDryRunWritesNothing(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	seedAtlasMigrations(c, dir)
	shadow := "sqlite://" + filepath.Join(t.TempDir(), "shadow.db")

	out, err := runCheckpoint("--shadow-db", shadow, "--migrations-dir", dir, "--dialect", "sqlite",
		"--dir-format", "atlas", "--dry-run")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "dry run")
	// The preview must show the artifact that would be written, directive and all.
	c.Assert(out, qt.Contains, "-- atlas:checkpoint")
	c.Assert(out, qt.Contains, "_checkpoint.sql")

	written, err := filepath.Glob(filepath.Join(dir, "*checkpoint*"))
	c.Assert(err, qt.IsNil)
	c.Assert(written, qt.HasLen, 0)
	_, err = os.Stat(filepath.Join(dir, "atlas.sum"))
	c.Assert(os.IsNotExist(err), qt.IsTrue)
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
