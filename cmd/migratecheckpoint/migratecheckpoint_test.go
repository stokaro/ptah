package migratecheckpoint_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/migratecheckpoint"
	"go.5x5.cz/ptah/internal/migratesum"
	"go.5x5.cz/ptah/migration/migrator"
)

// ptahMigrationFiles is the two-migration history in the ptah convention, as
// file names and bodies rather than as a writer, so a table can carry the
// directory a row starts from as data.
func ptahMigrationFiles() map[string]string {
	return map[string]string{
		"0000000001_init.up.sql":    "CREATE TABLE users (id INTEGER PRIMARY KEY);\n",
		"0000000001_init.down.sql":  "DROP TABLE users;\n",
		"0000000002_email.up.sql":   "ALTER TABLE users ADD COLUMN email TEXT;\n",
		"0000000002_email.down.sql": "ALTER TABLE users DROP COLUMN email;\n",
	}
}

// atlasMigrationFiles is the same history in the atlas convention: one file per
// migration, versioned by timestamp.
func atlasMigrationFiles() map[string]string {
	return map[string]string{
		"20250801000001_init.sql":  "CREATE TABLE users (id INTEGER PRIMARY KEY);\n",
		"20250801000002_email.sql": "ALTER TABLE users ADD COLUMN email TEXT;\n",
	}
}

func writeMigrationFiles(c *qt.C, dir string, files map[string]string) {
	c.Helper()
	for name, body := range files {
		c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600), qt.IsNil)
	}
}

func seedMigrations(c *qt.C, dir string) {
	c.Helper()
	writeMigrationFiles(c, dir, ptahMigrationFiles())
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

func TestMigrateCheckpointRejectsMalformedSQLiteToggleBeforeMissingShadowDB(t *testing.T) {
	c := qt.New(t)
	t.Setenv("PTAH_SQLITE_ALLOW_VIRTUAL_TABLE_DROP", "not-a-boolean")

	out, err := runCheckpoint(
		"--dialect", "sqlite",
		"--migrations-dir", t.TempDir(),
	)

	c.Assert(err, qt.IsNotNil)
	c.Assert(out, qt.Contains,
		`invalid boolean value "not-a-boolean" for PTAH_SQLITE_ALLOW_VIRTUAL_TABLE_DROP`)
	c.Assert(out, qt.Not(qt.Contains), "shadow database URL is required")
}

func TestMigrateCheckpointDoesNotApplySQLiteToggleToMissingPostgresShadowDB(t *testing.T) {
	c := qt.New(t)
	t.Setenv("PTAH_SQLITE_ALLOW_VIRTUAL_TABLE_DROP", "not-a-boolean")

	out, err := runCheckpoint(
		"--dialect", "postgres",
		"--migrations-dir", t.TempDir(),
	)

	c.Assert(err, qt.IsNotNil)
	c.Assert(out, qt.Contains, "shadow database URL is required")
	c.Assert(out, qt.Not(qt.Contains), "PTAH_SQLITE_ALLOW_VIRTUAL_TABLE_DROP")
}

func TestMigrateCheckpointValidatesExplicitSQLiteDialectBeforePostgresShadowURL(t *testing.T) {
	c := qt.New(t)
	t.Setenv("PTAH_SQLITE_ALLOW_VIRTUAL_TABLE_DROP", "not-a-boolean")

	out, err := runCheckpoint(
		"--dialect", "sqlite",
		"--shadow-db", "postgres://localhost/database",
		"--migrations-dir", t.TempDir(),
	)

	c.Assert(err, qt.IsNotNil)
	c.Assert(out, qt.Contains,
		`invalid boolean value "not-a-boolean" for PTAH_SQLITE_ALLOW_VIRTUAL_TABLE_DROP`)
	c.Assert(out, qt.Not(qt.Contains), "error connecting")
}

func TestMigrateCheckpointRejectsMalformedSQLiteToggleBeforeDirectoryAndDryRun(t *testing.T) {
	c := qt.New(t)
	t.Setenv("PTAH_SQLITE_ALLOW_VIRTUAL_TABLE_DROP", "not-a-boolean")

	out, err := runCheckpoint(
		"--shadow-db", "sqlite://"+filepath.Join(t.TempDir(), "shadow.db"),
		"--migrations-dir", "",
		"--dry-run",
	)

	c.Assert(err, qt.IsNotNil)
	c.Assert(out, qt.Contains,
		`invalid boolean value "not-a-boolean" for PTAH_SQLITE_ALLOW_VIRTUAL_TABLE_DROP`)
	c.Assert(out, qt.Not(qt.Contains), "migrations directory")
}

func TestMigrateCheckpointDoesNotApplySQLiteToggleToPostgres(t *testing.T) {
	c := qt.New(t)
	t.Setenv("PTAH_SQLITE_ALLOW_VIRTUAL_TABLE_DROP", "not-a-boolean")

	out, err := runCheckpoint(
		"--shadow-db", "postgres://localhost/database",
		"--migrations-dir", "",
		"--dry-run",
	)

	c.Assert(err, qt.IsNotNil)
	c.Assert(out, qt.Contains, "migrations directory is required")
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
	writeMigrationFiles(c, dir, atlasMigrationFiles())
}

func TestMigrateCheckpointCommand_RefusesAtlasIntoUnhashedPtahDirectory(t *testing.T) {
	tests := []struct {
		name  string
		extra []string
	}{
		{name: "explicit atlas", extra: []string{"--dir-format", "atlas"}},
		// The native default is ptah, so the explicit spelling is the only way
		// to reach this natively; the compat surface reaches it unflagged,
		// which is covered in cmd/atlas.
		{name: "atlas with a description", extra: []string{"--dir-format", "atlas", "--description", "snap"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			dir := t.TempDir()
			seedMigrations(c, dir) // ptah pair, deliberately NEVER hashed
			shadow := "sqlite://" + filepath.Join(t.TempDir(), "shadow.db")

			args := append([]string{"--shadow-db", shadow, "--migrations-dir", dir, "--dialect", "sqlite"}, tt.extra...)
			out, err := runCheckpoint(args...)

			// With no sum file the marker-shaped check cannot see the conflict;
			// only a content-shaped one can. Left unguarded, the two auto rules
			// then disagree about this directory forever: discovery prefers the
			// ptah files and never sees the checkpoint, while verification finds
			// the atlas.sum and calls the directory valid.
			c.Assert(err, qt.IsNotNil)
			c.Assert(out, qt.Contains, "holds ptah-format migrations")

			// Protected state: nothing written, and above all no atlas.sum —
			// that file is what would make the invisibility permanent.
			written, globErr := filepath.Glob(filepath.Join(dir, "*checkpoint*"))
			c.Assert(globErr, qt.IsNil)
			c.Assert(written, qt.HasLen, 0)
			sums, globErr := filepath.Glob(filepath.Join(dir, "*.sum"))
			c.Assert(globErr, qt.IsNil)
			c.Assert(sums, qt.HasLen, 0)
		})
	}
}

func TestMigrateCheckpointCommand_AtlasGuardDoesNotOverfire(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	seedAtlasMigrations(c, dir)
	shadow := "sqlite://" + filepath.Join(t.TempDir(), "shadow.db")

	// A pure Atlas name carries no direction component, so ParseMigrationFileName
	// yields nothing and the ptah-content guard must not fire. This separates
	// "holds ptah files" from "holds any files at all" — without it the guard
	// could refuse every directory and nothing would notice.
	out, err := runCheckpoint("--shadow-db", shadow, "--migrations-dir", dir, "--dialect", "sqlite",
		"--dir-format", "atlas")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))

	written, err := filepath.Glob(filepath.Join(dir, "*_checkpoint.sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(written, qt.HasLen, 1)
}

func TestMigrateCheckpointCommand_SurfacesUnreadableDirectory(t *testing.T) {
	c := qt.New(t)
	// A NUL in the path. os.Stat refuses it with EINVAL before it reaches any
	// syscall, on every operating system, and Go does not fold EINVAL into
	// fs.ErrNotExist -- so the branch under test is reached identically
	// everywhere and no privilege check is needed.
	//
	// The earlier fixture was a regular file where the directory should be,
	// relying on ENOTDIR from stat of <file>/ptah.sum. That is deterministic on
	// Unix only: Windows answers ERROR_PATH_NOT_FOUND, which Go maps to
	// fs.ErrNotExist, so the run took the "no conflict" branch and the test
	// asserted nothing it was written for.
	unreadable := filepath.Join(t.TempDir(), "migrations\x00hidden")

	// A stat failure that is not ErrNotExist means we could not determine
	// whether the conflict exists. Treating that as "no conflict" would write
	// into a directory whose state is unknown, so it must surface instead.
	out, err := runCheckpoint("--shadow-db", "sqlite://"+filepath.Join(t.TempDir(), "shadow.db"),
		"--migrations-dir", unreadable, "--dialect", "sqlite", "--dir-format", "atlas")
	c.Assert(err, qt.IsNotNil)
	c.Assert(out, qt.Contains, "failed to inspect migrations directory")
}

func TestMigrateCheckpointCommand_AtlasVersionGuardMeasuresRenderedValue(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	seedAtlasMigrations(c, dir)
	shadow := "sqlite://" + filepath.Join(t.TempDir(), "shadow.db")

	// Eleven characters, ten rendered digits: the file name is written with %d,
	// so this produces 1234567890_checkpoint.sql — precisely the ptah-width name
	// the guard exists to prevent. Measuring len(explicit) lets it through.
	out, err := runCheckpoint("--shadow-db", shadow, "--migrations-dir", dir, "--dialect", "sqlite",
		"--dir-format", "atlas", "--version", "01234567890")
	c.Assert(err, qt.IsNotNil)
	c.Assert(out, qt.Contains, "indistinguishable from a ptah migration name")

	written, err := filepath.Glob(filepath.Join(dir, "*.sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(written, qt.HasLen, 2) // the two seeded migrations, no checkpoint
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

// TestMigrateCheckpointCommand_DefaultVersionKeepsTenDigitCeiling pins the half
// of the ceiling the explicit --version guard next door never covered
// (stokaro/ptah#938). Without --version the resolver returned newest+1 with no
// bound at all, so a directory whose newest migration is 9999999999 got a
// checkpoint at 10000000000: an eleven-digit name ParseMigrationFileName
// refuses. Measured on master, that exited 0, printed "Wrote checkpoint version
// 10000000000", and a fresh database then replayed the history instead of
// bootstrapping from the checkpoint -- the one thing a checkpoint exists to
// prevent.
//
// The cheaper wrong implementation is `return latest + 1, nil`, which is what
// the branch said before.
func TestMigrateCheckpointCommand_DefaultVersionKeepsTenDigitCeiling(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	c.Assert(os.WriteFile(
		filepath.Join(dir, "9999999999_seed.up.sql"),
		[]byte("CREATE TABLE users (id INTEGER PRIMARY KEY);\n"), 0o600,
	), qt.IsNil)
	c.Assert(os.WriteFile(
		filepath.Join(dir, "9999999999_seed.down.sql"), []byte("DROP TABLE users;\n"), 0o600,
	), qt.IsNil)
	shadow := "sqlite://" + filepath.Join(t.TempDir(), "shadow.db")

	out, err := runCheckpoint("--shadow-db", shadow, "--migrations-dir", dir, "--dialect", "sqlite")

	c.Assert(err, qt.IsNotNil)
	c.Assert(out, qt.Contains, "9999999999")

	written, globErr := filepath.Glob(filepath.Join(dir, "*checkpoint*"))
	c.Assert(globErr, qt.IsNil)
	c.Assert(written, qt.HasLen, 0)
}

// TestMigrateCheckpointCommand_AtlasDefaultVersionIsAnInstantThatExists pins
// the `...235960` half of stokaro/ptah#938 on the verb that still bumps.
//
// `migrate checkpoint` cannot stamp the bare clock the way `migrate new` and
// `migrate diff` do: its version has to outrank every migration it squashes, or
// a fresh database bootstraps from it and then applies a migration whose SQL the
// checkpoint body already contains. So beside a future-dated neighbor it steps
// past it -- and stepping by ONE produced the version the issue names. Measured
// on this branch's parent, `ptah-compat migrate checkpoint` into a directory
// holding one `29991231235959_future.sql` wrote `29991231235960_cp1.sql` at exit
// 0, and a second checkpoint then wrote `29991231235961_cp2.sql` on top of it.
// Sixty and sixty-one seconds past the minute: neither is an instant, and
// time.Parse refuses both under the layout the rest of the file names use.
//
// The assertion is the round trip, not the literal 30000101000000, so the row
// states the property rather than one arithmetic answer.
//
// The cheaper wrong implementation is not deleting the step. It is
// `migrationversion.Next` in place of `migrationversion.Advance` here --
// integer arithmetic, bounded but not calendar-aware, which is exactly what the
// branch said before and what every reviewer would write first.
func TestMigrateCheckpointCommand_AtlasDefaultVersionIsAnInstantThatExists(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "29991231235959_future.sql"),
		[]byte("CREATE TABLE users (id INTEGER PRIMARY KEY);\n"), 0o600), qt.IsNil)
	shadow := "sqlite://" + filepath.Join(t.TempDir(), "shadow.db")

	out, err := runCheckpoint("--shadow-db", shadow, "--migrations-dir", dir, "--dialect", "sqlite",
		"--dir-format", "atlas")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))

	written, globErr := filepath.Glob(filepath.Join(dir, "*_checkpoint.sql"))
	c.Assert(globErr, qt.IsNil)
	c.Assert(written, qt.HasLen, 1)

	version, _, found := strings.Cut(filepath.Base(written[0]), "_")
	c.Assert(found, qt.IsTrue)

	// It outranks the neighbor, which is why the bump exists at all.
	c.Assert(version > "29991231235959", qt.IsTrue, qt.Commentf("version %s", version))

	// And it is an instant. `29991231235960` satisfies the line above and fails
	// this one, which is the whole point of the row.
	at, parseErr := time.Parse("20060102150405", version)
	c.Assert(parseErr, qt.IsNil, qt.Commentf("version %s", version))
	c.Assert(at.UTC().Format("20060102150405"), qt.Equals, version)
}

// TestMigrateCheckpointCommand_AtlasOrdinaryDirectoryStillTakesTheClock is the
// non-interference control for the row above. Reverting Advance to Next must
// NOT redden it: a directory whose newest migration is in the past is where
// every real project lives, and there the checkpoint keeps taking the clock
// untouched rather than stepping anywhere.
func TestMigrateCheckpointCommand_AtlasOrdinaryDirectoryStillTakesTheClock(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	seedAtlasMigrations(c, dir)
	shadow := "sqlite://" + filepath.Join(t.TempDir(), "shadow.db")

	before := time.Now().UTC().Format("20060102150405")
	out, err := runCheckpoint("--shadow-db", shadow, "--migrations-dir", dir, "--dialect", "sqlite",
		"--dir-format", "atlas")
	after := time.Now().UTC().Format("20060102150405")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))

	written, globErr := filepath.Glob(filepath.Join(dir, "*_checkpoint.sql"))
	c.Assert(globErr, qt.IsNil)
	c.Assert(written, qt.HasLen, 1)

	version, _, found := strings.Cut(filepath.Base(written[0]), "_")
	c.Assert(found, qt.IsTrue)
	c.Assert(version >= before, qt.IsTrue, qt.Commentf("version %s predates the call", version))
	c.Assert(version <= after, qt.IsTrue, qt.Commentf("version %s postdates the call", version))
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
		name      string
		dirFormat string
		// files is the directory the row starts from: a history in the OTHER
		// convention, so the run's own integrity file would be the second one.
		files      map[string]string
		foreignSum string
	}{
		{
			// The case the compat default (atlas) newly makes reachable: an
			// existing ptah directory checkpointed under the atlas convention.
			name:       "atlas checkpoint into a ptah.sum directory",
			dirFormat:  "atlas",
			files:      ptahMigrationFiles(),
			foreignSum: "ptah.sum",
		},
		{
			name:       "ptah checkpoint into an atlas.sum directory",
			dirFormat:  "ptah",
			files:      atlasMigrationFiles(),
			foreignSum: "atlas.sum",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := t.TempDir()
			writeMigrationFiles(c, dir, test.files)
			c.Assert(os.WriteFile(filepath.Join(dir, test.foreignSum), []byte("h1:seeded\n"), 0o600), qt.IsNil)
			shadow := "sqlite://" + filepath.Join(t.TempDir(), "shadow.db")

			out, err := runCheckpoint("--shadow-db", shadow, "--migrations-dir", dir, "--dialect", "sqlite",
				"--dir-format", test.dirFormat)
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
			body, readErr := os.ReadFile(filepath.Join(dir, test.foreignSum))
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
	//
	// It is a REAL atlas.sum rather than the `h1:stale` placeholder it used to
	// be. Checkpoint replays the history onto the shadow database, so it now
	// verifies the directory first, and a placeholder that no parser accepts is
	// refused before the question this test asks is reached. The refusal is
	// correct — the old fixture depended on checkpoint executing a directory
	// whose integrity file could not be read at all — and a valid sum keeps the
	// test measuring what its name says.
	sumBefore, err := migratesum.WriteWithFormat(dir, migrator.MigrationDirFormatAtlas)
	c.Assert(err, qt.IsNil)
	c.Assert(sumBefore, qt.IsNotNil)
	shadow := "sqlite://" + filepath.Join(t.TempDir(), "shadow.db")

	out, err := runCheckpoint("--shadow-db", shadow, "--migrations-dir", dir, "--dialect", "sqlite",
		"--dir-format", "atlas")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))

	sum, err := os.ReadFile(filepath.Join(dir, "atlas.sum"))
	c.Assert(err, qt.IsNil)
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
