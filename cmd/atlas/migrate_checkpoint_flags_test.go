package atlas_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/envbool/envbooltest"
	"go.5x5.cz/ptah/internal/migratesum"
	"go.5x5.cz/ptah/internal/testutils"
	"go.5x5.cz/ptah/migration/migrator"
)

// checkpointFlagFixture writes a hashed two-migration Atlas directory and
// returns it together with a fresh shadow database URL.
func checkpointFlagFixture(c *qt.C) (migrationsDir, devURL string) {
	c.Helper()
	root := c.TempDir()
	migrationsDir = filepath.Join(root, "migrations")
	c.Assert(os.MkdirAll(migrationsDir, 0o755), qt.IsNil)
	c.Assert(os.WriteFile(
		filepath.Join(migrationsDir, "20240101000001_init.sql"),
		[]byte("CREATE TABLE ck_users (id INTEGER PRIMARY KEY);\n"),
		0o600,
	), qt.IsNil)
	c.Assert(os.WriteFile(
		filepath.Join(migrationsDir, "20240101000002_orders.sql"),
		[]byte("CREATE TABLE ck_orders (id INTEGER PRIMARY KEY);\n"),
		0o600,
	), qt.IsNil)
	_, err := migratesum.WriteWithFormat(migrationsDir, migrator.MigrationDirFormatAtlas)
	c.Assert(err, qt.IsNil)
	return migrationsDir, "sqlite://" + filepath.Join(root, "shadow.db")
}

// writeAppendingEditor writes an editor command that appends a marker to every
// file it is handed and exits, which is the non-interactive editor the opt-out
// environment variable exists for.
func writeAppendingEditor(c *qt.C, marker string) string {
	testutils.SkipWithoutPOSIXShell(c)
	c.Helper()
	path := filepath.Join(c.TempDir(), "editor.sh")
	script := "#!/bin/sh\nfor f in \"$@\"; do\n  printf '%s\\n' \"" + marker + "\" >> \"$f\"\ndone\n"
	c.Assert(os.WriteFile(path, []byte(script), 0o700), qt.IsNil) // #nosec G306 -- the test editor must be executable
	return path
}

func checkpointFileNames(c *qt.C, migrationsDir string) []string {
	c.Helper()
	entries, err := os.ReadDir(migrationsDir)
	c.Assert(err, qt.IsNil)
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	return names
}

func checkpointBody(c *qt.C, migrationsDir string) string {
	c.Helper()
	written, err := filepath.Glob(filepath.Join(migrationsDir, "*_checkpoint.sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(written, qt.HasLen, 1)
	body, err := os.ReadFile(written[0])
	c.Assert(err, qt.IsNil)
	return string(body)
}

// TestCompatCommand_MigrateCheckpointEditRefusesWhatItCannotFinish pins the two
// deterministic refusals of --edit. Neither may write a checkpoint: a directory
// left with a file nobody could edit is the outcome the up-front check exists
// to prevent, and a run that blocks on an editor with no terminal is worse than
// either.
func TestCompatCommand_MigrateCheckpointEditRefusesWhatItCannotFinish(t *testing.T) {
	c := qt.New(t)
	// Written once, outside the table, so the row can carry the editor as the
	// value it is: what separates the two refusals is whether $EDITOR names a
	// command at all, and the second row needs a real one to get past the first
	// refusal and reach the terminal check.
	editor := writeAppendingEditor(c, "-- edited")

	tests := []struct {
		name    string
		editor  string
		wantErr string
	}{
		{
			name:    "no editor is configured",
			editor:  "",
			wantErr: `no editor configured: set \$EDITOR or \$VISUAL, or pass --editor`,
		},
		{
			name:    "an editor is configured but there is no terminal",
			editor:  editor,
			wantErr: `standard input is not a terminal.*set PTAH_ALLOW_NONINTERACTIVE_EDIT=1.*`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			envbooltest.Unset("PTAH_ALLOW_NONINTERACTIVE_EDIT")(c)
			c.Setenv("VISUAL", "")
			c.Setenv("EDITOR", test.editor)
			migrationsDir, devURL := checkpointFlagFixture(c)

			_, _, err := runCompatStreams(c,
				"migrate", "checkpoint",
				"--dir", "file://"+migrationsDir,
				"--dev-url", devURL,
				"--edit",
			)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(checkpointFileNames(c, migrationsDir), qt.HasLen, 3)
		})
	}
}

// TestCompatCommand_MigrateCheckpointEditRewritesAndRehashes is the --edit
// happy path: the written checkpoint carries the editor's change, and the
// directory's atlas.sum covers the EDITED bytes. The second half is the one
// that matters — a refresh that did not happen would leave a directory that
// every integrity check refuses, including Atlas's own.
func TestCompatCommand_MigrateCheckpointEditRewritesAndRehashes(t *testing.T) {
	c := qt.New(t)
	c.Setenv("VISUAL", "")
	c.Setenv("EDITOR", writeAppendingEditor(c, "-- edited by the test"))
	c.Setenv("PTAH_ALLOW_NONINTERACTIVE_EDIT", "1")
	migrationsDir, devURL := checkpointFlagFixture(c)

	stdout, _, err := runCompatStreams(c,
		"migrate", "checkpoint",
		"--dir", "file://"+migrationsDir,
		"--dev-url", devURL,
		"--edit",
	)

	c.Assert(err, qt.IsNil)
	c.Assert(stdout, qt.Contains, "Wrote checkpoint version")
	c.Assert(checkpointBody(c, migrationsDir), qt.Contains, "-- edited by the test")
	// The first line must still be the checkpoint directive: the editor appends.
	c.Assert(strings.HasPrefix(checkpointBody(c, migrationsDir), "-- atlas:checkpoint\n"), qt.IsTrue)

	_, _, validateErr := runCompatStreams(c,
		"migrate", "validate",
		"--dir", "file://"+migrationsDir,
	)
	c.Assert(validateErr, qt.IsNil)
}

// TestCompatCommand_MigrateCheckpointDeclaresItsAtlasFlags pins what this verb
// advertises, because that is the surface the conformance CLI tier compares.
//
// Atlas's published reference registers --dev-url, --dir, --dir-format,
// -s/--schema, --lock-timeout, --format, --qualifier, --edit and --lock-name on
// `migrate checkpoint`. Every flag below is on that list.
func TestCompatCommand_MigrateCheckpointDeclaresItsAtlasFlags(t *testing.T) {
	tests := []struct {
		name string
		flag string
	}{
		{name: "edit is declared", flag: "--edit"},
		{name: "qualifier is declared", flag: "--qualifier"},
		{name: "lock-timeout is declared", flag: "--lock-timeout"},
		{name: "schema is declared", flag: "--schema"},
		{name: "dir-format is still declared", flag: "--dir-format"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			stdout, _, err := runCompatStreams(c, "migrate", "checkpoint", "--help")

			c.Assert(err, qt.IsNil)
			c.Assert(stdout, qt.Contains, test.flag+" ")
		})
	}
}

// TestCompatCommand_MigrateCheckpointOmitsWhatItDoesNotAdvertise is the other
// half of the surface above, and the half a conformance comparison notices: a
// flag that appears here without an Atlas spelling behind it advertises a
// compatibility this verb does not have.
//
// --editor is native-only. --format and --lock-name are absent on purpose too:
// --lock-name belongs to the named-lock family landing separately, and --format
// needs the compat Go-template report path this change does not build.
func TestCompatCommand_MigrateCheckpointOmitsWhatItDoesNotAdvertise(t *testing.T) {
	tests := []struct {
		name string
		flag string
	}{
		{name: "editor is native-only", flag: "--editor"},
		{name: "format is not declared", flag: "--format"},
		{name: "lock-name is not declared", flag: "--lock-name"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			stdout, _, err := runCompatStreams(c, "migrate", "checkpoint", "--help")

			c.Assert(err, qt.IsNil)
			c.Assert(stdout, qt.Not(qt.Contains), test.flag+" ")
		})
	}
}

// TestCompatCommand_MigrateCheckpointLockTimeoutRefusesANonDuration is the
// first half of the flag: a value that cannot be a duration stops the run
// rather than being read as no bound at all.
func TestCompatCommand_MigrateCheckpointLockTimeoutRefusesANonDuration(t *testing.T) {
	c := qt.New(t)
	migrationsDir, devURL := checkpointFlagFixture(c)

	_, _, err := runCompatStreams(c,
		"migrate", "checkpoint",
		"--dir", "file://"+migrationsDir,
		"--dev-url", devURL,
		"--lock-timeout", "not-a-duration",
		"--dry-run",
	)

	c.Assert(err, qt.ErrorMatches, `invalid migration lock timeout: .*`)
}

// TestCompatCommand_MigrateCheckpointLockTimeoutSaysWhenItCannotBind is the
// second half: a value that IS a duration says so when the dev database's
// dialect implements no advisory locking, so a bound that cannot bind is never
// silently accepted.
func TestCompatCommand_MigrateCheckpointLockTimeoutSaysWhenItCannotBind(t *testing.T) {
	c := qt.New(t)
	migrationsDir, devURL := checkpointFlagFixture(c)

	stdout, stderr, err := runCompatStreams(c,
		"migrate", "checkpoint",
		"--dir", "file://"+migrationsDir,
		"--dev-url", devURL,
		"--lock-timeout", "10s",
		"--dry-run",
	)

	c.Assert(err, qt.IsNil)
	c.Assert(stderr, qt.Contains,
		`note: migration locking is not supported for dialect "sqlite"; --migration-lock-timeout is ignored`)
	// The note goes to stderr so it cannot contaminate the SQL a dry run
	// writes to stdout.
	c.Assert(stdout, qt.Not(qt.Contains), "note: migration locking")
	c.Assert(stdout, qt.Contains, "CREATE TABLE")
}

// TestCompatCommand_MigrateCheckpointLockTimeoutStaysQuietWhenUnset is the
// control for the note above: it must be a consequence of the flag, not
// something every SQLite checkpoint prints.
func TestCompatCommand_MigrateCheckpointLockTimeoutStaysQuietWhenUnset(t *testing.T) {
	c := qt.New(t)
	migrationsDir, devURL := checkpointFlagFixture(c)

	_, stderr, err := runCompatStreams(c,
		"migrate", "checkpoint",
		"--dir", "file://"+migrationsDir,
		"--dev-url", devURL,
		"--dry-run",
	)

	c.Assert(err, qt.IsNil)
	c.Assert(stderr, qt.Not(qt.Contains), "migration locking is not supported")
}

// TestCompatCommand_MigrateCheckpointQualifierReachesThePlan proves the
// qualifier value is carried into plan qualification rather than parsed and
// dropped: SQLite cannot qualify, and the refusal it raises is produced deep in
// the qualifier engine, which is only reached when a qualifier is set.
func TestCompatCommand_MigrateCheckpointQualifierReachesThePlan(t *testing.T) {
	c := qt.New(t)
	migrationsDir, devURL := checkpointFlagFixture(c)

	_, _, err := runCompatStreams(c,
		"migrate", "checkpoint",
		"--dir", "file://"+migrationsDir,
		"--dev-url", devURL,
		"--qualifier", "app",
		"--dry-run",
	)

	c.Assert(err, qt.ErrorMatches, `.*--qualifier is not supported for dialect "sqlite"`)
}

// TestCompatCommand_MigrateCheckpointQualifierUnsetPlansCleanly is the control
// for the refusal above: without the flag the same fixture plans and prints a
// checkpoint, so the refusal is attributable to the qualifier and not to the
// fixture.
func TestCompatCommand_MigrateCheckpointQualifierUnsetPlansCleanly(t *testing.T) {
	c := qt.New(t)
	migrationsDir, devURL := checkpointFlagFixture(c)

	stdout, _, err := runCompatStreams(c,
		"migrate", "checkpoint",
		"--dir", "file://"+migrationsDir,
		"--dev-url", devURL,
		"--dry-run",
	)

	c.Assert(err, qt.IsNil)
	c.Assert(stdout, qt.Contains, `CREATE TABLE "ck_users"`)
}
