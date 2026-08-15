package atlas_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/envbool/envbooltest"
	"go.5x5.cz/ptah/internal/migratesum"
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
	c.Helper()
	path := filepath.Join(c.TempDir(), "editor.sh")
	script := "#!/bin/sh\nfor f in \"$@\"; do\n  printf '%s\\n' \"" + marker + "\" >> \"$f\"\ndone\n"
	c.Assert(os.WriteFile(path, []byte(script), 0o700), qt.IsNil) //nolint:gosec // the test editor must be executable
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
	tests := []struct {
		name    string
		env     func(c *qt.C)
		wantErr string
	}{
		{
			name: "no editor is configured",
			env: func(c *qt.C) {
				c.Setenv("VISUAL", "")
				c.Setenv("EDITOR", "")
			},
			wantErr: `no editor configured: set \$EDITOR or \$VISUAL, or pass --editor`,
		},
		{
			name: "an editor is configured but there is no terminal",
			env: func(c *qt.C) {
				c.Setenv("VISUAL", "")
				c.Setenv("EDITOR", writeAppendingEditor(c, "-- edited"))
			},
			wantErr: `standard input is not a terminal.*set PTAH_ALLOW_NONINTERACTIVE_EDIT=1.*`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			envbooltest.Unset("PTAH_ALLOW_NONINTERACTIVE_EDIT")(c)
			test.env(c)
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

// TestCompatCommand_MigrateCheckpointDeclaredFlagSurface pins what this verb
// advertises, because that is the surface the conformance CLI tier compares.
//
// Atlas's published reference registers --dev-url, --dir, --dir-format,
// -s/--schema, --lock-timeout, --format, --qualifier, --edit and --lock-name on
// `migrate checkpoint`. Everything declared here is on that list, and the
// native-only editor selection (--editor) is deliberately NOT declared: it has
// no Atlas spelling, so it must not appear on this help surface.
//
// --format and --lock-name are absent on purpose too: --lock-name belongs to
// the named-lock family landing separately, and --format needs the compat
// Go-template report path this change does not build.
func TestCompatCommand_MigrateCheckpointDeclaredFlagSurface(t *testing.T) {
	tests := []struct {
		name   string
		flag   string
		assert func(c *qt.C, help, flag string)
	}{
		{name: "edit is declared", flag: "--edit", assert: assertHelpDeclaresFlag},
		{name: "qualifier is declared", flag: "--qualifier", assert: assertHelpDeclaresFlag},
		{name: "lock-timeout is declared", flag: "--lock-timeout", assert: assertHelpDeclaresFlag},
		{name: "schema is declared", flag: "--schema", assert: assertHelpDeclaresFlag},
		{name: "dir-format is still declared", flag: "--dir-format", assert: assertHelpDeclaresFlag},
		{name: "editor is native-only", flag: "--editor", assert: assertHelpOmitsFlag},
		{name: "format is not declared", flag: "--format", assert: assertHelpOmitsFlag},
		{name: "lock-name is not declared", flag: "--lock-name", assert: assertHelpOmitsFlag},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			stdout, _, err := runCompatStreams(c, "migrate", "checkpoint", "--help")

			c.Assert(err, qt.IsNil)
			test.assert(c, stdout, test.flag)
		})
	}
}

func assertHelpDeclaresFlag(c *qt.C, help, flag string) {
	c.Helper()
	c.Assert(help, qt.Contains, flag+" ")
}

func assertHelpOmitsFlag(c *qt.C, help, flag string) {
	c.Helper()
	c.Assert(help, qt.Not(qt.Contains), flag+" ")
}

// TestCompatCommand_MigrateCheckpointLockTimeout covers both halves of the
// flag: a value that cannot be a duration stops the run, and a value that can
// says so when the dev database's dialect implements no advisory locking, so a
// bound that cannot bind is never silently accepted.
func TestCompatCommand_MigrateCheckpointLockTimeout(t *testing.T) {
	tests := []struct {
		name   string
		value  string
		assert func(c *qt.C, stdout, stderr string, err error)
	}{
		{
			name:  "a value that is not a duration",
			value: "not-a-duration",
			assert: func(c *qt.C, _, _ string, err error) {
				c.Assert(err, qt.ErrorMatches, `invalid migration lock timeout: .*`)
			},
		},
		{
			name:  "a valid value on a dialect with no advisory lock",
			value: "10s",
			assert: func(c *qt.C, stdout, stderr string, err error) {
				c.Assert(err, qt.IsNil)
				c.Assert(stderr, qt.Contains,
					`note: migration locking is not supported for dialect "sqlite"; --migration-lock-timeout is ignored`)
				// The note goes to stderr so it cannot contaminate the SQL a
				// dry run writes to stdout.
				c.Assert(stdout, qt.Not(qt.Contains), "note: migration locking")
				c.Assert(stdout, qt.Contains, "CREATE TABLE")
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			migrationsDir, devURL := checkpointFlagFixture(c)

			stdout, stderr, err := runCompatStreams(c,
				"migrate", "checkpoint",
				"--dir", "file://"+migrationsDir,
				"--dev-url", devURL,
				"--lock-timeout", test.value,
				"--dry-run",
			)

			test.assert(c, stdout, stderr, err)
		})
	}
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
