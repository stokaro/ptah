package main_test

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/migratesum"
	"go.5x5.cz/ptah/migration/migrator"
)

// These tests build the ptah-compat binary at run time and exercise it as a
// subprocess, which is the only way to assert real process exit codes.
//
// Caution when iterating on behavior these tests pin: the Go test cache keys on
// this package's own inputs, so an edit under cmd/atlas does not invalidate a
// cached PASS here even though it changes the binary being built. Run
// `go test ./cmd/ptah-compat/... -count=1` after touching the command tree, or
// a mutation you expect to fail will silently report a stale PASS.

func TestCompatBinaryNamedAtlasResolvesRootCommands(t *testing.T) {
	c := qt.New(t)
	binPath := buildCompatBinary(c)

	run := newCompatProcess(binPath, "migrate", "down", "--help")
	runOut, err := run.CombinedOutput()

	output := string(runOut)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", runOut))
	c.Assert(output, qt.Contains, "Usage:")
	c.Assert(output, qt.Contains, "atlas migrate down")
	c.Assert(output, qt.Not(qt.Contains), "atlas atlas migrate down")
}

// TestCompatBinaryCommandFailuresExit1 pins the whole process-level diagnostic
// class of the compat surface in one table, byte-exact on stderr.
//
// Per stokaro/ptah#1019 the prefix is punctuation owned by the surface, not by
// the message: everything this binary prints as a process-level diagnostic is
// prefixed "Error: ", and the native ptah binary prefixes "error: " (pinned by
// TestNativeDiagnosticsKeepTheNativePrefix in cmd/root). Before that decision
// the two prefixes were split inside this one binary — `migrate set` was the
// only command that overrode a printer, so it answered "Error: unknown flag"
// while `migrate status`, `schema inspect`, `version` and the rest answered
// "error: unknown flag" for the identical failure. Keeping every route in a
// single table is what makes that kind of drift a red test: the rows reach the
// shared cmdutil printers, the compat-local printers, and the forwarded native
// targets respectively, and a fix that lands on only some of them fails here.
//
// This table is currently the only byte-exact guard on the class. The
// stokaro/ptah#1019 definition of done also asks for an exact-stderr assertion
// in the conformance harness, whose `cli-exit-behavior` "unknown flag" case
// still matches the substring "unknown flag" — true under either prefix, which
// is why the split went unnoticed. That assertion is deferred rather than
// forgotten, and the reason is sequencing, not effort:
// ptah-atlas-conformance builds its compat binary from the go.5x5.cz/ptah
// version its own go.mod pins, so the assertion can only be added after a
// release containing this change is pinned there. Measured, same argv
// (`migrate validate --totally-unknown-flag`), same machine:
//
//	this tree                   -> "Error: unknown flag: --totally-unknown-flag\n"
//	the currently pinned module -> "error: unknown flag: --totally-unknown-flag\n"
//
// Adding the exact-bytes assertion before that bump turns the conformance gate
// red against the module it actually builds.
func TestCompatBinaryCommandFailuresExit1(t *testing.T) {
	c := qt.New(t)
	binPath := buildCompatBinary(c)

	tests := []struct {
		name       string
		args       []string
		wantStderr string
	}{
		// Reaches cmdutil's shared flag-error printer.
		{
			name:       "unknown flag",
			args:       []string{"version", "--bogus-flag"},
			wantStderr: "Error: unknown flag: --bogus-flag\n",
		},
		{
			name:       "schema inspect unknown flag",
			args:       []string{"schema", "inspect", "--zzz"},
			wantStderr: "Error: unknown flag: --zzz\n",
		},
		{
			name:       "schema inspect flag without a value",
			args:       []string{"schema", "inspect", "--url"},
			wantStderr: "Error: flag needs an argument: --url\n",
		},
		{
			name:       "migrate status unknown flag",
			args:       []string{"migrate", "status", "--zzz"},
			wantStderr: "Error: unknown flag: --zzz\n",
		},
		// Reaches cmdutil's shared post-execution normalizer.
		{
			name: "mutually exclusive flags",
			args: []string{
				"schema", "apply",
				"--url", "sqlite://schema.db",
				"--to", "file://schema.sql",
				"--file", "schema.sql",
				"--dry-run",
			},
			wantStderr: "Error: if any flags in the group [file to] are set none of the others can be; [file to] were all set\n",
		},
		// The verb that used to be the only one answering "Error:".
		{
			name:       "migrate set unknown flag",
			args:       []string{"migrate", "set", "--unknown"},
			wantStderr: "Error: unknown flag: --unknown\n",
		},
		// Reaches a native command the adapter executes detached from this
		// tree, so its diagnostic is printed by the native package's own
		// cmdutil.Fail call rather than by anything under cmd/atlas.
		{
			name:       "forwarded native target failure",
			args:       []string{"migrate", "rm", "20990101000000"},
			wantStderr: "Error: migrations directory migrations: stat migrations: no such file or directory\n",
		},
		// Reaches the compat-local printers.
		{
			name:       "lazy completion command",
			args:       []string{"completion", "bash", "extra"},
			wantStderr: "Error: unknown command \"extra\" for \"atlas completion bash\"\n",
		},
		{
			name: "unknown root command",
			args: []string{"definitely-not-a-command"},
			wantStderr: "Error: unknown command \"definitely-not-a-command\" for \"atlas\"\n" +
				"Run 'atlas --help' for usage.\n",
		},
		{
			name:       "registered command without an implementation",
			args:       []string{"migrate", "push"},
			wantStderr: "Error: atlas migrate push is not implemented by Ptah\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			run := newCompatProcess(binPath, tt.args...)
			// An empty working directory: the forwarded-target row resolves the
			// default relative migration directory from the process cwd, and
			// must not find the repository's own.
			run.Dir = c.TempDir()
			var stdout, stderr bytes.Buffer
			run.Stdout = &stdout
			run.Stderr = &stderr

			err := run.Run()
			var exitErr *exec.ExitError

			c.Assert(err, qt.ErrorAs, &exitErr, qt.Commentf("stderr: %s", stderr.String()))
			c.Assert(exitErr.ExitCode(), qt.Equals, 1)
			c.Assert(stdout.String(), qt.Equals, "")
			c.Assert(stderr.String(), qt.Equals, tt.wantStderr)
		})
	}
}

func TestCompatBinaryAtlasSuccessPaths(t *testing.T) {
	c := qt.New(t)
	binPath := buildCompatBinary(c)

	t.Run("clean validation is silent", func(t *testing.T) {
		c := qt.New(t)
		dir := cleanAtlasDir(c)
		run := newCompatProcess(binPath, "migrate", "validate", "--dir", "file://"+dir)
		var stdout, stderr bytes.Buffer
		run.Stdout = &stdout
		run.Stderr = &stderr

		err := run.Run()

		c.Assert(err, qt.IsNil)
		c.Assert(stdout.String(), qt.Equals, "")
		c.Assert(stderr.String(), qt.Equals, "")
	})

	t.Run("nested extra token prints help", func(t *testing.T) {
		c := qt.New(t)
		run := newCompatProcess(binPath, "migrate", "aplly")
		var stdout, stderr bytes.Buffer
		run.Stdout = &stdout
		run.Stderr = &stderr

		err := run.Run()

		c.Assert(err, qt.IsNil)
		c.Assert(stdout.String(), qt.Contains, "Usage:\n  atlas migrate [command]")
		c.Assert(stderr.String(), qt.Equals, "")
	})

	t.Run("completion group extra token prints help", func(t *testing.T) {
		c := qt.New(t)
		run := newCompatProcess(binPath, "completion", "sh")
		var stdout, stderr bytes.Buffer
		run.Stdout = &stdout
		run.Stderr = &stderr

		err := run.Run()

		c.Assert(err, qt.IsNil)
		c.Assert(stdout.String(), qt.Contains, "Usage:\n  atlas completion [command]")
		c.Assert(stderr.String(), qt.Equals, "")
	})

	t.Run("completion script is generated for the Atlas executable", func(t *testing.T) {
		c := qt.New(t)
		run := newCompatProcess(binPath, "completion", "bash")
		var stdout, stderr bytes.Buffer
		run.Stdout = &stdout
		run.Stderr = &stderr

		err := run.Run()

		c.Assert(err, qt.IsNil)
		c.Assert(stdout.String(), qt.Contains, "# bash completion V2 for atlas")
		c.Assert(stdout.String(), qt.Contains, "__start_atlas")
		c.Assert(stderr.String(), qt.Equals, "")
	})
}

// TestCompatBinaryMigrateNewSuccessIsSilent pins the real process boundary for
// stokaro/ptah#1235 findings 3.1 and 3.2: exit 0, byte-empty stdout and stderr,
// and the Atlas migration plus atlas.sum still written and valid.
func TestCompatBinaryMigrateNewSuccessIsSilent(t *testing.T) {
	c := qt.New(t)
	binPath := buildCompatBinary(c)
	dir := c.TempDir()
	run := newCompatProcess(
		binPath,
		"migrate", "new", "manual_hotfix",
		"--dir", "file://"+dir,
	)
	var stdout, stderr bytes.Buffer
	run.Stdout = &stdout
	run.Stderr = &stderr

	err := run.Run()

	c.Assert(err, qt.IsNil, qt.Commentf("stderr: %s", stderr.String()))
	c.Assert(stdout.String(), qt.Equals, "")
	c.Assert(stderr.String(), qt.Equals, "")
	migrations, globErr := filepath.Glob(filepath.Join(dir, "*_manual_hotfix.sql"))
	c.Assert(globErr, qt.IsNil)
	c.Assert(migrations, qt.HasLen, 1)
	result, verifyErr := migratesum.VerifyDirWithFormat(dir, migrator.MigrationDirFormatAtlas)
	c.Assert(verifyErr, qt.IsNil)
	c.Assert(result.OK(), qt.IsTrue)
}

func TestCompatBinaryMigrateNewFailureStaysLoud(t *testing.T) {
	c := qt.New(t)
	binPath := buildCompatBinary(c)
	dir := c.TempDir()
	run := newCompatProcess(
		binPath,
		"migrate", "new", "manual_hotfix",
		"--dir", "file://"+dir,
		"--edit",
	)
	run.Env = append(os.Environ(), "VISUAL=", "EDITOR=")
	var stdout, stderr bytes.Buffer
	run.Stdout = &stdout
	run.Stderr = &stderr

	err := run.Run()
	var exitErr *exec.ExitError

	c.Assert(err, qt.ErrorAs, &exitErr)
	c.Assert(exitErr.ExitCode(), qt.Equals, 1)
	c.Assert(stdout.String(), qt.Equals, "")
	c.Assert(stderr.String(), qt.Equals,
		"Error: no editor configured: set $EDITOR or $VISUAL, or pass --editor\n",
	)
}

func TestCompatBinaryAtlasFailurePaths(t *testing.T) {
	c := qt.New(t)
	binPath := buildCompatBinary(c)

	t.Run("checksum mismatch", func(t *testing.T) {
		c := qt.New(t)
		dir := malformedAtlasDir(c)
		run := newCompatProcess(binPath, "migrate", "validate", "--dir", "file://"+dir)
		var stdout, stderr bytes.Buffer
		run.Stdout = &stdout
		run.Stderr = &stderr
		err := run.Run()
		var exitErr *exec.ExitError

		c.Assert(err, qt.ErrorAs, &exitErr)
		c.Assert(exitErr.ExitCode(), qt.Equals, 1)
		c.Assert(stdout.String(), qt.Equals, "You have a checksum error in your migration directory.\n"+
			"Please check your migration files and run 'atlas migrate hash' to re-hash the contents\n\n")
		c.Assert(stderr.String(), qt.Equals, "Error: checksum mismatch\n")
	})

	t.Run("missing checksum file", func(t *testing.T) {
		c := qt.New(t)
		dir := atlasDirWithoutSum(c)
		run := newCompatProcess(binPath, "migrate", "validate", "--dir", "file://"+dir)
		var stdout, stderr bytes.Buffer
		run.Stdout = &stdout
		run.Stderr = &stderr
		err := run.Run()
		var exitErr *exec.ExitError

		c.Assert(err, qt.ErrorAs, &exitErr)
		c.Assert(exitErr.ExitCode(), qt.Equals, 1)
		c.Assert(stdout.String(), qt.Equals, "You have a checksum error in your migration directory.\n"+
			"Please check your migration files and run 'atlas migrate hash' to re-hash the contents\n\n")
		c.Assert(stderr.String(), qt.Equals, "Error: checksum file not found\n")
	})

	t.Run("missing checksum file refuses apply", func(t *testing.T) {
		// Measured Atlas CE v1.2.0 on a directory with no atlas.sum
		// (stokaro/ptah#970): exit 1, the same output as validate above, and
		// the target database is never created.
		c := qt.New(t)
		dir := atlasDirWithoutSum(c)
		dbPath := filepath.Join(c.TempDir(), "unhashed.db")
		run := newCompatProcess(binPath,
			"migrate", "apply",
			"--url", "sqlite://"+dbPath,
			"--dir", "file://"+dir,
		)
		var stdout, stderr bytes.Buffer
		run.Stdout = &stdout
		run.Stderr = &stderr
		err := run.Run()
		var exitErr *exec.ExitError

		c.Assert(err, qt.ErrorAs, &exitErr)
		c.Assert(exitErr.ExitCode(), qt.Equals, 1)
		c.Assert(stdout.String(), qt.Equals, "You have a checksum error in your migration directory.\n"+
			"Please check your migration files and run 'atlas migrate hash' to re-hash the contents\n\n")
		c.Assert(stderr.String(), qt.Equals, "Error: checksum file not found\n")
		_, statErr := os.Stat(dbPath)
		c.Assert(os.IsNotExist(statErr), qt.IsTrue)
	})

	t.Run("migrate set operation error", func(t *testing.T) {
		c := qt.New(t)
		dir := cleanAtlasDir(c)
		run := newCompatProcess(
			binPath,
			"migrate", "set", "2",
			"--url", "sqlite://"+filepath.Join(c.TempDir(), "state.db"),
			"--dir", "file://"+dir,
		)
		var stdout, stderr bytes.Buffer
		run.Stdout = &stdout
		run.Stderr = &stderr
		err := run.Run()
		var exitErr *exec.ExitError

		c.Assert(err, qt.ErrorAs, &exitErr)
		c.Assert(exitErr.ExitCode(), qt.Equals, 1)
		c.Assert(stdout.String(), qt.Equals, "")
		c.Assert(stderr.String(), qt.Equals, "Error: migration with version \"2\" not found\n")
	})

	t.Run("migrate set missing environment precedes version", func(t *testing.T) {
		c := qt.New(t)
		run := newCompatProcess(binPath, "migrate", "set")
		var stdout, stderr bytes.Buffer
		run.Stdout = &stdout
		run.Stderr = &stderr
		err := run.Run()
		var exitErr *exec.ExitError

		c.Assert(err, qt.ErrorAs, &exitErr)
		c.Assert(exitErr.ExitCode(), qt.Equals, 1)
		c.Assert(stdout.String(), qt.Equals, "")
		c.Assert(stderr.String(), qt.Equals, "Error: sql/migrate: stat migrations: no such file or directory\n")
	})

	t.Run("migrate set missing driver precedes version", func(t *testing.T) {
		c := qt.New(t)
		dir := cleanAtlasDir(c)
		run := newCompatProcess(
			binPath,
			"migrate", "set",
			"--dir", "file://"+dir,
		)
		var stdout, stderr bytes.Buffer
		run.Stdout = &stdout
		run.Stderr = &stderr
		err := run.Run()
		var exitErr *exec.ExitError

		c.Assert(err, qt.ErrorAs, &exitErr)
		c.Assert(exitErr.ExitCode(), qt.Equals, 1)
		c.Assert(stdout.String(), qt.Equals, "")
		// The subtest's name is now what it asserts. Measured on 2026-08-13,
		// the pinned community binary v1.3.0 answers this exact invocation --
		// a hashed directory, no positional version, no --url -- with these 66
		// bytes on standard error and nothing on standard output, so the
		// missing driver really does precede the version. This verb has no
		// required-flag check on either binary; see cell 9.14 of
		// stokaro/ptah#1235 and cmd/atlas/compat_url_diagnostic.go.
		c.Assert(stderr.String(), qt.Equals,
			"Error: sql/sqlclient: missing driver. See: https://atlasgo.io/url\n")
	})

	t.Run("migrate set missing version after environment", func(t *testing.T) {
		c := qt.New(t)
		dir := cleanAtlasDir(c)
		dbPath := filepath.Join(c.TempDir(), "state.db")
		run := newCompatProcess(
			binPath,
			"migrate", "set",
			"--url", "sqlite://"+dbPath,
			"--dir", "file://"+dir,
		)
		var stdout, stderr bytes.Buffer
		run.Stdout = &stdout
		run.Stderr = &stderr
		err := run.Run()
		var exitErr *exec.ExitError

		c.Assert(err, qt.ErrorAs, &exitErr)
		c.Assert(exitErr.ExitCode(), qt.Equals, 1)
		c.Assert(stdout.String(), qt.Equals, "")
		c.Assert(stderr.String(), qt.Equals, "Error: accepts 1 arg(s), received 0\n")
		_, statErr := os.Stat(dbPath)
		c.Assert(statErr, qt.IsNil)
	})

	t.Run("migrate set extra version", func(t *testing.T) {
		c := qt.New(t)
		dir := cleanAtlasDir(c)
		run := newCompatProcess(
			binPath,
			"migrate", "set", "1", "2",
			"--url", "sqlite://"+filepath.Join(c.TempDir(), "state.db"),
			"--dir", "file://"+dir,
		)
		var stdout, stderr bytes.Buffer
		run.Stdout = &stdout
		run.Stderr = &stderr
		err := run.Run()
		var exitErr *exec.ExitError

		c.Assert(err, qt.ErrorAs, &exitErr)
		c.Assert(exitErr.ExitCode(), qt.Equals, 1)
		c.Assert(stdout.String(), qt.Equals, "")
		c.Assert(stderr.String(), qt.Equals, "Error: accepts 1 arg(s), received 2\n")
	})

	t.Run("migrate set unknown flag", func(t *testing.T) {
		c := qt.New(t)
		run := newCompatProcess(binPath, "migrate", "set", "1", "--unknown")
		var stdout, stderr bytes.Buffer
		run.Stdout = &stdout
		run.Stderr = &stderr
		err := run.Run()
		var exitErr *exec.ExitError

		c.Assert(err, qt.ErrorAs, &exitErr)
		c.Assert(exitErr.ExitCode(), qt.Equals, 1)
		c.Assert(stdout.String(), qt.Equals, "")
		c.Assert(stderr.String(), qt.Equals, "Error: unknown flag: --unknown\n")
	})

	t.Run("unknown root command", func(t *testing.T) {
		c := qt.New(t)
		run := newCompatProcess(binPath, "definitely-not-a-command")
		var stdout, stderr bytes.Buffer
		run.Stdout = &stdout
		run.Stderr = &stderr
		err := run.Run()
		var exitErr *exec.ExitError

		c.Assert(err, qt.ErrorAs, &exitErr)
		c.Assert(exitErr.ExitCode(), qt.Equals, 1)
		c.Assert(stdout.String(), qt.Equals, "")
		c.Assert(stderr.String(), qt.Equals,
			"Error: unknown command \"definitely-not-a-command\" for \"atlas\"\n"+
				"Run 'atlas --help' for usage.\n")
	})
}

func TestCompatBinaryMigrateApplyRejectsMalformedAtlasTxMode(t *testing.T) {
	c := qt.New(t)
	binPath := buildCompatBinary(c)
	tests := []struct {
		name      string
		filename  string
		directive string
		want      string
	}{
		{
			name:      "unknown directive",
			filename:  "1_unknown.sql",
			directive: "-- atlas:txmode bogus\n\n",
			want:      "Error: unknown txmode \"bogus\" found in file directive \"1_unknown.sql\"\n",
		},
		{
			name:      "duplicate directive",
			filename:  "1_duplicate.sql",
			directive: "-- atlas:txmode none\n-- atlas:txmode file\n\n",
			want:      "Error: multiple txmode values found in file \"1_duplicate.sql\": [\"none\" \"file\"]\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := malformedAtlasTxModeDir(c, test.filename, test.directive)
			run := newCompatProcess(
				binPath,
				"migrate", "apply",
				"--url", "sqlite://"+filepath.Join(c.TempDir(), "state.db"),
				"--dir", "file://"+dir,
			)
			var stdout, stderr bytes.Buffer
			run.Stdout = &stdout
			run.Stderr = &stderr

			err := run.Run()
			var exitErr *exec.ExitError

			c.Assert(err, qt.ErrorAs, &exitErr)
			c.Assert(exitErr.ExitCode(), qt.Equals, 1)
			c.Assert(stderr.String(), qt.Equals, test.want)
			c.Assert(stdout.String(), qt.Equals, "")
		})
	}
}

func buildCompatBinary(c *qt.C) string {
	c.Helper()
	binPath := filepath.Join(c.TempDir(), "atlas")
	build := exec.Command("go", "build", "-o", binPath, ".")
	build.Env = append(os.Environ(), "GOWORK=off")
	buildOut, err := build.CombinedOutput()
	c.Assert(err, qt.IsNil, qt.Commentf("%s", buildOut))
	return binPath
}

func newCompatProcess(binPath string, args ...string) *exec.Cmd {
	return exec.Command(binPath, args...)
}

func malformedAtlasDir(c *qt.C) string {
	c.Helper()
	dir := atlasDirWithoutSum(c)
	c.Assert(os.WriteFile(filepath.Join(dir, "atlas.sum"),
		[]byte("h1:tampered\n"), 0o600), qt.IsNil)
	return dir
}

func cleanAtlasDir(c *qt.C) string {
	c.Helper()
	dir := atlasDirWithoutSum(c)
	_, err := migratesum.WriteWithFormat(dir, migrator.MigrationDirFormatAtlas)
	c.Assert(err, qt.IsNil)
	return dir
}

func atlasDirWithoutSum(c *qt.C) string {
	c.Helper()
	dir := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "1_initial.sql"),
		[]byte("CREATE TABLE t (id INT);\n"), 0o600), qt.IsNil)
	return dir
}

func malformedAtlasTxModeDir(c *qt.C, filename, directive string) string {
	c.Helper()
	dir := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, filename), []byte(
		directive+"CREATE TABLE invalid_txmode (id INTEGER PRIMARY KEY);\n",
	), 0o600), qt.IsNil)
	_, err := migratesum.WriteWithFormat(dir, migrator.MigrationDirFormatAtlas)
	c.Assert(err, qt.IsNil)
	return dir
}
