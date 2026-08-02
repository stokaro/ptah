package main_test

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/internal/migratesum"
	"github.com/stokaro/ptah/migration/migrator"
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

func TestCompatBinaryCommandFailuresExit1(t *testing.T) {
	c := qt.New(t)
	binPath := buildCompatBinary(c)

	tests := []struct {
		name       string
		command    func(string) *exec.Cmd
		wantStderr string
	}{
		{
			name: "unknown flag",
			command: func(binPath string) *exec.Cmd {
				return newCompatProcess(binPath, "version", "--bogus-flag")
			},
			wantStderr: "error: unknown flag: --bogus-flag\n",
		},
		{
			name: "mutually exclusive flags",
			command: func(binPath string) *exec.Cmd {
				return newCompatProcess(
					binPath,
					"schema", "apply",
					"--url", "sqlite://schema.db",
					"--to", "file://schema.sql",
					"--file", "schema.sql",
					"--dry-run",
				)
			},
			wantStderr: "error: if any flags in the group [file to] are set none of the others can be; [file to] were all set\n",
		},
		{
			name: "lazy completion command",
			command: func(binPath string) *exec.Cmd {
				return newCompatProcess(binPath, "completion", "bash", "extra")
			},
			wantStderr: "Error: unknown command \"extra\" for \"atlas completion bash\"\n",
		},
		{
			name: "unknown root command",
			command: func(binPath string) *exec.Cmd {
				return exec.Command(binPath, "definitely-not-a-command")
			},
			wantStderr: "Error: unknown command \"definitely-not-a-command\" for \"atlas\"\n" +
				"Run 'atlas --help' for usage.\n",
		},
		{
			name: "registered command without an implementation",
			command: func(binPath string) *exec.Cmd {
				return newCompatProcess(binPath, "migrate", "push")
			},
			wantStderr: "Error: atlas migrate push is not implemented by Ptah\n",
		},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			run := tt.command(binPath)
			runOut, err := run.CombinedOutput()
			var exitErr *exec.ExitError

			c.Assert(err, qt.ErrorAs, &exitErr, qt.Commentf("%s", runOut))
			c.Assert(exitErr.ExitCode(), qt.Equals, 1)
			c.Assert(string(runOut), qt.Equals, tt.wantStderr)
		})
	}
}

func TestCompatBinaryAtlasSuccessPaths(t *testing.T) {
	c := qt.New(t)
	binPath := buildCompatBinary(c)

	c.Run("clean validation is silent", func(c *qt.C) {
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

	c.Run("nested extra token prints help", func(c *qt.C) {
		run := newCompatProcess(binPath, "migrate", "aplly")
		var stdout, stderr bytes.Buffer
		run.Stdout = &stdout
		run.Stderr = &stderr

		err := run.Run()

		c.Assert(err, qt.IsNil)
		c.Assert(stdout.String(), qt.Contains, "Usage:\n  atlas migrate [command]")
		c.Assert(stderr.String(), qt.Equals, "")
	})

	c.Run("completion group extra token prints help", func(c *qt.C) {
		run := newCompatProcess(binPath, "completion", "sh")
		var stdout, stderr bytes.Buffer
		run.Stdout = &stdout
		run.Stderr = &stderr

		err := run.Run()

		c.Assert(err, qt.IsNil)
		c.Assert(stdout.String(), qt.Contains, "Usage:\n  atlas completion [command]")
		c.Assert(stderr.String(), qt.Equals, "")
	})

	c.Run("completion script is generated for the Atlas executable", func(c *qt.C) {
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

func TestCompatBinaryAtlasFailurePaths(t *testing.T) {
	c := qt.New(t)
	binPath := buildCompatBinary(c)

	c.Run("checksum mismatch", func(c *qt.C) {
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

	c.Run("missing checksum file", func(c *qt.C) {
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

	c.Run("missing checksum file refuses apply", func(c *qt.C) {
		// Measured Atlas CE v1.2.0 on a directory with no atlas.sum
		// (stokaro/ptah#970): exit 1, the same output as validate above, and
		// the target database is never created.
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

	c.Run("migrate set operation error", func(c *qt.C) {
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

	c.Run("migrate set missing environment precedes version", func(c *qt.C) {
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

	c.Run("migrate set missing driver precedes version", func(c *qt.C) {
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
		c.Assert(stderr.String(), qt.Equals,
			"Error: database URL is required; pass --url\n")
	})

	c.Run("migrate set missing version after environment", func(c *qt.C) {
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

	c.Run("migrate set extra version", func(c *qt.C) {
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

	c.Run("migrate set unknown flag", func(c *qt.C) {
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

	c.Run("unknown root command", func(c *qt.C) {
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
