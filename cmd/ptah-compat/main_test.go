package main_test

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

func TestCompatBinaryNamedAtlasResolvesRootCommands(t *testing.T) {
	c := qt.New(t)
	binPath := buildCompatBinary(c)

	run := exec.Command(binPath, "migrate", "down", "--help")
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
				return exec.Command(binPath, "version", "--bogus-flag")
			},
			wantStderr: "error: unknown flag: --bogus-flag\n",
		},
		{
			name: "mutually exclusive flags",
			command: func(binPath string) *exec.Cmd {
				return exec.Command(
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
				return exec.Command(binPath, "completion", "bash", "extra")
			},
			wantStderr: "error: unknown command \"extra\" for \"atlas completion bash\"\n",
		},
		{
			name: "unknown root command",
			command: func(binPath string) *exec.Cmd {
				return exec.Command(binPath, "definitely-not-a-command")
			},
			wantStderr: "Error: unknown command \"definitely-not-a-command\" for \"atlas\"\n" +
				"Run 'atlas --help' for usage.\n",
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

func TestCompatBinaryChecksumMismatchMatchesAtlasStreams(t *testing.T) {
	c := qt.New(t)
	binPath := buildCompatBinary(c)
	dir := malformedAtlasDir(c)

	run := exec.Command(binPath, "migrate", "validate", "--dir", dir)
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

func malformedAtlasDir(c *qt.C) string {
	c.Helper()
	dir := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "1_initial.sql"),
		[]byte("CREATE TABLE t (id INT);\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "atlas.sum"),
		[]byte("h1:tampered\n"), 0o600), qt.IsNil)
	return dir
}
