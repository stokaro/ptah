package main_test

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

func TestPtahAtlasChecksumMismatchMatchesAtlasStreams(t *testing.T) {
	c := qt.New(t)
	binPath := buildPtahBinary(c)
	dir := malformedAtlasDir(c)

	run := exec.Command(binPath, "atlas", "migrate", "validate", "--dir", dir)
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

func TestPtahAtlasUnknownCommandMatchesAtlasDiagnostic(t *testing.T) {
	c := qt.New(t)
	binPath := buildPtahBinary(c)

	run := exec.Command(binPath, "atlas", "definitely-not-a-command")
	var stdout, stderr bytes.Buffer
	run.Stdout = &stdout
	run.Stderr = &stderr
	err := run.Run()
	var exitErr *exec.ExitError

	c.Assert(err, qt.ErrorAs, &exitErr)
	c.Assert(exitErr.ExitCode(), qt.Equals, 1)
	c.Assert(stdout.String(), qt.Equals, "")
	c.Assert(stderr.String(), qt.Equals,
		"Error: unknown command \"definitely-not-a-command\" for \"ptah atlas\"\n"+
			"Run 'ptah atlas --help' for usage.\n")
}

func buildPtahBinary(c *qt.C) string {
	c.Helper()
	binPath := filepath.Join(c.TempDir(), "ptah")
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
