package main_test

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

// TestPtahAtlasNamespaceRemoved pins the removal of the ptah atlas command
// tree (#850): the main binary is a purely native CLI, and Atlas-style
// invocations must fail with cobra's standard unknown-command error. The
// Atlas-compatible surface ships as the separate ptah-compat binary, covered
// by cmd/ptah-compat/main_test.go.
func TestPtahAtlasNamespaceRemoved(t *testing.T) {
	c := qt.New(t)
	binPath := buildPtahBinary(c)

	tests := []struct {
		name string
		args []string
	}{
		{name: "atlas migrate status", args: []string{"atlas", "migrate", "status"}},
		{name: "atlas schema inspect", args: []string{"atlas", "schema", "inspect"}},
		{name: "atlas alone", args: []string{"atlas"}},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			run := newPtahProcess(binPath, tt.args...)
			var stdout, stderr bytes.Buffer
			run.Stdout = &stdout
			run.Stderr = &stderr
			err := run.Run()
			var exitErr *exec.ExitError

			c.Assert(err, qt.ErrorAs, &exitErr)
			c.Assert(exitErr.ExitCode(), qt.Equals, 2)
			c.Assert(stdout.String(), qt.Equals, "")
			c.Assert(stderr.String(), qt.Contains, `unknown command "atlas" for "ptah"`)
		})
	}
}

func TestPtahNativeSchemaApplyHelpResolves(t *testing.T) {
	c := qt.New(t)
	binPath := buildPtahBinary(c)

	run := newPtahProcess(binPath, "schema", "apply", "--help")
	var stdout, stderr bytes.Buffer
	run.Stdout = &stdout
	run.Stderr = &stderr

	err := run.Run()

	c.Assert(err, qt.IsNil)
	c.Assert(stdout.String(), qt.Contains, "Usage:\n  ptah schema apply")
	c.Assert(stderr.String(), qt.Equals, "")
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

func newPtahProcess(binPath string, args ...string) *exec.Cmd {
	return exec.Command(binPath, args...)
}
