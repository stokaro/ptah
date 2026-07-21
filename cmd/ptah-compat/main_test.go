package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

func TestCompatBinaryNamedAtlasResolvesRootCommands(t *testing.T) {
	c := qt.New(t)
	binPath := filepath.Join(t.TempDir(), "atlas")

	build := exec.Command("go", "build", "-o", binPath, ".")
	build.Env = append(os.Environ(), "GOWORK=off")
	buildOut, err := build.CombinedOutput()
	c.Assert(err, qt.IsNil, qt.Commentf("%s", buildOut))

	run := exec.Command(binPath, "migrate", "down", "--help")
	runOut, err := run.CombinedOutput()

	output := string(runOut)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", runOut))
	c.Assert(output, qt.Contains, "Usage:")
	c.Assert(output, qt.Contains, "atlas migrate down")
	c.Assert(output, qt.Not(qt.Contains), "atlas atlas migrate down")
}
