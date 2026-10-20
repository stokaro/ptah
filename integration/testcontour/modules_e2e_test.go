//go:build integration

package testcontour_test

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/testcontour"
)

func TestNestedModuleOwnsItsIntegrationContour(t *testing.T) {
	c := qt.New(t)
	root := c.TempDir()
	command := exec.CommandContext(c.Context(), "git", "init", root)
	output, err := command.CombinedOutput()
	c.Assert(err, qt.IsNil, qt.Commentf("git init: %s", output))
	module := filepath.Join(root, "sdk")
	dir := filepath.Join(module, "integration")
	c.Assert(os.MkdirAll(dir, 0o750), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(root, "go.mod"), []byte("module example.org/host\n\ngo 1.26\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(module, "go.mod"), []byte("module example.org/sdk\n\ngo 1.26\n"), 0o600), qt.IsNil)
	source := []byte("//go:build integration\n\npackage integration_test\n\nimport \"testing\"\n\nfunc TestBoundary(t *testing.T) {}\n")
	c.Assert(os.WriteFile(filepath.Join(dir, "boundary_e2e_test.go"), source, 0o600), qt.IsNil)
	var log bytes.Buffer
	config := testcontour.Config{Package: "./integration/...", Tags: []string{"integration"}, Timeout: time.Minute, Dir: module, Stdout: &log, Stderr: &log}
	c.Assert(testcontour.Run(c.Context(), config), qt.IsNil, qt.Commentf("contour: %s", &log))
	c.Assert(log.String(), qt.Contains, "TestBoundary")
	c.Assert(os.Rename(dir, filepath.Join(module, "elsewhere")), qt.IsNil)
	c.Assert(testcontour.Run(c.Context(), config), qt.ErrorMatches, `integration test file sdk/elsewhere/boundary_e2e_test.go must live under integration/`)
}
