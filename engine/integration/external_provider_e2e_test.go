//go:build integration

package engine_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"

	qt "github.com/frankban/quicktest"
	"golang.org/x/mod/modfile"
)

func TestRuntime_ExternalProviderBuildsWithoutBuiltins(t *testing.T) {
	c := qt.New(t)
	dir, err := os.Getwd()
	c.Assert(err, qt.IsNil)
	root := filepath.Dir(filepath.Dir(dir))
	module, err := os.ReadFile(filepath.Join(root, "go.mod"))
	c.Assert(err, qt.IsNil)
	parsed, err := modfile.Parse("go.mod", module, nil)
	c.Assert(err, qt.IsNil)
	c.Assert(parsed.Go, qt.IsNotNil)
	goDirective := "go " + parsed.Go.Version
	external := t.TempDir()
	anchor, err := os.OpenRoot(external)
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() { c.Check(anchor.Close(), qt.IsNil) })
	manifest := "module example.org/external\n\n" + goDirective + "\n\nrequire ptah.run v0.0.0\nreplace ptah.run => " + strconv.Quote(root) + "\n"
	c.Assert(anchor.WriteFile("go.mod", []byte(manifest), 0o600), qt.IsNil)
	source, err := os.ReadFile(filepath.Join(root, "engine", "testdata", "provider", "main.go"))
	c.Assert(err, qt.IsNil)
	c.Assert(anchor.WriteFile("main.go", source, 0o600), qt.IsNil)
	run := exec.CommandContext(t.Context(), "go", "run", "-mod=mod", ".")
	run.Dir, run.Env = external, append(os.Environ(), "GOWORK=off")
	output, err := run.CombinedOutput()
	c.Assert(err, qt.IsNil, qt.Commentf("%s", output))
	c.Assert(string(output), qt.Equals, "external provider conformance passed\n")
	list := exec.CommandContext(t.Context(), "go", "list", "-mod=mod", "-deps", ".")
	list.Dir, list.Env = external, run.Env
	dependencies, err := list.CombinedOutput()
	c.Assert(err, qt.IsNil, qt.Commentf("%s", dependencies))
	for _, forbidden := range []string{"ptah.run/engine/builtin", "ptah.run/dialect/", "ptah.run/feature/", "ptah.run/dbschema", "github.com/ydb-platform/"} {
		c.Assert(string(dependencies), qt.Not(qt.Contains), forbidden)
	}
}
