//go:build integration

package engine_test

import (
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"golang.org/x/mod/modfile"

	"ptah.run/internal/featureinventory"
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

// TestRuntime_ExternalProviderImportsOnlyStableContracts holds the fixture to
// the contracts an embedder is promised. The compiler already keeps it out of
// internal packages, since it builds as another module; this keeps it to the
// packages docs/public_api.md lists under Stable Embedder API, so the fixture
// cannot pass by leaning on a sample package or on one the ledger never
// classified.
func TestRuntime_ExternalProviderImportsOnlyStableContracts(t *testing.T) {
	c := qt.New(t)
	dir, err := os.Getwd()
	c.Assert(err, qt.IsNil)
	root := filepath.Dir(filepath.Dir(dir))
	module, err := os.ReadFile(filepath.Join(root, "go.mod"))
	c.Assert(err, qt.IsNil)
	ledgerSource, err := os.ReadFile(filepath.Join(root, "docs", "public_api.md"))
	c.Assert(err, qt.IsNil)
	modulePath := featureinventory.ModulePathOf(module)
	ledger, err := featureinventory.ParseLedger(ledgerSource, modulePath)
	c.Assert(err, qt.IsNil)
	imported := moduleImports(c, filepath.Join(root, "engine", "testdata", "provider", "main.go"), modulePath)

	c.Assert(ledger.Stable, qt.Contains, modulePath+"/engine")
	c.Assert(imported, qt.Contains, modulePath+"/engine")
	for _, path := range imported {
		c.Assert(ledger.Stable, qt.Contains, path, qt.Commentf("the external provider imports %s, which is not a stable embedder contract", path))
	}
}

// moduleImports lists the imports of one Go file that name packages of
// modulePath.
func moduleImports(c *qt.C, file, modulePath string) []string {
	c.Helper()
	parsed, err := parser.ParseFile(token.NewFileSet(), file, nil, parser.ImportsOnly)
	c.Assert(err, qt.IsNil)
	var imported []string
	for _, spec := range parsed.Imports {
		path, err := strconv.Unquote(spec.Path.Value)
		c.Assert(err, qt.IsNil)
		if path == modulePath || strings.HasPrefix(path, modulePath+"/") {
			imported = append(imported, path)
		}
	}
	return imported
}
