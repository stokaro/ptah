package clirunguard_test

import (
	"bytes"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
)

// clirunPath is the import path this guard follows.
const clirunPath = "ptah.run/internal/clirun"

// minimumImporters is the fewest directories that import clirun this guard
// accepts finding. The tree had nine when the guard was written; a scan that
// finds none reports no finding and reads as a pass, so the floor is what says
// the scan still reaches them.
const minimumImporters = 5

// TestEveryPackageThatBuildsRemovesItsBuild is the rule this package exists
// for: see the package documentation.
func TestEveryPackageThatBuildsRemovesItsBuild(t *testing.T) {
	c := qt.New(t)
	root := repositoryRoot(c)

	findings, importers := scan(c, root, goFiles(c, root))

	c.Assert(len(importers) >= minimumImporters, qt.IsTrue, qt.Commentf(
		"found %d directories importing %s: %v; a scan that finds none is also green",
		len(importers), clirunPath, importers))
	c.Assert(findings, qt.HasLen, 0, qt.Commentf(
		"clirun.Build leaves a program of about 120 MB in the temp directory unless the test binary "+
			"removes it, which clirun.Main does:\n%s", strings.Join(findings, "\n")))
}

// TestGuardSeesAPackageThatDoesNotRemoveItsBuild is the self-test. Each row is
// a directory tree the scan reads, and the findings it has to report.
//
// Without it the check above is satisfied by a scan that finds nothing, which
// is what a wrong import path, a parse that skipped the tagged files or an
// inverted condition produces.
func TestGuardSeesAPackageThatDoesNotRemoveItsBuild(t *testing.T) {
	const imports = "import (\n\t\"testing\"\n\n\t\"ptah.run/internal/clirun\"\n)\n"
	const uses = "func TestRun(t *testing.T) { _ = clirun.Ptah }\n"
	const main = "func TestMain(m *testing.M) { clirun.Main(m) }\n"
	tests := []struct {
		name  string
		files map[string]string
		want  []string
	}{
		{
			name: "a package that builds without TestMain",
			files: map[string]string{
				"e2e/a_test.go": "//go:build integration\n\npackage e2e_test\n\n" + imports + uses,
			},
			want: []string{"e2e: e2e/a_test.go imports clirun, and no TestMain in e2e calls clirun.Main"},
		},
		{
			name: "a TestMain that does not call clirun.Main",
			files: map[string]string{
				"e2e/a_test.go": "package e2e_test\n\n" + imports + uses +
					"func TestMain(m *testing.M) { _ = clirun.Compat; m.Run() }\n",
			},
			want: []string{"e2e: e2e/a_test.go imports clirun, and no TestMain in e2e calls clirun.Main"},
		},
		{
			name: "clirun.Main called from a function other than TestMain",
			files: map[string]string{
				"e2e/a_test.go": "package e2e_test\n\n" + imports + uses +
					"func setUp(m *testing.M) { clirun.Main(m) }\n",
			},
			want: []string{"e2e: e2e/a_test.go imports clirun, and no TestMain in e2e calls clirun.Main"},
		},
		{
			name: "a TestMain that calls it",
			files: map[string]string{
				"e2e/a_test.go":    "package e2e_test\n\n" + imports + uses,
				"e2e/main_test.go": "//go:build integration\n\npackage e2e_test\n\n" + imports + main,
			},
			want: make([]string, 0),
		},
		{
			name: "a TestMain that calls it under another import name",
			files: map[string]string{
				"e2e/a_test.go": "package e2e_test\n\n" + imports + uses,
				"e2e/main_test.go": "package e2e_test\n\nimport (\n\t\"testing\"\n\n\tcr \"ptah.run/internal/clirun\"\n)\n\n" +
					"func TestMain(m *testing.M) { cr.Main(m) }\n",
			},
			want: make([]string, 0),
		},
		{
			name: "a TestMain calling Main of another package",
			files: map[string]string{
				"e2e/a_test.go": "package e2e_test\n\n" + imports + uses,
				"e2e/main_test.go": "package e2e_test\n\nimport (\n\t\"testing\"\n\n\tclirun \"example.com/other\"\n)\n\n" +
					"func TestMain(m *testing.M) { clirun.Main(m) }\n",
			},
			want: []string{"e2e: e2e/a_test.go imports clirun, and no TestMain in e2e calls clirun.Main"},
		},
		{
			// An internal test file and an external one build into one test
			// binary, so a TestMain in either serves both.
			name: "a TestMain in the internal test package of the directory",
			files: map[string]string{
				"e2e/a_test.go":             "package e2e_test\n\n" + imports + uses,
				"e2e/main_internal_test.go": "package e2e\n\n" + imports + main,
			},
			want: make([]string, 0),
		},
		{
			name: "a TestMain in another directory",
			files: map[string]string{
				"e2e/a_test.go":        "package e2e_test\n\n" + imports + uses,
				"other/main_test.go":   "package other_test\n\n" + imports + main,
				"e2e/nested/b_test.go": "package nested_test\n\n" + imports + uses,
			},
			want: []string{
				"e2e/nested: e2e/nested/b_test.go imports clirun, and no TestMain in e2e/nested calls clirun.Main",
				"e2e: e2e/a_test.go imports clirun, and no TestMain in e2e calls clirun.Main",
			},
		},
		{
			name: "a library file that imports clirun",
			files: map[string]string{
				"helper/helper.go": "package helper\n\nimport \"ptah.run/internal/clirun\"\n\nvar Target = clirun.Ptah\n",
			},
			want: []string{"helper/helper.go: only a test file may import clirun"},
		},
		{
			name: "a package that does not import clirun",
			files: map[string]string{
				"plain/a_test.go": "package plain_test\n\nimport \"testing\"\n\nfunc TestA(t *testing.T) {}\n",
			},
			want: make([]string, 0),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			root := c.TempDir()
			files := make([]string, 0, len(test.files))
			for name, content := range test.files {
				c.Assert(os.MkdirAll(filepath.Join(root, filepath.FromSlash(path.Dir(name))), 0o750), qt.IsNil)
				c.Assert(os.WriteFile(filepath.Join(root, filepath.FromSlash(name)), []byte(content), 0o600), qt.IsNil)
				files = append(files, name)
			}
			slices.Sort(files)

			findings, _ := scan(c, root, files)

			c.Assert(findings, qt.DeepEquals, test.want)
		})
	}
}

// TestMainKeepsTheCodeOfAFailingPackage runs a test package whose one test
// fails through clirun.Main, and expects go test to report the failure.
//
// clirun.Main returns instead of exiting, and the test binary exits with the
// code m.Run returned. Were it to lose that code, every end-to-end package
// would pass whatever its tests found. The check lives here rather than beside
// clirun because clirun's own tests run through clirun.Main, so a Main that
// lost the code would lose the failure of the test that checks it too. This
// package does not run through it.
func TestMainKeepsTheCodeOfAFailingPackage(t *testing.T) {
	c := qt.New(t)

	output, err := exec.Command("go", "test", "-count=1", "./testdata/failing").CombinedOutput()

	exit, isExit := errors.AsType[*exec.ExitError](err)
	c.Assert(isExit, qt.IsTrue, qt.Commentf("err: %v\noutput:\n%s", err, output))
	c.Assert(exit.ExitCode(), qt.Equals, 1)
	c.Assert(string(output), qt.Contains, "this test fails on purpose")
}

// scan reads files, slash-separated paths relative to root, and returns what
// breaks the rule and the directories whose test files import clirun, both
// sorted.
func scan(c *qt.C, root string, files []string) (findings, importers []string) {
	c.Helper()
	findings = make([]string, 0)
	importing := make(map[string][]string)
	for _, name := range files {
		file := parse(c, root, name, parser.ImportsOnly)
		if _, imported := clirunName(file); !imported {
			continue
		}
		if !strings.HasSuffix(name, "_test.go") {
			findings = append(findings, name+": only a test file may import clirun")
			continue
		}
		importing[path.Dir(name)] = append(importing[path.Dir(name)], name)
	}

	for dir, importingFiles := range importing {
		importers = append(importers, dir)
		if callsMain(c, root, dir, files) {
			continue
		}
		findings = append(findings, fmt.Sprintf(
			"%s: %s imports clirun, and no TestMain in %s calls clirun.Main",
			dir, strings.Join(importingFiles, ", "), dir))
	}
	slices.Sort(findings)
	slices.Sort(importers)
	return findings, importers
}

// callsMain reports whether a test file directly in dir declares a TestMain
// that calls clirun.Main.
func callsMain(c *qt.C, root, dir string, files []string) bool {
	c.Helper()
	for _, name := range files {
		if path.Dir(name) != dir || !strings.HasSuffix(name, "_test.go") {
			continue
		}
		file := parse(c, root, name, parser.SkipObjectResolution)
		local, imported := clirunName(file)
		if !imported {
			continue
		}
		for _, decl := range file.Decls {
			function, ok := decl.(*ast.FuncDecl)
			if ok && function.Recv == nil && function.Name.Name == "TestMain" && calls(function.Body, local, "Main") {
				return true
			}
		}
	}
	return false
}

// calls reports whether body calls local.name anywhere in it.
func calls(body *ast.BlockStmt, local, name string) bool {
	found := false
	ast.Inspect(body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return !found
		}
		selector, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return !found
		}
		receiver, ok := selector.X.(*ast.Ident)
		found = found || (ok && receiver.Name == local && selector.Sel.Name == name)
		return !found
	})
	return found
}

// clirunName returns the name file imports clirun under, and whether it
// imports it at all.
func clirunName(file *ast.File) (string, bool) {
	for _, imported := range file.Imports {
		importPath, err := strconv.Unquote(imported.Path.Value)
		if err != nil || importPath != clirunPath {
			continue
		}
		if imported.Name != nil {
			return imported.Name.Name, true
		}
		return path.Base(clirunPath), true
	}
	return "", false
}

// parse reads one file whatever its build constraints.
func parse(c *qt.C, root, name string, mode parser.Mode) *ast.File {
	c.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), filepath.Join(root, filepath.FromSlash(name)), nil, mode)
	c.Assert(err, qt.IsNil, qt.Commentf("parse %s", name))
	return file
}

// goFiles lists the repository's Go files, tests included, slash-separated.
//
// git is the path source for the reason scripts/check-test-style.sh gives: a
// filesystem walk descends into every linked worktree parked under the
// repository, and judges code that is not in this working tree at all.
func goFiles(c *qt.C, root string) []string {
	c.Helper()
	cmd := exec.Command("git", "-c", "core.quotePath=false",
		"ls-files", "--cached", "--others", "--exclude-standard", "--", "*.go")
	cmd.Dir = root
	var out bytes.Buffer
	cmd.Stdout = &out
	c.Assert(cmd.Run(), qt.IsNil)

	var paths []string
	for line := range strings.SplitSeq(strings.TrimSpace(out.String()), "\n") {
		if line == "" {
			continue
		}
		// --cached includes index entries that have been removed from the
		// working tree. Judge the files that exist in this working tree.
		_, err := os.Stat(filepath.Join(root, filepath.FromSlash(line)))
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		c.Assert(err, qt.IsNil, qt.Commentf("stat %s", line))
		paths = append(paths, line)
	}
	c.Assert(len(paths) > 100, qt.IsTrue, qt.Commentf(
		"selected %d files; a guard that scans nothing is also green", len(paths)))
	return paths
}

func repositoryRoot(c *qt.C) string {
	c.Helper()
	cmd := exec.Command("git", "rev-parse", "--show-toplevel")
	var out bytes.Buffer
	cmd.Stdout = &out
	c.Assert(cmd.Run(), qt.IsNil)
	return strings.TrimSpace(out.String())
}
