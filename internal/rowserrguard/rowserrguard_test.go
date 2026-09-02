package rowserrguard_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	yaml "go.yaml.in/yaml/v3"
)

// config is the sliver of .golangci.yml this test reads. Decoding the two keys
// it needs rather than scanning the file for text keeps it blind to comments
// and to a commented-out entry, which a scan would count as coverage while the
// package went unlinted.
type config struct {
	Linters struct {
		Enable   []string `yaml:"enable"`
		Settings struct {
			RowsErrCheck struct {
				Packages []string `yaml:"packages"`
			} `yaml:"rowserrcheck"`
		} `yaml:"settings"`
	} `yaml:"linters"`
}

// TestRowsErrCheckIsEnabledHappyPath is the premise every other assertion here
// rests on. A packages list kept in perfect order under a linter nobody runs is
// the same silence the list exists to prevent, with more evidence of care.
func TestRowsErrCheckIsEnabledHappyPath(t *testing.T) {
	c := qt.New(t)
	loaded := loadConfig(c)

	c.Assert(slices.Contains(loaded.Linters.Enable, "rowserrcheck"), qt.IsTrue,
		qt.Commentf("linters.enable: %v", loaded.Linters.Enable))
	c.Assert(loaded.Linters.Settings.RowsErrCheck.Packages, qt.Contains, "database/sql")
}

// TestConfiguredPackagesMatchTheDeclarationsHappyPath compares the configured
// list with what the tree actually declares.
//
// Both directions matter. An unlisted declaring package is coverage that is
// missing and says nothing about being missing. A listed package that declares
// nothing any more is a line whose reason has gone, and it reads to the next
// reader as coverage of something.
func TestConfiguredPackagesMatchTheDeclarationsHappyPath(t *testing.T) {
	c := qt.New(t)
	loaded := loadConfig(c)

	declaring := declaringPackages(c)
	c.Assert(len(declaring) > 1, qt.IsTrue,
		qt.Commentf("found %d packages declaring a Query returning *sql.Rows; the corpus is broken", len(declaring)))

	configured := slices.DeleteFunc(slices.Clone(loaded.Linters.Settings.RowsErrCheck.Packages),
		func(pkg string) bool { return !strings.HasPrefix(pkg, modulePrefix) })
	slices.Sort(configured)

	c.Assert(configured, qt.DeepEquals, declaring)
}

// modulePrefix scopes the comparison to this repository. `database/sql` is
// listed and is not ours to find.
const modulePrefix = "go.5x5.cz/ptah"

// declaringPackages returns the sorted import paths of every package in the
// repository that declares a Query or QueryContext returning (*sql.Rows,
// error) -- as a method, as a plain function, or as an interface method, since
// rowserrcheck follows the declaration and not the caller.
func declaringPackages(c *qt.C) []string {
	root := repositoryRoot(c)
	modules := moduleDirectories(c, root)

	found := map[string]struct{}{}
	fileSet := token.NewFileSet()
	for _, relative := range trackedGoFiles(c, root) {
		file, err := parser.ParseFile(fileSet, filepath.Join(root, relative), nil, parser.SkipObjectResolution)
		c.Assert(err, qt.IsNil, qt.Commentf("parsing %s", relative))
		for range declarationsIn(file) {
			found[importPath(c, modules, relative)] = struct{}{}
		}
	}

	paths := make([]string, 0, len(found))
	for declared := range found {
		paths = append(paths, declared)
	}
	slices.Sort(paths)
	return paths
}

// declarationsIn yields one entry per qualifying declaration in file.
func declarationsIn(file *ast.File) []ast.Node {
	var declarations []ast.Node
	ast.Inspect(file, func(node ast.Node) bool {
		declaration, ok := node.(*ast.FuncDecl)
		if ok && isRowsQuery(declaration.Name.Name, declaration.Type) {
			declarations = append(declarations, declaration)
		}
		method, ok := node.(*ast.Field)
		if !ok || len(method.Names) != 1 {
			return true
		}
		signature, ok := method.Type.(*ast.FuncType)
		if ok && isRowsQuery(method.Names[0].Name, signature) {
			declarations = append(declarations, method)
		}
		return true
	})
	return declarations
}

// isRowsQuery reports whether name and signature spell a Query returning rows.
// The result shape is what rowserrcheck follows, so a same-named method
// returning something else is correctly not a reason to configure a package.
func isRowsQuery(name string, signature *ast.FuncType) bool {
	if name != "Query" && name != "QueryContext" {
		return false
	}
	if signature.Results == nil || len(signature.Results.List) != 2 {
		return false
	}
	pointer, ok := signature.Results.List[0].Type.(*ast.StarExpr)
	if !ok {
		return false
	}
	selector, ok := pointer.X.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	qualifier, ok := selector.X.(*ast.Ident)
	return ok && qualifier.Name == "sql" && selector.Sel.Name == "Rows"
}

// importPath resolves a repository-relative file to the import path of the
// package holding it, using the innermost module that contains it. A nested
// module carries its own path, so the answer is not the root module's path with
// the directory appended.
func importPath(c *qt.C, modules map[string]string, relative string) string {
	directory := filepath.ToSlash(filepath.Dir(relative))
	for candidate := directory; ; candidate = path.Dir(candidate) {
		module, ok := modules[candidate]
		if ok {
			within, err := filepath.Rel(candidate, directory)
			c.Assert(err, qt.IsNil)
			return path.Join(module, filepath.ToSlash(within))
		}
		if candidate == "." {
			break
		}
	}
	c.Fatalf("no module contains %s", relative)
	return ""
}

// moduleDirectories maps each module's repository-relative directory to its
// declared module path.
func moduleDirectories(c *qt.C, root string) map[string]string {
	modules := map[string]string{}
	for _, relative := range trackedFiles(c, root, "go.mod") {
		contents, err := os.ReadFile(filepath.Join(root, relative))
		c.Assert(err, qt.IsNil)
		for line := range strings.Lines(string(contents)) {
			declared, found := strings.CutPrefix(strings.TrimSpace(line), "module ")
			if found {
				modules[filepath.ToSlash(filepath.Dir(relative))] = strings.TrimSpace(declared)
				break
			}
		}
	}
	c.Assert(len(modules) > 0, qt.IsTrue, qt.Commentf("no go.mod is tracked; the corpus is broken"))
	return modules
}

func trackedGoFiles(c *qt.C, root string) []string {
	return trackedFiles(c, root, "*.go")
}

// trackedFiles asks git rather than walking the filesystem, for the reason
// scripts/check-test-style.sh already documents: a walk descends into linked
// worktrees parked under this one and reports files belonging to a different
// checkout.
func trackedFiles(c *qt.C, root, pattern string) []string {
	command := exec.Command("git", "-c", "core.quotePath=false",
		"ls-files", "--cached", "--others", "--exclude-standard", "--", pattern)
	command.Dir = root
	output, err := command.Output()
	c.Assert(err, qt.IsNil)

	files := strings.Fields(string(output))
	c.Assert(len(files) > 0, qt.IsTrue, qt.Commentf("git listed no %s files", pattern))
	return files
}

func repositoryRoot(c *qt.C) string {
	output, err := exec.Command("git", "rev-parse", "--show-toplevel").Output()
	c.Assert(err, qt.IsNil)
	return strings.TrimSpace(string(output))
}

func loadConfig(c *qt.C) config {
	contents, err := os.ReadFile(filepath.Join(repositoryRoot(c), ".golangci.yml"))
	c.Assert(err, qt.IsNil)

	var loaded config
	c.Assert(yaml.Unmarshal(contents, &loaded), qt.IsNil)
	return loaded
}
