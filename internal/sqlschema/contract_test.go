package sqlschema_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
)

// This package reads SQL into the canonical model. It is not a general-purpose
// AST-to-model conversion service, which is what it looked like while it lived
// at internal/convert/toschema beside its inverse, and what #2725 asked to end.
//
// The distinction is not enforced by the export list: the per-node conversions
// stay exported because this package's own tests exercise them directly, and
// making fourteen black-box test files white-box to hide them would trade a
// real testing property for a naming one. What actually matters is that no
// other package builds on them, so that is what is asserted -- outside this
// package, the contract is Read.
//
// The file list is discovered from git rather than written down, so a package
// added later is covered by existing.
func TestPackageContract_OutsideCallersUseReadOnly(t *testing.T) {
	c := qt.New(t)
	root := repositoryRoot(c)

	used := referencedSymbolsOutsidePackage(c, root)

	c.Assert(used, qt.DeepEquals, []string{"Read"},
		qt.Commentf("another package reached past Read into the conversion internals; "+
			"either it wants SQL read into the model, which Read does, or it wants a "+
			"conversion service, which this package deliberately is not"))
}

// repositoryRoot is the top of the working tree, from git rather than from a
// relative path that breaks when this file moves.
func repositoryRoot(c *qt.C) string {
	c.Helper()
	out, err := exec.Command("git", "rev-parse", "--show-toplevel").Output()
	c.Assert(err, qt.IsNil)
	return strings.TrimSpace(string(out))
}

// referencedSymbolsOutsidePackage returns every sqlschema.X selector used by
// tracked Go files outside this package, sorted and deduplicated.
func referencedSymbolsOutsidePackage(c *qt.C, root string) []string {
	c.Helper()
	// Tracked files AND untracked ones git does not ignore. ls-files alone
	// sees only what is staged, so a file added in the change under review is
	// invisible to it -- measured: a planted caller was reported clean until it
	// was git add-ed. --exclude-standard is what keeps the linked worktrees
	// parked under .claude/ out, which is why this is not a filesystem walk.
	out, err := exec.Command("git", "-C", root,
		"ls-files", "--cached", "--others", "--exclude-standard", "*.go").Output()
	c.Assert(err, qt.IsNil)

	seen := make(map[string]bool)
	for relative := range strings.SplitSeq(strings.TrimSpace(string(out)), "\n") {
		for _, name := range selectorsIn(c, root, relative) {
			seen[name] = true
		}
	}
	names := make([]string, 0, len(seen))
	for name := range seen {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// selectorsIn returns the sqlschema.X names one file uses, and nothing for a
// file inside this package or one that does not import it.
func selectorsIn(c *qt.C, root, relative string) []string {
	c.Helper()
	if strings.HasPrefix(relative, "internal/sqlschema/") {
		return nil
	}
	body, err := os.ReadFile(filepath.Join(root, relative))
	if err != nil {
		return nil
	}
	if !strings.Contains(string(body), "internal/sqlschema") {
		return nil
	}
	file, err := parser.ParseFile(token.NewFileSet(), relative, body, 0)
	c.Assert(err, qt.IsNil)

	var names []string
	ast.Inspect(file, func(node ast.Node) bool {
		selector, ok := node.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		identifier, ok := selector.X.(*ast.Ident)
		if !ok || identifier.Name != "sqlschema" {
			return true
		}
		names = append(names, selector.Sel.Name)
		return true
	})
	return names
}
