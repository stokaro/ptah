package blankimportguard_test

import (
	"bytes"
	"go/ast"
	"go/parser"
	"go/token"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
)

// TestEveryBlankImportIsJustified is the rule this package exists for.
//
// It covers the whole tree rather than the tests alone. revive already reports
// a bare blank import outside a main or test package, so the non-test half is
// expected to be clean and is asserted here anyway: a rule enforced in one
// place and relied on in another is a rule that leaves when that place changes.
func TestEveryBlankImportIsJustified(t *testing.T) {
	c := qt.New(t)
	root := repositoryRoot(c)

	var unjustified []string
	files := goFiles(c, root)
	for _, path := range files {
		unjustified = append(unjustified, bareBlankImports(c, root, path)...)
	}

	c.Assert(unjustified, qt.HasLen, 0, qt.Commentf(
		"a blank import is present for a side effect the compiler cannot name, so it says which:\n%s",
		strings.Join(unjustified, "\n")))
}

// TestGuardSeesABareBlankImport is the self-test.
//
// Without it the check above is satisfied by a scanner that finds nothing, and
// a scanner that finds nothing is exactly what a broken parse, a wrong path or
// an inverted condition produces.
func TestGuardSeesABareBlankImport(t *testing.T) {
	tests := []struct {
		name string
		src  string
		want int
	}{
		{
			name: "bare",
			src:  "package p\n\nimport (\n\t_ \"hash/fnv\"\n)\n",
			want: 1,
		},
		{
			name: "line comment",
			src:  "package p\n\nimport (\n\t_ \"hash/fnv\" // registers a hash\n)\n",
			want: 0,
		},
		{
			name: "doc comment",
			src:  "package p\n\nimport (\n\t// registers a hash\n\t_ \"hash/fnv\"\n)\n",
			want: 0,
		},
		{
			name: "a group comment does not cover the import below it",
			src:  "package p\n\nimport (\n\t// registers hashes\n\t_ \"hash/fnv\"\n\t_ \"hash/crc32\"\n)\n",
			want: 1,
		},
		{
			name: "a named import is not a blank one",
			src:  "package p\n\nimport (\n\tfnv \"hash/fnv\"\n)\n",
			want: 0,
		},
		{
			name: "an ordinary import is not a blank one",
			src:  "package p\n\nimport (\n\t\"hash/fnv\"\n)\n",
			want: 0,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := t.TempDir()
			path := filepath.Join(dir, "p.go")
			c.Assert(writeFile(path, test.src), qt.IsNil)

			c.Assert(bareBlankImports(c, dir, "p.go"), qt.HasLen, test.want)
		})
	}
}

// bareBlankImports names every blank import in one file that carries no comment
// of its own, as "path:line: import".
func bareBlankImports(c *qt.C, root, rel string) []string {
	c.Helper()
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, filepath.Join(root, rel), nil, parser.ParseComments)
	c.Assert(err, qt.IsNil, qt.Commentf("parse %s", rel))

	var bare []string
	for _, imported := range file.Imports {
		if imported.Name == nil || imported.Name.Name != "_" {
			continue
		}
		if commentText(imported.Doc) != "" || commentText(imported.Comment) != "" {
			continue
		}
		bare = append(bare, strings.Join([]string{
			rel,
			":",
			fset.Position(imported.Pos()).String()[strings.LastIndex(fset.Position(imported.Pos()).String(), ":")+1:],
			": ",
			imported.Path.Value,
		}, ""))
	}
	return bare
}

func commentText(group *ast.CommentGroup) string {
	if group == nil {
		return ""
	}
	return strings.TrimSpace(group.Text())
}

// goFiles lists the repository's Go files, tests included.
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
