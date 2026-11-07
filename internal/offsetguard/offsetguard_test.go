package offsetguard_test

import (
	"bytes"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
)

// listPositions are the files whose "at position %d" counts entries of a list,
// such as the third column of an INCLUDE, which no line and column could name.
// The test compares in both directions, so an entry whose sentence has gone
// leaves with it.
var listPositions = []string{
	"engine/builtin/renderer.go",
	"internal/indexscope/indexscope.go",
}

// minimumFiles is far below the tree's size, and far above what a listing that
// stopped working finds: a scan of no files reports no sentence.
const minimumFiles = 1000

// TestNoSentenceWritesAnOffset is the rule this package exists for.
func TestNoSentenceWritesAnOffset(t *testing.T) {
	c := qt.New(t)
	root := repositoryRoot(c)
	files := productionFiles(c, root)
	c.Assert(len(files) >= minimumFiles, qt.IsTrue, qt.Commentf("%d files", len(files)))

	c.Assert(filesWritingAnOffset(c, root, files), qt.DeepEquals, listPositions, qt.Commentf(
		"a refusal states its place with ptaherr.At(offset, err) and writes no position in its sentence"))
}

// TestGuardSeesASentence is the self-test: a scanner that finds nothing is what a
// broken parse or an inverted condition produces too.
func TestGuardSeesASentence(t *testing.T) {
	tests := []struct {
		name string
		src  string
		want bool
	}{
		{name: "a sentence", src: "package p\n\nvar s = \"expected a name at position %d\"\n", want: true},
		{name: "a raw sentence", src: "package p\n\nvar s = `expected a name at position %d`\n", want: true},
		{name: "a comment", src: "package p\n\n// at position %d\nvar s = 1\n", want: false},
		{name: "another word", src: "package p\n\nvar s = \"at position\"\n", want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(writesAnOffset(c, "p.go", []byte(test.src)), qt.Equals, test.want)
		})
	}
}

func filesWritingAnOffset(c *qt.C, root string, files []string) []string {
	c.Helper()
	var found []string
	for _, file := range files {
		source, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(file)))
		c.Assert(err, qt.IsNil)
		if writesAnOffset(c, file, source) {
			found = append(found, file)
		}
	}
	return found
}

// writesAnOffset reports whether a string in source spells the sentence. It reads
// strings rather than text, so a comment describing the form is not a finding.
func writesAnOffset(c *qt.C, name string, source []byte) bool {
	c.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), name, source, parser.SkipObjectResolution)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", name))
	found := false
	ast.Inspect(file, func(node ast.Node) bool {
		literal, ok := node.(*ast.BasicLit)
		if ok && literal.Kind == token.STRING && strings.Contains(literal.Value, "at position %d") {
			found = true
		}
		return true
	})
	return found
}

// productionFiles lists the tree's Go files outside tests and test fixtures, from
// git rather than the filesystem, since a linked worktree parked under the
// repository holds another branch's files.
func productionFiles(c *qt.C, root string) []string {
	c.Helper()
	cmd := exec.Command("git", "-c", "core.quotePath=false", "ls-files", "--cached", "--others", "--exclude-standard", "--", "*.go")
	cmd.Dir = root
	var out bytes.Buffer
	cmd.Stdout = &out
	c.Assert(cmd.Run(), qt.IsNil)
	files := slices.DeleteFunc(strings.Split(strings.TrimSpace(out.String()), "\n"), func(file string) bool {
		if strings.HasSuffix(file, "_test.go") || strings.Contains("/"+file, "/testdata/") {
			return true
		}
		// --cached names a file a move has taken out of the working tree.
		_, err := os.Stat(filepath.Join(root, filepath.FromSlash(file)))
		return err != nil
	})
	slices.Sort(files)
	return files
}

func repositoryRoot(c *qt.C) string {
	c.Helper()
	cmd := exec.Command("git", "rev-parse", "--show-toplevel")
	var out bytes.Buffer
	cmd.Stdout = &out
	c.Assert(cmd.Run(), qt.IsNil)
	return strings.TrimSpace(out.String())
}
