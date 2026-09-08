package countsubjectguard_test

import (
	"bytes"
	"errors"
	"fmt"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
)

// countAsSubject matches a cardinal of two or more standing immediately in
// front of "things". It is the same rule docs/site/scripts/check-style.mjs
// applies to Markdown, written once per language because neither reader can
// see the other's corpus.
var countAsSubject = regexp.MustCompile(
	`(?i)\b(two|three|four|five|six|seven|eight|nine|ten|eleven|twelve|[2-9]|[1-9][0-9]+)\s+things\b`)

// TestNoCommentPutsACountInTheSubjectPosition is the rule this package exists
// for.
//
// It reads every Go file the repository tracks, tests included: a doc comment
// is documentation, and a test file's comments are where most of the reasoning
// in this tree is written down.
func TestNoCommentPutsACountInTheSubjectPosition(t *testing.T) {
	c := qt.New(t)
	root := repositoryRoot(c)

	var found []string
	for _, path := range goFiles(c, root) {
		found = append(found, countsInSubjectPosition(c, root, path)...)
	}

	c.Assert(found, qt.HasLen, 0, qt.Commentf(
		"the count states what the reader can already see, and \"things\" names nothing."+
			" In front of a list the lead-in has to carry what the list does not, or it goes;"+
			" in running prose, name the noun. See section 5.3 of docs/STYLE_GUIDE.md:\n%s",
		strings.Join(found, "\n")))
}

// TestGuardSeesACountInTheSubjectPosition is the self-test.
//
// Without it the check above is satisfied by a scanner that finds nothing, and
// a scanner that finds nothing is what a broken parse, a wrong path or an
// inverted condition produces. The exemption rows matter as much as the firing
// ones: a rule that flagged them would be worked around rather than obeyed.
func TestGuardSeesACountInTheSubjectPosition(t *testing.T) {
	tests := []struct {
		name string
		src  string
		want int
	}{
		{
			name: "in front of a list",
			src:  "package p\n\n// Two things follow:\n//   - one\n//   - other\nconst x = 1\n",
			want: 1,
		},
		{
			name: "in running prose",
			src:  "package p\n\n// Three things weigh against it.\nconst x = 1\n",
			want: 1,
		},
		{
			name: "a numeral counts too",
			src:  "package p\n\n// It asserts 4 things at once.\nconst x = 1\n",
			want: 1,
		},
		{
			name: "inside a function",
			src:  "package p\n\nfunc f() {\n\t// Two things happen here.\n\t_ = 1\n}\n",
			want: 1,
		},
		{
			// Emphatic English, not a list. The rule asks for a cardinal of two
			// or more, so the singular is out of reach by construction; this row
			// records the construction rather than discriminating between rules.
			name: "one thing",
			src:  "package p\n\n// One thing is deliberately absent: a window frame clause.\nconst x = 1\n",
			want: 0,
		},
		{
			// The count really is the subject here: the sentence is about a term
			// denoting distinct referents. A rule written as "any word before
			// things" fires on this row.
			name: "different things",
			src:  "package p\n\n// Read-only is two different things, and the difference matters.\nconst x = 1\n",
			want: 0,
		},
		{
			name: "other things",
			src:  "package p\n\n// Only two other things are tolerated.\nconst x = 1\n",
			want: 0,
		},
		{
			// A comment quoting the phrase in order to name it. Without the
			// skip, this package could not describe the rule it enforces.
			name: "backticked",
			src:  "package p\n\n// Never write `Two things follow:` in front of a list.\nconst x = 1\n",
			want: 0,
		},
		{
			name: "quoted",
			src:  "package p\n\n// The banned lead-in is \"Two things follow:\" and nothing else.\nconst x = 1\n",
			want: 0,
		},
		{
			// A quotation wrapping onto the next comment line, which is what a
			// citation of a documentation heading looks like. The phrase sits
			// entirely on the second line, inside a quote opened on the first,
			// so only carrying the delimiter between lines blanks it.
			name: "a quotation wrapping onto the next line",
			src: "package p\n\n// The heading is \"the rule about\n" +
				"// Three things reaching stderr\" and nothing else.\nconst x = 1\n",
			want: 0,
		},
		{
			// Source is not prose. A string literal carrying the phrase is data
			// the program moves around, and a regular expression over the file
			// would have reported it.
			name: "a string literal is not a comment",
			src:  "package p\n\nconst x = \"Two things follow:\"\n",
			want: 0,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := t.TempDir()
			c.Assert(writeFile(filepath.Join(dir, "p.go"), test.src), qt.IsNil)

			c.Assert(countsInSubjectPosition(c, dir, "p.go"), qt.HasLen, test.want)
		})
	}
}

// countsInSubjectPosition names every comment line in one file that puts a
// count where the noun belongs, as "path:line: phrase".
func countsInSubjectPosition(c *qt.C, root, rel string) []string {
	c.Helper()
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, filepath.Join(root, rel), nil, parser.ParseComments)
	c.Assert(err, qt.IsNil, qt.Commentf("parse %s", rel))

	var found []string
	for _, group := range file.Comments {
		// A quotation may wrap onto the next comment line, and a citation of a
		// documentation heading usually does. Blanking per line would leave
		// such a quote open and read its contents as prose, so the delimiter
		// parity carries across the group and resets with it -- the same
		// arithmetic docs/site/scripts/check-style.mjs carries between Markdown
		// lines, and for the same reason.
		open := ""
		for _, comment := range group.List {
			blanked, next := outsideQuotes(comment.Text, open)
			open = next
			for _, match := range countAsSubject.FindAllString(blanked, -1) {
				found = append(found, fmt.Sprintf("%s:%d: %q",
					rel, fset.Position(comment.Pos()).Line, match))
			}
		}
	}
	return found
}

// outsideQuotes blanks the backticked and quoted spans in one comment line,
// keeping its length so a match still reports a column a reader can count to,
// and returns the delimiter a span left open for the next line ("" when none).
func outsideQuotes(line, open string) (blanked, stillOpen string) {
	out := make([]byte, 0, len(line))
	for index := 0; index < len(line); index++ {
		char := line[index : index+1]
		switch {
		case open != "":
			out = append(out, ' ')
			if char == open {
				open = ""
			}
		case char == "`" || char == `"`:
			out = append(out, ' ')
			open = char
		default:
			out = append(out, line[index])
		}
	}
	return string(out), open
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
		// --cached includes index entries removed from the working tree, so a
		// move names both the deleted source and the untracked destination
		// until it is staged. Judge what exists here.
		_, err := os.Stat(filepath.Join(root, line))
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
