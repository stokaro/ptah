package countsubjectguard_test

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
		{
			// The construction every line-at-a-time reader in this tree missed.
			// Prose wraps at 80 columns, so the count lands at the end of one
			// comment line and its noun at the start of the next, with the
			// comment's own marker between them.
			name: "wrapped onto the next comment line",
			src: "package p\n\n// The refusal names two\n" +
				"// things that layout does not have.\nconst x = 1\n",
			want: 1,
		},
		{
			// The same wrap in a block comment, where the marker on the
			// continuation line is a bare asterisk rather than a slash pair.
			name: "wrapped inside a block comment",
			src:  "package p\n\n/* The refusal names two\n * things it cannot have.\n */\nconst x = 1\n",
			want: 1,
		},
		{
			// A blank comment line ends the paragraph, and a count that ends
			// one paragraph is not the subject of the next one's first noun.
			name: "across a paragraph break",
			src: "package p\n\n// The refusal names two\n//\n" +
				"// things that layout does not have are not counted.\nconst x = 1\n",
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

// TestGuardNamesTheLineTheCountIsOn pins the offset arithmetic a wrapped phrase
// needs.
//
// The table above counts findings, and a finding that names the wrong line is
// counted just the same. It sends the reader to a comment that reads fine,
// which is how a line number drifts and stays drifted -- so the blanking keeps
// every newline where it was, and these rows are what says so.
func TestGuardNamesTheLineTheCountIsOn(t *testing.T) {
	tests := []struct {
		name string
		src  string
		want string
	}{
		{
			name: "on one line",
			src:  "package p\n\n// A note.\n//\n// Two things follow:\nconst x = 1\n",
			want: `p.go:5: "Two things"`,
		},
		{
			name: "wrapped onto the next comment line",
			src: "package p\n\n// A note.\n//\n// The refusal names two\n" +
				"// things that layout does not have.\nconst x = 1\n",
			want: `p.go:5: "two things"`,
		},
		{
			// A quotation spanning lines inside a block comment. Blanking it
			// with spaces would swallow its newline and name line 3.
			name: "after a quotation that spans lines",
			src: "package p\n\n/* The heading is \"the rule\n" +
				" * about counting\" and the refusal names two\n" +
				" * things that layout does not have.\n */\nconst x = 1\n",
			want: `p.go:4: "two things"`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := t.TempDir()
			c.Assert(writeFile(filepath.Join(dir, "p.go"), test.src), qt.IsNil)

			c.Assert(countsInSubjectPosition(c, dir, "p.go"), qt.DeepEquals, []string{test.want})
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
		for _, para := range paragraphsOf(group, fset.Position(group.Pos()).Line) {
			for _, at := range countAsSubject.FindAllStringIndex(para.text, -1) {
				found = append(found, fmt.Sprintf("%s:%d: %q",
					rel, para.line+strings.Count(para.text[:at[0]], "\n"),
					strings.Join(strings.Fields(para.text[at[0]:at[1]]), " ")))
			}
		}
	}
	return found
}

// paragraph is one run of comment lines with no blank line in it, and the line
// that run starts on.
type paragraph struct {
	text string
	line int
}

// paragraphsOf splits a comment group at its blank lines, so a phrase is
// matched within a paragraph and never across one. A count that ends a
// paragraph is not the subject of the next paragraph's first noun, and a group
// joined whole would read it as one.
func paragraphsOf(group *ast.CommentGroup, first int) []paragraph {
	var (
		out     []paragraph
		current []string
		start   int
	)
	flush := func() {
		if len(current) > 0 {
			out = append(out, paragraph{text: strings.Join(current, "\n"), line: start})
			current = nil
		}
	}
	for offset, line := range strings.Split(commentProse(group), "\n") {
		if strings.TrimSpace(line) == "" {
			flush()
			continue
		}
		if len(current) == 0 {
			start = first + offset
		}
		current = append(current, line)
	}
	flush()
	return out
}

// commentMarker matches the punctuation a comment carries at the start of one
// of its lines: "//", the "/*" that opens a block comment, and the "*" its
// continuation lines are conventionally written with.
var commentMarker = regexp.MustCompile(`(?m)^[ \t]*(?:/[/*]+|\*+)`)

// commentProse renders one comment group as the text a phrase is matched
// across, rather than matching a line at a time.
//
// Prose wraps, and a wrapped count puts the comment's own marker between the
// number and its noun -- master carried "listing two\n// things", which every
// line-at-a-time reader in this tree reported as clean. So the marker is
// blanked rather than trimmed and the lines are joined: every offset stays
// where it was, which is what lets the finding still name the line the phrase
// ends on.
//
// A quotation may wrap the same way, and a citation of a documentation heading
// usually does. Blanking per line would leave such a quote open and read its
// contents as prose, so the delimiter parity carries across the group and
// resets with it -- the same arithmetic docs/site/scripts/check-style.mjs
// carries between Markdown lines, and for the same reason.
func commentProse(group *ast.CommentGroup) string {
	var joined strings.Builder
	open := ""
	for index, comment := range group.List {
		blanked, next := outsideQuotes(comment.Text, open)
		open = next
		joined.WriteString(commentMarker.ReplaceAllStringFunc(blanked,
			func(marker string) string { return strings.Repeat(" ", len(marker)) }))
		if index < len(group.List)-1 {
			joined.WriteString("\n")
		}
	}
	return joined.String()
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
			out = append(out, blank(line[index]))
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

// blank is the character a blanked span contributes: a space, except for a
// newline, which stays. A block comment's quotation can span lines, and a
// newline replaced by a space moves every finding after it up a line.
func blank(char byte) byte {
	if char == '\n' {
		return '\n'
	}
	return ' '
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
