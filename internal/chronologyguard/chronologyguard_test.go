package chronologyguard_test

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

// datedToAnIssue matches a preposition standing immediately in front of a Ptah
// issue reference, in either spelling a comment uses for one.
//
// The four prepositions are the ones that have only the backward reading. See
// the package doc comment for why `after` is not among them, and
// docs/site/scripts/check-implementation-chronology.mjs for the same rule over
// Markdown.
var datedToAnIssue = regexp.MustCompile(
	`(?i)\b(?:until|before|since|as of)\s+\[?(?:stokaro/ptah)?#[0-9]{3,5}\b`)

// narratedPast matches the past-habitual `used to` behind a subject that can
// only be Ptah -- a comment saying what Ptah did before rather than what it
// does.
//
// The subject list is the whole rule, and it is short on purpose. English
// writes the purpose sense with the same two words: `the key columns used to
// decide whether the source row matches` says what the columns are FOR, and
// nothing but the noun separates it from `the planner used to reconcile`. A
// pronoun cannot be an instrument that way, and neither can the product's name,
// so these four have one reading and no more.
//
// That leaves the wider shape to review, which is the same split section 16.1
// of the style guide already records for the broad words: the gate holds what
// has no second reading, and the sweep that added it went further by hand
// (stokaro/ptah#3134).
var narratedPast = regexp.MustCompile(`(?i)\b(?:it|this|that|ptah)[ \t\n]+used to[ \t\n]+[a-z]+`)

// TestNoCommentNarratesPtahsOwnPast is the second rule.
//
// It reads the comment GROUP rather than the line, because the clause wraps:
// prose written to a column budget puts `It used` at the end of one comment
// line and `to be` at the start of the next, and a line-oriented reader sees
// neither half.
func TestNoCommentNarratesPtahsOwnPast(t *testing.T) {
	c := qt.New(t)
	root := repositoryRoot(c)

	var found []string
	for _, path := range goFiles(c, root) {
		found = append(found, narratedPastIn(c, root, path)...)
	}

	c.Assert(found, qt.HasLen, 0, qt.Commentf(
		"a comment says what the code does, not what it did."+
			" The usual rewrite is the ablation -- \"without this the write path"+
			" renders an unconditional UPDATE\" says what \"it used to render one\""+
			" was reaching for, and stays true after the next change."+
			" See section 6.7 of docs/STYLE_GUIDE.md:\n%s",
		strings.Join(found, "\n")))
}

// TestNoCommentDatesAStatementToAPtahIssue is the rule this package exists for.
//
// It reads every Go file the repository tracks, tests included: a doc comment is
// documentation, and a test file's comments are where most of the reasoning in
// this tree is written down.
func TestNoCommentDatesAStatementToAPtahIssue(t *testing.T) {
	c := qt.New(t)
	root := repositoryRoot(c)

	var found []string
	for _, path := range goFiles(c, root) {
		found = append(found, datingClausesIn(c, root, path)...)
	}

	c.Assert(found, qt.HasLen, 0, qt.Commentf(
		"a comment says what holds now, and when it changed is in git."+
			" Keep the rule, the ablation or the measurement and drop the date;"+
			" cite an issue that still owns something as \"(stokaro/ptah#N)\"."+
			" See section 6.7 of docs/STYLE_GUIDE.md:\n%s",
		strings.Join(found, "\n")))
}

// TestGuardSeesAStatementDatedToAPtahIssue is the self-test.
//
// Without it the check above is satisfied by a scanner that finds nothing, and
// a scanner that finds nothing is what a broken parse, a wrong path or an
// inverted condition produces. The exemption rows matter as much as the firing
// ones: a rule that flagged them would be worked around rather than obeyed.
func TestGuardSeesAStatementDatedToAPtahIssue(t *testing.T) {
	tests := []struct {
		name string
		src  string
		want int
	}{
		{
			name: "until",
			src:  "package p\n\n// They were its entries until stokaro/ptah#1048 gave each a parser arm.\nconst x = 1\n",
			want: 1,
		},
		{
			name: "before",
			src:  "package p\n\n// The attribute had no parser arm before stokaro/ptah#934.\nconst x = 1\n",
			want: 1,
		},
		{
			name: "since",
			src:  "package p\n\n// The reader has honored this key since stokaro/ptah#942.\nconst x = 1\n",
			want: 1,
		},
		{
			name: "as of",
			src:  "package p\n\n// The quarantine is EMPTY as of stokaro/ptah#941.\nconst x = 1\n",
			want: 1,
		},
		{
			// The prefix is a convention, not the defect. A rule that asked for
			// it would be satisfied by dropping six characters.
			name: "the bare spelling of a reference",
			src:  "package p\n\n// Both verbs have read a foreign layout since #1013.\nconst x = 1\n",
			want: 1,
		},
		{
			name: "inside a function",
			src:  "package p\n\nfunc f() {\n\t// Read only since stokaro/ptah#2180.\n\t_ = 1\n}\n",
			want: 1,
		},
		{
			// A citation. The issue owns the measurement or the decision the
			// sentence rests on, and nothing about the sentence is dated.
			name: "a parenthesized citation",
			src:  "package p\n\n// A procedure is its own top-level block (stokaro/ptah#2209).\nconst x = 1\n",
			want: 0,
		},
		{
			name: "an issue that owns the work",
			src:  "package p\n\n// stokaro/ptah#2725 owns removing the SQL converter.\nconst x = 1\n",
			want: 0,
		},
		{
			// The forward reading. It names work with an owner rather than a
			// past state, and no regular expression separates the two, so
			// `after` is left to the prose rule.
			name: "a forward reference with after",
			src:  "package p\n\n// This collapses after stokaro/ptah#2725 removes the converter.\nconst x = 1\n",
			want: 0,
		},
		{
			// The prepositions are ordinary English about ordinary subjects.
			// Only a Ptah issue standing behind one is a date.
			name: "a preposition about the code itself",
			src:  "package p\n\n// The gate runs before the database connection is opened.\nconst x = 1\n",
			want: 0,
		},
		{
			name: "a preposition about a server",
			src:  "package p\n\n// MariaDB has had SEQUENCE since 10.3, and Ptah renders one.\nconst x = 1\n",
			want: 0,
		},
		{
			// A comment quoting the form in order to name it. Without the skip,
			// this package could not describe the rule it enforces.
			name: "backticked",
			src:  "package p\n\n// Never write `until stokaro/ptah#1048` in a comment.\nconst x = 1\n",
			want: 0,
		},
		{
			name: "quoted",
			src:  "package p\n\n// The banned shape is \"since stokaro/ptah#942\" and nothing else.\nconst x = 1\n",
			want: 0,
		},
		{
			// A quotation wrapping onto the next comment line. The phrase sits
			// entirely on the second line, inside a quote opened on the first,
			// so only carrying the delimiter between lines blanks it.
			name: "a quotation wrapping onto the next line",
			src: "package p\n\n// The finding reads \"a clause dating this to\n" +
				"// until stokaro/ptah#1048\" and nothing else.\nconst x = 1\n",
			want: 0,
		},
		{
			// Source is not prose. A string literal carrying the form is data
			// the program moves around, and a regular expression over the file
			// would have reported it.
			name: "a string literal is not a comment",
			src:  "package p\n\nconst x = \"until stokaro/ptah#1048\"\n",
			want: 0,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := t.TempDir()
			c.Assert(writeFile(filepath.Join(dir, "p.go"), test.src), qt.IsNil)

			c.Assert(datingClausesIn(c, dir, "p.go"), qt.HasLen, test.want)
		})
	}
}

// narratedPastIn names every comment group in one file that says what Ptah did
// before, as "path:line: phrase".
func narratedPastIn(c *qt.C, root, rel string) []string {
	c.Helper()
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, filepath.Join(root, rel), nil, parser.ParseComments)
	c.Assert(err, qt.IsNil, qt.Commentf("parse %s", rel))

	var found []string
	for _, group := range file.Comments {
		var joined strings.Builder
		open := ""
		for _, comment := range group.List {
			blanked, next := outsideQuotes(comment.Text, open)
			open = next
			joined.WriteString(strings.TrimPrefix(strings.TrimSpace(blanked), "//"))
			joined.WriteString("\n")
		}
		text := joined.String()
		for _, match := range narratedPast.FindAllString(text, -1) {
			found = append(found, fmt.Sprintf("%s:%d: %q",
				rel, fset.Position(group.Pos()).Line, strings.Join(strings.Fields(match), " ")))
		}
	}
	return found
}

// datingClausesIn names every comment line in one file that dates a statement
// to a Ptah issue, as "path:line: phrase".
func datingClausesIn(c *qt.C, root, rel string) []string {
	c.Helper()
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, filepath.Join(root, rel), nil, parser.ParseComments)
	c.Assert(err, qt.IsNil, qt.Commentf("parse %s", rel))

	var found []string
	for _, group := range file.Comments {
		// A quotation may wrap onto the next comment line. Blanking per line
		// would leave such a quote open and read its contents as prose, so the
		// delimiter parity carries across the group and resets with it -- the
		// same arithmetic internal/countsubjectguard carries, and for the same
		// reason.
		open := ""
		for _, comment := range group.List {
			blanked, next := outsideQuotes(comment.Text, open)
			open = next
			for _, match := range datedToAnIssue.FindAllString(blanked, -1) {
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

// repositoryRoot answers where the corpus starts, from git rather than from a
// relative path this file would have to keep in step with its own location.
func repositoryRoot(c *qt.C) string {
	c.Helper()
	cmd := exec.Command("git", "rev-parse", "--show-toplevel")
	var out bytes.Buffer
	cmd.Stdout = &out
	c.Assert(cmd.Run(), qt.IsNil)
	return strings.TrimSpace(out.String())
}
