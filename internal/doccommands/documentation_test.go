package doccommands_test

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlascompatpolicy"
	"ptah.run/internal/cli/root"
	"ptah.run/internal/cmdrefviews"
	"ptah.run/internal/doccommands"
)

// The corpus floors. A pattern that stopped matching would read as pages with
// no command lines in them, and a gate that read none reports exactly what a
// gate that read them all reports. Each floor sits below what the tree holds
// so that ordinary editing never trips it.
const (
	minDocumentationFiles = 250
	minDocumentedLines    = 2500
	minFencedLines        = 650
	minCompatLines        = 500
	minAuthoredHelpLines  = 30
)

// exemption is code that begins with a program name and is not a command line
// to run. It is named by page and by its exact text, and it says why.
type exemption struct {
	file, text, why string
}

const (
	absent = "names a spelling in order to say the native binary does not have it"
	quoted = "quotes text the binary prints, which begins with the program's name"
)

// notCommands are the exemptions. An entry covers every place its page writes
// its text. Every entry has to be refused by the tree it names: one the tree
// accepts is a command after all, and one no page writes any more is dead, and
// the gate fails on both. So the list cannot grow stale, and "there is no ptah
// generate" is a claim this test keeps true.
var notCommands = []exemption{
	{"AGENTS.md", "ptah generate", absent},
	{"AGENTS.md", "ptah compare", absent},
	{"AGENTS.md", "ptah read-db", absent},
	{"AGENTS.md", "ptah drop-all", absent},
	{"AGENTS.md", "ptah migrate", absent},
	{"docs/STYLE_GUIDE.md", "ptah migrate apply", absent},
	{"docs/conformance.md", "ptah-compat cloud", "shows the compatibility binary refusing the cloud group, as the pinned Atlas binary does"},
	{"docs/conformance.md", "ptah-compat cloud database list", "shows the compatibility binary refusing the cloud group, as the pinned Atlas binary does"},
	{"docs/conformance.md", "ptah-compat exits 0 where Atlas CE exits 1", "names a class of measured difference"},
	{"docs/adr/0004-constrained-artifact-mutation.md", "ptah - ptah_apply_patch", "quotes how a heading renders"},
	{"docs/site/src/content/docs/inference/troubleshooting.md", "ptah inference works against PostgreSQL with pgvector, against YDB, and against a provider engine", quoted},
}

// trees are the two binaries' command trees.
func trees() doccommands.Trees {
	return doccommands.NewTrees(root.NewRootCommand(), cmdrefviews.Compat(atlascompatpolicy.Full()))
}

// Every ptah and ptah-compat command line a page publishes resolves against
// the tree of the binary it names: the verbs exist and take the flags it
// passes. The pages are every Markdown and MDX file the repository tracks,
// which reaches the site, docs/*.md, the decision records, examples,
// integration notes, package READMEs, AGENTS.md and the agent skills.
func TestDocumentedCommandLinesResolve(t *testing.T) {
	c := qt.New(t)
	repository := repositoryRoot(c)
	files := documentationFiles(c, repository)
	lines := extractAll(c, repository, files)

	c.Assert(len(files) >= minDocumentationFiles, qt.IsTrue, qt.Commentf("read %d pages", len(files)))
	c.Assert(len(lines) >= minDocumentedLines, qt.IsTrue, qt.Commentf("found %d command lines", len(lines)))
	c.Assert(fencedLines(lines) >= minFencedLines, qt.IsTrue, qt.Commentf("found %d fenced command lines", fencedLines(lines)))
	c.Assert(programLines(lines, doccommands.Compat) >= minCompatLines, qt.IsTrue,
		qt.Commentf("found %d ptah-compat command lines", programLines(lines, doccommands.Compat)))

	findings, unused := judge(trees(), lines, notCommands)
	c.Assert(findings, qt.DeepEquals, []string(nil))
	c.Assert(unused, qt.DeepEquals, []string(nil))
}

// The --help texts are documentation the reader meets without opening a page,
// and a command line in one is copied as readily as one on the site. The floor
// counts the lines Ptah's authors write, since cobra's completion help and the
// adapter's sentence would hold any other count up on their own.
func TestHelpCommandLinesResolve(t *testing.T) {
	c := qt.New(t)
	native, compat := root.NewRootCommand(), cmdrefviews.Compat(atlascompatpolicy.Full())
	checker := doccommands.NewTrees(native, compat)
	lines := append(doccommands.ExtractHelp(native), doccommands.ExtractHelp(compat)...)

	c.Assert(authoredLines(lines) >= minAuthoredHelpLines, qt.IsTrue, qt.Commentf("found %d authored help command lines", authoredLines(lines)))
	findings, _ := judge(checker, lines, nil)
	c.Assert(findings, qt.DeepEquals, []string(nil))
}

// The controls hold the real trees to what the fixture tests show on a small
// one, so an empty tree or a Prepare that stopped preparing cannot pass the
// gate by refusing nothing.
func TestTheRealTreesRefuseAndAccept_HappyPath(t *testing.T) {
	checker := trees()
	for _, line := range []doccommands.Line{
		{Program: doccommands.Native, Args: []string{"migrations", "up", "--db-url", "sqlite://app.db", "--migrations-dir", "./migrations"}},
		{Program: doccommands.Native, Args: []string{"completion", "zsh"}},
		{Program: doccommands.Compat, Args: []string{"migrate", "apply", "--dir", "file://migrations", "--url", "sqlite://app.db"}},
		// A forwarding verb takes the flags its native target registers.
		{Program: doccommands.Compat, Args: []string{"migrate", "down", "--log-level", "info"}},
	} {
		t.Run(strings.Join(line.Args, " "), func(t *testing.T) {
			c := qt.New(t)
			c.Assert(checker.Check(line), qt.IsNil)
		})
	}
}

func TestTheRealTreesRefuseAndAccept_FailurePath(t *testing.T) {
	checker := trees()
	for _, test := range []struct {
		line doccommands.Line
		want string
	}{
		{doccommands.Line{Program: doccommands.Native, Args: []string{"migrations", "up", "--from", "x"}}, `"ptah migrations up" has no flag --from`},
		{doccommands.Line{Program: doccommands.Native, Args: []string{"migrate", "apply"}}, `"ptah" has no subcommand "migrate"`},
		// migrations runs, and refuses a word it has no verb for.
		{doccommands.Line{Program: doccommands.Native, Args: []string{"migrations", "apply"}}, `"ptah migrations" has no subcommand "apply"`},
		{doccommands.Line{Program: doccommands.Compat, Args: []string{"cloud"}}, `"ptah-compat" has no subcommand "cloud"`},
		{doccommands.Line{Program: doccommands.Compat, Args: []string{"migrate", "down", "--bogus-flag", "x"}}, `"ptah-compat migrate down" has no flag --bogus-flag`},
		// The target runs detached, so a flag the native tree would add is not
		// one it parses.
		{doccommands.Line{Program: doccommands.Compat, Args: []string{"migrate", "hash", "--migrations-dir", "x"}}, `"ptah-compat migrate hash" has no flag --migrations-dir`},
	} {
		t.Run(strings.Join(test.line.Args, " "), func(t *testing.T) {
			c := qt.New(t)
			c.Assert(checker.Check(test.line), qt.ErrorMatches, test.want)
		})
	}
}

// judge checks every line and returns the findings no exemption covers, and
// the exemptions that covered nothing.
func judge(checker doccommands.Trees, lines []doccommands.Line, exemptions []exemption) (findings, unused []string) {
	used := make(map[exemption]bool)
	for _, line := range lines {
		err := checker.Check(line)
		if err == nil {
			continue
		}
		if exempt, ok := exempted(exemptions, line); ok {
			used[exempt] = true
			continue
		}
		findings = append(findings, fmt.Sprintf("%s:%d: %v, in: %s", line.File, line.Line, err, line.Text))
	}
	for _, exempt := range exemptions {
		if !used[exempt] {
			unused = append(unused, fmt.Sprintf("%s: %q is accepted or no longer written; remove its exemption", exempt.file, exempt.text))
		}
	}
	return findings, unused
}

func exempted(exemptions []exemption, line doccommands.Line) (exemption, bool) {
	for _, exempt := range exemptions {
		if exempt.file == line.File && exempt.text == line.Text {
			return exempt, true
		}
	}
	return exemption{}, false
}

func authoredLines(lines []doccommands.Line) int {
	count := 0
	for _, line := range lines {
		if !line.Generated {
			count++
		}
	}
	return count
}

func fencedLines(lines []doccommands.Line) int {
	count := 0
	for _, line := range lines {
		if !line.Inline {
			count++
		}
	}
	return count
}

func programLines(lines []doccommands.Line, program string) int {
	count := 0
	for _, line := range lines {
		if line.Program == program {
			count++
		}
	}
	return count
}

func extractAll(c *qt.C, repository string, files []string) []doccommands.Line {
	c.Helper()
	var lines []doccommands.Line
	for _, file := range files {
		source, err := os.ReadFile(filepath.Join(repository, file))
		c.Assert(err, qt.IsNil)
		lines = append(lines, doccommands.Extract(file, string(source))...)
	}
	return lines
}

// documentationFiles lists the Markdown and MDX files the repository tracks,
// and the untracked ones it would track, so a page being written is read
// before it is committed.
//
// git is the path source for the reason scripts/check-test-style.sh gives: a
// filesystem walk descends into every linked worktree parked under the
// repository, and judges pages that are not in this working tree at all. A
// testdata file is a test's input rather than documentation, and may be wrong
// on purpose.
func documentationFiles(c *qt.C, repository string) []string {
	c.Helper()
	cmd := exec.Command("git", "-c", "core.quotePath=false",
		"ls-files", "--cached", "--others", "--exclude-standard", "--", "*.md", "*.mdx")
	cmd.Dir = repository
	var out bytes.Buffer
	cmd.Stdout = &out
	c.Assert(cmd.Run(), qt.IsNil)

	var files []string
	for line := range strings.SplitSeq(strings.TrimSpace(out.String()), "\n") {
		if line == "" || strings.Contains("/"+line, "/testdata/") {
			continue
		}
		// --cached names an index entry the working tree has removed, so a
		// move lists the deleted source until it is staged.
		_, err := os.Stat(filepath.Join(repository, line))
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		c.Assert(err, qt.IsNil, qt.Commentf("stat %s", line))
		files = append(files, line)
	}
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
