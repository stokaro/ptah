package embedguard_test

import (
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/embedguard"
)

// TestScan_ReadsNoLinkedWorktree is the rule AGENTS.md sets for every gate that
// enumerates files: ask git, never the filesystem.
//
// A linked worktree parked under the repository is an ordinary directory to a
// walk. Here the worktree holds the only caller of a declaration this checkout
// never calls, so a scan that read the worktree would count the call and stay
// silent about a decision the checkout does not make.
func TestScan_ReadsNoLinkedWorktree(t *testing.T) {
	c := qt.New(t)
	repository, worktree := moduleWithLinkedWorktree(c)
	writeCaller(c, worktree, "internal/caller")

	findings, err := embedguard.Scan(repository)

	c.Assert(err, qt.IsNil)
	c.Assert(findingNames(findings), qt.DeepEquals, []string{"Uncalled"})
}

// TestScan_CountsACallerInTheCheckout is the control for the test above. The
// same caller written in the checkout itself, and not yet staged, is a caller,
// so the finding above is about where the caller is and not about a caller the
// scan cannot read at all.
func TestScan_CountsACallerInTheCheckout(t *testing.T) {
	c := qt.New(t)
	repository, _ := moduleWithLinkedWorktree(c)
	writeCaller(c, repository, "internal/caller")

	findings, err := embedguard.Scan(repository)

	c.Assert(err, qt.IsNil)
	c.Assert(findings, qt.HasLen, 0)
}

// TestScan_ReadsNoTestdata holds the other trees the scan leaves out by name: a
// caller under testdata is a fixture, not a caller this repository maintains.
func TestScan_ReadsNoTestdata(t *testing.T) {
	c := qt.New(t)
	repository, _ := moduleWithLinkedWorktree(c)
	writeCaller(c, repository, "internal/caller/testdata")

	findings, err := embedguard.Scan(repository)

	c.Assert(err, qt.IsNil)
	c.Assert(findingNames(findings), qt.DeepEquals, []string{"Uncalled"})
}

// TestScan_ReadsAMoveBeforeItIsStaged covers the window in which git names a
// file twice: the index still holds the source of a move, and the destination
// is untracked. The scan reads the file that exists and does not fail on the
// one that does not.
func TestScan_ReadsAMoveBeforeItIsStaged(t *testing.T) {
	c := qt.New(t)
	repository, _ := moduleWithLinkedWorktree(c)
	source := filepath.Join(repository, "internal", "embedfixture", "fixture.go")
	c.Assert(os.Rename(source, filepath.Join(filepath.Dir(source), "moved.go")), qt.IsNil)

	findings, err := embedguard.Scan(repository)

	c.Assert(err, qt.IsNil)
	c.Assert(findings, qt.HasLen, 1)
	c.Assert(findings[0].File, qt.Equals, "internal/embedfixture/moved.go")
}

// TestScan_LeavesOutWhatExemptNames holds Scan to Exempt: a declaration
// nothing calls is not reported when Exempt names it.
func TestScan_LeavesOutWhatExemptNames(t *testing.T) {
	c := qt.New(t)
	repository, _ := moduleWithLinkedWorktree(c)
	exempt := slices.Sorted(maps.Keys(embedguard.Exempt))[0]
	writeSource(c, repository, "internal/embedfixture/exempt.go",
		"package embedfixture\n\n// "+exempt+" is exempt.\nfunc "+exempt+"() {}\n")

	findings, err := embedguard.Scan(repository)

	c.Assert(err, qt.IsNil)
	c.Assert(findingNames(findings), qt.DeepEquals, []string{"Uncalled"})
}

// TestDeclarations_ParsesOnlyTheVertical holds Declarations to the files it
// answers about: a file elsewhere in the module that does not parse leaves it
// unaffected. TestScan_FailurePath is the control, since Scan reads every file
// and has to refuse the same tree.
func TestDeclarations_ParsesOnlyTheVertical(t *testing.T) {
	c := qt.New(t)
	repository, _ := moduleWithLinkedWorktree(c)
	writeSource(c, repository, "internal/broken/broken.go", brokenSource)

	declared, err := embedguard.Declarations(repository)

	c.Assert(err, qt.IsNil)
	c.Assert(findingNames(declared), qt.DeepEquals, []string{"Uncalled"})
}

func TestScan_FailurePath(t *testing.T) {
	c := qt.New(t)
	repository, _ := moduleWithLinkedWorktree(c)
	writeSource(c, repository, "internal/broken/broken.go", brokenSource)

	findings, err := embedguard.Scan(repository)

	c.Assert(err, qt.ErrorMatches, `parse internal/broken/broken\.go: .*`)
	c.Assert(findings, qt.IsNil)
}

// brokenSource is a Go file outside the inference vertical that does not parse.
const brokenSource = "// Package broken does not parse.\npackage broken\n\nfunc {\n"

// moduleWithLinkedWorktree commits a module declaring one exported function in
// an inference package, and adds a linked worktree of it under the
// repository, where this repository's agents park theirs.
func moduleWithLinkedWorktree(c *qt.C) (repository, worktree string) {
	c.Helper()
	repository = c.TempDir()
	writeSource(c, repository, "go.mod", "module example.invalid/guarded\n\ngo 1.26\n")
	writeSource(c, repository, "internal/embedfixture/fixture.go",
		"// Package embedfixture declares what the scan looks for.\npackage embedfixture\n\n"+
			"// Uncalled is called only where the scan must not look.\nfunc Uncalled() {}\n")
	runGit(c, repository, "init", "--quiet")
	runGit(c, repository, "add", ".")
	runGit(c, repository, "commit", "--quiet", "-m", "fixture")
	worktree = filepath.Join(repository, ".claude", "worktrees", "parked")
	runGit(c, repository, "worktree", "add", "--quiet", "-b", "parked", worktree)
	return repository, worktree
}

// writeCaller adds a package in directory dir under root that calls the
// fixture's declaration.
func writeCaller(c *qt.C, root, dir string) {
	c.Helper()
	writeSource(c, root, dir+"/caller.go",
		"// Package caller calls the fixture.\npackage caller\n\n"+
			"import \"example.invalid/guarded/internal/embedfixture\"\n\n"+
			"// Use calls the fixture.\nfunc Use() { embedfixture.Uncalled() }\n")
}

func writeSource(c *qt.C, root, rel, content string) {
	c.Helper()
	path := filepath.Join(root, filepath.FromSlash(rel))
	c.Assert(os.MkdirAll(filepath.Dir(path), 0o755), qt.IsNil)
	c.Assert(os.WriteFile(path, []byte(content), 0o600), qt.IsNil)
}

// runGit runs git in dir with an identity of its own, so the machine's
// configuration does not decide whether the fixture can commit.
func runGit(c *qt.C, dir string, args ...string) {
	c.Helper()
	command := exec.Command("git", args...)
	command.Dir = dir
	command.Env = append(os.Environ(),
		"GIT_AUTHOR_NAME=ptah", "GIT_AUTHOR_EMAIL=ptah@example.invalid",
		"GIT_COMMITTER_NAME=ptah", "GIT_COMMITTER_EMAIL=ptah@example.invalid",
	)
	output, err := command.CombinedOutput()
	c.Assert(err, qt.IsNil, qt.Commentf("git %v: %s", args, output))
}

func findingNames(findings []embedguard.Finding) []string {
	names := make([]string, 0, len(findings))
	for _, finding := range findings {
		names = append(names, finding.Name)
	}
	return names
}
