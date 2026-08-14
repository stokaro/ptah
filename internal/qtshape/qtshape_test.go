package qtshape_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/qtshape"
)

type wantFinding struct {
	line int
	rule qtshape.Rule
}

func TestScanFileReportsExactlyTheViolations(t *testing.T) {
	tests := []struct {
		name    string
		fixture string
		want    []wantFinding
	}{
		{
			name:    "target shape produces nothing",
			fixture: "conforming.go.txt",
			want:    nil,
		},
		{
			// The class that made an earlier repository gate vacuous: a text scan
			// cannot tell a call from a comment or from the inside of a raw string,
			// and cannot tell quicktest's Run from an unrelated method of the same
			// name. Every pattern in this file is a decoy and none is a violation.
			name:    "comments, strings and an unrelated Run method produce nothing",
			fixture: "decoys.go.txt",
			want:    nil,
		},
		{
			name:    "every spelling of both violations is reported",
			fixture: "violating.go.txt",
			want: []wantFinding{
				{line: 10, rule: qtshape.RulePackageAssert},
				{line: 11, rule: qtshape.RulePackageAssert},
				{line: 17, rule: qtshape.RulePackageAssert},
				{line: 20, rule: qtshape.RuleCheckerSubtest},
				{line: 25, rule: qtshape.RuleCheckerSubtest},
				{line: 28, rule: qtshape.RuleCheckerSubtest},
			},
		},
		{
			// Every qt.Assert in the tree today passes the identifier `t`, so a rule
			// keyed on that argument would look correct against the tree and be
			// blind here.
			name:    "a first argument that is not t is still a violation",
			fixture: "nonttbargs.go.txt",
			want: []wantFinding{
				{line: 13, rule: qtshape.RulePackageAssert},
				{line: 17, rule: qtshape.RulePackageAssert},
			},
		},
		{
			name:    "a renamed quicktest import is reported before it can silence R1 and R2",
			fixture: "aliasrenamed.go.txt",
			want: []wantFinding{
				{line: 6, rule: qtshape.RuleImportAlias},
			},
		},
		{
			name:    "an unaliased quicktest import binds quicktest and is reported",
			fixture: "aliasdefault.go.txt",
			want: []wantFinding{
				{line: 6, rule: qtshape.RuleImportAlias},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			path := filepath.Join("testdata", test.fixture)
			src, err := os.ReadFile(path)
			c.Assert(err, qt.IsNil)

			got, err := qtshape.ScanFile(path, src)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.HasLen, len(test.want), qt.Commentf("findings: %v", got))

			for i, want := range test.want {
				c.Check(got[i].Line, qt.Equals, want.line, qt.Commentf("finding %d: %s", i, got[i]))
				c.Check(got[i].Rule, qt.Equals, want.rule, qt.Commentf("finding %d: %s", i, got[i]))
				c.Check(got[i].Path, qt.Equals, path)
			}
		})
	}
}

// TestScanFileNamesTheReceiverItFound pins the message text for the two receivers
// that are not the identifier `c`. Without this, a rule that reported only sites
// whose receiver is literally `c` could still satisfy the count assertion above
// by reporting three findings for the wrong three reasons.
func TestScanFileNamesTheReceiverItFound(t *testing.T) {
	c := qt.New(t)

	path := filepath.Join("testdata", "violating.go.txt")
	src, err := os.ReadFile(path)
	c.Assert(err, qt.IsNil)

	got, err := qtshape.ScanFile(path, src)
	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.HasLen, 6)

	c.Check(got[3].Message, qt.Contains, "c.Run")
	c.Check(got[4].Message, qt.Contains, "outer.Run")
	c.Check(got[5].Message, qt.Contains, "<expr>.Run")
}

func TestScanFilesRefusesAnEmptySelection(t *testing.T) {
	c := qt.New(t)

	findings, scanned, err := qtshape.ScanFiles(nil)

	c.Assert(err, qt.ErrorIs, qtshape.ErrNoFiles)
	c.Check(findings, qt.HasLen, 0)
	c.Check(scanned, qt.Equals, 0)
}

func TestScanFilesReportsAParseErrorRatherThanSkippingTheFile(t *testing.T) {
	c := qt.New(t)

	path := filepath.Join(t.TempDir(), "broken_test.go")
	c.Assert(os.WriteFile(path, []byte("package fixture\n\nfunc {\n"), 0o600), qt.IsNil)

	_, _, err := qtshape.ScanFiles([]string{path})

	c.Assert(err, qt.ErrorMatches, `qtshape: parsing .*broken_test\.go: .*`)
}

// TestScanCoversEveryTrackedTestFile is the selection-shrink assertion. 56% of
// the qt.Assert sites this gate exists to forbid are in files behind
// //go:build integration, and three more are in the separate testkit module, so
// a selector that quietly stopped reaching either tree would leave the gate
// green over most of its own subject. This asserts the shape of the selection,
// not a frozen total: the file count moves with every PR, but the integration
// and testkit trees do not disappear.
func TestScanCoversEveryTrackedTestFile(t *testing.T) {
	c := qt.New(t)

	root := repoRoot(c)
	paths := selectedTestFiles(c, root)

	c.Assert(len(paths) > 1000, qt.IsTrue, qt.Commentf("selected %d test files", len(paths)))
	c.Check(len(withPrefix(paths, "integration/")) > 150, qt.IsTrue,
		qt.Commentf("integration test files selected: %d", len(withPrefix(paths, "integration/"))))
	c.Check(len(withPrefix(paths, "testkit/")) > 0, qt.IsTrue,
		qt.Commentf("testkit test files selected: %d", len(withPrefix(paths, "testkit/"))))

	absolute := make([]string, 0, len(paths))
	for _, path := range paths {
		absolute = append(absolute, filepath.Join(root, path))
	}

	_, scanned, err := qtshape.ScanFiles(absolute)
	c.Assert(err, qt.IsNil)
	c.Check(scanned, qt.Equals, len(paths))
}

// repoRoot resolves the checkout this test belongs to. git is asked rather than
// walking up from the working directory so a linked worktree answers with its
// own root.
func repoRoot(c *qt.C) string {
	out, err := exec.Command("git", "rev-parse", "--show-toplevel").Output()
	c.Assert(err, qt.IsNil)
	return strings.TrimSpace(string(out))
}

// selectedTestFiles runs the gate's own selector so this test measures what the
// gate measures rather than a second, independently drifting idea of it.
func selectedTestFiles(c *qt.C, root string) []string {
	script := filepath.Join(root, "scripts", "check-quicktest-shape.sh")
	cmd := exec.Command(script, "--list-scan-paths")
	cmd.Dir = root
	out, err := cmd.Output()
	c.Assert(err, qt.IsNil)

	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	return slices.DeleteFunc(lines, func(line string) bool {
		return line == ""
	})
}

// withPrefix filters without a conditional statement, which test bodies and their
// helpers in this repository do not use.
func withPrefix(paths []string, prefix string) []string {
	return slices.DeleteFunc(slices.Clone(paths), func(path string) bool {
		return !strings.HasPrefix(path, prefix)
	})
}
