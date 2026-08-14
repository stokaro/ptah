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
			// The gap a callback-shape rule leaves. Every subtest here is
			// (*qt.C).Run with the callback named somewhere else, which is the
			// same object graph as the inline form.
			name:    "a subtest callback named elsewhere is still a violation",
			fixture: "indirectsubtest.go.txt",
			want: []wantFinding{
				{line: 33, rule: qtshape.RuleCheckerSubtest},
				{line: 34, rule: qtshape.RuleCheckerSubtest},
				{line: 35, rule: qtshape.RuleCheckerSubtest},
				{line: 38, rule: qtshape.RuleCheckerSubtest},
				{line: 40, rule: qtshape.RuleCheckerSubtest},
				{line: 48, rule: qtshape.RuleCheckerSubtest},
			},
		},
		{
			// The other gap. These have the required t.Run signature and the
			// required testing.TB receiver and still assert against the parent,
			// which Go reports as "subtest may have called FailNow on a parent
			// test". The two nested subtests at lines 37-40 share one borrowed
			// identifier and are reported once.
			name:    "a subtest that borrows the enclosing checker or TB is reported",
			fixture: "borrowedchecker.go.txt",
			want: []wantFinding{
				{line: 20, rule: qtshape.RuleBorrowedChecker},
				{line: 24, rule: qtshape.RuleBorrowedChecker},
				{line: 29, rule: qtshape.RuleBorrowedChecker},
				{line: 39, rule: qtshape.RuleBorrowedChecker},
				{line: 48, rule: qtshape.RuleBorrowedChecker},
				{line: 56, rule: qtshape.RuleBorrowedChecker},
			},
		},
		{
			// The gap a callback-shape rule leaves on the other side. Every
			// subtest here is handed a func(*testing.T) that was bound to a name
			// first, which is the same object graph as the inline spelling and
			// the same parent-FailNow failure.
			name:    "a subtest callback named elsewhere still borrows what it borrows",
			fixture: "namedcallback.go.txt",
			want: []wantFinding{
				{line: 18, rule: qtshape.RuleBorrowedChecker},
				{line: 24, rule: qtshape.RuleBorrowedChecker},
				{line: 61, rule: qtshape.RuleBorrowedChecker},
				{line: 70, rule: qtshape.RuleBorrowedChecker},
			},
		},
		{
			// Both functions in this fixture declare `callback`. A file-wide
			// index keyed on the name reports the unrelated one too, which fails
			// a repository-wide gate on correct code.
			name:    "a callback name is resolved where it is written, not file-wide",
			fixture: "callbackscope.go.txt",
			want: []wantFinding{
				{line: 25, rule: qtshape.RuleCheckerSubtest},
			},
		},
		{
			// R1 keyed on the identifier text reported all four calls here, in a
			// file that never imports quicktest.
			name:    "a local qt that is not the quicktest import produces nothing",
			fixture: "localqt.go.txt",
			want:    nil,
		},
		{
			// And the other half: quicktest is imported as qt, so the rule does
			// apply, but only where that name still means the import. The one
			// finding is the call written before the declaration that shadows it.
			name:    "a shadowed qt is reported only where the import is still in scope",
			fixture: "shadowedqt.go.txt",
			want: []wantFinding{
				{line: 35, rule: qtshape.RulePackageAssert},
			},
		},
		{
			name:    "a renamed quicktest import is reported, and no longer silences R1 and R2",
			fixture: "aliasrenamed.go.txt",
			want: []wantFinding{
				{line: 6, rule: qtshape.RuleImportAlias},
				{line: 16, rule: qtshape.RulePackageAssert},
				{line: 17, rule: qtshape.RuleCheckerSubtest},
			},
		},
		{
			name:    "an unaliased quicktest import binds quicktest and is reported",
			fixture: "aliasdefault.go.txt",
			want: []wantFinding{
				{line: 6, rule: qtshape.RuleImportAlias},
			},
		},
		{
			// R2's receiver keyed on the spelling reported line 25 here, where an
			// inner block binds c to a runner. A repository-wide gate that refuses
			// that code refuses correct code.
			name:    "a checker name shadowed by an unrelated value is not a checker receiver",
			fixture: "shadowedchecker.go.txt",
			want: []wantFinding{
				{line: 34, rule: qtshape.RuleCheckerSubtest},
			},
		},
		{
			// R2 keyed on the call reported none of these: the prohibited method
			// was bound to a name, written as a method expression, or handed to a
			// helper before it was called.
			name:    "the checker Run method is reported wherever it is referenced",
			fixture: "runmethodvalue.go.txt",
			want: []wantFinding{
				{line: 24, rule: qtshape.RuleCheckerSubtest},
				{line: 33, rule: qtshape.RuleCheckerSubtest},
				{line: 41, rule: qtshape.RuleCheckerSubtest},
			},
		},
		{
			// The enclosing-scope walk only reaches a parent TB that is a name it
			// can classify. These two spell it as an initializer and as a field,
			// and both produce the parent-FailNow failure.
			name:    "a checker built from a TB the closure was not handed is reported",
			fixture: "foreigntb.go.txt",
			want: []wantFinding{
				{line: 22, rule: qtshape.RuleBorrowedChecker},
				{line: 31, rule: qtshape.RuleBorrowedChecker},
			},
		},
		{
			// Suppressing by the set of names a closure declares somewhere lost
			// both of these and invented the one at line 52, where c is an int.
			name:    "a borrowed checker is decided at the position it is read",
			fixture: "latedeclaration.go.txt",
			want: []wantFinding{
				{line: 20, rule: qtshape.RuleBorrowedChecker},
				{line: 32, rule: qtshape.RuleBorrowedChecker},
			},
		},
		{
			// `alias := c` has no written type and no qt.New, so classifying a
			// declaration by its own syntax alone read every alias as an
			// ordinary value: the three borrowed checkers and the prohibited
			// Run below were all silent. The last two functions are the
			// negatives that keep the propagation positional.
			name:    "a checker or TB copied into another name keeps its identity",
			fixture: "aliasedidentity.go.txt",
			want: []wantFinding{
				{line: 24, rule: qtshape.RuleBorrowedChecker},
				{line: 32, rule: qtshape.RuleBorrowedChecker},
				{line: 42, rule: qtshape.RuleBorrowedChecker},
				{line: 50, rule: qtshape.RuleCheckerSubtest},
			},
		},
		{
			// Parentheses are a node, so every type assertion in this package
			// used to stop at them. All six violations here were silent, and
			// the only thing the gate did report was the conforming
			// `qt.New((t))` at line 62, which it called a borrowed TB.
			name:    "a parenthesized checker, callback or type is still what it is",
			fixture: "parenthesized.go.txt",
			want: []wantFinding{
				{line: 27, rule: qtshape.RuleCheckerSubtest},
				{line: 36, rule: qtshape.RuleCheckerSubtest},
				{line: 43, rule: qtshape.RuleCheckerSubtest},
				{line: 50, rule: qtshape.RuleBorrowedChecker},
				{line: 73, rule: qtshape.RuleCheckerSubtest},
				{line: 81, rule: qtshape.RuleBorrowedChecker},
			},
		},
		{
			// Where the name was written is not where the value came from. Both
			// of these copy the parent's TB into a name the closure declares,
			// which satisfied a check that only asked whether the declaration
			// was local.
			name:    "a TB copied into a local name is still not the one the closure was handed",
			fixture: "localaliasedtb.go.txt",
			want: []wantFinding{
				{line: 27, rule: qtshape.RuleBorrowedChecker},
				{line: 38, rule: qtshape.RuleBorrowedChecker},
				{line: 87, rule: qtshape.RuleBorrowedChecker},
				{line: 99, rule: qtshape.RuleBorrowedChecker},
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

// TestScanFileSaysWhichBranchMatched pins the two independent reasons a subtest
// can be a (*qt.C).Run. Without this, a rule that recognized only one of them
// could still satisfy the count assertion above by reporting the other five
// findings twice over, and the branch that survives matters: the receiver branch
// is the only thing that reaches a callback this package cannot resolve, and the
// callback branch is the only thing that reaches a checker held in a field.
func TestScanFileSaysWhichBranchMatched(t *testing.T) {
	c := qt.New(t)

	path := filepath.Join("testdata", "indirectsubtest.go.txt")
	src, err := os.ReadFile(path)
	c.Assert(err, qt.IsNil)

	got, err := qtshape.ScanFile(path, src)
	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.HasLen, 6)

	// A method value: nothing but the receiver identifies it.
	c.Check(got[2].Message, qt.Contains, "c.Run is a (*qt.C).Run subtest")
	// A checker reached through a struct field: nothing but the callback
	// identifies it.
	c.Check(got[5].Message, qt.Contains, "<expr>.Run with the func(*qt.C) callback declared")
}

// TestScanFileNamesWhatWasBorrowed pins which of the two borrowed kinds each R3
// finding is about. A rule that only ever reported the checker would satisfy the
// count above, and `qt.New(t)` inside a closure whose own parameter is `subT` is
// the spelling that has neither a *qt.C nor a wrong signature to give it away.
func TestScanFileNamesWhatWasBorrowed(t *testing.T) {
	c := qt.New(t)

	path := filepath.Join("testdata", "borrowedchecker.go.txt")
	src, err := os.ReadFile(path)
	c.Assert(err, qt.IsNil)

	got, err := qtshape.ScanFile(path, src)
	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.HasLen, 6)

	c.Check(got[0].Message, qt.Contains, "asserts through c, a *qt.C declared outside this subtest")
	c.Check(got[2].Message, qt.Contains, "builds its checker from t, which is not the testing.TB this subtest closure was handed")
}

// TestScanFileNamesWhatWasAliased pins which branch each finding in
// aliasedidentity.go.txt comes from. The counts alone cannot separate them: a
// propagation that carried only the checker would report the first and third
// and could satisfy a count by reporting one of them twice, and the second is
// the only site in the fixtures where the borrowed testing.TB message is the
// one that survives dedupe.
func TestScanFileNamesWhatWasAliased(t *testing.T) {
	c := qt.New(t)

	path := filepath.Join("testdata", "aliasedidentity.go.txt")
	src, err := os.ReadFile(path)
	c.Assert(err, qt.IsNil)

	got, err := qtshape.ScanFile(path, src)
	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.HasLen, 4)

	c.Check(got[0].Message, qt.Contains, "asserts through alias, a *qt.C declared outside this subtest")
	c.Check(got[1].Message, qt.Contains, "uses parent, a testing.TB from the enclosing scope")
	c.Check(got[2].Message, qt.Contains, "asserts through second, a *qt.C declared outside this subtest")
	c.Check(got[3].Message, qt.Contains, "alias.Run is a (*qt.C).Run subtest")
}

// TestScanFileNamesTheForeignCheckerSource pins what each R3 finding in
// foreigntb.go.txt is about. The two spellings are what make the rule
// necessary: one reaches the parent TB through a name an initializer bound and
// the other through a field, and a rule that recognized only names would
// satisfy the count above by reporting the first one twice.
func TestScanFileNamesTheForeignCheckerSource(t *testing.T) {
	c := qt.New(t)

	path := filepath.Join("testdata", "foreigntb.go.txt")
	src, err := os.ReadFile(path)
	c.Assert(err, qt.IsNil)

	got, err := qtshape.ScanFile(path, src)
	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.HasLen, 2)

	c.Check(got[0].Message, qt.Contains, "builds its checker from parent")
	c.Check(got[1].Message, qt.Contains, "builds its checker from held.tb")
}

// TestScanFileNamesTheRunReferenceItFound pins the receiver of each method-value
// finding. Without it a rule that reported the same reference three times, or
// that recognized only the `run := c.Run` binding and counted the two calls it
// could see, would satisfy the count above.
func TestScanFileNamesTheRunReferenceItFound(t *testing.T) {
	c := qt.New(t)

	path := filepath.Join("testdata", "runmethodvalue.go.txt")
	src, err := os.ReadFile(path)
	c.Assert(err, qt.IsNil)

	got, err := qtshape.ScanFile(path, src)
	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.HasLen, 3)

	c.Check(got[0].Col, qt.Equals, 9)
	c.Check(got[1].Message, qt.Contains, "<expr>.Run")
	c.Check(got[2].Col, qt.Equals, 11)
}

// TestScanFileNamesWhatWasParenthesized pins which branch each finding in
// parenthesized.go.txt comes from. The count alone cannot separate them: a
// change that unwrapped only the receiver would report the first two and could
// satisfy six by reporting one of them three times over, and each of the other
// four is the only row that reaches the assertion it is about.
func TestScanFileNamesWhatWasParenthesized(t *testing.T) {
	c := qt.New(t)

	path := filepath.Join("testdata", "parenthesized.go.txt")
	src, err := os.ReadFile(path)
	c.Assert(err, qt.IsNil)

	got, err := qtshape.ScanFile(path, src)
	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.HasLen, 6, qt.Commentf("findings: %v", got))

	// A method value on a parenthesized receiver: the selector is the finding,
	// and its column is the `(` the receiver opens with.
	c.Check(got[0].Message, qt.Contains, "c.Run is a (*qt.C).Run subtest")
	c.Check(got[0].Col, qt.Equals, 9)
	// A call whose callback this package cannot resolve, so only the receiver
	// can have reported it.
	c.Check(got[1].Message, qt.Contains, "c.Run is a (*qt.C).Run subtest")
	// The alias carries the checker across the parentheses in its initializer.
	c.Check(got[2].Message, qt.Contains, "alias.Run is a (*qt.C).Run subtest")
	// The parentheses around the closure no longer hide the subtest from R3.
	c.Check(got[3].Message, qt.Contains, "asserts through c, a *qt.C declared outside this subtest")
	// A checker whose declared type is parenthesized and that has no qt.New to
	// be classified by instead.
	c.Check(got[4].Message, qt.Contains, "held.Run is a (*qt.C).Run subtest")
	// And the same for a testing.TB, which is the other half of the name table.
	c.Check(got[5].Message, qt.Contains, "uses parent, a testing.TB from the enclosing scope")
}

// TestScanFileNamesTheLocalTBItRejected pins that R3 rejects each of the two
// local copies by name. Without it, a rule that stopped accepting every local
// declaration and started rejecting every one of them would report both of
// these and satisfy the count, while reporting the four conforming subtests in
// the same fixture too -- which the count assertion above is what catches.
func TestScanFileNamesTheLocalTBItRejected(t *testing.T) {
	c := qt.New(t)

	path := filepath.Join("testdata", "localaliasedtb.go.txt")
	src, err := os.ReadFile(path)
	c.Assert(err, qt.IsNil)

	got, err := qtshape.ScanFile(path, src)
	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.HasLen, 4, qt.Commentf("findings: %v", got))

	c.Check(got[0].Message, qt.Contains, "builds its checker from parent")
	c.Check(got[1].Message, qt.Contains, "builds its checker from second")
	// Seeing through testing.TB(x), so that a conversion of the closure's own
	// parameter is accepted, must not also accept a conversion of the parent's.
	c.Check(got[2].Message, qt.Contains, "builds its checker from tb")
	// And a call is not a conversion.
	c.Check(got[3].Message, qt.Contains, "builds its checker from tb")
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
