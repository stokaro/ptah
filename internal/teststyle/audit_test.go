package teststyle_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/internal/teststyle"
)

func TestScanReportsConditionalsAndWhiteBoxViolations(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	writeFile(c, dir, "sample/blackbox_test.go", `package sample_test

import "testing"

func TestDeclarative(t *testing.T) {}
`)
	writeFile(c, dir, "sample/mixed_test.go", `package sample

import "testing"

func TestBad(t *testing.T) {
	if true {}
	switch 1 { case 1: }
	goto done
done:
}
`)
	writeFile(c, dir, "sample/valid_internal_test.go", `package sample
// White-box testing required: verifies unexported parser state that is not observable through the exported API.

import "testing"

func TestInternal(t *testing.T) {}
`)
	writeFile(c, dir, "sample/missing_internal_test.go", `package sample

import "testing"

func TestMissingComment(t *testing.T) {}
`)

	got, err := teststyle.Scan(dir)

	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.DeepEquals, teststyle.Baseline{
		TestConditionals: []teststyle.ConditionalBaseline{
			{Path: "sample/mixed_test.go", Function: "TestBad", Kind: "goto", Count: 1},
			{Path: "sample/mixed_test.go", Function: "TestBad", Kind: "if", Count: 1},
			{Path: "sample/mixed_test.go", Function: "TestBad", Kind: "switch", Count: 1},
		},
		WhiteBoxFiles: []teststyle.WhiteBoxBaseline{
			{Path: "sample/missing_internal_test.go", Package: "sample", Reason: "missing immediate white-box justification comment"},
			{Path: "sample/mixed_test.go", Package: "sample", Reason: "same-package test file is not named *_internal_test.go"},
		},
	})
}

func TestBaselineRoundTrip(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "baseline.json")
	baseline := teststyle.Baseline{
		TestConditionals: []teststyle.ConditionalBaseline{
			{Path: "a_test.go", Function: "TestA", Kind: "if", Count: 1},
		},
		WhiteBoxFiles: []teststyle.WhiteBoxBaseline{
			{Path: "b_test.go", Package: "b", Reason: "same-package test file is not named *_internal_test.go"},
		},
	}

	err := teststyle.WriteBaseline(path, baseline)
	got, readErr := teststyle.ReadBaseline(path)

	c.Assert(err, qt.IsNil)
	c.Assert(readErr, qt.IsNil)
	c.Assert(got, qt.DeepEquals, baseline)
	c.Assert(teststyle.Diff(baseline, got), qt.Equals, "")
}

func TestDiffReportsNewAndStaleEntries(t *testing.T) {
	c := qt.New(t)
	want := teststyle.Baseline{
		TestConditionals: []teststyle.ConditionalBaseline{
			{Path: "old_test.go", Function: "TestOld", Kind: "if", Count: 1},
		},
	}
	got := teststyle.Baseline{
		TestConditionals: []teststyle.ConditionalBaseline{
			{Path: "new_test.go", Function: "TestNew", Kind: "switch", Count: 1},
		},
	}

	diff := teststyle.Diff(want, got)

	c.Assert(diff, qt.Contains, "test conditional baseline is stale or too high")
	c.Assert(diff, qt.Contains, "old_test.go")
	c.Assert(diff, qt.Contains, "new test conditional violations")
	c.Assert(diff, qt.Contains, "new_test.go")
}

func TestDiffDoesNotMutateInputs(t *testing.T) {
	c := qt.New(t)
	want := teststyle.Baseline{
		TestConditionals: []teststyle.ConditionalBaseline{
			{Path: "z_test.go", Function: "TestZ", Kind: "if", Count: 1},
			{Path: "a_test.go", Function: "TestA", Kind: "switch", Count: 1},
		},
	}
	got := teststyle.Baseline{
		WhiteBoxFiles: []teststyle.WhiteBoxBaseline{
			{Path: "z_internal_test.go", Package: "sample", Reason: "missing immediate white-box justification comment"},
			{Path: "a_test.go", Package: "sample", Reason: "same-package test file is not named *_internal_test.go"},
		},
	}
	wantBefore := want
	gotBefore := got

	_ = teststyle.Diff(want, got)

	c.Assert(want, qt.DeepEquals, wantBefore)
	c.Assert(got, qt.DeepEquals, gotBefore)
}

func writeFile(c *qt.C, root string, relativePath string, data string) {
	c.Helper()
	path := filepath.Join(root, filepath.FromSlash(relativePath))
	c.Assert(os.MkdirAll(filepath.Dir(path), 0o700), qt.IsNil)
	c.Assert(os.WriteFile(path, []byte(data), 0o600), qt.IsNil)
}
