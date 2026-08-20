package projectconfig

// White-box testing required: the in-memory handle is an internal registry key
// that never appears in a config or a diagnostic, so a collision between two
// keys is invisible from any exported surface.

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
)

func TestMemDirectoryURLGivesTheLabelOneSegment(t *testing.T) {
	c := qt.New(t)

	// Distinct strings are not enough. `mem:///remote_dir/a/../b` and
	// `mem:///remote_dir/b` differ as strings, yet any reader that resolves the
	// path -- and the previous implementation joined it with path.Join -- folds
	// them into one key, and the second registration replaces the first
	// filesystem while both HCL values still carry it. The label has to occupy
	// exactly ONE path segment, which is what keeps that fold impossible.
	for _, label := range []string{"a/../b", "./a", "a/b", ".."} {
		segment := strings.TrimPrefix(memDirectoryURL("/remote_dir", label), "mem:///remote_dir/")

		c.Assert(segment, qt.Not(qt.Contains), "/")
		c.Assert(segment, qt.Not(qt.Equals), "")
	}
}

func TestMemDirectoryURLKeepsDistinctLabelsDistinct(t *testing.T) {
	c := qt.New(t)

	c.Assert(memDirectoryURL("/remote_dir", "a/../b"), qt.Not(qt.Equals), memDirectoryURL("/remote_dir", "b"))
	c.Assert(memDirectoryURL("/remote_dir", "./a"), qt.Not(qt.Equals), memDirectoryURL("/remote_dir", "a"))
}

func TestMemDirectoryURLIsStableForAnOrdinaryLabel(t *testing.T) {
	c := qt.New(t)

	// The two callers keep the shapes they had before this helper existed: the
	// template handle is compared against the pinned binary's output, where the
	// slash count is part of the string.
	c.Assert(memDirectoryURL("/remote_dir", "app"), qt.Equals, "mem:///remote_dir/app")
	c.Assert(memDirectoryURL("templates", "selected"), qt.Equals, "mem://templates/selected")
}
