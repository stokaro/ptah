package projectconfig

// White-box testing required: the in-memory handle is an internal registry key
// that never appears in a config or a diagnostic, so a collision between two
// keys is invisible from any exported surface.

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

func TestMemDirectoryURLKeepsDistinctLabelsDistinct(t *testing.T) {
	c := qt.New(t)

	// Joining these as path segments normalizes both to the same key: the
	// second registration would replace the first filesystem while both HCL
	// values still carry it, so an env selecting the first block would run the
	// second block's migrations.
	c.Assert(memDirectoryURL("remote_dir", "a/../b"), qt.Not(qt.Equals), memDirectoryURL("remote_dir", "b"))
	c.Assert(memDirectoryURL("remote_dir", "./a"), qt.Not(qt.Equals), memDirectoryURL("remote_dir", "a"))
}

func TestMemDirectoryURLIsStableForAnOrdinaryLabel(t *testing.T) {
	c := qt.New(t)

	c.Assert(memDirectoryURL("remote_dir", "app"), qt.Equals, "mem:///remote_dir/app")
	c.Assert(memDirectoryURL("template_dir", "app"), qt.Equals, "mem:///template_dir/app")
}
