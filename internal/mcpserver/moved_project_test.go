//go:build !windows

package mcpserver_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/agentpolicy"
)

// TestApply_RefusesWhenTheArtifactDirectoryWasReplaced is the moved-project
// property, on the surface that makes the claim (stokaro/ptah#1490).
//
// Every artifact directory is bound to an operating-system handle when the
// server starts, and `docs/.../ai-agents.md` publishes what that buys:
// "renaming the directory afterwards does not retarget anything". The mechanism
// is real and measured one layer down, in internal/pathguard — but nothing
// exercised it through an agent operation, so the refusal at the top of
// agentpatch.applyLocked was reachable only in principle and the published
// sentence was bound to no test on the surface that makes it.
//
// The scenario is a swap rather than a plain rename, because a plain rename
// leaves nothing at the pathname and could be refused by anything that merely
// re-resolves it. Here the pathname still names a directory: a DIFFERENT one,
// which is what an attacker or a careless `mv` produces, and the bound handle
// is what tells them apart.
//
// It is !windows for the reason every rename test in internal/pathguard is: an
// open directory cannot be renamed there, so the scenario is not the same
// scenario.
func TestApply_RefusesWhenTheArtifactDirectoryWasReplaced(t *testing.T) {
	c := qt.New(t)
	fixture := newWorkspace(c, agentpolicy.VerdictAllow, nil)
	session := connect(c, fixture.config, nil)

	preview := previewPatch(c, session, migrationsDigest(c, session))

	// The project moves, and something else takes its place at the pathname.
	moved := filepath.Join(fixture.root, "migrations-moved")
	c.Assert(os.Rename(fixture.dir, moved), qt.IsNil)
	c.Assert(os.MkdirAll(fixture.dir, 0o755), qt.IsNil)

	result := call(c, session, "apply_patch", map[string]any{
		"preview_token": preview["preview_token"],
		"patch_id":      preview["patch_id"],
	})

	c.Assert(result.IsError, qt.IsTrue, qt.Commentf("%s", textOf(c, result)))
	// The reason is asserted, not just the refusal: an apply against a swapped
	// directory would also fail its digest check, and a test satisfied by that
	// would keep passing with the handle re-resolved from the pathname -- which
	// is the defect the handle exists to prevent.
	c.Assert(textOf(c, result), qt.Contains, "opened directory path changed")
	// Nothing was written into the directory that took the pathname, which is
	// the outcome the refusal exists for: the patch must not land somewhere the
	// operator never pointed the session at.
	entries, err := os.ReadDir(fixture.dir)
	c.Assert(err, qt.IsNil)
	c.Assert(entries, qt.HasLen, 0)
	// And it did not land in the moved directory either, so the refusal is a
	// refusal rather than a redirect.
	_, statErr := os.Stat(filepath.Join(moved, "1700000100_add_status.up.sql"))
	c.Assert(os.IsNotExist(statErr), qt.IsTrue)
}

// TestApply_StillWorksWhenTheDirectoryWasNotTouched is the control.
//
// Without it, a session that refused every apply would satisfy the test above.
func TestApply_StillWorksWhenTheDirectoryWasNotTouched(t *testing.T) {
	c := qt.New(t)
	fixture := newWorkspace(c, agentpolicy.VerdictAllow, nil)
	session := connect(c, fixture.config, nil)

	preview := previewPatch(c, session, migrationsDigest(c, session))

	result := call(c, session, "apply_patch", map[string]any{
		"preview_token": preview["preview_token"],
		"patch_id":      preview["patch_id"],
	})

	c.Assert(result.IsError, qt.IsFalse, qt.Commentf("%s", textOf(c, result)))
	_, statErr := os.Stat(filepath.Join(fixture.dir, "1700000100_add_status.up.sql"))
	c.Assert(statErr, qt.IsNil)
}
