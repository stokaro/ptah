package mcpserver_test

import (
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"go.5x5.cz/ptah/internal/agentpolicy"
)

// TestApply_TheSecondOfTwoPreviewsAgainstOneDigestIsRefused is #1490's
// concurrent-edit box.
//
// Two agents, or one agent that composed twice before applying, can hold two
// previews taken against the same artifact state. Both are valid when they are
// minted and only one can still be valid after either is applied: the second
// was composed against a directory that no longer exists, and applying it would
// be a lost update -- the first patch's files still on disk, the second's digest
// bookkeeping written over them.
//
// The refusal names both digests, because "somebody changed it" is not something
// an agent can act on and "it was X when you looked and is Y now" is.
func TestApply_TheSecondOfTwoPreviewsAgainstOneDigestIsRefused(t *testing.T) {
	c := qt.New(t)
	fixture := newWorkspace(c, agentpolicy.VerdictAllow, nil)
	session := connect(c, fixture.config, nil)
	before := callTool(c, session, "read_artifact", map[string]any{"artifact": "migrations"})

	first := previewAdding(c, session, before["digest"], "a")
	second := previewAdding(c, session, before["digest"], "b")

	c.Assert(first["preview_token"], qt.Not(qt.Equals), second["preview_token"])
	c.Assert(first["patch_id"], qt.Not(qt.Equals), second["patch_id"])

	applied := callTool(c, session, "apply_patch", map[string]any{
		"preview_token": first["preview_token"], "patch_id": first["patch_id"]})
	c.Assert(applied["rolled_back"], qt.Equals, false)
	c.Assert(applied["result_digest"], qt.Not(qt.Equals), before["digest"])

	refusal := callToolError(c, session, "apply_patch", map[string]any{
		"preview_token": second["preview_token"], "patch_id": second["patch_id"]})

	c.Assert(refusal, qt.Contains, "digest_mismatch")
	c.Assert(refusal, qt.Contains, fmt.Sprint(before["digest"]),
		qt.Commentf("the refusal does not say what the artifact was when the patch was composed"))
	c.Assert(refusal, qt.Contains, fmt.Sprint(applied["result_digest"]),
		qt.Commentf("the refusal does not say what the artifact is now"))
}

// TestApply_TheRefusedWriterComposesAgainAndSucceeds is the control the test
// above needs.
//
// Refusing the second patch is only correct if it is a "read again and compose
// again", not a dead end. A mechanism that left the second writer permanently
// unable to apply would satisfy every assertion above and be a worse bug than
// the lost update it prevents.
func TestApply_TheRefusedWriterComposesAgainAndSucceeds(t *testing.T) {
	c := qt.New(t)
	fixture := newWorkspace(c, agentpolicy.VerdictAllow, nil)
	session := connect(c, fixture.config, nil)
	before := callTool(c, session, "read_artifact", map[string]any{"artifact": "migrations"})

	first := previewAdding(c, session, before["digest"], "a")
	second := previewAdding(c, session, before["digest"], "b")
	callTool(c, session, "apply_patch", map[string]any{
		"preview_token": first["preview_token"], "patch_id": first["patch_id"]})
	callToolError(c, session, "apply_patch", map[string]any{
		"preview_token": second["preview_token"], "patch_id": second["patch_id"]})

	current := callTool(c, session, "read_artifact", map[string]any{"artifact": "migrations"})
	retry := previewAdding(c, session, current["digest"], "b")
	applied := callTool(c, session, "apply_patch", map[string]any{
		"preview_token": retry["preview_token"], "patch_id": retry["patch_id"]})

	c.Assert(applied["rolled_back"], qt.Equals, false)
	after := callTool(c, session, "read_artifact", map[string]any{"artifact": "migrations"})
	c.Assert(artifactPaths(c, after), qt.Contains, "17000001_a.up.sql")
	c.Assert(artifactPaths(c, after), qt.Contains, "17000001_b.up.sql",
		qt.Commentf("the writer that was refused never got its change in"))
}

// previewAdding previews one migration pair named for the tag.
func previewAdding(c *qt.C, session *mcp.ClientSession, digest any, tag string) map[string]any {
	c.Helper()
	return callTool(c, session, "preview_patch", map[string]any{
		"artifact":        "migrations",
		"expected_digest": digest,
		"summary":         "add " + tag,
		"changes": []map[string]any{
			{
				"path": "17000001_" + tag + ".up.sql", "operation": "create",
				"content": "CREATE TABLE t_" + tag + " (id BIGINT PRIMARY KEY);\n",
			},
			{
				"path": "17000001_" + tag + ".down.sql", "operation": "create",
				"content": "DROP TABLE t_" + tag + ";\n",
			},
		},
	})
}

// artifactPaths lists the files a read reported.
func artifactPaths(c *qt.C, read map[string]any) []string {
	c.Helper()
	entries, ok := read["entries"].([]any)
	c.Assert(ok, qt.IsTrue, qt.Commentf("no entries in the artifact read"))
	paths := make([]string, 0, len(entries))
	for _, entry := range entries {
		record, ok := entry.(map[string]any)
		c.Assert(ok, qt.IsTrue)
		path, ok := record["path"].(string)
		c.Assert(ok, qt.IsTrue)
		paths = append(paths, path)
	}
	return paths
}
