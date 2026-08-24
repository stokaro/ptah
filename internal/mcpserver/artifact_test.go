package mcpserver_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"go.5x5.cz/ptah/internal/agentapi"
	"go.5x5.cz/ptah/internal/agentgate"
	"go.5x5.cz/ptah/internal/agentpolicy"
	"go.5x5.cz/ptah/internal/agentworkspace"
	"go.5x5.cz/ptah/internal/mcpserver"
	"go.5x5.cz/ptah/internal/migrateops"
	"go.5x5.cz/ptah/migration/migrator"
)

// workspaceFixture is a project with one hashed migration pair, and the
// server configuration that reaches it.
type workspaceFixture struct {
	config mcpserver.Config
	root   string
	dir    string
}

// newWorkspace builds the fixture with artifact.write at the given verdict.
//
// The verdict is the parameter because the three interesting servers differ
// only in it: one where the operator granted writes outright, one where the
// policy asks, and one where writes were never enabled.
func newWorkspace(c *qt.C, write agentpolicy.Verdict, approver agentpolicy.Approver) workspaceFixture {
	c.Helper()
	root := c.TempDir()
	dir := filepath.Join(root, "migrations")
	c.Assert(os.MkdirAll(dir, 0o755), qt.IsNil)
	c.Assert(writeFile(dir, "1700000000_init.up.sql",
		"CREATE TABLE users (id BIGINT PRIMARY KEY);\n"), qt.IsNil)
	c.Assert(writeFile(dir, "1700000000_init.down.sql", "DROP TABLE users;\n"), qt.IsNil)
	_, err := migrateops.Rehash(dir, migrator.MigrationDirFormatAuto)
	c.Assert(err, qt.IsNil)

	workspace, err := agentworkspace.Open(agentworkspace.Config{
		Root: root,
		Classes: map[agentpolicy.ArtifactClass]agentworkspace.ClassConfig{
			agentpolicy.ClassMigrations: {Dir: "migrations", Writable: write != agentpolicy.VerdictDeny},
		},
		Dialect: "postgres",
	})
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = workspace.Close() })

	policy, err := agentpolicy.Assemble(agentpolicy.LayerRules{
		Layer:  agentpolicy.LayerInvocation,
		Source: "ptah mcp flags",
		Rules: []agentpolicy.Rule{{
			Capability: agentpolicy.ArtifactWrite,
			Artifact:   agentpolicy.ClassMigrations,
			Verdict:    write,
		}},
	})
	c.Assert(err, qt.IsNil)

	options := make([]agentpolicy.BrokerOption, 0, 1)
	if approver != nil {
		options = append(options, agentpolicy.WithApprover(approver))
	}
	gates, err := agentgate.New(agentgate.Options{Dialect: "postgres"})
	c.Assert(err, qt.IsNil)
	session, err := agentapi.NewSession(agentapi.SessionConfig{
		Workspace:   workspace,
		SourceRoots: []string{root},
		Broker:      agentpolicy.NewBroker(policy, options...),
		Gates:       gates,
	})
	c.Assert(err, qt.IsNil)

	return workspaceFixture{
		config: mcpserver.Config{Version: "test", Session: session},
		root:   root,
		dir:    dir,
	}
}

// addStatusChanges is the patch every apply test proposes.
func addStatusChanges() []any {
	return []any{
		map[string]any{
			"path":      "1700000100_add_status.up.sql",
			"operation": "create",
			"content":   "ALTER TABLE users ADD COLUMN status TEXT;\n",
		},
		map[string]any{
			"path":      "1700000100_add_status.down.sql",
			"operation": "create",
			"content":   "ALTER TABLE users DROP COLUMN status;\n",
		},
	}
}

// call runs one tool and returns the result.
func call(c *qt.C, session *mcp.ClientSession, name string, args map[string]any) *mcp.CallToolResult {
	c.Helper()
	result, err := session.CallTool(context.Background(), &mcp.CallToolParams{
		Name:      name,
		Arguments: args,
	})
	c.Assert(err, qt.IsNil)
	return result
}

// structured decodes a successful result's structured content.
func structured(c *qt.C, result *mcp.CallToolResult) map[string]any {
	c.Helper()
	c.Assert(result.IsError, qt.IsFalse, qt.Commentf("%s", textOf(c, result)))
	raw, err := json.Marshal(result.StructuredContent)
	c.Assert(err, qt.IsNil)
	decoded := make(map[string]any)
	c.Assert(json.Unmarshal(raw, &decoded), qt.IsNil)
	return decoded
}

// previewPatch previews the standard patch and returns the decoded response.
func previewPatch(c *qt.C, session *mcp.ClientSession, digest string) map[string]any {
	c.Helper()
	return structured(c, call(c, session, "preview_patch", map[string]any{
		"artifact":        "migrations",
		"expected_digest": digest,
		"changes":         addStatusChanges(),
		"summary":         "add a status column",
	}))
}

// migrationsDigest reads the artifact digest a patch must carry.
func migrationsDigest(c *qt.C, session *mcp.ClientSession) string {
	c.Helper()
	described := structured(c, call(c, session, "describe_session", make(map[string]any)))
	workspace, _ := described["workspace"].(map[string]any)
	artifacts, _ := workspace["artifacts"].([]any)
	c.Assert(artifacts, qt.HasLen, 1)
	first, _ := artifacts[0].(map[string]any)
	digest, _ := first["digest"].(string)
	c.Assert(digest, qt.Matches, "sha256:[0-9a-f]{64}")
	return digest
}

func TestServer_WithAWorkspaceOffersTheArtifactTools(t *testing.T) {
	c := qt.New(t)
	fixture := newWorkspace(c, agentpolicy.VerdictAllow, nil)

	offered := make(map[string]*mcp.Tool)
	for _, tool := range tools(c, fixture.config) {
		offered[tool.Name] = tool
	}

	c.Assert(offered, qt.HasLen, 9,
		qt.Commentf("six reading tools and three artifact tools"))
	for _, name := range []string{
		"describe_session",
		"read_artifact",
		"preview_patch",
		"apply_patch",
	} {
		t.Run(name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(offered[name], qt.IsNotNil)
			c.Assert(offered[name].Description, qt.Not(qt.Equals), "")
		})
	}
}

func TestServer_AnnotatesTheApplyToolAsTheOnlyWriter(t *testing.T) {
	// Three of the four artifact tools read. Annotating them all as writers
	// would tell a client to confirm a listing, and annotating the fourth as a
	// reader would tell it not to confirm the write.
	c := qt.New(t)
	fixture := newWorkspace(c, agentpolicy.VerdictAllow, nil)

	writers := make(map[string]bool)
	for _, tool := range tools(c, fixture.config) {
		writers[tool.Name] = !tool.Annotations.ReadOnlyHint
	}

	c.Assert(writers["apply_patch"], qt.IsTrue)
	c.Assert(writers["preview_patch"], qt.IsFalse)
	c.Assert(writers["read_artifact"], qt.IsFalse)
	c.Assert(writers["describe_session"], qt.IsFalse)
}

func TestDescribeSession_ReportsTheDigestsAndTheRefusals(t *testing.T) {
	c := qt.New(t)
	fixture := newWorkspace(c, agentpolicy.VerdictDeny, nil)
	session := connect(c, fixture.config, nil)

	described := structured(c, call(c, session, "describe_session", make(map[string]any)))

	workspace, _ := described["workspace"].(map[string]any)
	c.Assert(workspace["root"], qt.Equals, fixture.root)
	c.Assert(workspace["dialect"], qt.Equals, "postgres")
	artifacts, _ := workspace["artifacts"].([]any)
	c.Assert(artifacts, qt.HasLen, 1)
	first, _ := artifacts[0].(map[string]any)
	c.Assert(first["artifact"], qt.Equals, "migrations")
	c.Assert(first["write_verdict"], qt.Equals, "deny")
	c.Assert(first["files"], qt.Equals, float64(3))

	// The whole table, refusals included: a report of the grants alone answers
	// "nothing was granted" the same way a broken report does.
	capabilities, _ := described["capabilities"].([]any)
	c.Assert(len(capabilities) > 30, qt.IsTrue)
}

func TestReadArtifact_ReturnsContentLabelledAsData(t *testing.T) {
	c := qt.New(t)
	fixture := newWorkspace(c, agentpolicy.VerdictDeny, nil)
	session := connect(c, fixture.config, nil)

	read := structured(c, call(c, session, "read_artifact", map[string]any{
		"artifact": "migrations",
		"path":     "1700000000_init.up.sql",
	}))

	c.Assert(read["content"], qt.Equals, "CREATE TABLE users (id BIGINT PRIMARY KEY);\n")
	c.Assert(read["content_digest"], qt.Matches, "sha256:[0-9a-f]{64}")
	c.Assert(read["notice"], qt.Equals, agentapi.UntrustedContentNotice)
}

func TestReadArtifact_RefusesAPathOutsideTheArtifact(t *testing.T) {
	// Scenario 6 of #1487, at the read end: repository content telling the model
	// to fetch something else must not become a read of something else.
	c := qt.New(t)
	fixture := newWorkspace(c, agentpolicy.VerdictDeny, nil)
	c.Assert(writeFile(fixture.root, "secrets.env", "TOKEN=hunter2\n"), qt.IsNil)
	session := connect(c, fixture.config, nil)

	result := call(c, session, "read_artifact", map[string]any{
		"artifact": "migrations",
		"path":     "../secrets.env",
	})

	c.Assert(result.IsError, qt.IsTrue)
	c.Assert(textOf(c, result), qt.Contains, "leaves the artifact scope")
	c.Assert(textOf(c, result), qt.Not(qt.Contains), "hunter2")
}

func TestPreviewAndApply_HappyPath(t *testing.T) {
	c := qt.New(t)
	fixture := newWorkspace(c, agentpolicy.VerdictAllow, nil)
	session := connect(c, fixture.config, nil)

	preview := previewPatch(c, session, migrationsDigest(c, session))
	c.Assert(preview["patch_id"], qt.Matches, "sha256:[0-9a-f]{64}")
	c.Assert(preview["preview_token"], qt.Matches, "[0-9a-f]{32}")
	c.Assert(preview["requires_approval"], qt.IsFalse)
	c.Assert(preview["integrity_refresh"], qt.IsTrue)
	files, _ := preview["files"].([]any)
	c.Assert(files, qt.HasLen, 2)

	// Previewing writes nothing.
	_, statErr := os.Stat(filepath.Join(fixture.dir, "1700000100_add_status.up.sql"))
	c.Assert(os.IsNotExist(statErr), qt.IsTrue)

	applied := structured(c, call(c, session, "apply_patch", map[string]any{
		"preview_token": preview["preview_token"],
		"patch_id":      preview["patch_id"],
	}))

	c.Assert(applied["rolled_back"], qt.IsFalse)
	c.Assert(applied["integrity_refreshed"], qt.IsTrue)
	c.Assert(applied["result_digest"], qt.Matches, "sha256:[0-9a-f]{64}")
	written, err := os.ReadFile(filepath.Join(fixture.dir, "1700000100_add_status.up.sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(string(written), qt.Equals, "ALTER TABLE users ADD COLUMN status TEXT;\n")
}

func TestApply_RefusesAWriteTheOperatorNeverEnabled(t *testing.T) {
	c := qt.New(t)
	fixture := newWorkspace(c, agentpolicy.VerdictDeny, nil)
	session := connect(c, fixture.config, nil)

	preview := previewPatch(c, session, migrationsDigest(c, session))
	c.Assert(preview["requires_approval"], qt.IsFalse,
		qt.Commentf("a denial is not an approval that has not happened yet"))

	result := call(c, session, "apply_patch", map[string]any{
		"preview_token": preview["preview_token"],
		"patch_id":      preview["patch_id"],
	})

	c.Assert(result.IsError, qt.IsTrue)
	c.Assert(textOf(c, result), qt.Contains, `"artifact.write:migrations" denied by invocation policy`)
	c.Assert(textOf(c, result), qt.Contains, "describe_session reports what this session may do")
	_, statErr := os.Stat(filepath.Join(fixture.dir, "1700000100_add_status.up.sql"))
	c.Assert(os.IsNotExist(statErr), qt.IsTrue)
}

func TestApply_RefusesWhenThePolicyAsksAndTheClientCannot(t *testing.T) {
	// A client with no elicitation support is not a client whose user approved
	// everything. The refusal names the flag that removes the need to ask.
	c := qt.New(t)
	fixture := newWorkspace(c, agentpolicy.VerdictAsk, mcpserver.Approver{})
	session := connect(c, fixture.config, nil)

	preview := previewPatch(c, session, migrationsDigest(c, session))
	c.Assert(preview["requires_approval"], qt.IsTrue)

	result := call(c, session, "apply_patch", map[string]any{
		"preview_token": preview["preview_token"],
		"patch_id":      preview["patch_id"],
	})

	c.Assert(result.IsError, qt.IsTrue)
	c.Assert(textOf(c, result), qt.Contains, "requires approval")
	c.Assert(textOf(c, result), qt.Contains, "--allow-write=migrations")
	_, statErr := os.Stat(filepath.Join(fixture.dir, "1700000100_add_status.up.sql"))
	c.Assert(os.IsNotExist(statErr), qt.IsTrue)
}

func TestApply_AsksThroughTheClientAndProceedsOnAccept(t *testing.T) {
	c := qt.New(t)
	fixture := newWorkspace(c, agentpolicy.VerdictAsk, mcpserver.Approver{})
	prompts := make([]string, 0, 1)
	session := connect(c, fixture.config, &mcp.ClientOptions{
		ElicitationHandler: func(
			_ context.Context,
			req *mcp.ElicitRequest,
		) (*mcp.ElicitResult, error) {
			prompts = append(prompts, req.Params.Message)
			return &mcp.ElicitResult{
				Action:  "accept",
				Content: map[string]any{"decision": "allow once"},
			}, nil
		},
	})

	preview := previewPatch(c, session, migrationsDigest(c, session))
	applied := structured(c, call(c, session, "apply_patch", map[string]any{
		"preview_token": preview["preview_token"],
		"patch_id":      preview["patch_id"],
	}))

	c.Assert(applied["approved"], qt.IsTrue)
	c.Assert(applied["rolled_back"], qt.IsFalse)
	c.Assert(prompts, qt.HasLen, 1)
	c.Assert(prompts[0], qt.Contains, "artifact.write:migrations")
	c.Assert(prompts[0], qt.Contains, preview["patch_id"].(string))
	c.Assert(prompts[0], qt.Contains, "1700000100_add_status.up.sql")
	c.Assert(prompts[0], qt.Not(qt.Contains), "add a status column",
		qt.Commentf("the prompt is Ptah's text, not the requester's"))
}

func TestApply_StopsWhenTheAnswerIsNo(t *testing.T) {
	c := qt.New(t)
	fixture := newWorkspace(c, agentpolicy.VerdictAsk, mcpserver.Approver{})
	session := connect(c, fixture.config, &mcp.ClientOptions{
		ElicitationHandler: func(context.Context, *mcp.ElicitRequest) (*mcp.ElicitResult, error) {
			return &mcp.ElicitResult{Action: "decline"}, nil
		},
	})

	preview := previewPatch(c, session, migrationsDigest(c, session))
	result := call(c, session, "apply_patch", map[string]any{
		"preview_token": preview["preview_token"],
		"patch_id":      preview["patch_id"],
	})

	c.Assert(result.IsError, qt.IsTrue)
	c.Assert(textOf(c, result), qt.Contains, "approval refused")
	_, statErr := os.Stat(filepath.Join(fixture.dir, "1700000100_add_status.up.sql"))
	c.Assert(os.IsNotExist(statErr), qt.IsTrue)
}

func TestApply_SpendsThePreviewTokenOnce(t *testing.T) {
	// A replayable handle is a second apply of an approval a person gave once.
	c := qt.New(t)
	fixture := newWorkspace(c, agentpolicy.VerdictAllow, nil)
	session := connect(c, fixture.config, nil)

	preview := previewPatch(c, session, migrationsDigest(c, session))
	first := call(c, session, "apply_patch", map[string]any{
		"preview_token": preview["preview_token"],
		"patch_id":      preview["patch_id"],
	})
	second := call(c, session, "apply_patch", map[string]any{
		"preview_token": preview["preview_token"],
		"patch_id":      preview["patch_id"],
	})

	c.Assert(first.IsError, qt.IsFalse, qt.Commentf("%s", textOf(c, first)))
	c.Assert(second.IsError, qt.IsTrue)
	c.Assert(textOf(c, second), qt.Contains, "unknown or expired preview")
}

func TestApply_RefusesAPatchIdThatDoesNotBelongToTheToken(t *testing.T) {
	c := qt.New(t)
	fixture := newWorkspace(c, agentpolicy.VerdictAllow, nil)
	session := connect(c, fixture.config, nil)

	preview := previewPatch(c, session, migrationsDigest(c, session))
	result := call(c, session, "apply_patch", map[string]any{
		"preview_token": preview["preview_token"],
		"patch_id":      "sha256:0000000000000000000000000000000000000000000000000000000000000000",
	})

	c.Assert(result.IsError, qt.IsTrue)
	c.Assert(textOf(c, result), qt.Contains, "the token belongs to patch")
}

func TestApply_RefusesWhenTheArtifactChangedAfterThePreview(t *testing.T) {
	// Scenario 7 of #1487, driven through the protocol.
	c := qt.New(t)
	fixture := newWorkspace(c, agentpolicy.VerdictAllow, nil)
	session := connect(c, fixture.config, nil)

	preview := previewPatch(c, session, migrationsDigest(c, session))
	c.Assert(writeFile(fixture.dir, "1700000050_other.up.sql", "SELECT 1;\n"), qt.IsNil)

	result := call(c, session, "apply_patch", map[string]any{
		"preview_token": preview["preview_token"],
		"patch_id":      preview["patch_id"],
	})

	c.Assert(result.IsError, qt.IsTrue)
	c.Assert(textOf(c, result), qt.Contains, "artifact digest does not match")
	c.Assert(textOf(c, result), qt.Contains, "compose a new patch")
	_, statErr := os.Stat(filepath.Join(fixture.dir, "1700000100_add_status.up.sql"))
	c.Assert(os.IsNotExist(statErr), qt.IsTrue)
}

func TestPreview_RefusesEveryPathThatLeavesTheArtifact(t *testing.T) {
	// The adversarial set from #1487's scenario 6, as patch targets. Each is a
	// file a repository could plausibly tell a model to change, and none of
	// them is inside the one directory this session was pointed at.
	tests := []struct {
		name    string
		path    string
		wantErr string
	}{
		{
			name:    "a workflow file",
			path:    "../.github/workflows/release.yml",
			wantErr: `path "../.github/workflows/release.yml" leaves the artifact scope`,
		},
		{
			name:    "an absolute path",
			path:    "/etc/cron.d/ptah",
			wantErr: `path "/etc/cron.d/ptah" is absolute`,
		},
		{
			name:    "a shell profile",
			path:    "../../../.bashrc",
			wantErr: `path "../../../.bashrc" leaves the artifact scope`,
		},
		{
			name:    "the integrity file",
			path:    "ptah.sum",
			wantErr: `"ptah.sum" is the migration integrity file`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			fixture := newWorkspace(c, agentpolicy.VerdictAllow, nil)
			session := connect(c, fixture.config, nil)

			result := call(c, session, "preview_patch", map[string]any{
				"artifact": "migrations",
				"changes": []any{map[string]any{
					"path":      test.path,
					"operation": "create",
					"content":   "# whatever the file said to write\n",
				}},
			})

			c.Assert(result.IsError, qt.IsTrue)
			c.Assert(textOf(c, result), qt.Contains, test.wantErr)
		})
	}
}

func TestApply_UndoesAPatchThatBreaksTheDirectory(t *testing.T) {
	// The gates are Ptah's, not the model's: they run on the bytes that reached
	// the disk whether or not anybody asked, and a patch that introduced an
	// error is undone rather than reported as applied.
	c := qt.New(t)
	fixture := newWorkspace(c, agentpolicy.VerdictAllow, nil)
	session := connect(c, fixture.config, nil)

	preview := structured(c, call(c, session, "preview_patch", map[string]any{
		"artifact":        "migrations",
		"expected_digest": migrationsDigest(c, session),
		"changes": []any{map[string]any{
			"path":      "1700000100_add_status.up.sql",
			"operation": "create",
			"content":   "ALTER TABL users ADD COLUMN status TEXT;\n",
		}},
	}))
	result := call(c, session, "apply_patch", map[string]any{
		"preview_token": preview["preview_token"],
		"patch_id":      preview["patch_id"],
	})

	c.Assert(result.IsError, qt.IsTrue)
	c.Assert(textOf(c, result), qt.Contains, "verification gate failed")
	_, statErr := os.Stat(filepath.Join(fixture.dir, "1700000100_add_status.up.sql"))
	c.Assert(os.IsNotExist(statErr), qt.IsTrue)
}
