package mcpserver_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/internal/agentapi"
	"go.5x5.cz/ptah/internal/agentgate"
	"go.5x5.cz/ptah/internal/agentpolicy"
	"go.5x5.cz/ptah/internal/agentworkspace"
	"go.5x5.cz/ptah/internal/mcpserver"
	"go.5x5.cz/ptah/internal/migrateops"
	"go.5x5.cz/ptah/migration/migrator"
)

// TestServer_ServesAProjectInALinkedWorktree covers the worktree half of
// #1490's "worktree and moved-project" item.
//
// A linked git worktree is an ordinary directory with one difference that
// matters to anything walking upward: its `.git` is a FILE containing
// `gitdir: …`, not a directory. Ptah has no `.git` awareness anywhere — no Go
// file in the tree references it — so the expectation is that a worktree is
// simply a directory and everything works. That is exactly the kind of
// expectation worth a test rather than an argument: it is cheap to assert, it
// is how this repository is developed, and if some later change starts
// resolving a project root by looking for `.git`, this is what notices.
//
// The whole artifact cycle runs, not just a read: read, preview against the
// digest, apply, and read back. A read-only assertion would pass on a server
// that could not write there.
func TestServer_ServesAProjectInALinkedWorktree(t *testing.T) {
	c := qt.New(t)
	worktree := linkedWorktree(c)
	fixture := workspaceAt(c, worktree)
	session := connect(c, fixture.config, nil)

	preview := previewPatch(c, session, migrationsDigest(c, session))
	result := call(c, session, "apply_patch", map[string]any{
		"preview_token": preview["preview_token"],
		"patch_id":      preview["patch_id"],
	})

	c.Assert(result.IsError, qt.IsFalse, qt.Commentf("%s", textOf(c, result)))
	written, err := os.ReadFile(filepath.Join(fixture.dir, "1700000100_add_status.up.sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(string(written), qt.Contains, "ADD COLUMN status")
}

// TestServer_AWorktreeIsADirectoryLikeAnyOther is the control that keeps the
// row above from passing for a reason that has nothing to do with worktrees.
//
// It asserts the fixture really is one: `.git` present, and a FILE rather than
// a directory. Without it, a `git worktree add` that silently failed would
// leave an ordinary directory and the test would pass having measured nothing.
func TestServer_AWorktreeIsADirectoryLikeAnyOther(t *testing.T) {
	c := qt.New(t)

	info, err := os.Lstat(filepath.Join(linkedWorktree(c), ".git"))

	c.Assert(err, qt.IsNil)
	c.Assert(info.IsDir(), qt.IsFalse,
		qt.Commentf("a linked worktree's .git is a file; this fixture is not a linked worktree"))
}

// linkedWorktree builds a git repository and returns the path of a linked
// worktree of it, skipping when git is unavailable.
func linkedWorktree(c *qt.C) string {
	c.Helper()
	git, err := exec.LookPath("git")
	if err != nil {
		c.Skipf("SKIPPED: git is not on PATH, and a linked worktree cannot be made without it")
	}

	root := c.TempDir()
	repo := filepath.Join(root, "repo")
	c.Assert(os.MkdirAll(repo, 0o755), qt.IsNil)
	run := func(args ...string) {
		c.Helper()
		cmd := exec.Command(git, args...)
		cmd.Dir = repo
		// A commit needs an identity, and the machine's own configuration must
		// not decide whether this test can run.
		cmd.Env = append(os.Environ(),
			"GIT_AUTHOR_NAME=ptah", "GIT_AUTHOR_EMAIL=ptah@example.invalid",
			"GIT_COMMITTER_NAME=ptah", "GIT_COMMITTER_EMAIL=ptah@example.invalid",
		)
		output, runErr := cmd.CombinedOutput()
		c.Assert(runErr, qt.IsNil, qt.Commentf("git %v: %s", args, output))
	}
	run("init", "--quiet", "--initial-branch=main")
	c.Assert(os.WriteFile(filepath.Join(repo, "README.md"), []byte("ptah\n"), 0o600), qt.IsNil)
	run("add", "README.md")
	run("commit", "--quiet", "-m", "initial")

	worktree := filepath.Join(root, "linked")
	run("worktree", "add", "--quiet", "-b", "work", worktree)
	return worktree
}

// workspaceAt is newWorkspace with the project root supplied rather than
// invented, so a test can decide what kind of directory the project lives in.
func workspaceAt(c *qt.C, root string) workspaceFixture {
	c.Helper()
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
			agentpolicy.ClassMigrations: {Dir: "migrations", Writable: true},
		},
		Dialect: platform.Postgres,
	})
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = workspace.Close() })

	policy, err := agentpolicy.Assemble(agentpolicy.LayerRules{
		Layer:  agentpolicy.LayerInvocation,
		Source: "ptah mcp flags",
		Rules: []agentpolicy.Rule{{
			Capability: agentpolicy.ArtifactWrite,
			Artifact:   agentpolicy.ClassMigrations,
			Verdict:    agentpolicy.VerdictAllow,
		}},
	})
	c.Assert(err, qt.IsNil)
	gates, err := agentgate.New(agentgate.Options{Dialect: platform.Postgres})
	c.Assert(err, qt.IsNil)
	session, err := agentapi.NewSession(agentapi.SessionConfig{
		Workspace:   workspace,
		SourceRoots: []string{root},
		Broker:      agentpolicy.NewBroker(policy),
		Gates:       gates,
	})
	c.Assert(err, qt.IsNil)

	return workspaceFixture{
		config: mcpserver.Config{Version: "test", Session: session},
		root:   root,
		dir:    dir,
	}
}
