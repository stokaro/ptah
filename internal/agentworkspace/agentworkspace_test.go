package agentworkspace_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/agentpolicy"
	"go.5x5.cz/ptah/internal/agentworkspace"
)

// writeTree materializes a set of slash-separated paths under root.
func writeTree(c *qt.C, root string, files map[string]string) {
	for name, content := range files {
		full := filepath.Join(root, filepath.FromSlash(name))
		c.Assert(os.MkdirAll(filepath.Dir(full), 0o755), qt.IsNil)
		c.Assert(os.WriteFile(full, []byte(content), 0o600), qt.IsNil)
	}
}

// openWorkspace builds a workspace whose migrations class points at
// <root>/migrations, which is the shape every test below needs.
func openWorkspace(c *qt.C, files map[string]string) *agentworkspace.Workspace {
	root := c.TempDir()
	writeTree(c, root, files)
	c.Assert(os.MkdirAll(filepath.Join(root, "migrations"), 0o755), qt.IsNil)

	workspace, err := agentworkspace.Open(agentworkspace.Config{
		Root: root,
		Classes: map[agentpolicy.ArtifactClass]agentworkspace.ClassConfig{
			agentpolicy.ClassMigrations: {Dir: "migrations", Writable: true},
		},
		Dialect: "postgres",
	})
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = workspace.Close() })
	return workspace
}

func migrationScope(c *qt.C, workspace *agentworkspace.Workspace) *agentworkspace.Scope {
	scope, err := workspace.Scope(agentpolicy.ClassMigrations)
	c.Assert(err, qt.IsNil)
	return scope
}

func TestOpen_HappyPath(t *testing.T) {
	c := qt.New(t)

	workspace := openWorkspace(c, map[string]string{"migrations/0001.sql": "SELECT 1;\n"})

	c.Assert(workspace.Classes(), qt.DeepEquals,
		[]agentpolicy.ArtifactClass{agentpolicy.ClassMigrations})
	c.Assert(workspace.Dialect(), qt.Equals, "postgres")

	scope := migrationScope(c, workspace)
	c.Assert(scope.Class(), qt.Equals, agentpolicy.ClassMigrations)
	c.Assert(scope.Writable(), qt.IsTrue)
	c.Assert(filepath.Base(scope.Path()), qt.Equals, "migrations")
}

func TestOpen_FailurePath(t *testing.T) {
	t.Run("no root", func(t *testing.T) {
		c := qt.New(t)

		workspace, err := agentworkspace.Open(agentworkspace.Config{})

		c.Assert(err, qt.ErrorMatches, "workspace root is required")
		c.Assert(workspace, qt.IsNil)
	})

	t.Run("class directory outside the root", func(t *testing.T) {
		// The whole point of the root: a class the operator pointed outside it
		// is a configuration error rather than a wider workspace.
		c := qt.New(t)
		root := c.TempDir()
		outside := c.TempDir()

		workspace, err := agentworkspace.Open(agentworkspace.Config{
			Root: root,
			Classes: map[agentpolicy.ArtifactClass]agentworkspace.ClassConfig{
				agentpolicy.ClassMigrations: {Dir: outside},
			},
		})

		c.Assert(err, qt.ErrorMatches, `open migrations directory ".*": .*outside allowed root.*`)
		c.Assert(workspace, qt.IsNil)
	})

	t.Run("class directory reached through a symlink out of the root", func(t *testing.T) {
		c := qt.New(t)
		root := c.TempDir()
		outside := c.TempDir()
		c.Assert(os.Symlink(outside, filepath.Join(root, "migrations")), qt.IsNil)

		workspace, err := agentworkspace.Open(agentworkspace.Config{
			Root: root,
			Classes: map[agentpolicy.ArtifactClass]agentworkspace.ClassConfig{
				agentpolicy.ClassMigrations: {Dir: "migrations"},
			},
		})

		c.Assert(err, qt.ErrorMatches, `open migrations directory "migrations": .*outside allowed root.*`)
		c.Assert(workspace, qt.IsNil)
	})

	t.Run("class with no directory", func(t *testing.T) {
		c := qt.New(t)

		workspace, err := agentworkspace.Open(agentworkspace.Config{
			Root: c.TempDir(),
			Classes: map[agentpolicy.ArtifactClass]agentworkspace.ClassConfig{
				agentpolicy.ClassSchema: {},
			},
		})

		c.Assert(err, qt.ErrorMatches, `artifact class "schema" names no directory`)
		c.Assert(workspace, qt.IsNil)
	})
}

func TestScope_UnconfiguredClass(t *testing.T) {
	c := qt.New(t)
	workspace := openWorkspace(c, nil)

	scope, err := workspace.Scope(agentpolicy.ClassSchema)

	c.Assert(err, qt.ErrorIs, agentworkspace.ErrClassNotConfigured)
	c.Assert(scope, qt.IsNil)
}

func TestResolvePath_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		path string
	}{
		{name: "a plain name", path: "0001_init.up.sql"},
		{name: "a nested name", path: "cases/adds_status.yaml"},
		{name: "a dotfile", path: ".keep"},
		{name: "a name with a dot inside", path: "0001.up.sql"},
		{name: "a deeply nested name", path: "a/b/c/d.sql"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			scope := migrationScope(c, openWorkspace(c, nil))

			resolved, err := scope.ResolvePath(test.path)

			c.Assert(err, qt.IsNil)
			c.Assert(resolved, qt.Equals, test.path)
		})
	}
}

func TestResolvePath_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		path    string
		wantErr string
	}{
		{name: "empty", path: "", wantErr: `path is empty: unsafe artifact path`},
		{
			name:    "absolute",
			path:    "/etc/passwd",
			wantErr: `path "/etc/passwd" is absolute: unsafe artifact path`,
		},
		{
			name:    "parent traversal",
			path:    "../secrets.sql",
			wantErr: `path "../secrets.sql" leaves the artifact scope: unsafe artifact path`,
		},
		{
			name:    "traversal in the middle",
			path:    "a/../../b.sql",
			wantErr: `path "a/../../b.sql" leaves the artifact scope: unsafe artifact path`,
		},
		{
			// Refused on every platform, not only on Windows: a rule about what
			// a file may be called must not depend on the machine reading it.
			name:    "backslash separator",
			path:    `..\\windows\\system32`,
			wantErr: `path ".*" contains a backslash: unsafe artifact path`,
		},
		{
			name:    "drive letter",
			path:    "C:/Windows/System32/drivers/etc/hosts",
			wantErr: `path "C:/Windows/System32/drivers/etc/hosts" names a drive: unsafe artifact path`,
		},
		{
			name:    "control character",
			path:    "0001\n.sql",
			wantErr: `path "0001\\n.sql" contains a control character: unsafe artifact path`,
		},
		{
			name:    "empty component",
			path:    "a//b.sql",
			wantErr: `path "a//b.sql" has an empty component: unsafe artifact path`,
		},
		{
			name:    "trailing separator",
			path:    "cases/",
			wantErr: `path "cases/" has an empty component: unsafe artifact path`,
		},
		{
			name:    "current directory component",
			path:    "./0001.sql",
			wantErr: `path "./0001.sql" has a "." component: unsafe artifact path`,
		},
		{
			name:    "trailing dot",
			path:    "0001.sql.",
			wantErr: `path "0001.sql." has a component ending in a space or a dot: unsafe artifact path`,
		},
		{
			name:    "trailing space",
			path:    "0001.sql ",
			wantErr: `path "0001.sql " has a component ending in a space or a dot: unsafe artifact path`,
		},
		{
			name:    "reserved device name",
			path:    "CON.sql",
			wantErr: `path "CON.sql" names a reserved device: unsafe artifact path`,
		},
		{
			name:    "reserved device name in lower case",
			path:    "cases/nul.yaml",
			wantErr: `path "cases/nul.yaml" names a reserved device: unsafe artifact path`,
		},
		{
			name:    "reserved serial port",
			path:    "com1",
			wantErr: `path "com1" names a reserved device: unsafe artifact path`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			scope := migrationScope(c, openWorkspace(c, nil))

			resolved, err := scope.ResolvePath(test.path)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(err, qt.ErrorIs, agentworkspace.ErrUnsafePath)
			c.Assert(resolved, qt.Equals, "")
		})
	}
}

func TestReadFile_HappyPath(t *testing.T) {
	c := qt.New(t)
	scope := migrationScope(c, openWorkspace(c, map[string]string{
		"migrations/0001_init.up.sql": "CREATE TABLE users (id BIGINT);\n",
	}))

	content, err := scope.ReadFile("0001_init.up.sql")

	c.Assert(err, qt.IsNil)
	c.Assert(string(content), qt.Equals, "CREATE TABLE users (id BIGINT);\n")
}

func TestReadFile_FailurePath(t *testing.T) {
	t.Run("a path outside the scope", func(t *testing.T) {
		c := qt.New(t)
		scope := migrationScope(c, openWorkspace(c, map[string]string{
			"outside.txt": "not an artifact\n",
		}))

		content, err := scope.ReadFile("../outside.txt")

		c.Assert(err, qt.ErrorIs, agentworkspace.ErrUnsafePath)
		c.Assert(content, qt.IsNil)
	})

	t.Run("a directory", func(t *testing.T) {
		c := qt.New(t)
		scope := migrationScope(c, openWorkspace(c, map[string]string{
			"migrations/cases/one.yaml": "name: one\n",
		}))

		content, err := scope.ReadFile("cases")

		c.Assert(err, qt.ErrorIs, agentworkspace.ErrNotRegularFile)
		c.Assert(content, qt.IsNil)
	})

	t.Run("a symbolic link", func(t *testing.T) {
		// A link inside the scope is refused as a read target rather than
		// followed, because what it points at is not what the digest covered.
		c := qt.New(t)
		workspace := openWorkspace(c, map[string]string{"outside.txt": "not an artifact\n"})
		scope := migrationScope(c, workspace)
		c.Assert(os.Symlink(
			filepath.Join(workspace.Root(), "outside.txt"),
			filepath.Join(scope.Path(), "link.sql"),
		), qt.IsNil)

		content, err := scope.ReadFile("link.sql")

		c.Assert(err, qt.ErrorIs, agentworkspace.ErrNotRegularFile)
		c.Assert(content, qt.IsNil)
	})
}

func TestList_ReportsWhatTheScopeHolds(t *testing.T) {
	c := qt.New(t)
	workspace := openWorkspace(c, map[string]string{
		"migrations/0001.up.sql":      "SELECT 1;\n",
		"migrations/cases/one.yaml":   "name: one\n",
		"migrations/0001.down.sql":    "SELECT 2;\n",
		"outside-the-scope/other.sql": "SELECT 3;\n",
	})
	scope := migrationScope(c, workspace)
	c.Assert(os.Symlink(
		filepath.Join(workspace.Root(), "outside-the-scope", "other.sql"),
		filepath.Join(scope.Path(), "link.sql"),
	), qt.IsNil)

	entries, err := scope.List()

	c.Assert(err, qt.IsNil)
	c.Assert(entries, qt.HasLen, 4)
	c.Assert(entries[0].Path, qt.Equals, "0001.down.sql")
	c.Assert(entries[1].Path, qt.Equals, "0001.up.sql")
	c.Assert(entries[2].Path, qt.Equals, "cases/one.yaml")
	c.Assert(entries[3].Path, qt.Equals, "link.sql")
	c.Assert(entries[3].Kind, qt.Equals, "symlink")
	c.Assert(entries[3].Digest, qt.Equals, "")
	c.Assert(entries[0].Digest, qt.Matches, "sha256:[0-9a-f]{64}")
}

func TestDigest_ChangesWithContent(t *testing.T) {
	c := qt.New(t)
	workspace := openWorkspace(c, map[string]string{"migrations/0001.up.sql": "SELECT 1;\n"})
	scope := migrationScope(c, workspace)

	first, err := scope.Digest()
	c.Assert(err, qt.IsNil)

	again, err := scope.Digest()
	c.Assert(err, qt.IsNil)
	c.Assert(again, qt.Equals, first)

	c.Assert(os.WriteFile(
		filepath.Join(scope.Path(), "0001.up.sql"), []byte("SELECT 2;\n"), 0o600), qt.IsNil)
	changed, err := scope.Digest()
	c.Assert(err, qt.IsNil)
	c.Assert(changed, qt.Not(qt.Equals), first)
}

func TestDigest_ChangesWhenAFileAppears(t *testing.T) {
	// A digest that covered only the files somebody expected to matter would
	// answer "unchanged" for a directory that changed.
	c := qt.New(t)
	scope := migrationScope(c, openWorkspace(c, map[string]string{
		"migrations/0001.up.sql": "SELECT 1;\n",
	}))

	before, err := scope.Digest()
	c.Assert(err, qt.IsNil)

	c.Assert(os.WriteFile(filepath.Join(scope.Path(), ".hidden"), []byte("x"), 0o600), qt.IsNil)
	after, err := scope.Digest()

	c.Assert(err, qt.IsNil)
	c.Assert(after, qt.Not(qt.Equals), before)
}

func TestDigest_BindsContentToItsName(t *testing.T) {
	// The manifest is hashed rather than the concatenated bytes, so swapping
	// two files' contents changes the answer.
	c := qt.New(t)
	scope := migrationScope(c, openWorkspace(c, map[string]string{
		"migrations/a.sql": "SELECT 1;\n",
		"migrations/b.sql": "SELECT 2;\n",
	}))

	before, err := scope.Digest()
	c.Assert(err, qt.IsNil)

	c.Assert(os.WriteFile(filepath.Join(scope.Path(), "a.sql"), []byte("SELECT 2;\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(scope.Path(), "b.sql"), []byte("SELECT 1;\n"), 0o600), qt.IsNil)
	after, err := scope.Digest()

	c.Assert(err, qt.IsNil)
	c.Assert(after, qt.Not(qt.Equals), before)
}

func TestDigestOf_IsOrderIndependent(t *testing.T) {
	c := qt.New(t)
	entries := []agentworkspace.Entry{
		{Path: "b.sql", Digest: "sha256:bb"},
		{Path: "a.sql", Digest: "sha256:aa"},
	}
	reversed := []agentworkspace.Entry{entries[1], entries[0]}

	c.Assert(agentworkspace.DigestOf(entries), qt.Equals, agentworkspace.DigestOf(reversed))
}

func TestFoldKey_CollapsesCase(t *testing.T) {
	c := qt.New(t)

	c.Assert(agentworkspace.FoldKey("Users.SQL"), qt.Equals, agentworkspace.FoldKey("users.sql"))
	c.Assert(agentworkspace.FoldKey("users.sql"), qt.Not(qt.Equals), agentworkspace.FoldKey("user.sql"))
}
