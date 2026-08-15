//go:build integration

package integration_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
)

type lintMissingDirectoryProcessCase struct {
	name     string
	setup    func(c *qt.C, root string)
	args     func(root string) []string
	wantPath func(root string) string
}

func TestMigrateLintMissingDirectoryDiagnosticsE2E(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	t.Cleanup(cancel)

	repoRoot := e2eRepoRoot(t)
	compatBinary := filepath.Join(t.TempDir(), "ptah-compat")
	nativeBinary := filepath.Join(t.TempDir(), "ptah")
	buildPtahCompat(c, ctx, repoRoot, compatBinary)
	buildPtah(c, ctx, repoRoot, nativeBinary)

	compatCases := []lintMissingDirectoryProcessCase{
		{
			name:  "compat explicit Atlas directory",
			setup: leaveLintProjectEmpty,
			args: func(_ string) []string {
				return lintMissingDirectoryArgs("--dir", "file://nope")
			},
			wantPath: func(_ string) string { return "nope" },
		},
		{
			name:  "compat default Atlas directory",
			setup: leaveLintProjectEmpty,
			args: func(_ string) []string {
				return lintMissingDirectoryArgs()
			},
			wantPath: func(_ string) string { return "migrations" },
		},
		{
			name:  "compat Goose query",
			setup: leaveLintProjectEmpty,
			args: func(_ string) []string {
				return lintMissingDirectoryArgs("--dir", "file://nope?format=goose")
			},
			wantPath: func(_ string) string { return "nope" },
		},
		{
			name:  "compat Goose flag",
			setup: leaveLintProjectEmpty,
			args: func(_ string) []string {
				return lintMissingDirectoryArgs("--dir", "file://nope", "--dir-format", "goose")
			},
			wantPath: func(_ string) string { return "nope" },
		},
		{
			name:  "compat absolute path",
			setup: leaveLintProjectEmpty,
			args: func(root string) []string {
				return lintMissingDirectoryArgs("--dir", "file://"+filepath.Join(root, "nope"))
			},
			wantPath: func(root string) string { return filepath.Join(root, "nope") },
		},
		{
			name:  "compat cleaned nested path",
			setup: leaveLintProjectEmpty,
			args: func(_ string) []string {
				return lintMissingDirectoryArgs("--dir", "file://nested/../nope")
			},
			wantPath: func(_ string) string { return "nope" },
		},
		{
			name:  "compat atlas.hcl migration directory",
			setup: writeMissingLintProject,
			args: func(_ string) []string {
				return []string{"migrate", "lint", "--env", "local", "--latest", "1"}
			},
			wantPath: func(_ string) string { return "nope" },
		},
	}

	for _, test := range compatCases {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			root := c.TempDir()
			test.setup(c, root)

			stdout, stderr, err := runCLIProcess(ctx, root, compatBinary, test.args(root)...)
			wantStderr := "Error: sql/migrate: stat " + test.wantPath(root) + ": no such file or directory\n"

			c.Assert(exitStatusOf(c, err), qt.Equals, 1)
			c.Assert(stdout, qt.Equals, "")
			c.Assert(stderr, qt.Equals, wantStderr)
			c.Assert(stderr, qt.HasLen, len(wantStderr))
		})
	}

	t.Run("native missing directory keeps native diagnostic", func(t *testing.T) {
		c := qt.New(t)
		stdout, stderr, err := runCLIProcess(ctx, c.TempDir(), nativeBinary,
			"migrations", "lint", "--dir", "nope",
		)

		c.Assert(exitStatusOf(c, err), qt.Equals, 2)
		c.Assert(stdout, qt.Equals, "")
		c.Assert(stderr, qt.Equals,
			"error: migrations directory nope: stat nope: no such file or directory\n")
	})
}

func lintMissingDirectoryArgs(extra ...string) []string {
	args := []string{
		"migrate", "lint",
		"--dev-url", "sqlite://file?mode=memory&_fk=1",
		"--latest", "1",
	}
	return append(args, extra...)
}

func leaveLintProjectEmpty(_ *qt.C, _ string) {}

func writeMissingLintProject(c *qt.C, root string) {
	c.Helper()
	c.Assert(os.WriteFile(filepath.Join(root, "atlas.hcl"), []byte(`env "local" {
  dev = "sqlite://file?mode=memory&_fk=1"
  migration {
    dir = "file://nested/../nope"
  }
}
`), 0o600), qt.IsNil)
}
