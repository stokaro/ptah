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

func TestMigrateDirectorySchemeDiagnosticsE2E(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	t.Cleanup(cancel)

	repoRoot := e2eRepoRoot(t)
	compatBinary := filepath.Join(t.TempDir(), "ptah-compat")
	nativeBinary := filepath.Join(t.TempDir(), "ptah")
	buildPtahCompat(c, ctx, repoRoot, compatBinary)
	buildPtah(c, ctx, repoRoot, nativeBinary)

	compatCases := []struct {
		name string
		args []string
	}{
		{
			name: "compat hash",
			args: []string{"migrate", "hash", "--dir", "migrations"},
		},
		{
			name: "compat validate",
			args: []string{"migrate", "validate", "--dir", "migrations"},
		},
		{
			name: "compat status checks the directory before the database URL",
			args: []string{"migrate", "status", "--dir", "migrations"},
		},
		{
			name: "compat lint",
			args: []string{
				"migrate", "lint",
				"--dir", "migrations",
				"--dev-url", "sqlite://file?mode=memory&_fk=1",
				"--latest", "1",
			},
		},
		{
			name: "compat new",
			args: []string{"migrate", "new", "demo", "--dir", "migrations"},
		},
		{
			name: "compat diff",
			args: []string{"migrate", "diff", "demo", "--dir", "migrations"},
		},
	}

	for _, test := range compatCases {
		c.Run(test.name, func(c *qt.C) {
			root := c.TempDir()

			stdout, stderr, err := runCLIProcess(ctx, root, compatBinary, test.args...)

			const wantStderr = "Error: missing scheme for dir url. Did you mean \"file://migrations\"? \n"
			c.Assert(exitStatusOf(c, err), qt.Equals, 1)
			c.Assert(stdout, qt.Equals, "")
			c.Assert(stderr, qt.Equals, wantStderr)
			c.Assert(stderr, qt.HasLen, 70)
			c.Assert(readDirectoryNames(c, root), qt.HasLen, 0)
		})
	}

	c.Run("native validate and status retain bare paths", func(c *qt.C) {
		root := c.TempDir()
		migrationDir := filepath.Join(root, "migrations")
		c.Assert(os.MkdirAll(migrationDir, 0o755), qt.IsNil)
		c.Assert(os.WriteFile(
			filepath.Join(migrationDir, "20240101000000_init.sql"),
			[]byte("CREATE TABLE users (id INTEGER PRIMARY KEY);\n"),
			0o600,
		), qt.IsNil)

		stdout, stderr, err := runCLIProcess(ctx, root, compatBinary,
			"migrate", "hash", "--dir", "file://"+migrationDir,
		)
		c.Assert(err, qt.IsNil)
		c.Assert(stdout, qt.Equals, "")
		c.Assert(stderr, qt.Equals, "")

		_, stderr, err = runCLIProcess(ctx, root, nativeBinary,
			"migrations", "validate", "--dir", migrationDir, "--dir-format", "atlas",
		)
		c.Assert(err, qt.IsNil)
		c.Assert(stderr, qt.Equals, "")

		_, stderr, err = runCLIProcess(ctx, root, nativeBinary,
			"migrations", "status",
			"--migrations-dir", migrationDir,
			"--dir-format", "atlas",
			"--db-url", "sqlite://"+filepath.Join(root, "status.db"),
		)
		c.Assert(err, qt.IsNil)
		c.Assert(stderr, qt.Equals, "")
	})
}

func readDirectoryNames(c *qt.C, path string) []string {
	c.Helper()
	entries, err := os.ReadDir(path)
	c.Assert(err, qt.IsNil)
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	return names
}
