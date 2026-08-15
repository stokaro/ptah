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

func TestMigrateValidateDirectoryDiagnosticsE2E(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	t.Cleanup(cancel)

	repoRoot := e2eRepoRoot(t)
	compatBinary := filepath.Join(t.TempDir(), "ptah-compat")
	nativeBinary := filepath.Join(t.TempDir(), "ptah")
	buildPtahCompat(c.TB, ctx, repoRoot, compatBinary)
	buildPtah(c.TB, ctx, repoRoot, nativeBinary)

	compatCases := []struct {
		name       string
		args       []string
		wantStderr string
	}{
		{
			name:       "compat hash Atlas layout",
			args:       []string{"migrate", "hash", "--dir", "file://migrations"},
			wantStderr: "Error: migrations directory migrations: stat migrations: no such file or directory\n",
		},
		{
			name:       "compat hash Goose layout",
			args:       []string{"migrate", "hash", "--dir", "file://migrations?format=goose"},
			wantStderr: "Error: migrations directory migrations: stat migrations: no such file or directory\n",
		},
		{
			name:       "compat validate Atlas layout",
			args:       []string{"migrate", "validate", "--dir", "file://migrations"},
			wantStderr: "Error: sql/migrate: stat migrations: no such file or directory\n",
		},
		{
			name: "compat validate Goose layout via flag",
			args: []string{
				"migrate", "validate", "--dir", "file://migrations", "--dir-format", "goose",
			},
			wantStderr: "Error: sql/migrate: stat migrations: no such file or directory\n",
		},
		{
			name:       "compat validate Goose layout via URL",
			args:       []string{"migrate", "validate", "--dir", "file://migrations?format=goose"},
			wantStderr: "Error: sql/migrate: stat migrations: no such file or directory\n",
		},
	}

	for _, test := range compatCases {
		c.Run(test.name, func(c *qt.C) {
			dir := c.TempDir()

			stdout, stderr, err := runCLIProcess(ctx, dir, compatBinary, test.args...)

			c.Assert(exitStatusOf(c.TB, err), qt.Equals, 1)
			c.Assert(stdout, qt.Equals, "")
			c.Assert(stderr, qt.Equals, test.wantStderr)
		})
	}

	c.Run("compat regular file keeps prior diagnostic", func(c *qt.C) {
		dir := c.TempDir()
		c.Assert(os.WriteFile(filepath.Join(dir, "migration.sql"), []byte("SELECT 1;\n"), 0o600), qt.IsNil)

		stdout, stderr, err := runCLIProcess(ctx, dir, compatBinary,
			"migrate", "validate", "--dir", "file://migration.sql",
		)

		c.Assert(exitStatusOf(c.TB, err), qt.Equals, 1)
		c.Assert(stdout, qt.Equals, "")
		c.Assert(stderr, qt.Equals,
			"Error: migrations directory migration.sql: not a directory\n")
	})

	c.Run("native missing directory keeps native diagnostic", func(c *qt.C) {
		dir := c.TempDir()

		stdout, stderr, err := runCLIProcess(ctx, dir, nativeBinary,
			"migrations", "validate", "--dir", "migrations",
		)

		c.Assert(exitStatusOf(c.TB, err), qt.Equals, 2)
		c.Assert(stdout, qt.Equals, "")
		c.Assert(stderr, qt.Equals,
			"error: migrations directory migrations: stat migrations: no such file or directory\n")
	})
}
