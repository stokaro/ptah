//go:build integration

package integration_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
)

func TestMigrateUnknownDirectoryFormatDiagnosticsE2E(t *testing.T) {
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
			name: "compat hash query",
			args: []string{"migrate", "hash", "--dir", "file://migrations?format=bogus"},
		},
		{
			name: "compat hash flag",
			args: []string{"migrate", "hash", "--dir", "file://migrations", "--dir-format", "bogus"},
		},
		{
			name: "compat validate query",
			args: []string{"migrate", "validate", "--dir", "file://migrations?format=bogus"},
		},
		{
			name: "compat validate flag",
			args: []string{"migrate", "validate", "--dir", "file://migrations", "--dir-format", "bogus"},
		},
	}

	for _, test := range compatCases {
		c.Run(test.name, func(c *qt.C) {
			stdout, stderr, err := runCLIProcess(ctx, c.TempDir(), compatBinary, test.args...)

			c.Assert(exitStatusOf(c, err), qt.Equals, 1)
			c.Assert(stdout, qt.Equals, "")
			c.Assert(stderr, qt.Equals, "Error: unknown dir format \"bogus\"\n")
			c.Assert(stderr, qt.HasLen, 34)
		})
	}

	c.Run("native validate keeps native diagnostic", func(c *qt.C) {
		dir := c.TempDir()

		stdout, stderr, err := runCLIProcess(ctx, dir, nativeBinary,
			"migrations", "validate",
			"--dir", dir,
			"--dir-format", "bogus",
		)

		c.Assert(exitStatusOf(c, err), qt.Equals, 2)
		c.Assert(stdout, qt.Equals, "")
		c.Assert(stderr, qt.Equals,
			"error: unknown migration directory format \"bogus\": expected auto, ptah, or atlas\n")
	})
}
