//go:build integration && !windows

package integration_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
)

func TestAtlasProjectMigrationDirectorySymlinkConfinementE2E(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	t.Cleanup(cancel)

	repoRoot := e2eRepoRoot(t)
	compatBinary := filepath.Join(t.TempDir(), "ptah-compat")
	buildPtahCompat(c, ctx, repoRoot, compatBinary)

	t.Run("project root symlink retains the real project directory", func(t *testing.T) {
		c := qt.New(t)
		parentDir := c.TempDir()
		projectDir := filepath.Join(parentDir, "project")
		projectLink := filepath.Join(parentDir, "project-link")
		c.Assert(os.Mkdir(projectDir, 0o700), qt.IsNil)
		c.Assert(os.Symlink(projectDir, projectLink), qt.IsNil)
		writeAtlasProjectMigrateDiffFixture(c, projectDir, "file://migrations")

		stdout, stderr, err := runCLIProcess(ctx, parentDir, compatBinary,
			"migrate", "diff", "linked",
			"--config", "file://"+filepath.ToSlash(filepath.Join(projectLink, "atlas.hcl")),
			"--env", "local",
		)

		c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
		c.Assert(stderr, qt.Equals, "")
		matches, globErr := filepath.Glob(filepath.Join(projectDir, "migrations", "*_linked.sql"))
		c.Assert(globErr, qt.IsNil)
		c.Assert(matches, qt.HasLen, 1)
		c.Assert(readDirectoryNames(c, filepath.Join(projectDir, "migrations")), qt.HasLen, 2)
	})

	t.Run("migration directory symlink cannot leave project root", func(t *testing.T) {
		c := qt.New(t)
		projectDir := c.TempDir()
		outsideDir := c.TempDir()
		c.Assert(os.Symlink(outsideDir, filepath.Join(projectDir, "migrations")), qt.IsNil)
		writeAtlasProjectMigrateDiffFixture(c, projectDir, "file://migrations")

		stdout, stderr, err := runCLIProcess(ctx, projectDir, compatBinary,
			"migrate", "diff", "escaped",
			"--config", "file://"+filepath.ToSlash(filepath.Join(projectDir, "atlas.hcl")),
			"--env", "local",
		)

		c.Assert(exitStatusOf(c, err), qt.Equals, 1)
		c.Assert(stdout, qt.Equals, "")
		c.Assert(stderr, qt.Contains, "outside allowed root")
		c.Assert(readDirectoryNames(c, outsideDir), qt.HasLen, 0)
	})
}
