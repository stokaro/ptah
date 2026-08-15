//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
)

// TestAtlasProjectMigrationDirectoryConfinementE2E covers the config-to-writer
// boundary from stokaro/ptah#1118. Project-owned spellings are confined even
// when they are absolute, while an operator-owned absolute --dir keeps the
// explicit CLI behavior.
func TestAtlasProjectMigrationDirectoryConfinementE2E(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	t.Cleanup(cancel)

	repoRoot := e2eRepoRoot(t)
	compatBinary := filepath.Join(t.TempDir(), "ptah-compat")
	buildPtahCompat(c, ctx, repoRoot, compatBinary)

	escapeCases := []struct {
		name   string
		dirURL func(projectDir, outsideDir string) string
	}{
		{
			name: "parent-relative directory outside project root",
			dirURL: func(_, _ string) string {
				return "file://../outside"
			},
		},
		{
			name: "absolute directory outside project root",
			dirURL: func(_, outsideDir string) string {
				return "file://" + filepath.ToSlash(outsideDir)
			},
		},
	}

	for _, test := range escapeCases {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			parentDir := c.TempDir()
			projectDir := filepath.Join(parentDir, "project")
			outsideDir := filepath.Join(parentDir, "outside")
			c.Assert(os.Mkdir(projectDir, 0o700), qt.IsNil)
			c.Assert(os.Mkdir(outsideDir, 0o700), qt.IsNil)
			writeAtlasProjectMigrateDiffFixture(c, projectDir, test.dirURL(projectDir, outsideDir))

			stdout, stderr, err := runCLIProcess(ctx, projectDir, compatBinary,
				"migrate", "diff", "confined",
				"--config", "file://"+filepath.ToSlash(filepath.Join(projectDir, "atlas.hcl")),
				"--env", "local",
			)

			c.Assert(exitStatusOf(c, err), qt.Equals, 1)
			c.Assert(stdout, qt.Equals, "")
			c.Assert(stderr, qt.Contains, "outside allowed root")
			c.Assert(readDirectoryNames(c, outsideDir), qt.HasLen, 0)
		})
	}

	insideCases := []struct {
		name   string
		dirURL func(projectDir, migrationsDir string) string
	}{
		{
			name: "relative project directory",
			dirURL: func(_, _ string) string {
				return "file://migrations"
			},
		},
		{
			name: "absolute project directory inside root",
			dirURL: func(_, migrationsDir string) string {
				return "file://" + filepath.ToSlash(migrationsDir)
			},
		},
	}

	for _, test := range insideCases {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			projectDir := c.TempDir()
			migrationsDir := filepath.Join(projectDir, "migrations")
			writeAtlasProjectMigrateDiffFixture(c, projectDir, test.dirURL(projectDir, migrationsDir))

			stdout, stderr, err := runCLIProcess(ctx, projectDir, compatBinary,
				"migrate", "diff", "inside",
				"--config", "file://"+filepath.ToSlash(filepath.Join(projectDir, "atlas.hcl")),
				"--env", "local",
			)

			c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
			c.Assert(stderr, qt.Equals, "")
			matches, globErr := filepath.Glob(filepath.Join(migrationsDir, "*_inside.sql"))
			c.Assert(globErr, qt.IsNil)
			c.Assert(matches, qt.HasLen, 1)
			c.Assert(readDirectoryNames(c, migrationsDir), qt.HasLen, 2)
		})
	}

	t.Run("explicit CLI absolute directory remains unbounded", func(t *testing.T) {
		c := qt.New(t)
		parentDir := c.TempDir()
		projectDir := filepath.Join(parentDir, "project")
		outsideDir := filepath.Join(parentDir, "outside")
		c.Assert(os.Mkdir(projectDir, 0o700), qt.IsNil)
		c.Assert(os.Mkdir(outsideDir, 0o700), qt.IsNil)
		writeAtlasProjectMigrateDiffFixture(c, projectDir, "file://../blocked-by-cli-override")

		stdout, stderr, err := runCLIProcess(ctx, projectDir, compatBinary,
			"migrate", "diff", "explicit",
			"--config", "file://"+filepath.ToSlash(filepath.Join(projectDir, "atlas.hcl")),
			"--env", "local",
			"--dir", "file://"+filepath.ToSlash(outsideDir),
		)

		c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
		c.Assert(stderr, qt.Equals, "")
		matches, globErr := filepath.Glob(filepath.Join(outsideDir, "*_explicit.sql"))
		c.Assert(globErr, qt.IsNil)
		c.Assert(matches, qt.HasLen, 1)
		c.Assert(readDirectoryNames(c, outsideDir), qt.HasLen, 2)
	})
}

func writeAtlasProjectMigrateDiffFixture(c *qt.C, projectDir, migrationDirURL string) {
	c.Helper()
	c.Assert(os.WriteFile(
		filepath.Join(projectDir, "schema.sql"),
		[]byte("CREATE TABLE users (id INTEGER PRIMARY KEY);\n"),
		0o600,
	), qt.IsNil)
	c.Assert(os.WriteFile(
		filepath.Join(projectDir, "atlas.hcl"),
		fmt.Appendf(nil, `env "local" {
  dev = "sqlite://%s"
  schema {
    src = "file://schema.sql"
  }
  migration {
    dir = %q
  }
}
`, filepath.ToSlash(filepath.Join(projectDir, "dev.db")), migrationDirURL),
		0o600,
	), qt.IsNil)
}
