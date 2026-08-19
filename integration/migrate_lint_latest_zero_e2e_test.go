//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"testing/fstest"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/config/projectconfig"
	"go.5x5.cz/ptah/internal/migratesum"
	"go.5x5.cz/ptah/internal/migrationlintreport"
	migrationlint "go.5x5.cz/ptah/migration/lint"
	"go.5x5.cz/ptah/migration/migrator"
)

func TestCompatMigrateLintLatestZeroE2E(t *testing.T) {
	t.Setenv("PTAH_ATLAS_LINT_ALL_VERSIONS", "false")
	t.Setenv("PTAH_ATLAS_LINT_WITHOUT_DEV_URL", "false")
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	t.Cleanup(cancel)

	repoRoot := e2eRepoRoot(t)
	binaryPath := filepath.Join(t.TempDir(), "ptah-compat")
	buildPtahCompat(c, ctx, repoRoot, binaryPath)
	migrationsDir := writeLatestZeroAtlasDir(c)

	t.Run("zero without Git has no usable selector", func(t *testing.T) {
		c := qt.New(t)
		stdout, stderr, err := runLatestZeroCompatProcess(
			ctx,
			c.TempDir(),
			binaryPath,
			"migrate", "lint",
			"--dir", "file://migrations",
			"--dev-url", "sqlite://file?mode=memory",
			"--latest", "0",
		)

		c.Assert(exitStatusOf(c, err), qt.Equals, 1)
		c.Assert(stdout, qt.Equals, "")
		c.Assert(stderr, qt.Equals, "Error: --latest or --git-base is required\n")
	})

	t.Run("zero allows explicit Git", func(t *testing.T) {
		c := qt.New(t)
		stdout, stderr, err := runLatestZeroCompatProcess(
			ctx,
			"",
			binaryPath,
			"migrate", "lint",
			"--dir", "file://"+migrationsDir,
			"--dev-url", "sqlite://file?mode=memory",
			"--latest", "0",
			"--git-base", "HEAD",
			"--git-dir", c.TempDir(),
		)

		c.Assert(exitStatusOf(c, err), qt.Equals, 1)
		c.Assert(stdout, qt.Equals, "")
		c.Assert(stderr, qt.Contains, "Error: find git repository root:")
		c.Assert(stderr, qt.Not(qt.Contains), "mutually exclusive")
	})

	t.Run("zero suppresses project latest and allows project Git", func(t *testing.T) {
		c := qt.New(t)
		root := c.TempDir()
		project := `lint {
  latest = 1
}
env "ci" {
  lint {
    git {
      base = "-unsafe"
      dir  = "/not/a/repository"
    }
  }
}
`
		c.Assert(os.WriteFile(filepath.Join(root, "atlas.hcl"), []byte(project), 0o600), qt.IsNil)

		stdout, stderr, err := runLatestZeroCompatProcess(
			ctx,
			root,
			binaryPath,
			"migrate", "lint",
			"--env", "ci",
			"--dir", "file://"+migrationsDir,
			"--dev-url", "sqlite://file?mode=memory",
			"--latest", "0",
		)

		c.Assert(exitStatusOf(c, err), qt.Equals, 1)
		c.Assert(stdout, qt.Equals, "")
		c.Assert(stderr, qt.Equals, "Error: --git-base \"-unsafe\" is not a safe Git ref\n")
	})

	t.Run("positive latest still selects a version", func(t *testing.T) {
		c := qt.New(t)
		stdout, stderr, err := runLatestZeroCompatProcess(
			ctx,
			"",
			binaryPath,
			"migrate", "lint",
			"--dir", "file://"+migrationsDir,
			"--dev-url", "sqlite://"+filepath.Join(c.TempDir(), "dev.db"),
			"--latest", "1",
		)

		c.Assert(err, qt.IsNil, qt.Commentf("stderr: %s", stderr))
		c.Assert(stdout, qt.Contains, "-- 1 version ok")
		c.Assert(stderr, qt.Equals, "")
	})

	t.Run("positive latest and Git remain exclusive", func(t *testing.T) {
		c := qt.New(t)
		stdout, stderr, err := runLatestZeroCompatProcess(
			ctx,
			"",
			binaryPath,
			"migrate", "lint",
			"--dir", "file://"+migrationsDir,
			"--dev-url", "sqlite://"+filepath.Join(c.TempDir(), "dev.db"),
			"--latest", "1",
			"--git-base", "HEAD",
		)

		c.Assert(exitStatusOf(c, err), qt.Equals, 1)
		c.Assert(stdout, qt.Equals, "")
		c.Assert(stderr, qt.Equals, "Error: --latest and --git-base are mutually exclusive\n")
	})

	t.Run("all-versions opt-in remains usable with zero", func(t *testing.T) {
		c := qt.New(t)
		c.Setenv("PTAH_ATLAS_LINT_ALL_VERSIONS", "1")
		stdout, stderr, err := runLatestZeroCompatProcess(
			ctx,
			"",
			binaryPath,
			"migrate", "lint",
			"--dir", "file://"+migrationsDir,
			"--dev-url", "sqlite://"+filepath.Join(c.TempDir(), "dev.db"),
			"--latest", "0",
		)

		c.Assert(err, qt.IsNil, qt.Commentf("stderr: %s", stderr))
		c.Assert(stdout, qt.Contains, "-- 1 version ok")
		c.Assert(stderr, qt.Equals, "")
	})
}

func TestMigrationLintReportAtlasExplicitZeroAllowsExplicitGitSelectorE2E(t *testing.T) {
	c := qt.New(t)

	_, err := migrationlintreport.Build(t.Context(), migrationlintreport.Options{
		Dir:           t.TempDir(),
		FS:            fstest.MapFS{"1_init.sql": {Data: []byte("CREATE TABLE users (id int);\n")}},
		DirFormat:     string(migrator.MigrationDirFormatAtlas),
		Dialect:       "sqlite",
		GitBase:       "HEAD",
		GitDir:        t.TempDir(),
		FailOn:        migrationlintreport.FailOnNone,
		Compatibility: migrationlint.CompatibilityProfileAtlas,
		Changed: migrationlintreport.ChangedOptions{
			Dialect: true,
			GitBase: true,
			Latest:  true,
		},
	}, projectconfig.Config{})

	c.Assert(err, qt.ErrorMatches, `find git repository root: .*`)
}

func writeLatestZeroAtlasDir(c *qt.C) string {
	c.Helper()
	dir := c.TempDir()
	c.Assert(os.WriteFile(
		filepath.Join(dir, "1_initial.sql"),
		[]byte("CREATE TABLE t (id INT);\n"),
		0o600,
	), qt.IsNil)
	_, err := migratesum.WriteWithFormat(dir, migrator.MigrationDirFormatAtlas)
	c.Assert(err, qt.IsNil)
	return dir
}

func runLatestZeroCompatProcess(
	ctx context.Context,
	dir,
	binaryPath string,
	args ...string,
) (stdoutText, stderrText string, err error) {
	cmd := exec.CommandContext(ctx, binaryPath, args...)
	cmd.Dir = dir
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err = cmd.Run()
	return stdout.String(), stderr.String(), err
}
