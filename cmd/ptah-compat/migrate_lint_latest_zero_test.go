package main_test

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

func TestCompatBinaryMigrateLintLatestZeroWithoutGitMatchesAtlas(t *testing.T) {
	t.Setenv("PTAH_ATLAS_LINT_ALL_VERSIONS", "false")
	t.Setenv("PTAH_ATLAS_LINT_WITHOUT_DEV_URL", "false")
	c := qt.New(t)
	binPath := buildCompatBinary(c)
	run := newCompatProcess(
		binPath,
		"migrate", "lint",
		"--dir", "file://migrations",
		"--dev-url", "sqlite://file?mode=memory",
		"--latest", "0",
	)
	run.Dir = c.TempDir()
	var stdout, stderr bytes.Buffer
	run.Stdout = &stdout
	run.Stderr = &stderr

	err := run.Run()
	var exitErr *exec.ExitError

	c.Assert(err, qt.ErrorAs, &exitErr)
	c.Assert(exitErr.ExitCode(), qt.Equals, 1)
	c.Assert(stdout.String(), qt.Equals, "")
	c.Assert(stderr.String(), qt.Equals, "Error: --latest or --git-base is required\n")
}

func TestCompatBinaryMigrateLintLatestZeroUsesExplicitGit(t *testing.T) {
	t.Setenv("PTAH_ATLAS_LINT_ALL_VERSIONS", "false")
	t.Setenv("PTAH_ATLAS_LINT_WITHOUT_DEV_URL", "false")
	c := qt.New(t)
	binPath := buildCompatBinary(c)
	dir := cleanAtlasDir(c)
	gitDir := c.TempDir()
	run := newCompatProcess(
		binPath,
		"migrate", "lint",
		"--dir", "file://"+dir,
		"--dev-url", "sqlite://file?mode=memory",
		"--latest", "0",
		"--git-base", "HEAD",
		"--git-dir", gitDir,
	)
	var stdout, stderr bytes.Buffer
	run.Stdout = &stdout
	run.Stderr = &stderr

	err := run.Run()
	var exitErr *exec.ExitError

	c.Assert(err, qt.ErrorAs, &exitErr)
	c.Assert(exitErr.ExitCode(), qt.Equals, 1)
	c.Assert(stdout.String(), qt.Equals, "")
	c.Assert(stderr.String(), qt.Contains, "Error: find git repository root:")
	c.Assert(stderr.String(), qt.Not(qt.Contains), "mutually exclusive")
}

func TestCompatBinaryMigrateLintLatestZeroUsesConfiguredGit(t *testing.T) {
	t.Setenv("PTAH_ATLAS_LINT_ALL_VERSIONS", "false")
	t.Setenv("PTAH_ATLAS_LINT_WITHOUT_DEV_URL", "false")
	c := qt.New(t)
	binPath := buildCompatBinary(c)
	dir := cleanAtlasDir(c)
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
	run := newCompatProcess(
		binPath,
		"migrate", "lint",
		"--env", "ci",
		"--dir", "file://"+dir,
		"--dev-url", "sqlite://file?mode=memory",
		"--latest", "0",
	)
	run.Dir = root
	var stdout, stderr bytes.Buffer
	run.Stdout = &stdout
	run.Stderr = &stderr

	err := run.Run()
	var exitErr *exec.ExitError

	c.Assert(err, qt.ErrorAs, &exitErr)
	c.Assert(exitErr.ExitCode(), qt.Equals, 1)
	c.Assert(stdout.String(), qt.Equals, "")
	c.Assert(stderr.String(), qt.Equals, "Error: --git-base \"-unsafe\" is not a safe Git ref\n")
}

func TestCompatBinaryMigrateLintLatestControls(t *testing.T) {
	t.Setenv("PTAH_ATLAS_LINT_ALL_VERSIONS", "false")
	t.Setenv("PTAH_ATLAS_LINT_WITHOUT_DEV_URL", "false")
	c := qt.New(t)
	binPath := buildCompatBinary(c)
	dir := cleanAtlasDir(c)

	c.Run("positive latest still selects a version", func(c *qt.C) {
		devURL := "sqlite://" + filepath.Join(c.TempDir(), "dev.db")
		run := newCompatProcess(
			binPath,
			"migrate", "lint",
			"--dir", "file://"+dir,
			"--dev-url", devURL,
			"--latest", "1",
		)
		var stdout, stderr bytes.Buffer
		run.Stdout = &stdout
		run.Stderr = &stderr

		err := run.Run()

		c.Assert(err, qt.IsNil, qt.Commentf("stderr: %s", stderr.String()))
		c.Assert(stdout.String(), qt.Contains, "-- 1 version ok")
		c.Assert(stderr.String(), qt.Equals, "")
	})

	c.Run("positive latest and Git remain exclusive", func(c *qt.C) {
		devURL := "sqlite://" + filepath.Join(c.TempDir(), "dev.db")
		run := newCompatProcess(
			binPath,
			"migrate", "lint",
			"--dir", "file://"+dir,
			"--dev-url", devURL,
			"--latest", "1",
			"--git-base", "HEAD",
		)
		var stdout, stderr bytes.Buffer
		run.Stdout = &stdout
		run.Stderr = &stderr

		err := run.Run()
		var exitErr *exec.ExitError

		c.Assert(err, qt.ErrorAs, &exitErr)
		c.Assert(exitErr.ExitCode(), qt.Equals, 1)
		c.Assert(stdout.String(), qt.Equals, "")
		c.Assert(stderr.String(), qt.Equals, "Error: --latest and --git-base are mutually exclusive\n")
	})

	c.Run("all-versions opt-in remains usable with zero", func(c *qt.C) {
		devURL := "sqlite://" + filepath.Join(c.TempDir(), "dev.db")
		run := newCompatProcess(
			binPath,
			"migrate", "lint",
			"--dir", "file://"+dir,
			"--dev-url", devURL,
			"--latest", "0",
		)
		run.Env = append(os.Environ(), "PTAH_ATLAS_LINT_ALL_VERSIONS=1")
		var stdout, stderr bytes.Buffer
		run.Stdout = &stdout
		run.Stderr = &stderr

		err := run.Run()

		c.Assert(err, qt.IsNil, qt.Commentf("stderr: %s", stderr.String()))
		c.Assert(stdout.String(), qt.Contains, "-- 1 version ok")
		c.Assert(stderr.String(), qt.Equals, "")
	})
}
