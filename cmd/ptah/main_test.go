package main_test

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/internal/migratesum"
	"github.com/stokaro/ptah/migration/migrator"
)

func TestPtahAtlasCompatibilitySuccessPaths(t *testing.T) {
	c := qt.New(t)
	binPath := buildPtahBinary(c)

	c.Run("clean validation is silent", func(c *qt.C) {
		dir := cleanAtlasDir(c)
		run := newPtahProcess(binPath, "atlas", "migrate", "validate", "--dir", "file://"+dir)
		var stdout, stderr bytes.Buffer
		run.Stdout = &stdout
		run.Stderr = &stderr

		err := run.Run()

		c.Assert(err, qt.IsNil)
		c.Assert(stdout.String(), qt.Equals, "")
		c.Assert(stderr.String(), qt.Equals, "")
	})

	c.Run("nested extra token prints help", func(c *qt.C) {
		run := newPtahProcess(binPath, "atlas", "migrate", "aplly")
		var stdout, stderr bytes.Buffer
		run.Stdout = &stdout
		run.Stderr = &stderr

		err := run.Run()

		c.Assert(err, qt.IsNil)
		c.Assert(stdout.String(), qt.Contains, "Usage:\n  ptah atlas migrate [command]")
		c.Assert(stderr.String(), qt.Equals, "")
	})

	c.Run("completion group extra token prints help", func(c *qt.C) {
		run := newPtahProcess(binPath, "atlas", "completion", "sh")
		var stdout, stderr bytes.Buffer
		run.Stdout = &stdout
		run.Stderr = &stderr

		err := run.Run()

		c.Assert(err, qt.IsNil)
		c.Assert(stdout.String(), qt.Contains, "Usage:\n  ptah atlas completion [command]")
		c.Assert(stderr.String(), qt.Equals, "")
	})

	c.Run("completion script is generated for the full Ptah CLI", func(c *qt.C) {
		run := newPtahProcess(binPath, "atlas", "completion", "bash")
		var stdout, stderr bytes.Buffer
		run.Stdout = &stdout
		run.Stderr = &stderr

		err := run.Run()

		c.Assert(err, qt.IsNil)
		c.Assert(stdout.String(), qt.Contains, "# bash completion V2 for ptah")
		c.Assert(stdout.String(), qt.Contains, "__start_ptah")
		c.Assert(stderr.String(), qt.Equals, "")
	})
}

func TestPtahAtlasCompatibilityFailurePaths(t *testing.T) {
	c := qt.New(t)
	binPath := buildPtahBinary(c)

	c.Run("checksum mismatch", func(c *qt.C) {
		dir := malformedAtlasDir(c)
		run := newPtahProcess(binPath, "atlas", "migrate", "validate", "--dir", "file://"+dir)
		var stdout, stderr bytes.Buffer
		run.Stdout = &stdout
		run.Stderr = &stderr
		err := run.Run()
		var exitErr *exec.ExitError

		c.Assert(err, qt.ErrorAs, &exitErr)
		c.Assert(exitErr.ExitCode(), qt.Equals, 1)
		c.Assert(stdout.String(), qt.Equals, "You have a checksum error in your migration directory.\n"+
			"Please check your migration files and run 'atlas migrate hash' to re-hash the contents\n\n")
		c.Assert(stderr.String(), qt.Equals, "Error: checksum mismatch\n")
	})

	c.Run("missing checksum file", func(c *qt.C) {
		dir := atlasDirWithoutSum(c)
		run := newPtahProcess(binPath, "atlas", "migrate", "validate", "--dir", "file://"+dir)
		var stdout, stderr bytes.Buffer
		run.Stdout = &stdout
		run.Stderr = &stderr
		err := run.Run()
		var exitErr *exec.ExitError

		c.Assert(err, qt.ErrorAs, &exitErr)
		c.Assert(exitErr.ExitCode(), qt.Equals, 1)
		c.Assert(stdout.String(), qt.Equals, "You have a checksum error in your migration directory.\n"+
			"Please check your migration files and run 'atlas migrate hash' to re-hash the contents\n\n")
		c.Assert(stderr.String(), qt.Equals, "Error: checksum file not found\n")
	})

	c.Run("migrate set operation error", func(c *qt.C) {
		dir := cleanAtlasDir(c)
		run := newPtahProcess(
			binPath,
			"atlas", "migrate", "set", "2",
			"--url", "sqlite://"+filepath.Join(c.TempDir(), "state.db"),
			"--dir", "file://"+dir,
		)
		var stdout, stderr bytes.Buffer
		run.Stdout = &stdout
		run.Stderr = &stderr
		err := run.Run()
		var exitErr *exec.ExitError

		c.Assert(err, qt.ErrorAs, &exitErr)
		c.Assert(exitErr.ExitCode(), qt.Equals, 1)
		c.Assert(stdout.String(), qt.Equals, "")
		c.Assert(stderr.String(), qt.Equals, "Error: migration with version \"2\" not found\n")
	})

	c.Run("migrate set missing environment precedes version", func(c *qt.C) {
		run := newPtahProcess(binPath, "atlas", "migrate", "set")
		var stdout, stderr bytes.Buffer
		run.Stdout = &stdout
		run.Stderr = &stderr
		err := run.Run()
		var exitErr *exec.ExitError

		c.Assert(err, qt.ErrorAs, &exitErr)
		c.Assert(exitErr.ExitCode(), qt.Equals, 1)
		c.Assert(stdout.String(), qt.Equals, "")
		c.Assert(stderr.String(), qt.Equals, "Error: sql/migrate: stat migrations: no such file or directory\n")
	})

	c.Run("migrate set missing driver precedes version", func(c *qt.C) {
		dir := cleanAtlasDir(c)
		run := newPtahProcess(
			binPath,
			"atlas", "migrate", "set",
			"--dir", "file://"+dir,
		)
		var stdout, stderr bytes.Buffer
		run.Stdout = &stdout
		run.Stderr = &stderr
		err := run.Run()
		var exitErr *exec.ExitError

		c.Assert(err, qt.ErrorAs, &exitErr)
		c.Assert(exitErr.ExitCode(), qt.Equals, 1)
		c.Assert(stdout.String(), qt.Equals, "")
		c.Assert(stderr.String(), qt.Equals,
			"Error: sql/sqlclient: missing driver. See: https://atlasgo.io/url\n")
	})

	c.Run("migrate set missing version after environment", func(c *qt.C) {
		dir := cleanAtlasDir(c)
		dbPath := filepath.Join(c.TempDir(), "state.db")
		run := newPtahProcess(
			binPath,
			"atlas", "migrate", "set",
			"--url", "sqlite://"+dbPath,
			"--dir", "file://"+dir,
		)
		var stdout, stderr bytes.Buffer
		run.Stdout = &stdout
		run.Stderr = &stderr
		err := run.Run()
		var exitErr *exec.ExitError

		c.Assert(err, qt.ErrorAs, &exitErr)
		c.Assert(exitErr.ExitCode(), qt.Equals, 1)
		c.Assert(stdout.String(), qt.Equals, "")
		c.Assert(stderr.String(), qt.Equals, "Error: accepts 1 arg(s), received 0\n")
		_, statErr := os.Stat(dbPath)
		c.Assert(statErr, qt.IsNil)
	})

	c.Run("migrate set extra version", func(c *qt.C) {
		dir := cleanAtlasDir(c)
		run := newPtahProcess(
			binPath,
			"atlas", "migrate", "set", "1", "2",
			"--url", "sqlite://"+filepath.Join(c.TempDir(), "state.db"),
			"--dir", "file://"+dir,
		)
		var stdout, stderr bytes.Buffer
		run.Stdout = &stdout
		run.Stderr = &stderr
		err := run.Run()
		var exitErr *exec.ExitError

		c.Assert(err, qt.ErrorAs, &exitErr)
		c.Assert(exitErr.ExitCode(), qt.Equals, 1)
		c.Assert(stdout.String(), qt.Equals, "")
		c.Assert(stderr.String(), qt.Equals, "Error: accepts 1 arg(s), received 2\n")
	})

	c.Run("migrate set unknown flag", func(c *qt.C) {
		run := newPtahProcess(binPath, "atlas", "migrate", "set", "1", "--unknown")
		var stdout, stderr bytes.Buffer
		run.Stdout = &stdout
		run.Stderr = &stderr
		err := run.Run()
		var exitErr *exec.ExitError

		c.Assert(err, qt.ErrorAs, &exitErr)
		c.Assert(exitErr.ExitCode(), qt.Equals, 1)
		c.Assert(stdout.String(), qt.Equals, "")
		c.Assert(stderr.String(), qt.Equals, "Error: unknown flag: --unknown\n")
	})

	c.Run("unknown root command", func(c *qt.C) {
		run := newPtahProcess(binPath, "atlas", "definitely-not-a-command")
		var stdout, stderr bytes.Buffer
		run.Stdout = &stdout
		run.Stderr = &stderr
		err := run.Run()
		var exitErr *exec.ExitError

		c.Assert(err, qt.ErrorAs, &exitErr)
		c.Assert(exitErr.ExitCode(), qt.Equals, 1)
		c.Assert(stdout.String(), qt.Equals, "")
		c.Assert(stderr.String(), qt.Equals,
			"Error: unknown command \"definitely-not-a-command\" for \"atlas\"\n"+
				"Run 'atlas --help' for usage.\n")
	})

	c.Run("completion shell extra token", func(c *qt.C) {
		run := newPtahProcess(binPath, "atlas", "completion", "bash", "extra")
		var stdout, stderr bytes.Buffer
		run.Stdout = &stdout
		run.Stderr = &stderr
		err := run.Run()
		var exitErr *exec.ExitError

		c.Assert(err, qt.ErrorAs, &exitErr)
		c.Assert(exitErr.ExitCode(), qt.Equals, 1)
		c.Assert(stdout.String(), qt.Equals, "")
		c.Assert(stderr.String(), qt.Equals,
			"Error: unknown command \"extra\" for \"atlas completion bash\"\n")
	})
}

func buildPtahBinary(c *qt.C) string {
	c.Helper()
	binPath := filepath.Join(c.TempDir(), "ptah")
	build := exec.Command("go", "build", "-o", binPath, ".")
	build.Env = append(os.Environ(), "GOWORK=off")
	buildOut, err := build.CombinedOutput()
	c.Assert(err, qt.IsNil, qt.Commentf("%s", buildOut))
	return binPath
}

func newPtahProcess(binPath string, args ...string) *exec.Cmd {
	return exec.Command(binPath, args...)
}

func malformedAtlasDir(c *qt.C) string {
	c.Helper()
	dir := atlasDirWithoutSum(c)
	c.Assert(os.WriteFile(filepath.Join(dir, "atlas.sum"),
		[]byte("h1:tampered\n"), 0o600), qt.IsNil)
	return dir
}

func cleanAtlasDir(c *qt.C) string {
	c.Helper()
	dir := atlasDirWithoutSum(c)
	_, err := migratesum.WriteWithFormat(dir, migrator.MigrationDirFormatAtlas)
	c.Assert(err, qt.IsNil)
	return dir
}

func atlasDirWithoutSum(c *qt.C) string {
	c.Helper()
	dir := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "1_initial.sql"),
		[]byte("CREATE TABLE t (id INT);\n"), 0o600), qt.IsNil)
	return dir
}
