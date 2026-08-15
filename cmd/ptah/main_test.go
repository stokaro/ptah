package main_test

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/migratesum"
	"go.5x5.cz/ptah/internal/testutils"
	"go.5x5.cz/ptah/migration/migrator"
)

// TestPtahAtlasNamespaceRemoved pins the removal of the ptah atlas command
// tree (#850): the main binary is a purely native CLI, and Atlas-style
// invocations must fail with cobra's standard unknown-command error. The
// Atlas-compatible surface ships as the separate ptah-compat binary, covered
// by cmd/ptah-compat/main_test.go.
func TestPtahAtlasNamespaceRemoved(t *testing.T) {
	c := qt.New(t)
	binPath := buildPtahBinary(c)

	tests := []struct {
		name string
		args []string
	}{
		{name: "atlas migrate status", args: []string{"atlas", "migrate", "status"}},
		{name: "atlas schema inspect", args: []string{"atlas", "schema", "inspect"}},
		{name: "atlas alone", args: []string{"atlas"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			run := newPtahProcess(binPath, tt.args...)
			var stdout, stderr bytes.Buffer
			run.Stdout = &stdout
			run.Stderr = &stderr
			err := run.Run()
			var exitErr *exec.ExitError

			c.Assert(err, qt.ErrorAs, &exitErr)
			c.Assert(exitErr.ExitCode(), qt.Equals, 2)
			c.Assert(stdout.String(), qt.Equals, "")
			c.Assert(stderr.String(), qt.Contains, `unknown command "atlas" for "ptah"`)
		})
	}
}

func TestPtahNativeSchemaApplyHelpResolves(t *testing.T) {
	c := qt.New(t)
	binPath := buildPtahBinary(c)

	run := newPtahProcess(binPath, "schema", "apply", "--help")
	var stdout, stderr bytes.Buffer
	run.Stdout = &stdout
	run.Stderr = &stderr

	err := run.Run()

	c.Assert(err, qt.IsNil)
	c.Assert(stdout.String(), qt.Contains, "Usage:\n  ptah schema apply")
	c.Assert(stderr.String(), qt.Equals, "")
}

// TestPtahNativeMigrationsCreateKeepsSuccessReport is the negative control for
// the compatibility-only silence of `ptah-compat migrate new`. Native Ptah
// keeps reporting the two paths it created.
func TestPtahNativeMigrationsCreateKeepsSuccessReport(t *testing.T) {
	c := qt.New(t)
	binPath := buildPtahBinary(c)
	dir := c.TempDir()
	run := newPtahProcess(
		binPath,
		"migrations", "create", "manual_hotfix",
		"--migrations-dir", dir,
	)
	var stdout, stderr bytes.Buffer
	run.Stdout = &stdout
	run.Stderr = &stderr

	err := run.Run()

	c.Assert(err, qt.IsNil, qt.Commentf("stderr: %s", stderr.String()))
	c.Assert(stderr.String(), qt.Equals, "")
	files, globErr := filepath.Glob(filepath.Join(dir, "*_manual_hotfix.*.sql"))
	c.Assert(globErr, qt.IsNil)
	c.Assert(files, qt.HasLen, 2)
	downPath := files[0]
	upPath := files[1]
	c.Assert(downPath, qt.Matches, `^.*\.down\.sql$`)
	c.Assert(upPath, qt.Matches, `^.*\.up\.sql$`)
	upPath, pathErr := filepath.EvalSymlinks(upPath)
	c.Assert(pathErr, qt.IsNil)
	downPath, pathErr = filepath.EvalSymlinks(downPath)
	c.Assert(pathErr, qt.IsNil)
	c.Assert(stdout.String(), qt.Equals,
		"Generated empty migration files:\n"+
			"UP:   "+upPath+"\n"+
			"DOWN: "+downPath+"\n",
	)
}

func TestPtahNativeMigrationsUpRejectsMalformedAtlasTxMode(t *testing.T) {
	c := qt.New(t)
	binPath := buildPtahBinary(c)
	dir := malformedAtlasTxModeDir(c)
	run := newPtahProcess(
		binPath,
		"migrations", "up",
		"--db-url", "sqlite://"+filepath.Join(c.TempDir(), "state.db"),
		"--migrations-dir", dir,
		"--dir-format", "atlas",
	)
	var stdout, stderr bytes.Buffer
	run.Stdout = &stdout
	run.Stderr = &stderr

	err := run.Run()
	var exitErr *exec.ExitError

	c.Assert(err, qt.ErrorAs, &exitErr)
	c.Assert(exitErr.ExitCode(), qt.Equals, 2)
	c.Assert(stderr.String(), qt.Equals,
		"error: error running migrations: "+
			"unknown txmode \"bogus\" found in file directive \"1_invalid.sql\"\n")
	c.Assert(stdout.String(), qt.Equals, "")
}

// TestPtahVersionSpellingsPrintIdenticalBytes pins every spelling of a version
// query on the native binary to the same bytes (stokaro/ptah#1064).
//
// Before this, `ptah version` printed buildinfo's five-line block while
// `ptah --version` and `ptah -v` rendered cobra's built-in one-liner
// "ptah version <v>" -- two code paths, two formats, neither aware of the
// other, so a caller parsing the output had to know which spelling it had
// used. The rows are compared against the `version` subcommand rather than
// against a literal: the Version line differs between a stamped release build
// and a `go build` from a checkout, and the property under test is mutual
// equality, not any particular version string.
func TestPtahVersionSpellingsPrintIdenticalBytes(t *testing.T) {
	c := qt.New(t)
	binPath := buildPtahBinary(c)

	want := capturePtahStdout(c, binPath, "version")
	c.Assert(want, qt.Matches,
		`Version: [^\n]+\nCommit: [^\n]+\nDate: [^\n]+\nGo: [^\n]+\nPlatform: [^\n]+\n`)

	tests := []struct {
		name string
		args []string
	}{
		{name: "long flag", args: []string{"--version"}},
		{name: "short flag", args: []string{"-v"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(capturePtahStdout(c, binPath, tt.args...), qt.Equals, want)
		})
	}
}

func capturePtahStdout(c *qt.C, binPath string, args ...string) string {
	c.Helper()
	run := newPtahProcess(binPath, args...)
	var stdout, stderr bytes.Buffer
	run.Stdout = &stdout
	run.Stderr = &stderr

	c.Assert(run.Run(), qt.IsNil, qt.Commentf("stderr: %s", stderr.String()))
	c.Assert(stderr.String(), qt.Equals, "")
	return stdout.String()
}

func buildPtahBinary(c *qt.C) string {
	c.Helper()
	binPath := filepath.Join(c.TempDir(), "ptah"+testutils.ExecutableSuffix)
	build := exec.Command("go", "build", "-o", binPath, ".")
	build.Env = append(os.Environ(), "GOWORK=off")
	buildOut, err := build.CombinedOutput()
	c.Assert(err, qt.IsNil, qt.Commentf("%s", buildOut))
	return binPath
}

func newPtahProcess(binPath string, args ...string) *exec.Cmd {
	return exec.Command(binPath, args...)
}

func malformedAtlasTxModeDir(c *qt.C) string {
	c.Helper()
	dir := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "1_invalid.sql"), []byte(
		"-- atlas:txmode bogus\n\nCREATE TABLE invalid_txmode (id INTEGER PRIMARY KEY);\n",
	), 0o600), qt.IsNil)
	_, err := migratesum.WriteWithFormat(dir, migrator.MigrationDirFormatAtlas)
	c.Assert(err, qt.IsNil)
	return dir
}
