//go:build integration

package integration_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/cli/atlas"
	"ptah.run/internal/dbtarget"
	"ptah.run/migration/lint"
)

// The compatibility surface reads the same `.ptah-lint.yaml` the native one
// does, so it owes the same sentence: a run that planned against a release
// line the operator did not name says so. It goes to stderr after the report,
// which keeps stdout and the exit code byte-identical to a run without it
// (stokaro/ptah#3420).
//
// It is a live test because `migrate lint` requires a dev database and refuses
// a dialect that disagrees with the one the URL names, so the only way to run
// a policy that declares a PostgreSQL version is against PostgreSQL.
func TestCompatMigrateLintReportsAFallbackTargetE2E(t *testing.T) {
	c := qt.New(t)
	dir := writeCompatLintVersionDir(c, t, "dialect: postgres\nserver-version: \"99\"\n")

	stdout, stderr, err := runCompatLintVersion(c, t, dir)

	c.Assert(err, qt.IsNil, qt.Commentf("%s%s", stdout, stderr))
	c.Assert(stderr, qt.Contains,
		"warning: postgres 99 is newer than the newest measured release line")
	c.Assert(stdout, qt.Not(qt.Contains), "warning:")
}

// A version that names a measured release line says nothing, so an ordinary
// run is unchanged.
func TestCompatMigrateLintSaysNothingForAMeasuredTargetE2E(t *testing.T) {
	c := qt.New(t)
	dir := writeCompatLintVersionDir(c, t, "dialect: postgres\nserver-version: \"16\"\n")

	_, stderr, err := runCompatLintVersion(c, t, dir)

	c.Assert(err, qt.IsNil)
	c.Assert(stderr, qt.Not(qt.Contains), "warning:")
}

func runCompatLintVersion(c *qt.C, t *testing.T, migrationsDir string) (stdout, stderr string, err error) {
	c.Helper()
	cmd := atlas.NewCompatCommand("atlas")
	var out, errOut bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	cmd.SetArgs([]string{
		"migrate", "lint",
		"--dir", "file://" + migrationsDir,
		"--dev-url", dbtarget.URL(c, dbtarget.PostgreSQL),
		"--latest", "1",
	})
	err = cmd.Execute()
	return out.String(), errOut.String(), err
}

func writeCompatLintVersionDir(c *qt.C, t *testing.T, policy string) string {
	c.Helper()
	dir := t.TempDir()
	files := map[string]string{
		"1_users.sql":       "CREATE TABLE ptah_compat_lint_version (id BIGINT PRIMARY KEY);\n",
		lint.ConfigFileName: policy,
	}
	for name, content := range files {
		c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600), qt.IsNil)
	}
	return dir
}

// A replay that fails still analyzed against whatever release line the policy
// named, and a partial report is where a reader is least able to tell. The
// Atlas error renderers carry no field for it, so the sentence goes out before
// the error does.
func TestCompatMigrateLintReportsAFallbackTargetOnAReplayErrorE2E(t *testing.T) {
	c := qt.New(t)
	dir := writeCompatLintVersionDir(c, t, "dialect: postgres\nserver-version: \"99\"\n")
	// A migration the server refuses, so the replay fails after the version
	// was already resolved.
	c.Assert(os.WriteFile(
		filepath.Join(dir, "2_broken.sql"),
		[]byte("CREATE TABLE ptah_compat_lint_broken (id NOT A TYPE);\n"), 0o600,
	), qt.IsNil)

	_, stderr, err := runCompatLintVersionLatest(c, t, dir, "2")

	c.Assert(err, qt.IsNotNil)
	c.Assert(stderr, qt.Contains,
		"warning: postgres 99 is newer than the newest measured release line")
}

func runCompatLintVersionLatest(c *qt.C, t *testing.T, migrationsDir, latest string) (stdout, stderr string, err error) {
	c.Helper()
	cmd := atlas.NewCompatCommand("atlas")
	var out, errOut bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	cmd.SetArgs([]string{
		"migrate", "lint",
		"--dir", "file://" + migrationsDir,
		"--dev-url", dbtarget.URL(c, dbtarget.PostgreSQL),
		"--latest", latest,
	})
	err = cmd.Execute()
	return out.String(), errOut.String(), err
}
