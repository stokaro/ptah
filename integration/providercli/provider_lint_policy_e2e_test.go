//go:build integration

package providercli_test

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/providercatalog"
	"ptah.run/internal/providerinstall"
	"ptah.run/internal/providerrelease"
	"ptah.run/internal/testutils"
)

// installYDBLint puts the ydb-lint fixture in a fresh user store as `ptah
// providers install ydb` leaves the official YDB provider: the executable
// below the provider's directory and the install record naming it.
func installYDBLint(c *qt.C) {
	c.Helper()
	// The build runs the go command, which reads HOME, so it comes before the
	// user cache moves to an empty directory.
	binary, err := os.ReadFile(testutils.BuildFixtureProvider(c, "ydb-lint"))
	c.Assert(err, qt.IsNil)
	isolateStore(c)
	entry, found := providercatalog.Lookup("ydb")
	c.Assert(found, qt.IsTrue)
	store, err := providerinstall.DefaultRoot()
	c.Assert(err, qt.IsNil)
	platform := providerrelease.Running()
	executable := "ptah-plugin-ydb" + testutils.ExecutableSuffix
	dir := providerinstall.Dir(store, entry.ProviderID, "0.1.0", platform)
	c.Assert(os.MkdirAll(dir, 0o700), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, executable), binary, 0o700), qt.IsNil) // #nosec G306 G703 -- a store under the test's own temporary directory, executable by its owner
	digest := sha256.Sum256(binary)
	c.Assert(providerinstall.WriteRecord(store, platform, providerinstall.Record{
		Schema: providerinstall.RecordSchema, ProviderID: entry.ProviderID, Version: "0.1.0",
		MinContract: providerrelease.HostContract(), Engines: []string{"ydb"}, SHA256: hex.EncodeToString(digest[:]),
		Executable: "0.1.0/" + platform.OS + "_" + platform.Arch + "/" + executable,
	}), qt.IsNil)
}

// policyReport is the JSON report of a lint run, findings and error.
type policyReport struct {
	Findings []lintedFinding `json:"findings"`
	Error    string          `json:"error"`
}

// A lint policy that names an engine its installed provider serves selects
// that provider as --dialect would, with no provider flag: `migrations lint`
// lints with the provider's rules, FX101 and FX102 among them, where the
// policy's dialect was refused as one no runtime knew.
func TestMigrationsLintOfAPolicysDialectReachesTheInstalledProvider(t *testing.T) {
	c := qt.New(t)
	installYDBLint(c)
	dir := lintDirectory(c, "CREATE TABLE records (id Uint64, PRIMARY KEY (id));\n", "DROP TABLE records;\n")
	c.Assert(os.WriteFile(filepath.Join(dir, ".ptah-lint.yaml"), []byte("dialect: ydb\n"), 0o600), qt.IsNil)

	out, diagnostics, code := executeProviderCLI(c.Context(), []string{"migrations", "lint", "--dir", dir, "--format", "json"})

	c.Assert(code, qt.Equals, 1, qt.Commentf("%s", diagnostics))
	var report policyReport
	c.Assert(json.Unmarshal([]byte(out), &report), qt.IsNil, qt.Commentf("stdout %q, stderr %q", out, diagnostics))
	c.Assert(report.Error, qt.Equals, "")
	c.Assert(rulesOf(report.Findings), qt.DeepEquals, []string{"BC103", "DS101", "DS107", "FX101", "FX102"})
}

// `sql lint` reads the severities of the policy in the working directory, and
// validates the policy against the runtime of the provider --dialect selects,
// which is the one runtime that knows the dialect the policy names. The run was
// refused as `unsupported lint dialect "ydb"`, the policy read with the
// bundled engines alone.
func TestSQLLintReadsAPolicyThatNamesTheSelectedProvidersDialect(t *testing.T) {
	c := qt.New(t)
	f := newLintProvider(c, "lint-facts", ydbProviderID, "ydb", "FIXTURE_LINT_ENGINE=ydb")
	dir := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, ".ptah-lint.yaml"), []byte("dialect: ydb\n"), 0o600), qt.IsNil)
	c.Chdir(dir)

	findings, refusal, code := f.sqlLint(c, "CREATE TABLE b (id Uint64);\n", "--dialect", "ydb", "--server-version", "26.2")

	c.Assert(refusal, qt.Equals, "")
	c.Assert(findings, qt.DeepEquals, []string{`1:1 DDL001 table "b" has no primary key`})
	c.Assert(code, qt.Equals, 0)
}

// With no --dialect, the policy's own dialect selects the installed provider
// that serves it, as it does for `migrations lint`, and the policy is read
// with that provider's runtime.
func TestSQLLintReadsAPolicyThatNamesAnInstalledProvidersDialect(t *testing.T) {
	c := qt.New(t)
	installYDBLint(c)
	dir := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, ".ptah-lint.yaml"), []byte("dialect: ydb\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "change.sql"), []byte("CREATE TABLE records (id Uint64, PRIMARY KEY (id));\n"), 0o600), qt.IsNil)
	c.Chdir(dir)

	out, diagnostics, code := executeProviderCLI(c.Context(), []string{"sql", "lint", "--format", "json", "change.sql"})

	c.Assert(code, qt.Equals, 0, qt.Commentf("stdout %q, stderr %q", out, diagnostics))
	var report policyReport
	c.Assert(json.Unmarshal([]byte(out), &report), qt.IsNil, qt.Commentf("stdout %q, stderr %q", out, diagnostics))
	c.Assert(report.Error, qt.Equals, "")
}
