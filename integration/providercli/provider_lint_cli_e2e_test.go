//go:build integration

package providercli_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/lintext"
	"ptah.run/engine"
	"ptah.run/engine/builtin"
	"ptah.run/internal/lintcatalog"
	"ptah.run/internal/providerconfig"
	"ptah.run/internal/providerhost"
	"ptah.run/internal/providerpublished"
	"ptah.run/internal/testutils"
)

// ydbProviderID is the official YDB provider's identity, which the ydb-lint
// fixture serves under so that a run naming the ydb dialect reaches it.
const ydbProviderID = "github.com/stokaro/ptah-ydb-plugin"

// fixtureAliases are the engine aliases a fixture command declares, by
// command: the ydb-lint fixture declares ydbs, as the official YDB provider
// does, and a manifest must name the engines exactly as the process declares
// them.
var fixtureAliases = map[string][]string{"ydb-lint": {"ydbs"}}

// lintProvider is a manifest naming one fixture lint provider and the file
// that provider records its process ID in.
type lintProvider struct{ manifest, pidPath string }

// newLintProvider builds the fixture command and writes a manifest that
// claims engine for it under providerID.
func newLintProvider(c *qt.C, command, providerID, engineID string, environment ...string) lintProvider {
	c.Helper()
	executable := testutils.BuildFixtureProvider(c, command)
	data, err := os.ReadFile(executable)
	c.Assert(err, qt.IsNil)
	digest := sha256.Sum256(data)
	dir := c.TempDir()
	f := lintProvider{manifest: filepath.Join(dir, "providers.json"), pidPath: filepath.Join(dir, "provider.pid")}
	spec := providerhost.Spec{Executable: executable, SHA256: hex.EncodeToString(digest[:]), ProviderID: providerID,
		Environment: append(environment, "PTAH_FIXTURE_PID_FILE="+f.pidPath)}
	manifest, err := json.Marshal(providerconfig.Configuration{FormatVersion: providerconfig.FormatVersion,
		Providers: []providerconfig.Installation{{Spec: spec, Engines: []providerconfig.Engine{{ID: engineID, Aliases: fixtureAliases[command]}}}}})
	c.Assert(err, qt.IsNil)
	c.Assert(os.WriteFile(f.manifest, manifest, 0o600), qt.IsNil)
	return f
}

// lint runs `ptah migrations lint` over dir with the provider's manifest and
// returns the JSON report's findings, the diagnostics and the exit code, once
// the provider process has ended.
func (f lintProvider) lint(c *qt.C, dir string, args ...string) ([]lintedFinding, string, int) {
	c.Helper()
	out, diagnostics, code := executeProviderCLI(c.Context(),
		append([]string{"migrations", "lint", "--provider-config", f.manifest, "--dir", dir, "--format", "json"}, args...))
	providerCLI{pidPath: f.pidPath}.assertStopped(c)
	var report struct {
		Findings []lintedFinding `json:"findings"`
		Error    string          `json:"error"`
	}
	c.Assert(json.Unmarshal([]byte(out), &report), qt.IsNil, qt.Commentf("stdout %q, stderr %q", out, diagnostics))
	c.Assert(report.Error, qt.Equals, "")
	return report.Findings, diagnostics, code
}

// lintedFinding is the part of a reported finding these tests read.
type lintedFinding struct {
	Rule    string `json:"rule"`
	File    string `json:"file"`
	Message string `json:"message"`
}

func rulesOf(findings []lintedFinding) []string {
	codes := make([]string, 0, len(findings))
	for _, finding := range findings {
		codes = append(codes, finding.Rule)
	}
	slices.Sort(codes)
	return slices.Compact(codes)
}

// lintDirectory writes a migration directory of two versions, the second of
// which drops the table the first creates.
func lintDirectory(c *qt.C, create, drop string) string {
	c.Helper()
	dir := c.TempDir()
	for name, content := range map[string]string{
		"0000000001_records.up.sql":   create,
		"0000000001_records.down.sql": "-- nothing to undo\n",
		"0000000002_drop.up.sql":      drop,
		"0000000002_drop.down.sql":    "-- nothing to undo\n",
	} {
		c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600), qt.IsNil)
	}
	return dir
}

// TestMigrationsLintThroughAProviderWithoutADialect selects the provider from
// its manifest alone and names no dialect. The host splits the files without
// a dialect, so the provider's rules say nothing (FX101 would report the DROP
// TABLE), and the provider still annotates the split: DS107 reports the drop
// only because the fixture marked it as one that loses state.
func TestMigrationsLintThroughAProviderWithoutADialect(t *testing.T) {
	c := qt.New(t)
	f := newLintProvider(c, "lint", "example.org/fixture", "fixture")
	dir := lintDirectory(c, "CREATE TABLE records (id INTEGER);\n", "DROP TABLE records;\n")

	findings, diagnostics, code := f.lint(c, dir)

	c.Assert(code, qt.Equals, 1, qt.Commentf("%s", diagnostics))
	c.Assert(rulesOf(findings), qt.DeepEquals, []string{"BC103", "DS101", "DS107"})
}

// TestMigrationsLintThroughAProviderOfTheNamedDialect_FailurePath names the
// ydb dialect, which the ydb-lint fixture serves under the official
// provider's identity, with a server version. The release lines of an engine
// a provider serves are the provider's, and this fixture declares no facts
// for any, so the run is refused before the provider is asked, rather than
// linting against a target the version did not select.
func TestMigrationsLintThroughAProviderOfTheNamedDialect_FailurePath(t *testing.T) {
	c := qt.New(t)
	f := newLintProvider(c, "ydb-lint", ydbProviderID, "ydb")
	dir := lintDirectory(c, "CREATE TABLE records (id Uint64, PRIMARY KEY (id));\n", "ALTER TABLE records ADD COLUMN note Utf8;\n")

	out, diagnostics, code := executeProviderCLI(c.Context(), []string{"migrations", "lint", "--provider-config", f.manifest,
		"--dir", dir, "--format", "json", "--dialect", "ydb", "--server-version", "26.2"})

	c.Assert(code, qt.Equals, 2, qt.Commentf("%s", diagnostics))
	var report struct {
		Findings []lintedFinding `json:"findings"`
		Error    string          `json:"error"`
	}
	c.Assert(json.Unmarshal([]byte(out+diagnostics), &report), qt.IsNil, qt.Commentf("stdout %q, stderr %q", out, diagnostics))
	c.Assert(report.Error, qt.Equals, `lint server-version: "26.2" names no release line of ydb: Ptah holds no `+
		`release-line capabilities for an engine its provider serves, and the provider declares none`)
	c.Assert(report.Findings, qt.HasLen, 0)
}

// TestMigrationsLintOfTheNamedDialectReportsTheProvidersFindings is the
// control for the run above: the same provider reports FX101 on a DROP TABLE,
// the shared rules read the words its split carries, and DS107 comes from
// that split rather than from an annotation.
func TestMigrationsLintOfTheNamedDialectReportsTheProvidersFindings(t *testing.T) {
	c := qt.New(t)
	f := newLintProvider(c, "ydb-lint", ydbProviderID, "ydb")
	dir := lintDirectory(c, "CREATE TABLE records (id Uint64, PRIMARY KEY (id));\n", "DROP TABLE records;\n")

	findings, diagnostics, code := f.lint(c, dir, "--dialect", "ydb")

	c.Assert(code, qt.Equals, 1, qt.Commentf("%s", diagnostics))
	c.Assert(rulesOf(findings), qt.DeepEquals, []string{"BC103", "DS101", "DS107", "FX101", "FX102"})
}

// TestMigrationsLintReportsAProviderThatDoesNotStart keeps the command's own
// error report when the selected provider cannot run.
func TestMigrationsLintReportsAProviderThatDoesNotStart(t *testing.T) {
	c := qt.New(t)
	f := newLintProvider(c, "lint", "example.org/fixture", "fixture")
	var manifest providerconfig.Configuration
	data, err := os.ReadFile(f.manifest)
	c.Assert(err, qt.IsNil)
	c.Assert(json.Unmarshal(data, &manifest), qt.IsNil)
	manifest.Providers[0].SHA256 = strings.Repeat("0", 64)
	data, err = json.Marshal(manifest)
	c.Assert(err, qt.IsNil)
	c.Assert(os.WriteFile(f.manifest, data, 0o600), qt.IsNil)

	out, diagnostics, code := executeProviderCLI(c.Context(),
		[]string{"migrations", "lint", "--provider-config", f.manifest, "--dir", c.TempDir(), "--format", "json"})

	c.Assert(code, qt.Equals, 2)
	c.Assert(out, qt.Equals, "")
	var report struct {
		Error string `json:"error"`
	}
	c.Assert(json.Unmarshal([]byte(diagnostics), &report), qt.IsNil, qt.Commentf("%s", diagnostics))
	c.Assert(report.Error, qt.Contains, "checksum")
	_, err = os.Stat(f.pidPath)
	c.Assert(err, qt.ErrorIs, os.ErrNotExist)
}

// catalogSource lists the bundled owners' rules and the rules a provider
// published beside them: the source the rule catalog reads from a started
// provider. It lists rules and runs none.
type catalogSource struct{ bundled, provider lintext.Runtime }

var errCatalogOnly = errors.New("a rule catalog runs no rule")

func (s catalogSource) LintRules(target string) ([]lintext.Rule, error) {
	bundled, err := s.bundled.LintRules(target)
	if err != nil {
		return nil, err
	}
	published, err := s.provider.LintRules(target)
	if err != nil {
		return nil, err
	}
	rules := append(bundled, published...)
	slices.SortFunc(rules, func(a, b lintext.Rule) int { return strings.Compare(a.Code, b.Code) })
	return rules, nil
}

func (catalogSource) SplitStatements(context.Context, lintext.SplitRequest) (lintext.SplitResult, error) {
	return lintext.SplitResult{}, errCatalogOnly
}

func (catalogSource) AnnotateStatements(context.Context, lintext.AnnotateRequest) (lintext.AnnotateResult, error) {
	return lintext.AnnotateResult{}, errCatalogOnly
}

func (catalogSource) CheckMigrations(context.Context, lintext.CheckRequest) (lintext.CheckResult, error) {
	return lintext.CheckResult{}, errCatalogOnly
}

// publishedYDBFamily is the YD family the recorded YDB provider publishes
// (internal/providerpublished), in the JSON shape the ydb-lint fixture
// publishes from PTAH_FIXTURE_LINT_RULES.
func publishedYDBFamily(c *qt.C) string {
	c.Helper()
	type publishedRule struct {
		Code          string   `json:"code"`
		Title         string   `json:"title"`
		Severity      string   `json:"severity"`
		AppliesToDown bool     `json:"applies_to_down"`
		Subsumes      []string `json:"subsumes"`
	}
	var rules []publishedRule
	for _, rule := range recordedYDBRules(c) {
		rules = append(rules, publishedRule{Code: rule.Code, Title: rule.Title, Severity: string(rule.Severity),
			AppliesToDown: rule.AppliesToDown, Subsumes: rule.Subsumes})
	}
	data, err := json.Marshal(rules)
	c.Assert(err, qt.IsNil)
	return string(data)
}

// recordedYDBRules is the YD family of the recorded YDB provider, which no
// bundled owner serves.
func recordedYDBRules(c *qt.C) []lintext.Rule {
	c.Helper()
	bundled, err := builtin.New()
	c.Assert(err, qt.IsNil)
	rules, err := providerpublished.LintRules(bundled)
	c.Assert(err, qt.IsNil)
	c.Assert(rules, qt.Not(qt.HasLen), 0)
	return rules
}

// TestRuleCatalogReadsAProvidersPublishedRules builds the documented rule
// catalog with the YD family taken from a provider process instead of the
// record. The provider publishes the family in its description, the host
// reads it when the process starts, and the catalog comes out as the one the
// documentation renders from the record.
func TestRuleCatalogReadsAProvidersPublishedRules(t *testing.T) {
	c := qt.New(t)
	executable := testutils.BuildFixtureProvider(c, "ydb-lint")
	data, err := os.ReadFile(executable)
	c.Assert(err, qt.IsNil)
	digest := sha256.Sum256(data)
	host, err := providerhost.Start(c.Context(), providerhost.Spec{Executable: executable, SHA256: hex.EncodeToString(digest[:]), ProviderID: ydbProviderID,
		Environment: []string{"PTAH_FIXTURE_LINT_RULES=" + publishedYDBFamily(c)}})
	c.Assert(err, qt.IsNil)
	c.Cleanup(host.Close)
	provider, err := engine.New(host.Provider())
	c.Assert(err, qt.IsNil)
	bundled, err := builtin.New()
	c.Assert(err, qt.IsNil)
	want, err := lintcatalog.MigrationEntries()
	c.Assert(err, qt.IsNil)

	got, err := lintcatalog.MigrationEntriesFrom(catalogSource{bundled: bundled, provider: provider})

	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.DeepEquals, want)
	published, err := provider.LintRules("")
	c.Assert(err, qt.IsNil)
	c.Assert(published, qt.HasLen, len(recordedYDBRules(c)))
}
