//go:build integration

package integration_test

import (
	"crypto/sha256"
	"encoding/hex"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	transport "github.com/stokaro/ptah/provider-sdk/runtime"
)

// contract25SDK is the newest SDK release of contract 25.0. The
// mixed-version tests build fixture programs against it, so that one side of
// the exchange is a peer of an older contract minor.
const contract25SDK = "v0.2.0"

// buildContract25Fixture builds a fixture program against [contract25SDK],
// from a copy of the fixture module, with no workspace. Such a program speaks
// contract 25.0 and knows none of the features a later minor adds. Only a
// program whose packages use nothing of a later minor builds this way:
// provider, lint and contract-host.
func buildContract25Fixture(c *qt.C, program string) string {
	c.Helper()
	_, source, _, ok := runtime.Caller(0)
	c.Assert(ok, qt.IsTrue)
	dir := c.TempDir()
	c.Assert(os.CopyFS(dir, os.DirFS(filepath.Join(filepath.Dir(source), "..", "testdata", "fixture"))), qt.IsNil)
	runGo(c, dir, "get", "github.com/stokaro/ptah/provider-sdk@"+contract25SDK)
	binary := filepath.Join(c.TempDir(), program+"-contract25.exe")
	runGo(c, dir, "build", "-o", binary, "./cmd/"+program)
	// The build records the module versions it linked, which is what shows
	// that this side of the exchange is the older peer.
	c.Assert(runGo(c, dir, "version", "-m", binary), qt.Matches, `(?s).*\sgithub\.com/stokaro/ptah/provider-sdk\s+`+regexp.QuoteMeta(contract25SDK)+`\s.*`)
	return binary
}

func runGo(c *qt.C, dir string, args ...string) string {
	c.Helper()
	command := exec.CommandContext(c.Context(), "go", args...)
	command.Dir = dir
	command.Env = append(os.Environ(), "GOWORK=off")
	output, err := command.CombinedOutput()
	c.Assert(err, qt.IsNil, qt.Commentf("go %v: %s", args, output))
	return string(output)
}

// newHostOptions start a provider as a host of this SDK does: it announces
// its contract and lists every two-way feature.
func newHostOptions(c *qt.C, binary string) transport.Options {
	c.Helper()
	data, err := os.ReadFile(binary)
	c.Assert(err, qt.IsNil)
	checksum := sha256.Sum256(data)
	return transport.Options{Executable: binary, SHA256: hex.EncodeToString(checksum[:]), ProviderID: "example.org/fixture",
		HostFeatures: provider.TwoWayFeatures()}
}

func offered(description *protocol.Description) []string {
	features := slices.Clone(description.GetFeatures())
	for _, engine := range description.GetEngines() {
		features = append(features, engine.GetFeatures()...)
	}
	return features
}

// A host of this SDK starts a provider built from the SDK of contract 25.0.
// The provider declares no minimum, so it needs contract 25.0, and offers none
// of the additions. What contract 25.0 had works; an addition is
// refused before anything is sent.
func TestNewHostWithAContract25Provider(t *testing.T) {
	c := qt.New(t)
	client := start(c, newHostOptions(c, buildContract25Fixture(c, "provider")))
	description := client.Description()
	c.Assert(provider.ContractString(provider.MinimumContract(description)), qt.Equals, "25.0")
	added := append(provider.TwoWayFeatures(), provider.TypeRowsFeature, provider.ReadRowsFeature, provider.RenderRowWritesFeature,
		provider.SeedsFeature, provider.DevCleanupFeature, provider.ReplayGuardFeature)
	for _, feature := range added {
		c.Assert(offered(description), qt.Not(qt.Contains), feature)
	}
	c.Assert(description.GetEngines()[0].GetQueryDialect(), qt.IsNil)
	session := open(c, client, "")
	result, err := client.Execute(c.Context(), execution(session.Id))
	c.Assert(err, qt.IsNil)
	c.Assert(result.GetOutcome(), qt.Equals, protocol.Execution_APPLIED)
	timed := execution(session.Id)
	timed.RequestId, timed.StatementTimeoutNanoseconds = "timed", int64(time.Minute)
	_, err = client.Execute(c.Context(), timed)
	c.Assert(err, qt.ErrorMatches, `.*statement timeouts were not negotiated`)
	_, err = client.ReadRows(c.Context(), &protocol.ReadRowsRequest{SessionId: session.Id, Table: &protocol.Reference{Kind: "table", Name: "first"},
		Columns: []string{"id"}, KeyColumns: []string{"id"}, MaxRows: 1})
	c.Assert(err, qt.ErrorMatches, `.*data\.read-rows was not negotiated`)
	_, err = client.ListDevObjects(c.Context(), &protocol.ListDevObjectsRequest{SessionId: session.Id})
	c.Assert(err, qt.ErrorMatches, `.*database\.dev-cleanup was not negotiated`)
	after, err := client.Inspect(c.Context(), &protocol.InspectRequest{SessionId: session.Id})
	c.Assert(err, qt.IsNil)
	c.Assert(after.GetTables(), qt.HasLen, 2)
}

// A lint provider built from the SDK of contract 25.0 splits and annotates
// with the facts of that contract. A host of this SDK does not ask it for a statement fact it does not
// offer.
func TestNewHostWithAContract25Linter(t *testing.T) {
	c := qt.New(t)
	client := start(c, newHostOptions(c, buildContract25Fixture(c, "lint")))
	sources := []*protocol.LintSource{{Name: "1.sql", Sql: "CREATE TABLE a (id INTEGER);\nDROP TABLE a;", Up: true}}
	split, err := client.SplitLintStatements(c.Context(), &protocol.SplitLintRequest{Engine: "fixture", Sources: sources})
	c.Assert(err, qt.IsNil)
	c.Assert(split.Files[0].Statements, qt.HasLen, 2)
	files := []*protocol.LintFile{{Name: "1.sql", Up: true, Statements: split.Files[0].Statements}}
	_, err = client.AnnotateLintStatements(c.Context(), &protocol.AnnotateLintRequest{Engine: "fixture",
		Facts: []protocol.LintFact{protocol.LintFact_LINT_FACT_STATEMENT_KIND}, Sources: sources, Files: files})
	c.Assert(err, qt.ErrorMatches, `.*unnegotiated.*`)
	annotated, err := client.AnnotateLintStatements(c.Context(), &protocol.AnnotateLintRequest{Engine: "fixture",
		Facts: []protocol.LintFact{protocol.LintFact_LINT_FACT_STATEFUL_DROP}, Sources: sources, Files: files})
	c.Assert(err, qt.IsNil)
	c.Assert(annotated.Files[0].Statements[1].Effects[0].Fact, qt.Equals, protocol.LintFact_LINT_FACT_STATEFUL_DROP)
}

// runContractHost runs the contract-25.0 host against a provider and returns the
// lines it printed.
func runContractHost(c *qt.C, host, executable, mode, address string) []string {
	c.Helper()
	command := exec.CommandContext(c.Context(), host, executable, mode, address) // #nosec G204 -- the test built both executables
	output, err := command.CombinedOutput()
	c.Assert(err, qt.IsNil, qt.Commentf("contract host: %s", output))
	return strings.Split(strings.TrimSpace(strings.ReplaceAll(string(output), "\r\n", "\n")), "\n")
}

// A host built from the SDK of contract 25.0 announces no version and lists
// no feature. A provider built from this SDK
// serves it everything contract 25.0 had, and its replies pass that host's
// own checks.
func TestContract25HostWithAProviderOfThisSDK(t *testing.T) {
	c := qt.New(t)
	host := buildContract25Fixture(c, "contract-host")
	database := "/" + strings.TrimPrefix(filepath.ToSlash(filepath.Join(c.TempDir(), "database.db")), "/")
	address := (&url.URL{Scheme: "fixture", Path: database}).String()
	lines := runContractHost(c, host, buildFixture(c, "provider"), "session", address)
	c.Assert(lines[0], qt.Matches, `features=.*units\.execute.*`)
	c.Assert(lines[1:], qt.DeepEquals, []string{"outcome=APPLIED", "table=items"})
	lines = runContractHost(c, host, buildFixture(c, "lint"), "lint", "")
	c.Assert(lines[0], qt.Matches, `features=.*lint\.migrations.*`)
	c.Assert(lines[0], qt.Not(qt.Matches), `.*lint\.statement-facts.*`)
	c.Assert(lines[1:], qt.DeepEquals, []string{"judged=LINT_FACT_STATEFUL_DROP", "statements=2"})
}
