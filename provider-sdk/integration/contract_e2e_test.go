//go:build integration

package integration_test

import (
	"crypto/sha256"
	"encoding/hex"
	"io/fs"
	"net/url"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	protocol "ptah.run/provider-sdk/protocol/experimental"
	"ptah.run/provider-sdk/provider"
	transport "ptah.run/provider-sdk/runtime"
)

// contract25SDK is the newest SDK release of contract 25.0, as a module query.
// The mixed-version tests build fixture programs against it, so that one side
// of the exchange is a peer of an older contract minor.
const contract25SDK = "github.com/stokaro/ptah/provider-sdk@v0.2.0"

// sdkModule is the module path of the in-tree SDK, the one the fixture module
// imports.
const sdkModule = "ptah.run/provider-sdk"

// buildContract25Fixture builds a fixture program against [contract25SDK].
// Such a program speaks contract 25.0 and knows none of the features a later
// minor adds.
func buildContract25Fixture(c *qt.C, program string) string {
	c.Helper()
	return buildReleasedFixture(c, contract25SDK, program)
}

// buildReleasedFixture builds a fixture program against an SDK release, from
// a copy of the fixture module, with no workspace. release is a module query,
// such as [contract25SDK]. Only a program whose packages use nothing of a later
// minor builds this way: provider, lint and contract-host.
//
// The releases these tests reach declare another module path than the in-tree
// SDK, so the copy's imports are rewritten to the release's path before the
// build. A replace directive cannot stand in for the rewrite: the release
// imports its own packages by the path it declares, so the build would link
// two copies of the SDK whose types do not meet.
func buildReleasedFixture(c *qt.C, release, program string) string {
	c.Helper()
	module, version, ok := strings.Cut(release, "@")
	c.Assert(ok, qt.IsTrue)
	_, source, _, ok := runtime.Caller(0)
	c.Assert(ok, qt.IsTrue)
	dir := c.TempDir()
	c.Assert(os.CopyFS(dir, os.DirFS(filepath.Join(filepath.Dir(source), "..", "testdata", "fixture"))), qt.IsNil)
	changed, err := rewriteSDKImports(dir, module)
	c.Assert(err, qt.IsNil)
	c.Assert(changed > 0, qt.IsTrue)
	runGo(c, dir, "mod", "edit", "-droprequire="+sdkModule)
	runGo(c, dir, "get", release)
	binary := filepath.Join(c.TempDir(), program+"-"+version+".exe")
	runGo(c, dir, "build", "-o", binary, "./cmd/"+program)
	assertLinkedSDK(c, dir, binary, release)
	return binary
}

// rewriteSDKImports rewrites the in-tree SDK's import paths in the Go files
// below dir to module, and reports how many files it changed.
func rewriteSDKImports(dir, module string) (int, error) {
	root, err := os.OpenRoot(dir)
	if err != nil {
		return 0, err
	}
	defer root.Close()
	changed := 0
	err = fs.WalkDir(root.FS(), ".", func(name string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() || path.Ext(name) != ".go" {
			return err
		}
		data, err := root.ReadFile(name)
		if err != nil {
			return err
		}
		rewritten := strings.ReplaceAll(string(data), `"`+sdkModule+`/`, `"`+module+`/`)
		if rewritten == string(data) {
			return nil
		}
		changed++
		return root.WriteFile(name, []byte(rewritten), 0o600)
	})
	return changed, err
}

// assertLinkedSDK asserts the SDK release a binary linked. The build records
// the module versions it linked, which is what shows that this side of the
// exchange is the peer of that release.
func assertLinkedSDK(c *qt.C, dir, binary, release string) {
	c.Helper()
	module, version, ok := strings.Cut(release, "@")
	c.Assert(ok, qt.IsTrue)
	c.Assert(runGo(c, dir, "version", "-m", binary), qt.Matches, `(?s).*\s`+regexp.QuoteMeta(module)+`\s+`+regexp.QuoteMeta(version)+`\s.*`)
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
	// A provider that offers the services of contract 25.1 lists them, and
	// the older host, which calls none of them, works with it all the same.
	database = "/" + strings.TrimPrefix(filepath.ToSlash(filepath.Join(c.TempDir(), "data.db")), "/")
	lines = runContractHost(c, host, buildFixture(c, "data"), "session", (&url.URL{Scheme: "fixture", Path: database}).String())
	c.Assert(lines[0], qt.Matches, `features=.*data\.read-rows.*`)
	c.Assert(lines[1:], qt.DeepEquals, []string{"outcome=APPLIED", "table=items"})
	lines = runContractHost(c, host, buildFixture(c, "lint"), "lint", "")
	c.Assert(lines[0], qt.Matches, `features=.*lint\.migrations.*`)
	c.Assert(lines[0], qt.Not(qt.Matches), `.*lint\.statement-facts.*`)
	c.Assert(lines[1:], qt.DeepEquals, []string{"judged=LINT_FACT_STATEFUL_DROP", "statements=2"})
}
