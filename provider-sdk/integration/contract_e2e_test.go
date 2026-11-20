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

// A host of this SDK refuses a provider built from an SDK of contract 25. The
// provider refuses the handshake before it describes itself, so the host names
// what it speaks and what it needs instead of failing on the first message it
// cannot read: the lint rules of that contract are identified by code, and the
// rules of this one by name.
func TestNewHostRefusesAProviderOfContract25(t *testing.T) {
	c := qt.New(t)

	_, err := transport.Start(c.Context(), newHostOptions(c, buildContract25Fixture(c, "provider")))

	c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	c.Assert(err, qt.ErrorMatches, `.*another contract revision than this host, which speaks contract 26\.0; rebuild the provider with an SDK of revision 26.*`)
}

// A host built from the SDK of contract 25 announces revision 25, and a
// provider of this SDK refuses it at the handshake. The older host does not
// read the reason, so it reports only that the negotiation failed.
func TestContract25HostIsRefusedByAProviderOfThisSDK(t *testing.T) {
	c := qt.New(t)
	host := buildContract25Fixture(c, "contract-host")
	database := "/" + strings.TrimPrefix(filepath.ToSlash(filepath.Join(c.TempDir(), "database.db")), "/")
	address := (&url.URL{Scheme: "fixture", Path: database}).String()

	output, err := exec.CommandContext(c.Context(), host, buildFixture(c, "provider"), "session", address).CombinedOutput() // #nosec G204 -- the test built both executables

	c.Assert(err, qt.ErrorMatches, "exit status 1")
	c.Assert(string(output), qt.Contains, "provider negotiate failed (FailedPrecondition)")
}
