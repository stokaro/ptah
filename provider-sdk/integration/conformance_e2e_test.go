//go:build integration

package integration_test

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/stokaro/ptah/provider-sdk/conformance"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
)

// fixtureTable is the table the conformance runs create and drop.
func fixtureTable() *protocol.Reference {
	return &protocol.Reference{Kind: "table", Name: "conformance_items"}
}

func createFixtureTable() *protocol.Change {
	return &protocol.Change{Id: "create", Object: fixtureTable(), Operation: &protocol.Change_CreateTable{CreateTable: &protocol.Table{Columns: []*protocol.Column{
		{Name: "id", Type: "INTEGER", PrimaryKey: true}, {Name: "value", Type: "TEXT", Nullable: true},
	}}}}
}

// fixtureDatabase is the address of a database file of the fixture engine in
// a directory of its own.
func fixtureDatabase(c *qt.C) string {
	c.Helper()
	path := filepath.Join(c.TempDir(), "conformance.db")
	return (&url.URL{Scheme: "fixture", Path: "/" + strings.TrimPrefix(filepath.ToSlash(path), "/")}).String()
}

func digestOf(c *qt.C, path string) string {
	c.Helper()
	content, err := os.ReadFile(path)
	c.Assert(err, qt.IsNil)
	sum := sha256.Sum256(content)
	return hex.EncodeToString(sum[:])
}

// The independent fixture provider passes the suite through its executable,
// offline and against a database file. It makes no claim about reasons or
// transactions, so those checks do not apply to it.
func TestConformanceOfTheFixtureProvider(t *testing.T) {
	c := qt.New(t)
	binary := buildFixture(c, "provider")
	config := conformance.Config{
		Target:           conformance.Executable(binary, digestOf(c, binary)),
		ProviderID:       "example.org/fixture",
		RequiredFeatures: []string{provider.InspectFeature, provider.RenderFeature, provider.ExecuteFeature},
		Calls: []conformance.Call{
			{Name: "render-create", Request: &protocol.RenderRequest{Engine: "fixture", Changes: []*protocol.Change{createFixtureTable()}}},
			{Name: "render-nullable-key", Request: &protocol.RenderRequest{Engine: "fixture", Changes: []*protocol.Change{{
				Id: "create", Object: fixtureTable(), Operation: &protocol.Change_CreateTable{CreateTable: &protocol.Table{Columns: []*protocol.Column{
					{Name: "id", Type: "INTEGER", PrimaryKey: true, Nullable: true},
				}}},
			}}}, Refused: true},
		},
		Live: &conformance.Live{
			Address: fixtureDatabase(c),
			Changes: []conformance.Change{{
				Name:   "items",
				Apply:  conformance.Step{Render: &protocol.RenderRequest{Changes: []*protocol.Change{createFixtureTable()}}},
				Revert: conformance.Step{Render: &protocol.RenderRequest{Changes: []*protocol.Change{{Id: "drop", Object: fixtureTable(), Operation: &protocol.Change_DropTable{DropTable: &protocol.Empty{}}}}}},
			}},
		},
	}

	report, err := conformance.Check(c.Context(), config)

	c.Assert(err, qt.IsNil)
	c.Assert(report.Err(), qt.IsNil)
	c.Assert(report.Target, qt.Equals, "executable")
	c.Assert(report.Provider.ID, qt.Equals, "example.org/fixture")
}

// The command runs the same suite against an executable and prints the
// machine-readable report; a provider that describes another identity fails
// it, and the command exits 1.
func TestConformanceCommand(t *testing.T) {
	c := qt.New(t)
	binary := buildFixture(c, "provider")
	command := filepath.Join(c.TempDir(), "ptah-provider-conformance.exe")
	build := exec.CommandContext(c.Context(), "go", "build", "-o", command, "./cmd/ptah-provider-conformance")
	build.Dir = fixtureSDK(c)
	build.Env = append(os.Environ(), "GOWORK=off")
	output, err := build.CombinedOutput()
	c.Assert(err, qt.IsNil, qt.Commentf("%s", output))
	for _, test := range []struct {
		name       string
		providerID string
		exit       int
		failed     []string
	}{
		{name: "the identity the fixture describes", providerID: "example.org/fixture", exit: 0},
		{name: "another identity", providerID: "example.org/other", exit: 1, failed: []string{"description.identity"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			run := exec.CommandContext(c.Context(), command, "-json", "-executable", binary, "-provider-id", test.providerID, "-require", provider.InspectFeature)
			run.Env = append(os.Environ(), "PTAH_CONFORMANCE_ADDRESS="+fixtureDatabase(c))

			stdout, err := run.Output()

			c.Assert(run.ProcessState.ExitCode(), qt.Equals, test.exit, qt.Commentf("%v: %s", err, stdout))
			var report conformance.Report
			c.Assert(json.Unmarshal(stdout, &report), qt.IsNil)
			var failed []string
			for _, result := range report.Failures() {
				failed = append(failed, result.ID)
			}
			c.Assert(failed, qt.DeepEquals, test.failed)
			c.Assert(report.Suite, qt.Equals, "ptah-provider-conformance")
		})
	}
}
