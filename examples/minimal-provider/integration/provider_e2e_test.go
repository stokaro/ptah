//go:build integration

package integration_test

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"example.com/ptah-tutorial-provider/backend"
	qt "github.com/frankban/quicktest"

	protocol "ptah.run/provider-sdk/protocol/experimental"
	"ptah.run/provider-sdk/provider"
	"ptah.run/provider-sdk/runtime"
)

// build compiles the provider executable from the module root. The name ends
// in .exe on every platform, which Windows needs and others ignore.
func build(c *qt.C) (executable, digest string) {
	c.Helper()
	executable = filepath.Join(c.TempDir(), "ptah-tutorial-provider.exe")
	command := exec.CommandContext(c.Context(), "go", "build", "-o", executable, ".")
	command.Dir = ".."
	output, err := command.CombinedOutput()
	c.Assert(err, qt.IsNil, qt.Commentf("go build: %s", output))
	content, err := os.ReadFile(executable)
	c.Assert(err, qt.IsNil)
	sum := sha256.Sum256(content)
	return executable, hex.EncodeToString(sum[:])
}

// start starts the provider as Ptah does: the executable is named by path and
// digest, the identity is checked, and the features are negotiated before any
// session opens.
func start(c *qt.C, executable, digest string, hostFeatures []string) *runtime.Client {
	c.Helper()
	client, err := runtime.Start(c.Context(), runtime.Options{
		Executable:       executable,
		SHA256:           digest,
		ProviderID:       backend.ProviderID,
		RequiredFeatures: hostRequires,
		HostFeatures:     hostFeatures,
	})
	c.Assert(err, qt.IsNil)
	c.Cleanup(client.Close)
	return client
}

// The features Ptah requires of every provider it starts.
var hostRequires = []string{provider.InspectFeature, provider.RenderFeature, provider.ExecuteFeature, provider.AnalyzeSQLFeature}

var unsupportedColumn = &protocol.Change{Id: "create", Object: &protocol.Reference{Kind: "table", Name: "notes"}, Operation: &protocol.Change_CreateTable{CreateTable: &protocol.Table{Columns: []*protocol.Column{
	{Name: "title", Type: "VARCHAR(80)", Nullable: true},
}}}}

// The whole loop through the process boundary: start, negotiate, open, read,
// render, run, read again.
func TestProviderProcess_HappyPath(t *testing.T) {
	c := qt.New(t)
	executable, digest := build(c)
	client := start(c, executable, digest, provider.TwoWayFeatures())
	c.Assert(client.Description().GetFeatures(), qt.Contains, provider.ErrorReasonsFeature)

	_, location := address(c)
	session, err := client.Open(c.Context(), &protocol.OpenRequest{Engine: backend.Engine, Address: location})
	c.Assert(err, qt.IsNil)
	before, err := client.Inspect(c.Context(), &protocol.InspectRequest{SessionId: session.GetId()})
	c.Assert(err, qt.IsNil)
	c.Assert(before.GetTables(), qt.HasLen, 0)

	rendered, err := client.Render(c.Context(), &protocol.RenderRequest{Engine: backend.Engine, Changes: []*protocol.Change{createNotes("create")}})
	c.Assert(err, qt.IsNil)
	result, err := client.Execute(c.Context(), &protocol.ExecuteRequest{SessionId: session.GetId(), RequestId: "apply", Units: rendered.GetUnits()})
	c.Assert(err, qt.IsNil)
	c.Assert(result.GetOutcome(), qt.Equals, protocol.Execution_APPLIED)

	after, err := client.Inspect(c.Context(), &protocol.InspectRequest{SessionId: session.GetId()})
	c.Assert(err, qt.IsNil)
	c.Assert(after.GetTables(), qt.HasLen, 1)
	c.Assert(client.CloseSession(c.Context(), session.GetId()), qt.IsNil)
}

// A host that lists errors.reasons sees the provider's reason after the
// code; a host that does not sees the code alone, because the SDK server
// removes the reason before the reply leaves the process.
func TestProviderProcess_FailurePath(t *testing.T) {
	c := qt.New(t)
	executable, digest := build(c)
	for _, test := range []struct {
		name         string
		hostFeatures []string
		want         string
	}{
		{"with reasons", []string{provider.ErrorReasonsFeature}, `provider render failed \(InvalidArgument\): the tutorial engine stores the column types INTEGER, TEXT, REAL and BLOB only`},
		{"without reasons", nil, `provider render failed \(InvalidArgument\)`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			client := start(c, executable, digest, test.hostFeatures)
			result, err := client.Render(c.Context(), &protocol.RenderRequest{Engine: backend.Engine, Changes: []*protocol.Change{unsupportedColumn}})
			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(result, qt.IsNil)
		})
	}
}

// A digest that does not match the file refuses the start before the
// process runs anything.
func TestProviderStart_FailurePath(t *testing.T) {
	c := qt.New(t)
	executable, _ := build(c)
	client, err := runtime.Start(c.Context(), runtime.Options{
		Executable: executable, SHA256: hex.EncodeToString(make([]byte, sha256.Size)), ProviderID: backend.ProviderID, RequiredFeatures: hostRequires,
	})
	c.Assert(err, qt.IsNotNil)
	c.Assert(client, qt.IsNil)
}
