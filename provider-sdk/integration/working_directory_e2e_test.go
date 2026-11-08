//go:build integration

package integration_test

import (
	"crypto/sha256"
	"encoding/hex"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	protocol "ptah.run/provider-sdk/protocol/experimental"
	"ptah.run/provider-sdk/provider"
	transport "ptah.run/provider-sdk/runtime"
)

// A provider starts in the host's working directory, so a relative file
// address resolves for it as it does for the host. The program names its
// working directory as the session's database.
func TestProviderStartsInTheHostsWorkingDirectory(t *testing.T) {
	c := qt.New(t)
	binary := buildSDKFixture(c, "sdk-working-directory")
	data, err := os.ReadFile(binary)
	c.Assert(err, qt.IsNil)
	checksum := sha256.Sum256(data)
	database := filepath.Join(c.TempDir(), "relative.db")
	invocation, err := filepath.EvalSymlinks(c.TempDir())
	c.Assert(err, qt.IsNil)
	t.Chdir(invocation)

	client, err := transport.Start(c.Context(), transport.Options{Executable: binary, SHA256: hex.EncodeToString(checksum[:]), ProviderID: "example.org/fixture",
		RequiredFeatures: []string{provider.InspectFeature, provider.RenderFeature, provider.ExecuteFeature}})
	c.Assert(err, qt.IsNil)
	c.Cleanup(client.Close)
	session, err := client.Open(c.Context(), &protocol.OpenRequest{Engine: "fixture",
		Address: (&url.URL{Scheme: "fixture", Path: "/" + strings.TrimPrefix(filepath.ToSlash(database), "/")}).String()})

	c.Assert(err, qt.IsNil)
	reported, err := filepath.EvalSymlinks(session.GetDatabase())
	c.Assert(err, qt.IsNil)
	c.Assert(reported, qt.Equals, invocation)
}
