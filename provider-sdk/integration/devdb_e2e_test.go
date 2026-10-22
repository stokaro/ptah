//go:build integration

package integration_test

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	transport "github.com/stokaro/ptah/provider-sdk/runtime"
)

func TestIndependentProviderProvisionsDevDatabaseWithoutSession(t *testing.T) {
	c := qt.New(t)
	binary := buildFixture(c, "devdb")
	data, err := os.ReadFile(binary)
	c.Assert(err, qt.IsNil)
	checksum := sha256.Sum256(data)
	client := start(c, transport.Options{Executable: binary, SHA256: hex.EncodeToString(checksum[:]), ProviderID: "example.org/fixture", RequiredFeatures: []string{provider.DevDatabaseFeature}})

	first, err := client.ProvisionDevDatabase(c.Context(), &protocol.ProvisionDevRequest{Engine: "fixture", Spec: "docker://fixture/1"})
	c.Assert(err, qt.IsNil)
	second, err := client.ProvisionDevDatabase(c.Context(), &protocol.ProvisionDevRequest{Engine: "fixture"})
	c.Assert(err, qt.IsNil)
	c.Assert(first.Lease, qt.Not(qt.Equals), second.Lease)
	c.Assert(first.Url, qt.Equals, "fixture://localhost/"+first.Lease)

	refused, err := client.ProvisionDevDatabase(c.Context(), &protocol.ProvisionDevRequest{Engine: "fixture", Spec: "docker://none"})
	c.Assert(err, qt.IsNil)
	c.Assert(refused.Refusal.Kind, qt.Equals, protocol.DevDatabaseRefusal_UNSUPPORTED)
	c.Assert(refused.Lease, qt.Equals, "")

	_, err = client.ReleaseDevDatabase(c.Context(), &protocol.ReleaseDevRequest{Engine: "fixture", Lease: first.Lease})
	c.Assert(err, qt.IsNil)
	_, err = client.ReleaseDevDatabase(c.Context(), &protocol.ReleaseDevRequest{Engine: "fixture", Lease: first.Lease})
	c.Assert(err, qt.IsNil)

	_, err = client.ProvisionDevDatabase(c.Context(), &protocol.ProvisionDevRequest{Engine: "other"})
	c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
}
