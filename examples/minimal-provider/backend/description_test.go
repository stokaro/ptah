package backend_test

import (
	"testing"

	"example.com/ptah-tutorial-provider/backend"
	qt "github.com/frankban/quicktest"

	"ptah.run/provider-sdk/provider"
)

// The features Ptah requires of every provider it starts.
var hostRequires = []string{provider.InspectFeature, provider.RenderFeature, provider.ExecuteFeature, provider.AnalyzeSQLFeature}

func TestServer_HappyPath(t *testing.T) {
	c := qt.New(t)
	server := backend.Server()
	c.Assert(server.Validate(), qt.IsNil)
	c.Assert(provider.ValidateDescription(server.Description, hostRequires), qt.IsNil)
	c.Assert(server.Description.GetProviderId(), qt.Equals, backend.ProviderID)
	c.Assert(server.Description.GetEngines(), qt.HasLen, 1)
	c.Assert(server.Description.GetEngines()[0].GetId(), qt.Equals, backend.Engine)
}

// A service the description offers without an implementation is a false
// advertisement, and Serve refuses it before the process answers anything.
func TestServer_FailurePath(t *testing.T) {
	c := qt.New(t)
	server := backend.Server()
	server.Locks = nil
	c.Assert(server.Validate(), qt.ErrorIs, provider.ErrProtocol)
}

// Negotiation keeps the reasons feature for a host that lists it and drops it
// for one that does not, so a reason never reaches a host that cannot show it.
func TestDescription_Negotiation(t *testing.T) {
	c := qt.New(t)
	withReasons := provider.NegotiatedDescription(backend.Description(), []string{provider.ErrorReasonsFeature})
	c.Assert(withReasons.GetFeatures(), qt.Contains, provider.ErrorReasonsFeature)
	without := provider.NegotiatedDescription(backend.Description(), nil)
	c.Assert(without.GetFeatures(), qt.Not(qt.Contains), provider.ErrorReasonsFeature)
}
