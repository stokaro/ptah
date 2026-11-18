package runtime

// White-box testing required: the client's own refusal of trust it did not
// negotiate is observable only where the network would be. Through the
// in-memory transport the server refuses the same request, so a client that
// sent it would still read as refused; the peer has to replace the transport
// for the test to see whether the client sent anything.

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"google.golang.org/grpc"

	protocol "ptah.run/provider-sdk/protocol/experimental"
	"ptah.run/provider-sdk/provider"
)

// guardPeer records the trust of each GuardReplay request that reached it and
// accepts every statement.
type guardPeer struct {
	protocol.ProviderClient
	trusted []bool
}

func (p *guardPeer) GuardReplay(_ context.Context, request *protocol.GuardReplayRequest, _ ...grpc.CallOption) (*protocol.GuardedReplay, error) {
	p.trusted = append(p.trusted, request.GetScope().GetTrustedSql())
	reply := &protocol.GuardedReplay{}
	for range request.GetStatements() {
		reply.Verdicts = append(reply.Verdicts, &protocol.ReplayVerdict{})
	}
	return reply, nil
}

func trustedGuardRequest() *protocol.GuardReplayRequest {
	return &protocol.GuardReplayRequest{Engine: "fixture", Scope: &protocol.ReplayScope{Database: "dev", TrustedSql: true}, Statements: []string{"VACUUM INTO 'copy.db'"}}
}

// A negotiated trust reaches the provider through Client.GuardReplay, which
// is the control for the refusals below: the peer is reachable.
func TestClientGuardsATrustedReplay_HappyPath(t *testing.T) {
	c := qt.New(t)
	peer := &guardPeer{}
	negotiated := provider.NegotiatedDescription(trustSessionDescription(provider.ReplayGuardFeature, provider.ReplayTrustFeature), []string{provider.ReplayTrustFeature})
	client := &Client{description: negotiated, rpc: peer}

	guarded, err := client.GuardReplay(c.Context(), trustedGuardRequest())

	c.Assert(err, qt.IsNil)
	c.Assert(guarded.GetVerdicts(), qt.HasLen, 1)
	c.Assert(peer.trusted, qt.DeepEquals, []bool{true})
}

// Client.GuardReplay refuses a trusted scope the description did not
// negotiate before it sends anything: an older provider that does not declare
// the feature, and a host that did not list it, which negotiates it away.
func TestClientGuardsATrustedReplay_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name        string
		description *protocol.Description
	}{
		{"an older provider", trustSessionDescription(provider.ReplayGuardFeature)},
		{"a host that does not list the feature", provider.NegotiatedDescription(trustSessionDescription(provider.ReplayGuardFeature, provider.ReplayTrustFeature), nil)},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			peer := &guardPeer{}
			client := &Client{description: test.description, rpc: peer}

			guarded, err := client.GuardReplay(c.Context(), trustedGuardRequest())

			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
			c.Assert(err, qt.ErrorMatches, `.*trusted replay SQL requires database\.dev-replay-trust for engine fixture`)
			c.Assert(guarded, qt.IsNil)
			c.Assert(peer.trusted, qt.HasLen, 0)
		})
	}
}
