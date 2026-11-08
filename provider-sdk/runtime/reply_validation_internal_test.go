package runtime

// White-box testing required: the client's checks run on its own copy of
// the request, which only a peer injected in place of the connection can
// change between the request's check and the reply's.

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	protocol "ptah.run/provider-sdk/protocol/experimental"
	"ptah.run/provider-sdk/provider"
)

// changingPlanningPeer answers with reply after it changes the request it
// was sent so that the request validator would refuse it, which a peer
// across a connection cannot do: the client sent a marshaled copy.
type changingPlanningPeer struct {
	protocol.ProviderClient
	reply *protocol.PlannedFeatures
}

func (p *changingPlanningPeer) PlanFeatures(_ context.Context, request *protocol.PlanFeaturesRequest, _ ...grpc.CallOption) (*protocol.PlannedFeatures, error) {
	request.Capabilities = map[string]bool{"": true}
	return proto.CloneOf(p.reply), nil
}

// The client validates the request once, before it sends it, and then the
// reply alone: a change to the request after it was validated is not read
// again. The full reply validator would refuse it.
func TestPlanningClientValidatesTheRequestOnce(t *testing.T) {
	c := qt.New(t)
	d, request, reply := runtimePlanningExchange()
	client := &Client{description: d, rpc: &changingPlanningPeer{reply: reply}}
	changed := proto.CloneOf(request)
	changed.Capabilities = map[string]bool{"": true}
	c.Assert(provider.ValidatePlannedFeatures(d, changed, reply), qt.ErrorIs, provider.ErrProtocol)

	result, err := client.PlanFeatures(c.Context(), request)

	c.Assert(err, qt.IsNil)
	c.Assert(proto.Equal(result, reply), qt.IsTrue)
}
