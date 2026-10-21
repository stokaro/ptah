package runtime

// White-box testing required: non-SDK peers bypass server response validation,
// and a mutating service must not overwrite the adapter's original provenance.

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func runtimeConversionExchange() (*protocol.Description, *protocol.ConvertValuesRequest, *protocol.NormalizedValues) {
	d, _, result := normalizationExchange()
	d.Features = append(d.Features, provider.ConvertValuesFeature)
	observed := proto.CloneOf(d.Engines[0].ValueModels[0])
	observed.Representation = protocol.ValueInput_OBSERVED
	d.Engines[0].ValueModels = append(d.Engines[0].ValueModels, observed)
	return d, &protocol.ConvertValuesRequest{Engine: "test", From: protocol.ValueInput_DESIRED, To: protocol.ValueInput_OBSERVED, Values: proto.CloneOf(result).Values}, result
}

func TestConversionClientDiscardsMalformedPeerResults(t *testing.T) {
	c := qt.New(t)
	d, r, result := runtimeConversionExchange()
	peer := &conversionPeer{result: result}
	client := &Client{description: d, rpc: peer}
	converted, err := client.ConvertValues(c.Context(), r)
	c.Assert(err, qt.IsNil)
	c.Assert(proto.Equal(converted, result), qt.IsTrue)
	peer.result.Complete = false
	converted, err = client.ConvertValues(c.Context(), r)
	c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	c.Assert(converted, qt.IsNil)
	peer.err = status.Error(codes.Unavailable, "password=secret")
	converted, err = client.ConvertValues(c.Context(), r)
	c.Assert(err, qt.ErrorMatches, `provider convert values failed \(Unavailable\)`)
	c.Assert(converted, qt.IsNil)
	r.Values[0].Value.ProviderId = "foreign"
	converted, err = client.ConvertValues(c.Context(), r)
	c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	c.Assert(converted, qt.IsNil)
	c.Assert(peer.calls, qt.Equals, 3)
}

func TestConversionServerFreezesIdentityAndHonorsCancellation(t *testing.T) {
	c := qt.New(t)
	d, r, _ := runtimeConversionExchange()
	original := proto.CloneOf(r)
	owner := &mutatingConverter{}
	server := &serverAdapter{server: Server{Description: d, ValueConverter: owner}}
	result, err := server.ConvertValues(c.Context(), r)
	c.Assert(status.Code(err), qt.Equals, codes.Internal)
	c.Assert(result, qt.IsNil)
	c.Assert(proto.Equal(r, original), qt.IsTrue)
	ctx, cancel := context.WithCancel(c.Context())
	cancel()
	result, err = server.ConvertValues(ctx, r)
	c.Assert(status.Code(err), qt.Equals, codes.Canceled)
	c.Assert(result, qt.IsNil)
	c.Assert(owner.calls, qt.Equals, 1)
	ctx, cancel = context.WithCancel(c.Context())
	defer cancel()
	owner.cancel = cancel
	result, err = server.ConvertValues(ctx, r)
	c.Assert(status.Code(err), qt.Equals, codes.Canceled)
	c.Assert(result, qt.IsNil)
	c.Assert(owner.calls, qt.Equals, 2)
}

type conversionPeer struct {
	protocol.ProviderClient
	result *protocol.NormalizedValues
	err    error
	calls  int
}

func (p *conversionPeer) ConvertValues(context.Context, *protocol.ConvertValuesRequest, ...grpc.CallOption) (*protocol.NormalizedValues, error) {
	p.calls++
	return p.result, p.err
}

type mutatingConverter struct {
	calls  int
	cancel context.CancelFunc
}

func (p *mutatingConverter) ConvertValues(_ context.Context, r *protocol.ConvertValuesRequest) (*protocol.NormalizedValues, error) {
	p.calls++
	r.Values[0].Value.Owner.Name = "changed"
	if p.cancel != nil {
		p.cancel()
	}
	return &protocol.NormalizedValues{Complete: true, Values: r.Values}, nil
}
