package runtime

// White-box testing required: malformed non-SDK peers must not bypass client
// validation, and a provider must not mutate the original validation input.

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

func emptySourceExchange() (*protocol.Description, *protocol.DecodeSourceRequest, *protocol.DecodedSource) {
	d, _, _ := normalizationExchange()
	d.Features = append(d.Features, provider.DecodeSourceFeature)
	request := &protocol.DecodeSourceRequest{Engine: "test", Documents: []*protocol.SourceDocument{{Id: "empty", File: "empty.yaml", Format: protocol.SourceDocument_YAML}}}
	result := &protocol.DecodedSource{Complete: true, Documents: []*protocol.DecodedSourceDocument{{Id: "empty", Coverage: &protocol.ModelCoverage{Representation: protocol.ValueInput_DESIRED}}}}
	return d, request, result
}

func TestSourceClientValidatesNonSDKResponses(t *testing.T) {
	c := qt.New(t)
	d, r, valid := emptySourceExchange()
	peer := &sourcePeer{result: valid}
	client := &Client{description: d, rpc: peer}
	result, err := client.DecodeSource(c.Context(), r)
	c.Assert(err, qt.IsNil)
	c.Assert(proto.Equal(result, valid), qt.IsTrue)
	peer.result = &protocol.DecodedSource{Complete: true}
	result, err = client.DecodeSource(c.Context(), r)
	c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	c.Assert(result, qt.IsNil)
	peer.err = status.Error(codes.Unavailable, "password=secret")
	result, err = client.DecodeSource(c.Context(), r)
	c.Assert(err, qt.ErrorMatches, `provider decode source failed \(Unavailable\)`)
	c.Assert(result, qt.IsNil)
	c.Assert(peer.calls, qt.Equals, 3)
	r.Engine = "foreign"
	result, err = client.DecodeSource(c.Context(), r)
	c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	c.Assert(result, qt.IsNil)
	c.Assert(peer.calls, qt.Equals, 3)
}

func TestSourceServerKeepsCapturedInputImmutable(t *testing.T) {
	c := qt.New(t)
	d, r, _ := emptySourceExchange()
	original := proto.CloneOf(r)
	server := &serverAdapter{server: Server{Description: d, SourceDecoder: &mutatingSourceDecoder{}}}
	result, err := server.DecodeSource(c.Context(), r)
	c.Assert(status.Code(err), qt.Equals, codes.Internal)
	c.Assert(result, qt.IsNil)
	c.Assert(proto.Equal(r, original), qt.IsTrue)
}

type sourcePeer struct {
	protocol.ProviderClient
	result *protocol.DecodedSource
	err    error
	calls  int
}

func (p *sourcePeer) DecodeSource(context.Context, *protocol.DecodeSourceRequest, ...grpc.CallOption) (*protocol.DecodedSource, error) {
	p.calls++
	return p.result, p.err
}

type mutatingSourceDecoder struct{}

func (*mutatingSourceDecoder) DecodeSource(_ context.Context, request *protocol.DecodeSourceRequest) (*protocol.DecodedSource, error) {
	request.Documents[0].Id = "changed"
	return &protocol.DecodedSource{Complete: true, Documents: []*protocol.DecodedSourceDocument{{Id: "changed", Coverage: &protocol.ModelCoverage{Representation: protocol.ValueInput_DESIRED}}}}, nil
}
