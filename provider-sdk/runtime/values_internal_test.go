package runtime

// White-box testing required: the client must reject malformed non-SDK peers.
// Constructing the private adapters bypasses the SDK server validation so the
// tests can check that client boundary and mutation of the server request.

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

func TestNormalizeValuesClientRejectsMalformedPeer(t *testing.T) {
	c := qt.New(t)
	description, request, valid := normalizationExchange()
	peer := &normalizationPeer{result: proto.CloneOf(valid)}
	client := &Client{description: description, rpc: peer}
	result, err := client.NormalizeValues(c.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(proto.Equal(result, valid), qt.IsTrue)
	peer.result.Complete = false
	result, err = client.NormalizeValues(c.Context(), request)
	c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	c.Assert(result, qt.IsNil)
	peer.result = proto.CloneOf(valid)
	peer.result.Values[0].Id = "foreign"
	result, err = client.NormalizeValues(c.Context(), request)
	c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	c.Assert(result, qt.IsNil)
	peer.err = status.Error(codes.Unavailable, "password=secret")
	result, err = client.NormalizeValues(c.Context(), request)
	c.Assert(err, qt.ErrorMatches, `provider normalize values failed \(Unavailable\)`)
	c.Assert(result, qt.IsNil)
	c.Assert(peer.calls, qt.Equals, 4)
	request.Values[0].Value.ProviderId = "foreign"
	result, err = client.NormalizeValues(c.Context(), request)
	c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	c.Assert(result, qt.IsNil)
	c.Assert(peer.calls, qt.Equals, 4)
}

func TestNormalizeValuesServerRetainsOriginalProvenance(t *testing.T) {
	c := qt.New(t)
	description, request, _ := normalizationExchange()
	original := proto.CloneOf(request)
	server := &serverAdapter{server: Server{Description: description, ValueNormalizer: &mutatingNormalizer{}}}
	result, err := server.NormalizeValues(c.Context(), request)
	c.Assert(status.Code(err), qt.Equals, codes.Internal)
	c.Assert(result, qt.IsNil)
	c.Assert(proto.Equal(request, original), qt.IsTrue)
}

type normalizationPeer struct {
	protocol.ProviderClient
	result *protocol.NormalizedValues
	err    error
	calls  int
}

func (p *normalizationPeer) NormalizeValues(context.Context, *protocol.NormalizeValuesRequest, ...grpc.CallOption) (*protocol.NormalizedValues, error) {
	p.calls++
	return p.result, p.err
}

type mutatingNormalizer struct{}

func (*mutatingNormalizer) NormalizeValues(_ context.Context, request *protocol.NormalizeValuesRequest) (*protocol.NormalizedValues, error) {
	request.Values[0].Value.Owner.Name = "changed"
	return &protocol.NormalizedValues{Complete: true, Values: []*protocol.NormalizedValue{{
		Id: request.Values[0].Id, Value: request.Values[0].Value, NormalizedOwner: request.Values[0].Value.Owner,
		CanonicalFormat: "example.org/value-v1", CanonicalProjection: []byte("canonical"),
	}}}, nil
}

func normalizationExchange() (*protocol.Description, *protocol.NormalizeValuesRequest, *protocol.NormalizedValues) {
	description := &protocol.Description{ProviderId: "example.org/test", Features: []string{provider.NormalizeValuesFeature}, Engines: []*protocol.Engine{{Id: "test"}}}
	value := &protocol.Envelope{ProviderId: "example.org/test", Kind: "example.org/test/value", SchemaVersion: 1, Encoding: "json",
		Owner: &protocol.Reference{Engine: "test", Kind: "table", Name: "original"}, Payload: []byte("{}")}
	request := &protocol.NormalizeValuesRequest{Engine: "test", Values: []*protocol.ValueInput{{Id: "first", Representation: protocol.ValueInput_DESIRED, Value: value}}}
	result := &protocol.NormalizedValues{Complete: true, Values: []*protocol.NormalizedValue{{Id: "first", Value: proto.CloneOf(value),
		NormalizedOwner: proto.CloneOf(value.Owner), CanonicalFormat: "example.org/value-v1", CanonicalProjection: []byte("canonical")}}}
	return description, request, result
}
