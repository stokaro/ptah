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

const testModelDigest = "a2c799262a3ce3c19ef5cdd983bf3d12b43ab3c426227091b909dcb7054738c0"

func normalizationExchange() (*protocol.Description, *protocol.NormalizeValuesRequest, *protocol.NormalizedValues) {
	description := &protocol.Description{ProviderId: "example.org/test", Features: []string{provider.NormalizeValuesFeature}, Engines: []*protocol.Engine{{Id: "test", ValueModels: []*protocol.ValueModel{{Kind: "example.org/test/value", Representation: protocol.ValueInput_DESIRED, SchemaVersion: 1, Encoding: "json", DefinitionJson: []byte(`{"type":"object"}`), DefinitionSha256: testModelDigest, CanonicalFormat: "example.org/value-v1"}}}}}
	value := &protocol.Envelope{DefinitionSha256: testModelDigest, ProviderId: "example.org/test", Kind: "example.org/test/value", SchemaVersion: 1, Encoding: "json",
		Owner: &protocol.Reference{Engine: "test", Kind: "table", Name: "original"}, Payload: []byte("{}")}
	request := &protocol.NormalizeValuesRequest{Engine: "test", Values: []*protocol.ValueInput{{Id: "first", Representation: protocol.ValueInput_DESIRED, Value: value}}}
	result := &protocol.NormalizedValues{Complete: true, Values: []*protocol.NormalizedValue{{Id: "first", Value: proto.CloneOf(value),
		NormalizedOwner: proto.CloneOf(value.Owner), CanonicalFormat: "example.org/value-v1", CanonicalProjection: []byte("canonical")}}}
	return description, request, result
}

func inspectionExchange() (*protocol.Description, *protocol.InspectRequest, *protocol.Schema) {
	description, values, _ := normalizationExchange()
	description.Features = append(description.Features, provider.InspectFeature)
	model := description.Engines[0].ValueModels[0]
	model.Representation = protocol.ValueInput_OBSERVED
	value := values.Values[0].Value
	value.Owner.Kind = value.Kind
	coverage := &protocol.ModelCoverage{Representation: protocol.ValueInput_OBSERVED,
		Kinds:    []*protocol.ModelKindCoverage{{Model: proto.CloneOf(model), Knowledge: &protocol.ModelKnowledge{State: protocol.ModelKnowledge_COMPLETE}}},
		Subjects: []*protocol.ModelSubjectCoverage{{Kind: value.Kind, Subject: proto.CloneOf(value.Owner), NormalizedSubject: proto.CloneOf(value.Owner), Knowledge: &protocol.ModelKnowledge{State: protocol.ModelKnowledge_COMPLETE}}},
	}
	return description, &protocol.InspectRequest{SessionId: "session"}, &protocol.Schema{Complete: true, Engine: "test", Coverage: coverage, NativeObjects: []*protocol.Envelope{value}}
}

func TestInspectClientRejectsUnboundPeerState(t *testing.T) {
	c := qt.New(t)
	description, request, snapshot := inspectionExchange()
	peer := &inspectionPeer{result: snapshot}
	client := &Client{description: description, rpc: peer}
	result, err := client.Inspect(c.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(proto.Equal(result, snapshot), qt.IsTrue)
	snapshot.Coverage = nil
	result, err = client.Inspect(c.Context(), request)
	c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	c.Assert(result, qt.IsNil)
	peer.err = status.Error(codes.Unavailable, "password=secret")
	result, err = client.Inspect(c.Context(), request)
	c.Assert(err, qt.ErrorMatches, `provider inspect failed \(Unavailable\)`)
	c.Assert(result, qt.IsNil)
}

func TestInspectServerCannotEraseRequestedScope(t *testing.T) {
	c := qt.New(t)
	description, request, snapshot := inspectionExchange()
	request.Schemas = []string{"selected"}
	original := proto.CloneOf(request)
	server := &serverAdapter{server: Server{Description: description, Inspector: &scopeErasingInspector{snapshot: snapshot}}}
	result, err := server.Inspect(c.Context(), request)
	c.Assert(status.Code(err), qt.Equals, codes.FailedPrecondition)
	c.Assert(result, qt.IsNil)
	c.Assert(proto.Equal(request, original), qt.IsTrue)
}

type inspectionPeer struct {
	protocol.ProviderClient
	result *protocol.Schema
	err    error
}

func (p *inspectionPeer) Inspect(context.Context, *protocol.InspectRequest, ...grpc.CallOption) (*protocol.Schema, error) {
	return p.result, p.err
}

type scopeErasingInspector struct{ snapshot *protocol.Schema }

func (p *scopeErasingInspector) Inspect(_ context.Context, request *protocol.InspectRequest) (*protocol.Schema, error) {
	request.Schemas = nil
	return p.snapshot, nil
}
