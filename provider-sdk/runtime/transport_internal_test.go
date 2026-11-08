package runtime

// White-box testing required: model references and the message bound live in
// the client's dial options and the server's interceptors, which a test can
// only reach over a real gRPC connection, and only an interceptor between the
// SDK's and the connection sees the message as it travels.

import (
	"context"
	"net"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"

	protocol "ptah.run/provider-sdk/protocol/experimental"
	"ptah.run/provider-sdk/provider"
)

// referenceExchange is a description whose engine declares one desired model
// a YAML document may name, a decode request that names it, and a reply that
// covers it. The provider lists both transport features.
func referenceExchange() (*protocol.Description, *protocol.DecodeSourceRequest, *protocol.DecodedSource) {
	d, request, _ := emptySourceExchange()
	d.Features = append(d.Features, provider.ModelReferencesFeature, provider.LargeMessagesFeature)
	model := d.Engines[0].ValueModels[0]
	model.SourceFormats = []protocol.SourceDocument_Format{protocol.SourceDocument_YAML}
	d.Engines[0].YamlSourceKeys = []*protocol.YAMLSourceKey{{Scope: protocol.YAMLSourceKey_DOCUMENT, Key: "declared", Selector: "declared", Models: []string{model.Kind}}}
	request.Documents[0].Models = []*protocol.ValueModel{proto.CloneOf(model)}
	reply := &protocol.DecodedSource{Complete: true, Documents: []*protocol.DecodedSourceDocument{{Id: "empty", Coverage: &protocol.ModelCoverage{
		Representation: protocol.ValueInput_DESIRED,
		Kinds:          []*protocol.ModelKindCoverage{{Model: proto.CloneOf(model), Knowledge: &protocol.ModelKnowledge{State: protocol.ModelKnowledge_COMPLETE}}},
	}}}}
	return d, request, reply
}

// undeclaredModel is the exchange's model under a definition the description
// does not declare.
func undeclaredModel(d *protocol.Description) *protocol.ValueModel {
	model := proto.CloneOf(d.Engines[0].ValueModels[0])
	model.DefinitionJson, model.DefinitionSha256 = []byte(`{"type":"string"}`), strings.Repeat("cd", 32)
	return model
}

// recordingDecoder answers every decode with reply and keeps the models of
// each request it is given.
type recordingDecoder struct {
	reply  *protocol.DecodedSource
	models []*protocol.ValueModel
	calls  int
}

func (d *recordingDecoder) DecodeSource(_ context.Context, request *protocol.DecodeSourceRequest) (*protocol.DecodedSource, error) {
	d.calls++
	d.models = append(d.models, request.GetDocuments()[0].GetModels()...)
	return proto.CloneOf(d.reply), nil
}

// wireRecorder keeps each request and reply as they cross the connection.
type wireRecorder struct {
	requests, replies []proto.Message
}

func (w *wireRecorder) record(ctx context.Context, method string, request, reply any, connection *grpc.ClientConn, invoker grpc.UnaryInvoker, options ...grpc.CallOption) error {
	err := invoker(ctx, method, request, reply, connection, options...)
	w.requests = append(w.requests, proto.CloneOf(request.(proto.Message)))
	w.replies = append(w.replies, proto.CloneOf(reply.(proto.Message)))
	return err
}

// transportPeers connects a client and a server over a buffer. The server is
// the SDK's, with service registered; the client dials as Start does, with a
// recorder between its interceptors and the connection, and its exchange
// negotiated as the server's. hostFeatures are what the host lists.
func transportPeers(c *qt.C, d *protocol.Description, service protocol.ProviderServer, hostFeatures ...string) (*Client, *wireRecorder) {
	c.Helper()
	adapter := negotiatedServer(Server{Description: d}, hostFeatures...)
	server := grpcServer(adapter)(nil)
	protocol.RegisterProviderServer(server, service)
	listener := bufconn.Listen(1 << 20)
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	c.Cleanup(func() {
		server.Stop()
		c.Check(listener.Close(), qt.IsNil)
		c.Check(<-done, qt.IsNil)
	})
	exchange, wire := &negotiatedTransport{}, &wireRecorder{}
	options := append(dialOptions(exchange), grpc.WithChainUnaryInterceptor(wire.record), grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }))
	connection, err := grpc.NewClient("passthrough:///transport", options...)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(connection.Close(), qt.IsNil) })
	negotiated := adapter.negotiated()
	exchange.negotiated(negotiated)
	return &Client{description: negotiated, rpc: protocol.NewProviderClient(connection)}, wire
}

// decodingServer is the SDK's server adapter of d with decoder as its source
// decoder, negotiated with the host features given.
func decodingServer(d *protocol.Description, decoder provider.SourceDecoder, hostFeatures ...string) *serverAdapter {
	return negotiatedServer(Server{Description: d, SourceDecoder: decoder}, hostFeatures...)
}

func TestModelReferences_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name         string
		hostFeatures []string
		// definitions reports whether the models travel with their
		// definitions.
		definitions bool
	}{
		{name: "both list the feature", hostFeatures: []string{provider.ModelReferencesFeature}, definitions: false},
		{name: "the host does not list it", definitions: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, request, reply := referenceExchange()
			c.Assert(provider.ValidateDecodedSource(provider.NegotiatedDescription(d, test.hostFeatures), request, reply), qt.IsNil)
			decoder := &recordingDecoder{reply: reply}
			client, wire := transportPeers(c, d, decodingServer(d, decoder, test.hostFeatures...), test.hostFeatures...)
			before := proto.CloneOf(request)

			result, err := client.DecodeSource(c.Context(), request)

			c.Assert(err, qt.IsNil)
			c.Assert(proto.Equal(result, reply), qt.IsTrue)
			c.Assert(proto.Equal(request, before), qt.IsTrue)
			c.Assert(decoder.models, qt.HasLen, 1)
			c.Assert(proto.Equal(decoder.models[0], d.Engines[0].ValueModels[0]), qt.IsTrue)
			sent := wire.requests[0].(*protocol.DecodeSourceRequest).GetDocuments()[0].GetModels()[0]
			received := wire.replies[0].(*protocol.DecodedSource).GetDocuments()[0].GetCoverage().GetKinds()[0].GetModel()
			c.Assert(len(sent.GetDefinitionJson()) != 0, qt.Equals, test.definitions)
			c.Assert(len(received.GetDefinitionJson()) != 0, qt.Equals, test.definitions)
		})
	}
}

// A server refuses a reference it cannot resolve before any service reads it:
// one to a model the description does not declare, and any reference from a
// host that did not negotiate the feature.
func TestServerModelReferences_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name         string
		hostFeatures []string
		reference    func(*protocol.Description) *protocol.ValueModel
	}{
		{name: "a reference to an undeclared model", hostFeatures: []string{provider.ModelReferencesFeature},
			reference: func(d *protocol.Description) *protocol.ValueModel { return provider.ModelReference(undeclaredModel(d)) }},
		{name: "a reference without the feature",
			reference: func(d *protocol.Description) *protocol.ValueModel {
				return provider.ModelReference(d.Engines[0].ValueModels[0])
			}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			d, request, reply := referenceExchange()
			decoder := &recordingDecoder{reply: reply}
			client, _ := transportPeers(c, d, decodingServer(d, decoder, test.hostFeatures...))
			request.Documents[0].Models = []*protocol.ValueModel{test.reference(d)}

			_, err := client.rpc.DecodeSource(c.Context(), request)

			c.Assert(status.Code(err), qt.Equals, codes.InvalidArgument)
			c.Assert(decoder.calls, qt.Equals, 0)
		})
	}
}

// replyingProvider answers DecodeSource with reply and Inspect with an empty
// schema, without the SDK's checks: a peer that does not keep the contract.
type replyingProvider struct {
	protocol.UnimplementedProviderServer
	reply *protocol.DecodedSource
}

func (p *replyingProvider) DecodeSource(context.Context, *protocol.DecodeSourceRequest) (*protocol.DecodedSource, error) {
	return proto.CloneOf(p.reply), nil
}

func (*replyingProvider) Inspect(context.Context, *protocol.InspectRequest) (*protocol.Schema, error) {
	return &protocol.Schema{}, nil
}

// A client refuses a reply whose reference names no model of the description.
func TestClientModelReferences_FailurePath(t *testing.T) {
	c := qt.New(t)
	d, request, reply := referenceExchange()
	reply.Documents[0].Coverage.Kinds[0].Model = provider.ModelReference(undeclaredModel(d))
	client, _ := transportPeers(c, d, &replyingProvider{reply: reply}, provider.ModelReferencesFeature)

	result, err := client.DecodeSource(c.Context(), request)

	c.Assert(err, qt.ErrorMatches, `provider decode source failed \(Internal\)`)
	c.Assert(result, qt.IsNil)
}

// largeRequest is a request of more than MaxMessageBytes.
func largeRequest() *protocol.InspectRequest {
	return &protocol.InspectRequest{SessionId: strings.Repeat("s", MaxMessageBytes+1)}
}

func TestLargeMessages_HappyPath(t *testing.T) {
	c := qt.New(t)
	d, _, _ := referenceExchange()
	client, _ := transportPeers(c, d, &replyingProvider{}, provider.LargeMessagesFeature)

	_, err := client.rpc.Inspect(c.Context(), largeRequest())

	c.Assert(err, qt.IsNil)
}

// A host that did not negotiate the feature sends no more than
// MaxMessageBytes, which is what a provider of an older contract accepts.
func TestLargeMessages_FailurePath(t *testing.T) {
	c := qt.New(t)
	d, _, _ := referenceExchange()
	client, _ := transportPeers(c, d, &replyingProvider{})

	_, err := client.rpc.Inspect(c.Context(), largeRequest())

	c.Assert(status.Code(err), qt.Equals, codes.ResourceExhausted)
}
