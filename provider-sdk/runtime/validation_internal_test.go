package runtime

// White-box testing required: malformed peers must bypass the SDK server, and
// cancellation after owner dispatch must exercise the private adapter directly.

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

func runtimeValidationExchange() (*protocol.Description, *protocol.ValidateSchemaRequest) {
	d, q, _ := runtimeCreationExchange()
	d.Features = append(d.Features, provider.ValidateSchemaFeature)
	d.Engines[0].Features = append(d.Engines[0].Features, provider.ValidateSchemaFeature)
	return d, &protocol.ValidateSchemaRequest{Engine: q.Engine, Identifiers: q.Identifiers, Features: proto.CloneOf(q.Tables[0].Declaration.Features), Tables: []*protocol.ValidationTable{{Subject: q.Tables[0].Subject, Declaration: q.Tables[0].Declaration}}}
}
func TestSchemaValidationClientDiscardsMalformedSuffixAndErrors(t *testing.T) {
	c := qt.New(t)
	d, q := runtimeValidationExchange()
	peer := &validationPeer{result: &protocol.SchemaValidation{Complete: true}}
	client := &Client{description: d, rpc: peer}
	result, err := client.ValidateSchema(c.Context(), q)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Complete, qt.IsTrue)
	peer.result.Diagnostics = []*protocol.SchemaDiagnostic{{Code: protocol.SchemaDiagnostic_INVALID_SCHEMA, Kind: "table", Message: "bad"}, nil}
	result, err = client.ValidateSchema(c.Context(), q)
	c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	c.Assert(result, qt.IsNil)
	peer.err = status.Error(codes.Unavailable, "password=secret")
	result, err = client.ValidateSchema(c.Context(), q)
	c.Assert(err, qt.ErrorMatches, `provider validate schema failed \(Unavailable\)`)
	c.Assert(result, qt.IsNil)
	q.Features = nil
	result, err = client.ValidateSchema(c.Context(), q)
	c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	c.Assert(result, qt.IsNil)
	c.Assert(peer.calls, qt.Equals, 3)
}
func TestSchemaValidationServerProtectsSourceAndCancellation(t *testing.T) {
	c := qt.New(t)
	d, q := runtimeValidationExchange()
	original := proto.CloneOf(q)
	owner := &validationOwner{}
	server := &serverAdapter{server: Server{Description: d, SchemaValidator: owner}}
	result, err := server.ValidateSchema(c.Context(), q)
	c.Assert(err, qt.IsNil)
	c.Assert(result.Complete, qt.IsTrue)
	c.Assert(proto.Equal(original, q), qt.IsTrue)
	ctx, cancel := context.WithCancel(c.Context())
	cancel()
	result, err = server.ValidateSchema(ctx, q)
	c.Assert(status.Code(err), qt.Equals, codes.Canceled)
	c.Assert(result, qt.IsNil)
	c.Assert(owner.calls, qt.Equals, 1)
	ctx, cancel = context.WithCancel(c.Context())
	defer cancel()
	owner.cancel = cancel
	result, err = server.ValidateSchema(ctx, q)
	c.Assert(status.Code(err), qt.Equals, codes.Canceled)
	c.Assert(result, qt.IsNil)
	c.Assert(owner.calls, qt.Equals, 2)
}

type validationPeer struct {
	protocol.ProviderClient
	result *protocol.SchemaValidation
	err    error
	calls  int
}

func (p *validationPeer) ValidateSchema(context.Context, *protocol.ValidateSchemaRequest, ...grpc.CallOption) (*protocol.SchemaValidation, error) {
	p.calls++
	return p.result, p.err
}

type validationOwner struct {
	calls  int
	cancel context.CancelFunc
}

func (p *validationOwner) ValidateSchema(_ context.Context, q *protocol.ValidateSchemaRequest) (*protocol.SchemaValidation, error) {
	p.calls++
	q.Tables[0].Declaration.Name = "mutated"
	if p.cancel != nil {
		p.cancel()
	}
	return &protocol.SchemaValidation{Complete: true}, nil
}
