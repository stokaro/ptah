package runtime_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	transport "github.com/stokaro/ptah/provider-sdk/runtime"
)

func TestServerRequiresAdvertisedServices(t *testing.T) {
	c := qt.New(t)
	description := &protocol.Description{Revision: provider.Revision, ProviderId: "example.org/test", Version: "1", SemanticIdentity: "schema-1", Engines: []*protocol.Engine{{Id: "test", Identifiers: &protocol.IdentifierRules{TableNames: protocol.IdentifierRules_EXACT, ColumnNames: protocol.IdentifierRules_EXACT, IndexNames: protocol.IdentifierRules_EXACT, IndexNamespace: protocol.IdentifierRules_TABLE}}}}
	server := transport.Server{Description: description, Sessions: &sessions{}}
	c.Assert(server.Validate(), qt.IsNil)
	server.Sessions = (*sessions)(nil)
	c.Assert(server.Validate(), qt.ErrorIs, provider.ErrProtocol)
	server.Sessions = &sessions{}
	description.Features = []string{provider.InspectFeature}
	c.Assert(server.Validate(), qt.ErrorIs, provider.ErrProtocol)
	description.Revision++
	c.Assert(server.Validate(), qt.ErrorIs, provider.ErrProtocol)
}

type sessions struct{}

func (*sessions) Open(context.Context, *protocol.OpenRequest) (*protocol.Session, error) {
	return nil, nil
}
func (*sessions) Close(context.Context, *protocol.CloseRequest) error { return nil }

func TestCanceledStartupDoesNotNeedAnExecutable(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithCancel(c.Context())
	cancel()
	client, err := transport.Start(ctx, transport.Options{})
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(client, qt.IsNil)
}
