package runtime

// White-box testing required: these tests inject malformed peer replies before
// transport serialization and inspect handle cleanup. The public process API
// cannot expose that boundary.

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

func sessionReply() *protocol.Session {
	return &protocol.Session{Id: "session", Engine: "fixture", Capabilities: map[string]bool{"transactional_ddl": false},
		Identifiers: &protocol.IdentifierRules{TableNames: protocol.IdentifierRules_EXACT, ColumnNames: protocol.IdentifierRules_EXACT,
			IndexNames: protocol.IdentifierRules_EXACT, IndexNamespace: protocol.IdentifierRules_TABLE}}
}

type sessionPeer struct {
	protocol.ProviderClient
	reply  *protocol.Session
	closed []string
}

func (p *sessionPeer) Open(context.Context, *protocol.OpenRequest, ...grpc.CallOption) (*protocol.Session, error) {
	return p.reply, nil
}
func (p *sessionPeer) Close(_ context.Context, request *protocol.CloseRequest, _ ...grpc.CallOption) (*protocol.Empty, error) {
	p.closed = append(p.closed, request.GetSessionId())
	return &protocol.Empty{}, nil
}

func TestSessionCapabilitiesRetainPresenceAndIndependentCopies(t *testing.T) {
	c := qt.New(t)
	peer := &sessionPeer{reply: sessionReply()}
	client := &Client{rpc: peer}
	result, err := client.Open(c.Context(), &protocol.OpenRequest{Engine: "fixture"})
	c.Assert(err, qt.IsNil)
	value, known := result.Capabilities["transactional_ddl"]
	c.Assert(value, qt.IsFalse)
	c.Assert(known, qt.IsTrue)
	_, known = result.Capabilities["unestablished"]
	c.Assert(known, qt.IsFalse)
	peer.reply.Capabilities["transactional_ddl"] = true
	c.Assert(result.Capabilities["transactional_ddl"], qt.IsFalse)
	encoded, err := proto.Marshal(result)
	c.Assert(err, qt.IsNil)
	decoded := &protocol.Session{}
	c.Assert(proto.Unmarshal(encoded, decoded), qt.IsNil)
	c.Assert(proto.Equal(result, decoded), qt.IsTrue)
}

func TestInvalidSessionCapabilityClosesTheReturnedHandle(t *testing.T) {
	for _, name := range []string{"", " transactional_ddl", "transactional_ddl\n"} {
		t.Run(name, func(t *testing.T) {
			c := qt.New(t)
			peer := &sessionPeer{reply: sessionReply()}
			peer.reply.Capabilities = map[string]bool{name: false}
			client := &Client{rpc: peer}
			result, err := client.Open(c.Context(), &protocol.OpenRequest{Engine: "fixture"})
			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
			c.Assert(result, qt.IsNil)
			c.Assert(peer.closed, qt.DeepEquals, []string{"session"})
		})
	}
}

type sessionOwner struct {
	reply  *protocol.Session
	closed []string
}

func (p *sessionOwner) Open(context.Context, *protocol.OpenRequest) (*protocol.Session, error) {
	return p.reply, nil
}
func (p *sessionOwner) Close(_ context.Context, request *protocol.CloseRequest) error {
	p.closed = append(p.closed, request.GetSessionId())
	return nil
}

func TestServerRefusesInvalidSessionFactsAndReleasesHandle(t *testing.T) {
	c := qt.New(t)
	owner := &sessionOwner{reply: sessionReply()}
	owner.reply.Capabilities["bad\nname"] = true
	server := &serverAdapter{server: Server{Description: &protocol.Description{Engines: []*protocol.Engine{{Id: "fixture"}}}, Sessions: owner}}
	result, err := server.Open(c.Context(), &protocol.OpenRequest{Engine: "fixture", Address: "fixture://opaque"})
	c.Assert(status.Code(err), qt.Equals, codes.Internal)
	c.Assert(result, qt.IsNil)
	c.Assert(owner.closed, qt.DeepEquals, []string{"session"})
}
