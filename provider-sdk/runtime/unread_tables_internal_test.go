package runtime

// White-box testing required: the negotiated state of the server adapter is
// set by Describe and read by every later call, and a snapshot from a peer
// that is not the SDK server must reach the client's own checks.

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	protocol "ptah.run/provider-sdk/protocol/experimental"
	"ptah.run/provider-sdk/provider"
)

// unreadExchange is the inspection exchange of an engine that lists
// UnreadTablesFeature, whose snapshot names one table it leaves out.
func unreadExchange() (*protocol.Description, *protocol.InspectRequest, *protocol.Schema) {
	description, request, snapshot := inspectionExchange()
	description.Features = append(description.Features, provider.UnreadTablesFeature)
	description.Engines[0].Features = append(description.Engines[0].Features, provider.UnreadTablesFeature)
	snapshot.TargetFeatures = []string{provider.UnreadTablesFeature}
	snapshot.UnreadTables = []*protocol.UnreadTable{{Table: &protocol.Reference{Engine: "test", Database: "db", Kind: "table", Name: "rep"},
		Diagnostic: &protocol.ProviderDiagnostic{Reason: "rep is a replica table no replication writes any more"}}}
	return description, request, snapshot
}

// unreadInspector answers every inspection with its snapshot.
type unreadInspector struct{ snapshot *protocol.Schema }

func (i *unreadInspector) Inspect(context.Context, *protocol.InspectRequest) (*protocol.Schema, error) {
	return proto.CloneOf(i.snapshot), nil
}

// unreadLoopback forwards Inspect to a server adapter, or answers with a
// canned snapshot when one is set, as a peer that is not the SDK server could.
type unreadLoopback struct {
	protocol.ProviderClient
	server *serverAdapter
	canned *protocol.Schema
}

func (l *unreadLoopback) Inspect(ctx context.Context, request *protocol.InspectRequest, _ ...grpc.CallOption) (*protocol.Schema, error) {
	if l.canned != nil {
		return l.canned, nil
	}
	return l.server.Inspect(ctx, request)
}

func TestInspectUnreadTablesConformance_HappyPath(t *testing.T) {
	c := qt.New(t)
	description, request, snapshot := unreadExchange()
	server := negotiatedServer(Server{Description: description, Inspector: &unreadInspector{snapshot: snapshot}}, provider.UnreadTablesFeature)
	client := &Client{description: server.negotiated(), rpc: &unreadLoopback{server: server}}

	result, err := client.Inspect(c.Context(), request)

	c.Assert(err, qt.IsNil)
	c.Assert(proto.Equal(result, snapshot), qt.IsTrue)
}

// An entry passes only when both sides list the feature: the server refuses
// its owner's snapshot for a host that did not list it, and a client whose
// negotiated description lacks it refuses the same snapshot from a peer that
// is not the SDK server. The snapshot carries the target features of a
// session opened without the feature.
func TestInspectUnreadTablesConformance_FailurePath(t *testing.T) {
	c := qt.New(t)
	description, request, snapshot := unreadExchange()
	snapshot.TargetFeatures = nil
	server := negotiatedServer(Server{Description: description, Inspector: &unreadInspector{snapshot: snapshot}})
	peer := &unreadLoopback{server: server}
	client := &Client{description: server.negotiated(), rpc: peer}

	result, err := server.Inspect(c.Context(), request)
	c.Assert(status.Code(err), qt.Equals, codes.FailedPrecondition)
	c.Assert(status.Convert(err).Message(), qt.Matches, `.*unread tables: the feature was not negotiated`)
	c.Assert(result, qt.IsNil)

	peer.canned = snapshot
	result, err = client.Inspect(c.Context(), request)
	c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	c.Assert(err, qt.ErrorMatches, `.*unread tables: the feature was not negotiated`)
	c.Assert(result, qt.IsNil)
}
