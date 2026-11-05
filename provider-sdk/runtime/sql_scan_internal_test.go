package runtime

// White-box testing required: the negotiated state of the server adapter is
// set by Describe and read by every later call, and a reply from a peer that
// is not the SDK server must reach the client's own checks.

import (
	"context"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// scanDescription offers SQL scanning on one engine.
func scanDescription() *protocol.Description {
	return &protocol.Description{
		Revision: provider.Revision, ProviderId: "example.org/scan", Version: "1", SemanticIdentity: "scan-1", Features: []string{provider.ScanSQLFeature},
		Engines: []*protocol.Engine{{
			Id: "scan", Features: []string{provider.ScanSQLFeature},
			Identifiers: &protocol.IdentifierRules{TableNames: protocol.IdentifierRules_EXACT, ColumnNames: protocol.IdentifierRules_EXACT,
				IndexNames: protocol.IdentifierRules_EXACT, IndexNamespace: protocol.IdentifierRules_TABLE},
		}},
	}
}

// unscannedDescription is scanDescription without the feature.
func unscannedDescription() *protocol.Description {
	d := scanDescription()
	scan := func(f string) bool { return f == provider.ScanSQLFeature }
	d.Features = slices.DeleteFunc(d.Features, scan)
	d.Engines[0].Features = slices.DeleteFunc(d.Engines[0].Features, scan)
	return d
}

// scanOwner reads the text of scanSQLRequest as one identifier, or answers
// with its canned reply. It counts its calls.
type scanOwner struct {
	calls int
	reply *protocol.SQLScan
}

func (o *scanOwner) ScanSQL(_ context.Context, request *protocol.ScanSQLRequest) (*protocol.SQLScan, error) {
	o.calls++
	if o.reply != nil {
		return o.reply, nil
	}
	return &protocol.SQLScan{Tokens: []*protocol.SQLToken{{Kind: protocol.SQLToken_IDENTIFIER, End: 5, Value: new(request.Text)}}}, nil
}

// scanLoopback forwards ScanSQL to a server adapter, or answers with a canned
// reply when one is set, as a peer that is not the SDK server could.
type scanLoopback struct {
	protocol.ProviderClient
	server *serverAdapter
	calls  int
	canned *protocol.SQLScan
}

func (l *scanLoopback) ScanSQL(ctx context.Context, request *protocol.ScanSQLRequest, _ ...grpc.CallOption) (*protocol.SQLScan, error) {
	l.calls++
	if l.canned != nil {
		return l.canned, nil
	}
	return l.server.ScanSQL(ctx, request)
}

// newScanLoopback is a client and a server for a provider that offers
// description. The feature is one-way, so the host lists none.
func newScanLoopback(description *protocol.Description, owner *scanOwner) (*Client, *scanLoopback) {
	server := negotiatedServer(Server{Description: description, SQLScanner: owner})
	peer := &scanLoopback{server: server}
	return &Client{description: server.negotiated(), rpc: peer}, peer
}

func scanSQLRequest() *protocol.ScanSQLRequest {
	return &protocol.ScanSQLRequest{Engine: "scan", Text: "users", Head: true}
}

func TestScanSQLConformance_HappyPath(t *testing.T) {
	c := qt.New(t)
	owner := &scanOwner{}
	client, peer := newScanLoopback(scanDescription(), owner)
	request := scanSQLRequest()
	before := proto.CloneOf(request)
	scan, err := client.ScanSQL(c.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(scan.Tokens, qt.HasLen, 1)
	c.Assert(scan.Tokens[0].GetKind(), qt.Equals, protocol.SQLToken_IDENTIFIER)
	c.Assert(scan.Tokens[0].GetValue(), qt.Equals, "users")
	c.Assert(peer.calls, qt.Equals, 1)
	c.Assert(owner.calls, qt.Equals, 1)
	c.Assert(proto.Equal(before, request), qt.IsTrue)
}

// A host never sends the call to a provider that does not list the feature,
// and that provider's server refuses it before its owner sees it.
func TestScanSQLNeedsTheFeature(t *testing.T) {
	c := qt.New(t)
	owner := &scanOwner{}
	client, peer := newScanLoopback(unscannedDescription(), owner)
	scan, err := client.ScanSQL(c.Context(), scanSQLRequest())
	c.Assert(err, qt.ErrorMatches, `.*SQL scan: the provider does not scan SQL`)
	c.Assert(scan, qt.IsNil)
	c.Assert(peer.calls, qt.Equals, 0)
	scan, err = peer.server.ScanSQL(c.Context(), scanSQLRequest())
	c.Assert(status.Code(err), qt.Equals, codes.InvalidArgument)
	c.Assert(scan, qt.IsNil)
	c.Assert(owner.calls, qt.Equals, 0)
}

// An invalid reply reaches no caller: the server refuses its owner's, and the
// client refuses one from a peer that is not the SDK server. A canceled call
// sends nothing.
func TestScanSQLConformance_FailurePath(t *testing.T) {
	c := qt.New(t)
	owner := &scanOwner{reply: &protocol.SQLScan{Tokens: []*protocol.SQLToken{{Kind: protocol.SQLToken_IDENTIFIER, End: 2}}}}
	client, peer := newScanLoopback(scanDescription(), owner)
	scan, err := peer.server.ScanSQL(c.Context(), scanSQLRequest())
	c.Assert(status.Code(err), qt.Equals, codes.Internal)
	c.Assert(scan, qt.IsNil)
	scan, err = client.ScanSQL(c.Context(), scanSQLRequest())
	c.Assert(err, qt.ErrorMatches, `provider scan SQL failed \(Internal\)`)
	c.Assert(scan, qt.IsNil)

	peer.canned = owner.reply
	scan, err = client.ScanSQL(c.Context(), scanSQLRequest())
	c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	c.Assert(err, qt.ErrorMatches, `.*SQL scan: the tokens do not reach the end of the text`)
	c.Assert(scan, qt.IsNil)

	canceled, cancel := context.WithCancel(c.Context())
	cancel()
	peer.calls = 0
	_, err = client.ScanSQL(canceled, scanSQLRequest())
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(peer.calls, qt.Equals, 0)
}

// The service and its feature are advertised together, and a server without
// the service answers the call as unavailable.
func TestSQLScanServiceMatchesItsAdvertisement(t *testing.T) {
	c := qt.New(t)
	server := Server{Description: scanDescription(), Sessions: &sessionOwner{}, SQLScanner: &scanOwner{}}
	c.Assert(server.Validate(), qt.IsNil)
	without := server
	without.SQLScanner = nil
	c.Assert(without.Validate(), qt.ErrorMatches, `.*sql\.scan advertisement differs from implementation`)
	unadvertised := server
	unadvertised.Description = unscannedDescription()
	c.Assert(unadvertised.Validate(), qt.ErrorMatches, `.*sql\.scan advertisement differs from implementation`)
	_, err := negotiatedServer(without).ScanSQL(c.Context(), scanSQLRequest())
	c.Assert(status.Code(err), qt.Equals, codes.Unimplemented)
}
