package runtime

// White-box testing required: malformed remote receipts and dispatch counts
// cannot be injected through an independently built, valid provider process.

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

type metadataService struct {
	provider.Metadata
	opens, closes, writes, pages    int
	result                          *protocol.MetadataResult
	failure                         error
	foreign, badPage                bool
	fence                           *protocol.MetadataReadRequest
	cleanupCanceled, cleanupBounded bool
}

func (m *metadataService) OpenMetadataRead(_ context.Context, request *protocol.MetadataReadRequest) (*protocol.MetadataReadResult, error) {
	m.opens++
	receipt := readReceipt(request)
	if m.foreign {
		receipt.ReaderId = "foreign"
	}
	return receipt, m.failure
}
func (m *metadataService) CloseMetadataRead(ctx context.Context, request *protocol.MetadataReadRequest) (*protocol.MetadataReadResult, error) {
	m.closes++
	m.fence = proto.CloneOf(request)
	m.cleanupCanceled = ctx.Err() != nil
	_, m.cleanupBounded = ctx.Deadline()
	return readReceipt(request), nil
}
func (m *metadataService) MutateMetadata(_ context.Context, _ *protocol.MutateMetadataRequest) (*protocol.MetadataResult, error) {
	m.writes++
	return m.result, m.failure
}
func (m *metadataService) ReadMetadataPage(_ context.Context, request *protocol.ReadMetadataPageRequest) (*protocol.MetadataPage, error) {
	m.pages++
	return &protocol.MetadataPage{Reader: readReceipt(request.Reader), Page: request.Page, Exists: true, Complete: !m.badPage}, m.failure
}

type metadataPeer struct {
	protocol.ProviderClient
	owner *metadataService
}

func (p metadataPeer) OpenMetadataRead(ctx context.Context, r *protocol.MetadataReadRequest, _ ...grpc.CallOption) (*protocol.MetadataReadResult, error) {
	return p.owner.OpenMetadataRead(ctx, r)
}
func (p metadataPeer) CloseMetadataRead(ctx context.Context, r *protocol.MetadataReadRequest, _ ...grpc.CallOption) (*protocol.MetadataReadResult, error) {
	return p.owner.CloseMetadataRead(ctx, r)
}
func (p metadataPeer) MutateMetadata(ctx context.Context, r *protocol.MutateMetadataRequest, _ ...grpc.CallOption) (*protocol.MetadataResult, error) {
	return p.owner.MutateMetadata(ctx, r)
}
func (p metadataPeer) ReadMetadataPage(ctx context.Context, r *protocol.ReadMetadataPageRequest, _ ...grpc.CallOption) (*protocol.MetadataPage, error) {
	return p.owner.ReadMetadataPage(ctx, r)
}
func metadataClient(owner *metadataService) *Client {
	return &Client{rpc: metadataPeer{owner: owner}, description: &protocol.Description{Features: []string{provider.MetadataFeature}}}
}
func readRequest() *protocol.MetadataReadRequest {
	return &protocol.MetadataReadRequest{Scope: &protocol.MetadataScope{SessionId: "session", Schema: "main", Table: "tags", Kind: protocol.MetadataScope_TAGS}, ReaderId: "reader", RequestId: "request"}
}
func readReceipt(r *protocol.MetadataReadRequest) *protocol.MetadataReadResult {
	return &protocol.MetadataReadResult{Scope: proto.CloneOf(r.Scope), ReaderId: r.ReaderId, RequestId: r.RequestId}
}
func TestMetadataClientFencesUncertainReaderOpen(t *testing.T) {
	for _, test := range []struct {
		name    string
		failure error
		foreign bool
	}{
		{"lost reply", status.Error(codes.Unavailable, "secret-address"), false},
		{"wrong handle", nil, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			owner := &metadataService{failure: test.failure, foreign: test.foreign}
			client := metadataClient(owner)
			request := readRequest()
			result, err := client.OpenMetadataRead(c.Context(), request)
			c.Assert(err, qt.ErrorIs, provider.ErrOutcomeUnknown)
			c.Assert(result, qt.IsNil)
			c.Assert(owner.opens, qt.Equals, 1)
			c.Assert(owner.closes, qt.Equals, 1)
			c.Assert(owner.fence.ReaderId, qt.Equals, request.ReaderId)
			c.Assert(proto.Equal(owner.fence.Scope, request.Scope), qt.IsTrue)
			c.Assert(owner.fence.RequestId, qt.Not(qt.Equals), request.RequestId)
			c.Assert(owner.cleanupCanceled, qt.IsFalse)
			c.Assert(owner.cleanupBounded, qt.IsTrue)
		})
	}
}
func TestMetadataClientDoesNotDispatchInvalidOrCanceledOpen(t *testing.T) {
	c := qt.New(t)
	owner := &metadataService{}
	client := metadataClient(owner)
	ctx, cancel := context.WithCancel(c.Context())
	cancel()
	_, err := client.OpenMetadataRead(ctx, readRequest())
	c.Assert(err, qt.ErrorIs, context.Canceled)
	request := readRequest()
	request.ReaderId = ""
	_, err = client.OpenMetadataRead(c.Context(), request)
	c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	c.Assert(owner.opens, qt.Equals, 0)
	c.Assert(owner.closes, qt.Equals, 0)
}
func TestMetadataClientNeverReplaysUncertainMutation(t *testing.T) {
	c := qt.New(t)
	owner := &metadataService{failure: status.Error(codes.Unavailable, "secret")}
	client := metadataClient(owner)
	request := &protocol.MutateMetadataRequest{Scope: readRequest().Scope, RequestId: "delete", Mutation: &protocol.MutateMetadataRequest_DeleteTag{DeleteTag: "release"}}
	result, err := client.MutateMetadata(c.Context(), request)
	c.Assert(result, qt.IsNil)
	c.Assert(err, qt.ErrorIs, provider.ErrOutcomeUnknown)
	c.Assert(owner.writes, qt.Equals, 1)
	owner.failure = nil
	owner.result = &protocol.MetadataResult{Scope: request.Scope, RequestId: request.RequestId, Outcome: protocol.MetadataResult_REJECTED}
	_, err = client.MutateMetadata(c.Context(), request)
	c.Assert(err, qt.ErrorIs, provider.ErrExecution)
	c.Assert(owner.writes, qt.Equals, 2)
	owner.result.Outcome = protocol.MetadataResult_APPLIED
	result, err = client.MutateMetadata(c.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(result.GetDeleted(), qt.IsFalse)
	c.Assert(owner.writes, qt.Equals, 3)
}
func TestMetadataClientClosesInvalidPageWithoutRetry(t *testing.T) {
	c := qt.New(t)
	owner := &metadataService{badPage: true}
	client := metadataClient(owner)
	result, err := client.ReadMetadataPage(c.Context(), &protocol.ReadMetadataPageRequest{Reader: readRequest(), MaxRecords: 1})
	c.Assert(result, qt.IsNil)
	c.Assert(err, qt.ErrorIs, provider.ErrOutcomeUnknown)
	c.Assert(owner.pages, qt.Equals, 1)
	c.Assert(owner.closes, qt.Equals, 1)
}
func TestMetadataServerCleansMalformedOpenReceipt(t *testing.T) {
	c := qt.New(t)
	owner := &metadataService{foreign: true}
	server := &serverAdapter{server: Server{Metadata: owner}}
	result, err := server.OpenMetadataRead(c.Context(), readRequest())
	c.Assert(result, qt.IsNil)
	c.Assert(status.Code(err), qt.Equals, codes.Internal)
	c.Assert(owner.opens, qt.Equals, 1)
	c.Assert(owner.closes, qt.Equals, 1)
	c.Assert(owner.cleanupBounded, qt.IsTrue)
	c.Assert(owner.cleanupCanceled, qt.IsFalse)
}
