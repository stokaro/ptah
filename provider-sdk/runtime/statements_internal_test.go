package runtime

// White-box testing required: the loopback sends each client call through the
// server adapter without a process, and a malformed reply from a peer that is
// not the SDK server must reach the client's own checks.

import (
	"context"
	"fmt"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// statementsDescription serves operator statements, and transactions to run
// them in, on one engine.
func statementsDescription() *protocol.Description {
	features := []string{provider.StatementsFeature, provider.ExecuteFeature, provider.TransactionsFeature}
	return &protocol.Description{
		Revision: provider.Revision, ProviderId: "example.org/statements", Version: "1", SemanticIdentity: "statements-1", Features: features,
		Engines: []*protocol.Engine{{Id: "statements", Features: features, Identifiers: &protocol.IdentifierRules{TableNames: protocol.IdentifierRules_EXACT,
			ColumnNames: protocol.IdentifierRules_EXACT, IndexNames: protocol.IdentifierRules_EXACT, IndexNamespace: protocol.IdentifierRules_TABLE}}},
	}
}

func statementText(value string) *protocol.DataValue {
	return &protocol.DataValue{Value: &protocol.DataValue_StringValue{StringValue: value}}
}

// statementOwner answers SELECT with three rows of one column in pages, a
// write with one changed row, FAIL with the server's message and a READ_ONLY
// write with a refusal. It records each token it released.
type statementOwner struct {
	released []string
}

func (o *statementOwner) RunStatement(_ context.Context, request *protocol.RunStatementRequest) (*protocol.StatementResult, error) {
	result := &protocol.StatementResult{RequestId: request.RequestId}
	switch {
	case strings.HasPrefix(request.Sql, "FAIL"):
		result.Outcome, result.Error, result.ErrorCode = protocol.StatementResult_FAILED, "duplicate key value (code)=(CZ)", "23505"
	case !strings.HasPrefix(request.Sql, "SELECT") && request.Access == protocol.RunStatementRequest_READ_ONLY:
		result.Outcome, result.Refusal = protocol.StatementResult_REFUSED, "the statement writes"
	case strings.HasPrefix(request.Sql, "SELECT"):
		result.Outcome = protocol.StatementResult_SUCCEEDED
		result.Columns = []*protocol.DataColumn{{Name: "code", Type: "Utf8", Kind: protocol.DataKind_DATA_KIND_STRING}}
		result.Rows, result.NextPageToken = o.page(0, request.MaxRows)
	default:
		result.Outcome, result.RowsAffected = protocol.StatementResult_SUCCEEDED, new(uint64(1))
	}
	return result, nil
}

var statementCodes = []string{"CZ", "SK", "PL"}

// page returns the rows from start, at most limit of them, and the token of
// the next page.
func (*statementOwner) page(start int, limit uint32) ([]*protocol.Row, string) {
	end := min(start+int(limit), len(statementCodes))
	rows := make([]*protocol.Row, 0, end-start)
	for _, code := range statementCodes[start:end] {
		rows = append(rows, &protocol.Row{Values: []*protocol.DataValue{statementText(code)}})
	}
	if end == len(statementCodes) {
		return rows, ""
	}
	return rows, fmt.Sprint("from-", end)
}

func (o *statementOwner) ReadStatementRows(_ context.Context, request *protocol.ReadStatementRowsRequest) (*protocol.StatementRows, error) {
	var start int
	if _, err := fmt.Sscanf(request.PageToken, "from-%d", &start); err != nil {
		return nil, status.Error(codes.NotFound, "no open result")
	}
	rows, next := o.page(start, request.MaxRows)
	return &protocol.StatementRows{Rows: rows, NextPageToken: next}, nil
}

func (o *statementOwner) CloseStatement(_ context.Context, request *protocol.CloseStatementRequest) (*protocol.Empty, error) {
	o.released = append(o.released, request.PageToken)
	return &protocol.Empty{}, nil
}

func statementServer(owner *statementOwner) Server {
	return Server{Description: statementsDescription(), Sessions: &sessionOwner{}, Statements: owner, Executor: executorOwner{}, Transactions: &idleTransactions{}}
}

// statementLoopback forwards the statement calls to a server adapter, or
// answers with a canned reply when one is set.
type statementLoopback struct {
	protocol.ProviderClient
	server *serverAdapter
	calls  int
	canned proto.Message
}

func statementCall[Q, R proto.Message](ctx context.Context, l *statementLoopback, serve func(context.Context, Q) (R, error), request Q) (R, error) {
	l.calls++
	if canned, ok := l.canned.(R); ok {
		return canned, nil
	}
	return serve(ctx, request)
}

func (l *statementLoopback) RunStatement(ctx context.Context, request *protocol.RunStatementRequest, _ ...grpc.CallOption) (*protocol.StatementResult, error) {
	return statementCall(ctx, l, l.server.RunStatement, request)
}

func (l *statementLoopback) ReadStatementRows(ctx context.Context, request *protocol.ReadStatementRowsRequest, _ ...grpc.CallOption) (*protocol.StatementRows, error) {
	return statementCall(ctx, l, l.server.ReadStatementRows, request)
}

func (l *statementLoopback) CloseStatement(ctx context.Context, request *protocol.CloseStatementRequest, _ ...grpc.CallOption) (*protocol.Empty, error) {
	return statementCall(ctx, l, l.server.CloseStatement, request)
}

func newStatementLoopback(owner *statementOwner) (*Client, *statementLoopback) {
	server := negotiatedServer(statementServer(owner))
	peer := &statementLoopback{server: server}
	return &Client{description: server.negotiated(), rpc: peer}, peer
}

func selectStatement(access protocol.RunStatementRequest_Access) *protocol.RunStatementRequest {
	return &protocol.RunStatementRequest{SessionId: "session", RequestId: "request", Sql: "SELECT code FROM rates", Access: access, MaxRows: 2}
}

// A statement's first page comes with its reply, the next pages follow its
// tokens, and a result is released before its last page. A write reports the
// rows it changed.
func TestStatementsConformance_HappyPath(t *testing.T) {
	c := qt.New(t)
	owner := &statementOwner{}
	client, peer := newStatementLoopback(owner)
	c.Assert(peer.server.server.Validate(), qt.IsNil)
	request := selectStatement(protocol.RunStatementRequest_READ_ONLY)
	before := proto.CloneOf(request)

	result, err := client.RunStatement(c.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(proto.Equal(before, request), qt.IsTrue)
	c.Assert(result.Outcome, qt.Equals, protocol.StatementResult_SUCCEEDED)
	c.Assert(result.Rows, qt.HasLen, 2)
	c.Assert(result.NextPageToken, qt.Equals, "from-2")
	page, err := client.ReadStatementRows(c.Context(), &protocol.ReadStatementRowsRequest{SessionId: "session", PageToken: result.NextPageToken,
		MaxRows: 2, ColumnCount: 1})
	c.Assert(err, qt.IsNil)
	c.Assert(page.Rows[0].Values[0].GetStringValue(), qt.Equals, "PL")
	c.Assert(page.NextPageToken, qt.Equals, "")

	_, err = client.CloseStatement(c.Context(), &protocol.CloseStatementRequest{SessionId: "session", PageToken: "from-2"})
	c.Assert(err, qt.IsNil)
	c.Assert(owner.released, qt.DeepEquals, []string{"from-2"})

	write, err := client.RunStatement(c.Context(), &protocol.RunStatementRequest{SessionId: "session", RequestId: "write", Sql: "UPDATE rates SET rate = ?",
		Arguments: []*protocol.DataValue{statementText("1.5")}, Access: protocol.RunStatementRequest_READ_WRITE, MaxRows: 1, TransactionId: "transaction"})
	c.Assert(err, qt.IsNil)
	c.Assert(write.GetRowsAffected(), qt.Equals, uint64(1))
	c.Assert(peer.calls, qt.Equals, 4)

	failed, err := client.RunStatement(c.Context(), &protocol.RunStatementRequest{SessionId: "session", RequestId: "fail", Sql: "FAIL",
		Access: protocol.RunStatementRequest_READ_WRITE, MaxRows: 1})
	c.Assert(err, qt.IsNil)
	c.Assert(failed.Error, qt.Equals, "duplicate key value (code)=(CZ)")
	c.Assert(provider.CheckStatement(&protocol.RunStatementRequest{SessionId: "session", RequestId: "fail", Sql: "FAIL",
		Access: protocol.RunStatementRequest_READ_WRITE, MaxRows: 1}, failed), qt.ErrorIs, provider.ErrExecution)
	refused, err := client.RunStatement(c.Context(), &protocol.RunStatementRequest{SessionId: "session", RequestId: "check", Sql: "DELETE FROM rates",
		Access: protocol.RunStatementRequest_READ_ONLY, MaxRows: 1})
	c.Assert(err, qt.IsNil)
	c.Assert(refused.Outcome, qt.Equals, protocol.StatementResult_REFUSED)
}

// A provider that does not serve statements is never called, an invalid
// request is refused before dispatch on both sides, an invalid reply exposes
// nothing, and a write whose reply cannot be read is left unknown, which a
// read-only statement never is.
func TestStatementsConformance_FailurePath(t *testing.T) {
	c := qt.New(t)
	client, peer := newStatementLoopback(&statementOwner{})
	client.description = &protocol.Description{Features: []string{provider.ExecuteFeature, provider.TransactionsFeature}}
	_, err := client.RunStatement(c.Context(), selectStatement(protocol.RunStatementRequest_READ_ONLY))
	c.Assert(err, qt.ErrorMatches, `.*sessions\.statements was not negotiated`)
	c.Assert(peer.calls, qt.Equals, 0)

	client, peer = newStatementLoopback(&statementOwner{})
	invalid := selectStatement(protocol.RunStatementRequest_READ_ONLY)
	invalid.TransactionId = "transaction"
	_, err = client.RunStatement(c.Context(), invalid)
	c.Assert(err, qt.ErrorMatches, `.*a read-only statement runs in a transaction of its own`)
	c.Assert(peer.calls, qt.Equals, 0)
	_, err = peer.server.RunStatement(c.Context(), invalid)
	c.Assert(status.Code(err), qt.Equals, codes.InvalidArgument)

	peer.canned = &protocol.StatementResult{RequestId: "request", Outcome: protocol.StatementResult_FAILED, Error: "no",
		Rows: []*protocol.Row{{Values: []*protocol.DataValue{statementText("CZ")}}}}
	result, err := client.RunStatement(c.Context(), selectStatement(protocol.RunStatementRequest_READ_WRITE))
	c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	c.Assert(err, qt.ErrorIs, provider.ErrOutcomeUnknown)
	c.Assert(result, qt.IsNil)
	_, err = client.RunStatement(c.Context(), selectStatement(protocol.RunStatementRequest_READ_ONLY))
	c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	c.Assert(err, qt.Not(qt.ErrorIs), provider.ErrOutcomeUnknown)

	peer.canned = &protocol.StatementRows{Rows: []*protocol.Row{{Values: []*protocol.DataValue{statementText("CZ"), statementText("SK")}}}}
	_, err = client.ReadStatementRows(c.Context(), &protocol.ReadStatementRowsRequest{SessionId: "session", PageToken: "from-2", MaxRows: 2, ColumnCount: 1})
	c.Assert(err, qt.ErrorMatches, `.*do not align with its columns`)

	canceled, cancel := context.WithCancel(c.Context())
	cancel()
	peer.canned, peer.calls = nil, 0
	_, err = client.RunStatement(canceled, selectStatement(protocol.RunStatementRequest_READ_ONLY))
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(peer.calls, qt.Equals, 0)

	client.description = &protocol.Description{Features: []string{provider.StatementsFeature}}
	write := selectStatement(protocol.RunStatementRequest_READ_WRITE)
	write.TransactionId = "transaction"
	_, err = client.RunStatement(c.Context(), write)
	c.Assert(err, qt.ErrorMatches, `.*transaction service was not negotiated`)
	c.Assert(peer.calls, qt.Equals, 0)
}

// The SDK server refuses an owner's reply that breaks the contract, and the
// service is advertised exactly when it is implemented.
func TestStatementsServer_FailurePath(t *testing.T) {
	c := qt.New(t)
	server := negotiatedServer(Server{Description: statementsDescription(), Sessions: &sessionOwner{}, Executor: executorOwner{},
		Transactions: &idleTransactions{}, Statements: brokenStatementOwner{}})
	_, err := server.RunStatement(c.Context(), selectStatement(protocol.RunStatementRequest_READ_ONLY))
	c.Assert(status.Code(err), qt.Equals, codes.Internal)
	_, err = server.ReadStatementRows(c.Context(), &protocol.ReadStatementRowsRequest{SessionId: "session", PageToken: "from-2", MaxRows: 1, ColumnCount: 1})
	c.Assert(status.Code(err), qt.Equals, codes.Internal)

	without := statementServer(&statementOwner{})
	without.Statements = nil
	c.Assert(without.Validate(), qt.ErrorMatches, `.*sessions\.statements advertisement differs from implementation`)
	_, err = negotiatedServer(without).RunStatement(c.Context(), selectStatement(protocol.RunStatementRequest_READ_ONLY))
	c.Assert(err, qt.ErrorMatches, `.*statements is unavailable`)
	unlisted := statementServer(&statementOwner{})
	unlisted.Description.Features = unlisted.Description.Features[1:]
	unlisted.Description.Engines[0].Features = unlisted.Description.Engines[0].Features[1:]
	c.Assert(unlisted.Validate(), qt.ErrorMatches, `.*sessions\.statements advertisement differs from implementation`)
}

// brokenStatementOwner answers with another request's ID and a page of more
// rows than asked for.
type brokenStatementOwner struct{}

func (brokenStatementOwner) RunStatement(context.Context, *protocol.RunStatementRequest) (*protocol.StatementResult, error) {
	return &protocol.StatementResult{RequestId: "other", Outcome: protocol.StatementResult_UNKNOWN}, nil
}

func (brokenStatementOwner) ReadStatementRows(context.Context, *protocol.ReadStatementRowsRequest) (*protocol.StatementRows, error) {
	row := &protocol.Row{Values: []*protocol.DataValue{statementText("CZ")}}
	return &protocol.StatementRows{Rows: []*protocol.Row{row, proto.CloneOf(row)}}, nil
}

func (brokenStatementOwner) CloseStatement(context.Context, *protocol.CloseStatementRequest) (*protocol.Empty, error) {
	return &protocol.Empty{}, nil
}
