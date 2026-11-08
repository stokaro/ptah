package runtime

// White-box testing required: the negotiated state of the server adapter is
// set by Describe and read by every later call, and a reply from a peer that
// is not the SDK server must reach the client's own checks.

import (
	"context"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	protocol "ptah.run/provider-sdk/protocol/experimental"
	"ptah.run/provider-sdk/provider"
)

// splitFeatures are the provider features a migration split needs, and the
// feature itself.
func splitFeatures() []string {
	return []string{provider.ExecuteFeature, provider.TransactionsFeature, provider.HistoryFeature, provider.MigrationSplitFeature}
}

// splitDescription offers migration splits on one engine, beside the
// transactions and history the host runs the units with.
func splitDescription() *protocol.Description {
	return &protocol.Description{
		Revision: provider.Revision, ProviderId: "example.org/split", Version: "1", SemanticIdentity: "split-1", Features: splitFeatures(),
		Engines: []*protocol.Engine{{
			Id: "split", Features: splitFeatures()[1:],
			Identifiers: &protocol.IdentifierRules{TableNames: protocol.IdentifierRules_EXACT, ColumnNames: protocol.IdentifierRules_EXACT,
				IndexNames: protocol.IdentifierRules_EXACT, IndexNamespace: protocol.IdentifierRules_TABLE},
		}},
	}
}

// unsplitDescription is splitDescription without the feature.
func unsplitDescription() *protocol.Description {
	d := splitDescription()
	unsplit := func(f string) bool { return f == provider.MigrationSplitFeature }
	d.Features = slices.DeleteFunc(d.Features, unsplit)
	d.Engines[0].Features = slices.DeleteFunc(d.Engines[0].Features, unsplit)
	return d
}

// splitOwner runs each body as one data unit and finds every assertion
// read-only, or answers with its canned reply. It counts its calls.
type splitOwner struct {
	calls int
	reply *protocol.MigrationSplit
}

func (o *splitOwner) SplitMigration(_ context.Context, request *protocol.SplitMigrationRequest) (*protocol.MigrationSplit, error) {
	o.calls++
	if o.reply != nil {
		return o.reply, nil
	}
	result := &protocol.MigrationSplit{Units: []*protocol.MigrationUnit{{Sql: request.Body, Source: request.Body, Placement: protocol.MigrationUnit_WITH_CHECKPOINT}}}
	for range request.Assertions {
		result.Assertions = append(result.Assertions, &protocol.AssertionVerdict{ReadOnly: true})
	}
	return result, nil
}

// splitLoopback forwards SplitMigration to a server adapter, or answers with a
// canned reply when one is set, as a peer that is not the SDK server could.
type splitLoopback struct {
	protocol.ProviderClient
	server *serverAdapter
	calls  int
	canned *protocol.MigrationSplit
}

func (l *splitLoopback) SplitMigration(ctx context.Context, request *protocol.SplitMigrationRequest, _ ...grpc.CallOption) (*protocol.MigrationSplit, error) {
	l.calls++
	if l.canned != nil {
		return l.canned, nil
	}
	return l.server.SplitMigration(ctx, request)
}

// newSplitLoopback is a client and a server after a host listed hostFeatures
// to a provider that offers description.
func newSplitLoopback(description *protocol.Description, owner *splitOwner, hostFeatures ...string) (*Client, *splitLoopback) {
	server := negotiatedServer(Server{Description: description, MigrationSplits: owner}, hostFeatures...)
	peer := &splitLoopback{server: server}
	return &Client{description: server.negotiated(), rpc: peer}, peer
}

func migrationSplitRequest() *protocol.SplitMigrationRequest {
	return &protocol.SplitMigrationRequest{Engine: "split", Body: "INSERT INTO t VALUES (1);", Assertions: []string{"SELECT 1"}}
}

func TestSplitMigrationConformance_HappyPath(t *testing.T) {
	c := qt.New(t)
	owner := &splitOwner{}
	client, peer := newSplitLoopback(splitDescription(), owner, provider.MigrationSplitFeature)
	request := migrationSplitRequest()
	before := proto.CloneOf(request)
	split, err := client.SplitMigration(c.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(split.Units, qt.HasLen, 1)
	c.Assert(split.Units[0].Placement, qt.Equals, protocol.MigrationUnit_WITH_CHECKPOINT)
	c.Assert(split.Assertions, qt.HasLen, 1)
	c.Assert(split.Assertions[0].ReadOnly, qt.IsTrue)
	c.Assert(peer.calls, qt.Equals, 1)
	c.Assert(owner.calls, qt.Equals, 1)
	c.Assert(proto.Equal(before, request), qt.IsTrue)
}

// The call is used only when both sides list the feature: the client sends
// nothing to a provider that does not offer it, and the server refuses a host
// that did not list it.
func TestSplitMigrationNeedsBothSides(t *testing.T) {
	for _, test := range []struct {
		name        string
		description *protocol.Description
		listed      []string
	}{
		{"a provider that does not list it", unsplitDescription(), []string{provider.MigrationSplitFeature}},
		{"a host that does not list it", splitDescription(), nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			owner := &splitOwner{}
			client, peer := newSplitLoopback(test.description, owner, test.listed...)
			split, err := client.SplitMigration(c.Context(), migrationSplitRequest())
			c.Assert(err, qt.ErrorMatches, `.*migration split: the feature was not negotiated`)
			c.Assert(split, qt.IsNil)
			c.Assert(peer.calls, qt.Equals, 0)
			split, err = peer.server.SplitMigration(c.Context(), migrationSplitRequest())
			c.Assert(status.Code(err), qt.Equals, codes.InvalidArgument)
			c.Assert(split, qt.IsNil)
			c.Assert(owner.calls, qt.Equals, 0)
		})
	}
	c := qt.New(t)
	// Before Describe, the host is taken to list nothing.
	owner := &splitOwner{}
	undescribed := newServerAdapter(Server{Description: splitDescription(), MigrationSplits: owner})
	_, err := undescribed.SplitMigration(c.Context(), migrationSplitRequest())
	c.Assert(status.Code(err), qt.Equals, codes.InvalidArgument)
	c.Assert(owner.calls, qt.Equals, 0)
}

// Both ends refuse a request without a body before the owner sees it, and an
// invalid reply reaches no caller: the server refuses its owner's, and the
// client refuses one from a peer that is not the SDK server.
func TestSplitMigrationConformance_FailurePath(t *testing.T) {
	c := qt.New(t)
	owner := &splitOwner{}
	client, peer := newSplitLoopback(splitDescription(), owner, provider.MigrationSplitFeature)
	empty := migrationSplitRequest()
	empty.Body = ""
	_, err := client.SplitMigration(c.Context(), empty)
	c.Assert(err, qt.ErrorMatches, `.*migration split: the request carries no body`)
	c.Assert(peer.calls, qt.Equals, 0)
	_, err = peer.server.SplitMigration(c.Context(), empty)
	c.Assert(status.Code(err), qt.Equals, codes.InvalidArgument)
	c.Assert(owner.calls, qt.Equals, 0)

	owner.reply = &protocol.MigrationSplit{Units: []*protocol.MigrationUnit{{Sql: "INSERT INTO t VALUES (1);", Source: "INSERT INTO t VALUES (1);"}},
		Assertions: []*protocol.AssertionVerdict{{ReadOnly: true}}}
	split, err := peer.server.SplitMigration(c.Context(), migrationSplitRequest())
	c.Assert(status.Code(err), qt.Equals, codes.Internal)
	c.Assert(split, qt.IsNil)
	split, err = client.SplitMigration(c.Context(), migrationSplitRequest())
	c.Assert(err, qt.ErrorMatches, `provider split migration failed \(Internal\)`)
	c.Assert(split, qt.IsNil)

	peer.canned = owner.reply
	split, err = client.SplitMigration(c.Context(), migrationSplitRequest())
	c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	c.Assert(err, qt.ErrorMatches, `.*migration split: a unit needs a known placement`)
	c.Assert(split, qt.IsNil)

	canceled, cancel := context.WithCancel(c.Context())
	cancel()
	peer.calls = 0
	_, err = client.SplitMigration(canceled, migrationSplitRequest())
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(peer.calls, qt.Equals, 0)
}

// splitServer serves every service splitDescription advertises.
func splitServer() Server {
	return Server{Description: splitDescription(), Sessions: &sessionOwner{}, Executor: executorOwner{}, Transactions: &idleTransactions{},
		History: &historyOwner{}, MigrationSplits: &splitOwner{}}
}

// The service and its feature are advertised together, and a server without
// the service answers the call as unavailable.
func TestMigrationSplitServiceMatchesItsAdvertisement(t *testing.T) {
	c := qt.New(t)
	c.Assert(splitServer().Validate(), qt.IsNil)
	without := splitServer()
	without.MigrationSplits = nil
	c.Assert(without.Validate(), qt.ErrorMatches, `.*units\.migration-split advertisement differs from implementation`)
	unadvertised := splitServer()
	unadvertised.Description = unsplitDescription()
	c.Assert(unadvertised.Validate(), qt.ErrorMatches, `.*units\.migration-split advertisement differs from implementation`)
	_, err := negotiatedServer(without, provider.MigrationSplitFeature).SplitMigration(c.Context(), migrationSplitRequest())
	c.Assert(status.Code(err), qt.Equals, codes.Unimplemented)
}

// abortingOwner executes nothing and commits nothing: the engine aborts
// every transaction for a conflict, whatever it was asked.
type abortingOwner struct{ idleTransactions }

func (abortingOwner) Execute(context.Context, *protocol.ExecuteRequest) (*protocol.Execution, error) {
	return &protocol.Execution{Outcome: protocol.Execution_NOT_APPLIED, TransactionAborted: true}, nil
}

func (abortingOwner) FinishTransaction(context.Context, *protocol.FinishTransactionRequest) (*protocol.TransactionResult, error) {
	return &protocol.TransactionResult{Outcome: protocol.TransactionResult_ROLLED_BACK, Aborted: true}, nil
}

func unitRequest() *protocol.ExecuteRequest {
	return &protocol.ExecuteRequest{SessionId: "session", RequestId: "unit-1", TransactionId: "transaction",
		Units: []*protocol.Unit{{ChangeId: "unit-1", Body: &protocol.Unit_Sql{Sql: "INSERT INTO t VALUES (1);"}}}}
}

// abortClient is a client whose peer aborts everything, after a host listed
// hostFeatures.
func abortClient(hostFeatures ...string) *Client {
	return &Client{description: provider.NegotiatedDescription(splitDescription(), hostFeatures), rpc: &abortPeer{}}
}

// abortPeer stands for a provider that aborts every transaction, without the
// SDK server's checks.
type abortPeer struct{ protocol.ProviderClient }

func (*abortPeer) Execute(ctx context.Context, request *protocol.ExecuteRequest, _ ...grpc.CallOption) (*protocol.Execution, error) {
	return abortingOwner{}.Execute(ctx, request)
}

func (*abortPeer) FinishTransaction(ctx context.Context, request *protocol.FinishTransactionRequest, _ ...grpc.CallOption) (*protocol.TransactionResult, error) {
	return abortingOwner{}.FinishTransaction(ctx, request)
}

// Under the feature, both ends pass an abort through: the caller receives the
// reply with its flag and the error of its outcome.
func TestTransactionAbortReachesTheHost_HappyPath(t *testing.T) {
	c := qt.New(t)
	server := negotiatedServer(Server{Description: splitDescription(), Executor: &abortingOwner{}, Transactions: &abortingOwner{}}, provider.MigrationSplitFeature)
	executed, err := server.Execute(c.Context(), unitRequest())
	c.Assert(err, qt.IsNil)
	c.Assert(executed.TransactionAborted, qt.IsTrue)
	finished, err := server.FinishTransaction(c.Context(), finishRequest(protocol.FinishTransactionRequest_COMMIT))
	c.Assert(err, qt.IsNil)
	c.Assert(finished.Aborted, qt.IsTrue)

	client := abortClient(provider.MigrationSplitFeature)
	executed, err = client.Execute(c.Context(), unitRequest())
	c.Assert(err, qt.ErrorIs, provider.ErrExecution)
	c.Assert(executed.GetTransactionAborted(), qt.IsTrue)
	finished, err = client.FinishTransaction(c.Context(), finishRequest(protocol.FinishTransactionRequest_COMMIT))
	c.Assert(err, qt.ErrorIs, provider.ErrExecution)
	c.Assert(finished.GetAborted(), qt.IsTrue)
}

// Without the feature, neither end passes an abort through. The server
// refuses its owner's reply; the client reports an unknown outcome and
// returns no reply, so no caller reads the flag and retries.
func TestTransactionAbortReachesTheHost_FailurePath(t *testing.T) {
	c := qt.New(t)
	server := negotiatedServer(Server{Description: splitDescription(), Executor: &abortingOwner{}, Transactions: &abortingOwner{}})
	executed, err := server.Execute(c.Context(), unitRequest())
	c.Assert(status.Code(err), qt.Equals, codes.Internal)
	c.Assert(executed, qt.IsNil)
	finished, err := server.FinishTransaction(c.Context(), finishRequest(protocol.FinishTransactionRequest_COMMIT))
	c.Assert(status.Code(err), qt.Equals, codes.Internal)
	c.Assert(finished, qt.IsNil)

	client := abortClient()
	executed, err = client.Execute(c.Context(), unitRequest())
	c.Assert(err, qt.ErrorIs, provider.ErrOutcomeUnknown)
	c.Assert(err, qt.ErrorMatches, `(?s).*transaction abort: aborts were not negotiated.*`)
	c.Assert(executed, qt.IsNil)
	finished, err = client.FinishTransaction(c.Context(), finishRequest(protocol.FinishTransactionRequest_COMMIT))
	c.Assert(err, qt.ErrorIs, provider.ErrOutcomeUnknown)
	c.Assert(err, qt.ErrorMatches, `(?s).*transaction abort: aborts were not negotiated.*`)
	c.Assert(finished, qt.IsNil)
}
