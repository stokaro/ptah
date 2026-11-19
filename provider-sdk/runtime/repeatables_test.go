package runtime_test

import (
	"context"
	"fmt"
	"math"
	"slices"
	"strings"
	"sync"
	"testing"

	qt "github.com/frankban/quicktest"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	protocol "ptah.run/provider-sdk/protocol/experimental"
	"ptah.run/provider-sdk/provider"
	transport "ptah.run/provider-sdk/runtime"
)

// recordFeatures are what a provider that keeps the record lists, on itself
// and on its engine.
var recordFeatures = []string{provider.HistoryFeature, provider.RepeatableRecordFeature}

// recordDescription declares the history and the record on the provider and
// its engine.
func recordDescription() *protocol.Description {
	return &protocol.Description{Revision: provider.Revision, ProviderId: "example.org/test", Version: "1", SemanticIdentity: "schema-1",
		Features: slices.Clone(recordFeatures),
		Engines: []*protocol.Engine{{Id: "test", Features: slices.Clone(recordFeatures), Identifiers: &protocol.IdentifierRules{TableNames: protocol.IdentifierRules_EXACT,
			ColumnNames: protocol.IdentifierRules_EXACT, IndexNames: protocol.IdentifierRules_EXACT, IndexNamespace: protocol.IdentifierRules_TABLE}}}}
}

// recordKeeper keeps one record in memory: the claims and writes the
// migrator sends, decided by the run each row names.
type recordKeeper struct {
	mu   sync.Mutex
	rows map[string]*protocol.RepeatableRow
	body map[string][]byte
	// duplicate makes a read answer every row twice, which no valid
	// snapshot does.
	duplicate bool
}

func newRecordKeeper() *recordKeeper {
	return &recordKeeper{rows: make(map[string]*protocol.RepeatableRow), body: make(map[string][]byte)}
}

func (*recordKeeper) Open(_ context.Context, request *protocol.OpenRequest) (*protocol.Session, error) {
	return &protocol.Session{Id: "session", Engine: request.GetEngine(), Identifiers: recordDescription().GetEngines()[0].GetIdentifiers()}, nil
}
func (*recordKeeper) Close(context.Context, *protocol.CloseRequest) error { return nil }

func (*recordKeeper) ReadHistory(_ context.Context, request *protocol.ReadHistoryRequest) (*protocol.HistorySnapshot, error) {
	return &protocol.HistorySnapshot{Scope: request.GetScope(), Complete: true}, nil
}
func (*recordKeeper) PrepareHistory(_ context.Context, request *protocol.PrepareHistoryRequest) (*protocol.HistoryResult, error) {
	return &protocol.HistoryResult{Scope: request.GetScope(), RequestId: request.GetRequestId(), Outcome: protocol.HistoryResult_APPLIED}, nil
}
func (*recordKeeper) MutateHistory(_ context.Context, request *protocol.MutateHistoryRequest) (*protocol.HistoryResult, error) {
	return &protocol.HistoryResult{Scope: request.GetScope(), RequestId: request.GetRequestId(), Outcome: protocol.HistoryResult_APPLIED}, nil
}

func (k *recordKeeper) ReadRepeatables(_ context.Context, request *protocol.ReadRepeatablesRequest) (*protocol.RepeatableSnapshot, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	snapshot := &protocol.RepeatableSnapshot{Scope: request.GetScope(), Exists: true, Complete: true}
	for _, row := range k.rows {
		snapshot.Rows = append(snapshot.Rows, proto.CloneOf(row))
		if k.duplicate {
			snapshot.Rows = append(snapshot.Rows, proto.CloneOf(row))
		}
	}
	return snapshot, nil
}

func (*recordKeeper) PrepareRepeatables(_ context.Context, request *protocol.PrepareRepeatablesRequest) (*protocol.RepeatableResult, error) {
	return &protocol.RepeatableResult{Scope: request.GetScope(), RequestId: request.GetRequestId(), Outcome: protocol.RepeatableResult_APPLIED}, nil
}

func (k *recordKeeper) ReadRepeatableBody(_ context.Context, request *protocol.ReadRepeatableBodyRequest) (*protocol.RepeatableBody, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	row, found := k.rows[request.GetIdentity()]
	return &protocol.RepeatableBody{Scope: request.GetScope(), Identity: request.GetIdentity(), Found: found, Kept: found && row.GetBodyKept(),
		Body: slices.Clone(k.body[request.GetIdentity()])}, nil
}

func (k *recordKeeper) MutateRepeatable(_ context.Context, request *protocol.MutateRepeatableRequest) (*protocol.RepeatableResult, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	result := &protocol.RepeatableResult{Scope: request.GetScope(), RequestId: request.GetRequestId(), Outcome: protocol.RepeatableResult_APPLIED}
	conflict := func(identity string) *protocol.RepeatableResult {
		result.Outcome, result.HolderRunId = protocol.RepeatableResult_CONFLICT, k.rows[identity].GetRunId()
		return result
	}
	switch mutation := request.GetMutation().(type) {
	case *protocol.MutateRepeatableRequest_Claim:
		claim := mutation.Claim
		row, recorded := k.rows[claim.GetIdentity()]
		if recorded == claim.GetInsert() || recorded && row.GetRunId() != claim.GetExpectedRunId() {
			return conflict(claim.GetIdentity()), nil
		}
		if !recorded {
			row = &protocol.RepeatableRow{Identity: claim.GetIdentity()}
			k.rows[claim.GetIdentity()] = row
		}
		row.State, row.AttemptChecksum, row.AttemptOrder, row.RunId = protocol.RepeatableRow_RUNNING, claim.GetAttemptChecksum(), claim.GetAttemptOrder(), claim.GetRunId()
		row.Transactional, row.AttemptedAtSeconds, row.AttemptedAtNanos, row.Error = claim.GetTransactional(), claim.GetAttemptedAtSeconds(), claim.GetAttemptedAtNanos(), ""
	case *protocol.MutateRepeatableRequest_Complete:
		completion := mutation.Complete
		row := k.rows[completion.GetIdentity()]
		if row.GetRunId() != completion.GetRunId() {
			return conflict(completion.GetIdentity()), nil
		}
		if row.GetAttemptChecksum() != completion.GetChecksum() {
			result.Outcome = protocol.RepeatableResult_REFUSED
			return result, nil
		}
		row.State, row.Checksum, row.Applied, row.AppliedAtSeconds, row.BodyKept = protocol.RepeatableRow_APPLIED, completion.GetChecksum(), true, completion.GetAppliedAtSeconds(), true
		k.body[completion.GetIdentity()] = slices.Clone(completion.GetBody())
	case *protocol.MutateRepeatableRequest_Fail:
		row := k.rows[mutation.Fail.GetIdentity()]
		if row.GetRunId() != mutation.Fail.GetRunId() {
			return conflict(mutation.Fail.GetIdentity()), nil
		}
		row.State, row.Error = protocol.RepeatableRow_FAILED, mutation.Fail.GetError()
	case *protocol.MutateRepeatableRequest_Remove:
		identity := mutation.Remove.GetIdentity()
		_, result.Removed = k.rows[identity]
		delete(k.rows, identity)
		delete(k.body, identity)
	}
	return result, nil
}

// serveRecord serves keeper, negotiated with a host that lists hostFeatures.
func serveRecord(c *qt.C, keeper *recordKeeper, hostFeatures []string) protocol.ProviderClient {
	c.Helper()
	client, stop, err := transport.ServeInMemory(transport.Server{Description: recordDescription(), Sessions: keeper, History: keeper, Repeatables: keeper})
	c.Assert(err, qt.IsNil)
	c.Cleanup(stop)
	_, err = client.Describe(c.Context(), &protocol.DescribeRequest{Revision: provider.Revision, HostContract: provider.Contract(), HostFeatures: hostFeatures})
	c.Assert(err, qt.IsNil)
	return client
}

func recordScope() *protocol.RepeatableScope {
	return &protocol.RepeatableScope{SessionId: "session", Table: "schema_migrations_repeatable"}
}

func claimRequest(id string, insert bool, expected, run string) *protocol.MutateRepeatableRequest {
	return &protocol.MutateRepeatableRequest{Scope: recordScope(), RequestId: id, Mutation: &protocol.MutateRepeatableRequest_Claim{
		Claim: &protocol.RepeatableClaim{Identity: "views", Insert: insert, ExpectedRunId: expected, RunId: run, AttemptChecksum: "h1:new",
			AttemptOrder: 1, AttemptedAtSeconds: 1_700_000_000}}}
}

// Through a host that lists the feature, a run prepares the record, claims a
// migration, records its content, and reads the row and the body back. A
// second run that read the record before the first claimed it gets a
// conflict naming the first, and a removal deletes the row.
func TestRepeatableRecordThroughTheServer_HappyPath(t *testing.T) {
	c := qt.New(t)
	keeper := newRecordKeeper()
	client := serveRecord(c, keeper, []string{provider.RepeatableRecordFeature})

	prepared, err := client.PrepareRepeatables(c.Context(), &protocol.PrepareRepeatablesRequest{Scope: recordScope(), RequestId: "prepare"})
	c.Assert(err, qt.IsNil)
	c.Assert(prepared.GetOutcome(), qt.Equals, protocol.RepeatableResult_APPLIED)
	claimed, err := client.MutateRepeatable(c.Context(), claimRequest("claim", true, "", "run-1"))
	c.Assert(err, qt.IsNil)
	c.Assert(claimed.GetOutcome(), qt.Equals, protocol.RepeatableResult_APPLIED)
	lost, err := client.MutateRepeatable(c.Context(), claimRequest("late", true, "", "run-2"))
	c.Assert(err, qt.IsNil)
	completed, err := client.MutateRepeatable(c.Context(), &protocol.MutateRepeatableRequest{Scope: recordScope(), RequestId: "complete",
		Mutation: &protocol.MutateRepeatableRequest_Complete{Complete: &protocol.RepeatableCompletion{Identity: "views", RunId: "run-1", Checksum: "h1:new",
			Body: []byte("CREATE VIEW v AS SELECT 1;"), AppliedAtSeconds: 1_700_000_001}}})
	c.Assert(err, qt.IsNil)
	snapshot, err := client.ReadRepeatables(c.Context(), &protocol.ReadRepeatablesRequest{Scope: recordScope(), MaxRows: provider.MaxRepeatableRows})
	c.Assert(err, qt.IsNil)
	body, err := client.ReadRepeatableBody(c.Context(), &protocol.ReadRepeatableBodyRequest{Scope: recordScope(), Identity: "views"})
	c.Assert(err, qt.IsNil)
	removed, err := client.MutateRepeatable(c.Context(), &protocol.MutateRepeatableRequest{Scope: recordScope(), RequestId: "remove",
		Mutation: &protocol.MutateRepeatableRequest_Remove{Remove: &protocol.RepeatableRemoval{Identity: "views"}}})
	c.Assert(err, qt.IsNil)

	c.Assert(lost.GetOutcome(), qt.Equals, protocol.RepeatableResult_CONFLICT)
	c.Assert(lost.GetHolderRunId(), qt.Equals, "run-1")
	c.Assert(completed.GetOutcome(), qt.Equals, protocol.RepeatableResult_APPLIED)
	c.Assert(snapshot.GetRows(), qt.HasLen, 1)
	c.Assert(snapshot.GetRows()[0].GetState(), qt.Equals, protocol.RepeatableRow_APPLIED)
	c.Assert(snapshot.GetRows()[0].GetChecksum(), qt.Equals, "h1:new")
	c.Assert(snapshot.GetRows()[0].GetRunId(), qt.Equals, "run-1")
	c.Assert(string(body.GetBody()), qt.Equals, "CREATE VIEW v AS SELECT 1;")
	c.Assert(removed.GetRemoved(), qt.IsTrue)
}

// worstRow is a row of the record with every field at its bound, unique by
// position.
func worstRow(position int) *protocol.RepeatableRow {
	token := strings.Repeat("t", provider.MaxRepeatableTokenBytes)
	return &protocol.RepeatableRow{Identity: fmt.Sprintf("%0*d", provider.MaxRepeatableIdentityBytes, position), State: protocol.RepeatableRow_APPLIED,
		Checksum: token, AttemptChecksum: token, AttemptOrder: math.MaxUint32, Transactional: true, RunId: token,
		AttemptedAtSeconds: -62135596799, AttemptedAtNanos: 999_999_999, Applied: true, AppliedAtSeconds: -62135596799, AppliedAtNanos: 999_999_999,
		Error: strings.Repeat("e", provider.MaxRepeatableErrorBytes), BodyKept: true}
}

// A record of MaxRepeatableRows rows, each at the bound of every field, is
// read through the server in one snapshot: the bounds keep the largest
// record under what a host receives in one message.
func TestRepeatableRecordReadsTheLargestRecord(t *testing.T) {
	c := qt.New(t)
	keeper := newRecordKeeper()
	for position := range provider.MaxRepeatableRows {
		row := worstRow(position)
		keeper.rows[row.GetIdentity()] = row
	}
	client := serveRecord(c, keeper, []string{provider.RepeatableRecordFeature})

	snapshot, err := client.ReadRepeatables(c.Context(), &protocol.ReadRepeatablesRequest{Scope: recordScope(), MaxRows: provider.MaxRepeatableRows})

	c.Assert(err, qt.IsNil)
	c.Assert(snapshot.GetRows(), qt.HasLen, provider.MaxRepeatableRows)
	c.Assert(proto.Size(snapshot) < transport.LargeMessageBytes, qt.IsTrue)
}

// A completion whose run holds the row but whose digest is not the claimed
// one is REFUSED, and the row keeps the claim.
func TestRepeatableRecordRefusesACompletionOfAnotherDigest(t *testing.T) {
	c := qt.New(t)
	keeper := newRecordKeeper()
	client := serveRecord(c, keeper, []string{provider.RepeatableRecordFeature})
	_, err := client.MutateRepeatable(c.Context(), claimRequest("claim", true, "", "run-1"))
	c.Assert(err, qt.IsNil)

	refused, err := client.MutateRepeatable(c.Context(), &protocol.MutateRepeatableRequest{Scope: recordScope(), RequestId: "complete",
		Mutation: &protocol.MutateRepeatableRequest_Complete{Complete: &protocol.RepeatableCompletion{Identity: "views", RunId: "run-1", Checksum: "h1:other",
			AppliedAtSeconds: 1_700_000_001}}})
	c.Assert(err, qt.IsNil)
	snapshot, err := client.ReadRepeatables(c.Context(), &protocol.ReadRepeatablesRequest{Scope: recordScope(), MaxRows: provider.MaxRepeatableRows})
	c.Assert(err, qt.IsNil)

	c.Assert(refused.GetOutcome(), qt.Equals, protocol.RepeatableResult_REFUSED)
	c.Assert(refused.GetHolderRunId(), qt.Equals, "")
	c.Assert(provider.CheckRepeatableResult(refused), qt.ErrorIs, provider.ErrExecution)
	c.Assert(snapshot.GetRows()[0].GetState(), qt.Equals, protocol.RepeatableRow_RUNNING)
	c.Assert(snapshot.GetRows()[0].GetApplied(), qt.IsFalse)
}

// The server refuses what the provider must not see: a call from a host that
// did not list the feature, a transaction it serves no transactions for, and
// a claim that both inserts and expects a run. It also refuses to pass on a
// snapshot that repeats an identity.
func TestRepeatableRecordThroughTheServer_FailurePath(t *testing.T) {
	t.Run("a host that did not list the feature", func(t *testing.T) {
		c := qt.New(t)
		client := serveRecord(c, newRecordKeeper(), nil)
		_, err := client.ReadRepeatables(c.Context(), &protocol.ReadRepeatablesRequest{Scope: recordScope(), MaxRows: provider.MaxRepeatableRows})
		c.Assert(status.Code(err), qt.Equals, codes.Unimplemented)
	})
	t.Run("a transaction the provider does not serve", func(t *testing.T) {
		c := qt.New(t)
		client := serveRecord(c, newRecordKeeper(), []string{provider.RepeatableRecordFeature})
		scope := recordScope()
		scope.TransactionId = "tx-1"
		_, err := client.MutateRepeatable(c.Context(), &protocol.MutateRepeatableRequest{Scope: scope, RequestId: "remove",
			Mutation: &protocol.MutateRepeatableRequest_Remove{Remove: &protocol.RepeatableRemoval{Identity: "views"}}})
		c.Assert(status.Code(err), qt.Equals, codes.Unimplemented)
	})
	t.Run("an invalid write", func(t *testing.T) {
		c := qt.New(t)
		client := serveRecord(c, newRecordKeeper(), []string{provider.RepeatableRecordFeature})
		_, err := client.MutateRepeatable(c.Context(), claimRequest("claim", true, "run-0", "run-1"))
		c.Assert(status.Code(err), qt.Equals, codes.InvalidArgument)
	})
	t.Run("a snapshot that repeats an identity", func(t *testing.T) {
		c := qt.New(t)
		keeper := newRecordKeeper()
		client := serveRecord(c, keeper, []string{provider.RepeatableRecordFeature})
		_, err := client.MutateRepeatable(c.Context(), claimRequest("claim", true, "", "run-1"))
		c.Assert(err, qt.IsNil)
		keeper.mu.Lock()
		keeper.duplicate = true
		keeper.mu.Unlock()
		snapshot, err := client.ReadRepeatables(c.Context(), &protocol.ReadRepeatablesRequest{Scope: recordScope(), MaxRows: provider.MaxRepeatableRows})
		c.Assert(status.Code(err), qt.Equals, codes.Internal)
		c.Assert(snapshot, qt.IsNil)
	})
}
