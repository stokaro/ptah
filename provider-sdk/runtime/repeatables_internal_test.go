package runtime

// White-box testing required: the client's own refusals and its reading of a
// receipt are observable only where the network would be. Through the
// in-memory transport the server refuses the same requests and answers with
// valid receipts, so the peer has to replace the transport for the test to
// see whether the client sent anything and how it read what came back.

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	protocol "ptah.run/provider-sdk/protocol/experimental"
	"ptah.run/provider-sdk/provider"
)

// recordPeer answers every write with reply and records each request that
// reached it.
type recordPeer struct {
	protocol.ProviderClient
	reply    func(*protocol.MutateRepeatableRequest) (*protocol.RepeatableResult, error)
	mutated  int
	appended []*protocol.MigrationLogRecord
}

func (p *recordPeer) MutateRepeatable(_ context.Context, request *protocol.MutateRepeatableRequest, _ ...grpc.CallOption) (*protocol.RepeatableResult, error) {
	p.mutated++
	return p.reply(request)
}

func (p *recordPeer) MutateMetadata(_ context.Context, request *protocol.MutateMetadataRequest, _ ...grpc.CallOption) (*protocol.MetadataResult, error) {
	p.appended = append(p.appended, request.GetAppendLog())
	return &protocol.MetadataResult{Scope: request.GetScope(), RequestId: request.GetRequestId(), Outcome: protocol.MetadataResult_APPLIED}, nil
}

func recordClaim() *protocol.MutateRepeatableRequest {
	return &protocol.MutateRepeatableRequest{Scope: &protocol.RepeatableScope{SessionId: "session", Table: "schema_migrations_repeatable"}, RequestId: "claim",
		Mutation: &protocol.MutateRepeatableRequest_Claim{Claim: &protocol.RepeatableClaim{Identity: "views", ExpectedRunId: "run-1", RunId: "run-2",
			AttemptChecksum: "h1:new", AttemptOrder: 1, AttemptedAtSeconds: 1_700_000_000}}}
}

func answering(outcome protocol.RepeatableResult_Outcome, holder string) func(*protocol.MutateRepeatableRequest) (*protocol.RepeatableResult, error) {
	return func(request *protocol.MutateRepeatableRequest) (*protocol.RepeatableResult, error) {
		return &protocol.RepeatableResult{Scope: request.GetScope(), RequestId: request.GetRequestId(), Outcome: outcome, HolderRunId: holder}, nil
	}
}

func repeatableLogEntry() *protocol.MutateMetadataRequest {
	return &protocol.MutateMetadataRequest{Scope: &protocol.MetadataScope{SessionId: "session", Table: "schema_migrations_log", Kind: protocol.MetadataScope_LOG},
		RequestId: "append", Mutation: &protocol.MutateMetadataRequest_AppendLog{AppendLog: &protocol.MigrationLogRecord{RunId: "run-2", Sequence: 1,
			Operation: "up", State: "started", ActorSource: "unknown", AtSeconds: 1_700_000_000, Repeatable: "views", Checksum: "h1:new"}}}
}

// A negotiated claim reaches the provider and an applied receipt is accepted,
// and a log entry about a repeatable migration reaches the metadata service
// with its identity.
func TestClientWritesTheRepeatableRecord_HappyPath(t *testing.T) {
	c := qt.New(t)
	peer := &recordPeer{reply: answering(protocol.RepeatableResult_APPLIED, "")}
	features := []string{provider.HistoryFeature, provider.MetadataFeature, provider.RepeatableRecordFeature}
	client := &Client{description: trustSessionDescription(features...), rpc: peer}

	result, err := client.MutateRepeatable(c.Context(), recordClaim())
	c.Assert(err, qt.IsNil)
	_, err = client.MutateMetadata(c.Context(), repeatableLogEntry())
	c.Assert(err, qt.IsNil)

	c.Assert(result.GetOutcome(), qt.Equals, protocol.RepeatableResult_APPLIED)
	c.Assert(peer.mutated, qt.Equals, 1)
	c.Assert(peer.appended, qt.HasLen, 1)
	c.Assert(peer.appended[0].GetRepeatable(), qt.Equals, "views")
}

// The client sends nothing a provider did not negotiate: a write to the
// record, and a log entry naming a repeatable migration, which a provider
// without the field would store as one about version 0.
func TestClientWritesTheRepeatableRecord_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name        string
		description *protocol.Description
	}{
		{"an older provider", trustSessionDescription(provider.HistoryFeature, provider.MetadataFeature)},
		{"a host that does not list the feature", provider.NegotiatedDescription(
			trustSessionDescription(provider.HistoryFeature, provider.MetadataFeature, provider.RepeatableRecordFeature), nil)},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			peer := &recordPeer{reply: answering(protocol.RepeatableResult_APPLIED, "")}
			client := &Client{description: test.description, rpc: peer}

			result, err := client.MutateRepeatable(c.Context(), recordClaim())
			c.Assert(err, qt.ErrorMatches, `.*the repeatable migration record was not negotiated`)
			c.Assert(result, qt.IsNil)
			appended, err := client.MutateMetadata(c.Context(), repeatableLogEntry())
			c.Assert(err, qt.ErrorMatches, `.*a migration log entry about a repeatable migration requires sessions\.repeatable-record`)
			c.Assert(appended, qt.IsNil)
			c.Assert(peer.mutated, qt.Equals, 0)
			c.Assert(peer.appended, qt.HasLen, 0)
		})
	}
}

// A write that did not apply comes back as a RepeatableError carrying its
// outcome: a conflict names the run that holds the row and wraps
// ErrExecution; a receipt for another request, an UNKNOWN outcome and a lost
// call wrap ErrOutcomeUnknown, so nothing replays the write.
func TestClientReadsTheRepeatableReceipt_FailurePath(t *testing.T) {
	features := []string{provider.HistoryFeature, provider.RepeatableRecordFeature}
	t.Run("a conflict", func(t *testing.T) {
		c := qt.New(t)
		client := &Client{description: trustSessionDescription(features...), rpc: &recordPeer{reply: answering(protocol.RepeatableResult_CONFLICT, "run-3")}}
		_, err := client.MutateRepeatable(c.Context(), recordClaim())
		var failure *provider.RepeatableError
		c.Assert(err, qt.ErrorAs, &failure)
		c.Assert(err, qt.ErrorIs, provider.ErrExecution)
		c.Assert(failure.Holder, qt.Equals, "run-3")
	})
	t.Run("an unknown outcome", func(t *testing.T) {
		c := qt.New(t)
		client := &Client{description: trustSessionDescription(features...), rpc: &recordPeer{reply: answering(protocol.RepeatableResult_UNKNOWN, "")}}
		_, err := client.MutateRepeatable(c.Context(), recordClaim())
		c.Assert(err, qt.ErrorIs, provider.ErrOutcomeUnknown)
	})
	t.Run("a receipt for another request", func(t *testing.T) {
		c := qt.New(t)
		reply := func(request *protocol.MutateRepeatableRequest) (*protocol.RepeatableResult, error) {
			return &protocol.RepeatableResult{Scope: request.GetScope(), RequestId: "other", Outcome: protocol.RepeatableResult_APPLIED}, nil
		}
		client := &Client{description: trustSessionDescription(features...), rpc: &recordPeer{reply: reply}}
		_, err := client.MutateRepeatable(c.Context(), recordClaim())
		c.Assert(err, qt.ErrorIs, provider.ErrOutcomeUnknown)
		c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	})
	t.Run("a lost call", func(t *testing.T) {
		c := qt.New(t)
		reply := func(*protocol.MutateRepeatableRequest) (*protocol.RepeatableResult, error) {
			return nil, status.Error(codes.Unavailable, "connection reset")
		}
		client := &Client{description: trustSessionDescription(features...), rpc: &recordPeer{reply: reply}}
		_, err := client.MutateRepeatable(c.Context(), recordClaim())
		c.Assert(err, qt.ErrorIs, provider.ErrOutcomeUnknown)
	})
}
