package provider_test

import (
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"
)

func embeddingDescription() *protocol.Description {
	rules := &protocol.IdentifierRules{TableNames: protocol.IdentifierRules_EXACT, ColumnNames: protocol.IdentifierRules_EXACT,
		IndexNames: protocol.IdentifierRules_EXACT, IndexNamespace: protocol.IdentifierRules_TABLE}
	return &protocol.Description{
		Revision: provider.Revision, ProviderId: "example.org/vectors", Version: "1", SemanticIdentity: "vectors-1",
		Features: []string{provider.EmbeddingStoreFeature},
		Engines: []*protocol.Engine{{Id: "vectors", Identifiers: rules, Features: []string{provider.EmbeddingStoreFeature},
			Embeddings: &protocol.EmbeddingEngine{ConsistencyModes: []string{"immutable"}}}},
	}
}

func at(seconds int64) *protocol.TimestampValue { return &protocol.TimestampValue{Seconds: seconds} }

func generationRecord(identity string) *protocol.EmbeddingGeneration {
	return &protocol.EmbeddingGeneration{Identity: identity, SpecDigest: "spec-1", SpecDocument: "source: documents", Reproducibility: "full",
		Dimension: 3, TargetTable: "documents", TargetColumn: "embedding", SourceTable: "documents", ConsistencyMode: "immutable",
		CreatedAt: at(1700000000)}
}

func runRecord(id, generation string) *protocol.EmbeddingRun {
	return &protocol.EmbeddingRun{Id: id, SpecDigest: "spec-1", GenerationIdentity: generation, Phase: "backfilling", Status: "running",
		LeaseOwner: "worker-1", LeaseExpires: at(1700000900), FencingToken: 2, Cursor: []string{"acme", "41"},
		Progress:  &protocol.EmbeddingProgress{RowsScanned: 40, RowsEmbedded: 38, RowsSkipped: 2, BatchesCommitted: 4, RetryCount: 1},
		CreatedAt: at(1700000000), UpdatedAt: at(1700000600)}
}

func pointerRecord(schema, table string) *protocol.EmbeddingPointer {
	return &protocol.EmbeddingPointer{TargetSchema: schema, TargetTable: table, ActiveGeneration: "gen-1", CutOverAt: at(1700001000)}
}

func everything() *protocol.EmbeddingStateScope {
	return &protocol.EmbeddingStateScope{Scope: &protocol.EmbeddingStateScope_Everything{Everything: &protocol.EmbeddingStateScope_AllRecords{}}}
}

func runScope(id string) *protocol.EmbeddingStateScope {
	return &protocol.EmbeddingStateScope{Scope: &protocol.EmbeddingStateScope_Run{Run: id}}
}

func stateExchange() (*protocol.ReadEmbeddingStateRequest, *protocol.EmbeddingState) {
	return &protocol.ReadEmbeddingStateRequest{SessionId: "session", Scope: everything(), MaxRecords: 10},
		&protocol.EmbeddingState{Scope: everything(), Exists: true, Token: "token-1", Records: &protocol.EmbeddingRecords{
			Generations: []*protocol.EmbeddingGeneration{generationRecord("gen-1"), generationRecord("gen-2")},
			Runs:        []*protocol.EmbeddingRun{runRecord("run-1", "gen-1"), runRecord("run-2", "gen-2")},
			Pointers:    []*protocol.EmbeddingPointer{pointerRecord("", "documents"), pointerRecord("app", "documents")},
		}}
}

func mutateExchange() (*protocol.MutateEmbeddingStateRequest, *protocol.EmbeddingWriteResult) {
	records := &protocol.EmbeddingRecords{Generations: []*protocol.EmbeddingGeneration{generationRecord("gen-1")},
		Runs: []*protocol.EmbeddingRun{runRecord("run-1", "gen-1")}}
	written := proto.CloneOf(records)
	written.Runs[0].UpdatedAt = &protocol.TimestampValue{Seconds: 1700000600, Nanos: 123000}
	return &protocol.MutateEmbeddingStateRequest{SessionId: "session", RequestId: "mutate-1", Scope: everything(), ExpectedToken: "token-1", Records: records},
		&protocol.EmbeddingWriteResult{RequestId: "mutate-1", Outcome: protocol.EmbeddingWriteResult_APPLIED, Written: written}
}

func refusal(reason protocol.EmbeddingRefusal_Reason) *protocol.EmbeddingRefusal {
	return &protocol.EmbeddingRefusal{Reason: reason, Message: "the store layout is not this provider's"}
}

func TestEmbeddingStoreDescription_HappyPath(t *testing.T) {
	c := qt.New(t)
	c.Assert(provider.ValidateDescription(embeddingDescription(), nil), qt.IsNil)
	c.Assert(provider.ValidateDescription(embeddingDescription(), []string{provider.EmbeddingStoreFeature}), qt.IsNil)
	c.Assert(provider.TwoWayFeatures(), qt.Contains, provider.EmbeddingStoreFeature)
}

func TestEmbeddingStoreDescription_FailurePath(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*protocol.Description)
	}{
		{"an engine store the provider does not serve", func(d *protocol.Description) { d.Features = nil }},
		{"an engine that lists the store and declares none", func(d *protocol.Description) { d.Engines[0].Embeddings = nil }},
		{"an engine that declares a store it does not list", func(d *protocol.Description) { d.Engines[0].Features = nil }},
		{"a store without a consistency mode", func(d *protocol.Description) { d.Engines[0].Embeddings.ConsistencyModes = nil }},
		{"a consistency mode that is not a name", func(d *protocol.Description) { d.Engines[0].Embeddings.ConsistencyModes = []string{"Immutable"} }},
		{"a repeated consistency mode", func(d *protocol.Description) {
			d.Engines[0].Embeddings.ConsistencyModes = []string{"immutable", "immutable"}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			description := embeddingDescription()
			test.mutate(description)
			c.Assert(provider.ValidateDescription(description, nil), qt.ErrorIs, provider.ErrProtocol)
		})
	}
}

// A host that does not list the store gets neither the feature nor the
// declaration, from the provider or from its engines; a host that lists it
// gets both.
func TestNegotiatedDescriptionKeepsTheEmbeddingStoreOnlyWhenListed(t *testing.T) {
	c := qt.New(t)
	unlisted := provider.NegotiatedDescription(embeddingDescription(), nil)
	c.Assert(unlisted.Features, qt.HasLen, 0)
	c.Assert(unlisted.Engines[0].Features, qt.HasLen, 0)
	c.Assert(unlisted.Engines[0].Embeddings, qt.IsNil)
	c.Assert(provider.ValidateDescription(unlisted, nil), qt.IsNil)
	listed := provider.NegotiatedDescription(embeddingDescription(), []string{provider.EmbeddingStoreFeature})
	c.Assert(proto.Equal(listed, embeddingDescription()), qt.IsTrue)
}

func TestValidateEmbeddingState_HappyPath(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*protocol.ReadEmbeddingStateRequest, *protocol.EmbeddingState)
	}{
		{"every record", func(*protocol.ReadEmbeddingStateRequest, *protocol.EmbeddingState) {}},
		{"one run", func(q *protocol.ReadEmbeddingStateRequest, r *protocol.EmbeddingState) {
			q.Scope, r.Scope = runScope("run-1"), runScope("run-1")
			r.Records = &protocol.EmbeddingRecords{Runs: []*protocol.EmbeddingRun{runRecord("run-1", "gen-1")}}
		}},
		{"a run that is not there", func(q *protocol.ReadEmbeddingStateRequest, r *protocol.EmbeddingState) {
			q.Scope, r.Scope, r.Records = runScope("run-9"), runScope("run-9"), nil
		}},
		{"one generation", func(q *protocol.ReadEmbeddingStateRequest, r *protocol.EmbeddingState) {
			scope := &protocol.EmbeddingStateScope{Scope: &protocol.EmbeddingStateScope_Generation{Generation: "gen-2"}}
			q.Scope, r.Scope = scope, proto.CloneOf(scope)
			r.Records = &protocol.EmbeddingRecords{Generations: []*protocol.EmbeddingGeneration{generationRecord("gen-2")}}
		}},
		{"the runs of a generation", func(q *protocol.ReadEmbeddingStateRequest, r *protocol.EmbeddingState) {
			scope := &protocol.EmbeddingStateScope{Scope: &protocol.EmbeddingStateScope_RunsOfGeneration{RunsOfGeneration: "gen-1"}}
			q.Scope, r.Scope = scope, proto.CloneOf(scope)
			r.Records = &protocol.EmbeddingRecords{Runs: []*protocol.EmbeddingRun{runRecord("run-1", "gen-1"), runRecord("run-3", "gen-1")}}
		}},
		{"one pointer", func(q *protocol.ReadEmbeddingStateRequest, r *protocol.EmbeddingState) {
			key := &protocol.EmbeddingPointerKey{TargetSchema: "app", TargetTable: "documents"}
			scope := &protocol.EmbeddingStateScope{Scope: &protocol.EmbeddingStateScope_Pointer{Pointer: key}}
			q.Scope, r.Scope = scope, proto.CloneOf(scope)
			r.Records = &protocol.EmbeddingRecords{Pointers: []*protocol.EmbeddingPointer{pointerRecord("app", "documents")}}
		}},
		{"a store that does not exist", func(_ *protocol.ReadEmbeddingStateRequest, r *protocol.EmbeddingState) {
			r.Exists, r.Records = false, nil
		}},
		{"times at both ends of the range", func(_ *protocol.ReadEmbeddingStateRequest, r *protocol.EmbeddingState) {
			r.Records.Generations[0].CreatedAt = &protocol.TimestampValue{Seconds: -62135596800}
			r.Records.Generations[1].CreatedAt = &protocol.TimestampValue{Seconds: 253402300799, Nanos: 999999999}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request, result := stateExchange()
			test.mutate(request, result)
			c.Assert(provider.ValidateEmbeddingState(request, result), qt.IsNil)
		})
	}
}

func TestValidateReadEmbeddingStateRequest_FailurePath(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*protocol.ReadEmbeddingStateRequest)
	}{
		{"no session", func(r *protocol.ReadEmbeddingStateRequest) { r.SessionId = "" }},
		{"no scope", func(r *protocol.ReadEmbeddingStateRequest) { r.Scope = nil }},
		{"an empty everything", func(r *protocol.ReadEmbeddingStateRequest) {
			r.Scope = &protocol.EmbeddingStateScope{Scope: &protocol.EmbeddingStateScope_Everything{}}
		}},
		{"an unnamed run", func(r *protocol.ReadEmbeddingStateRequest) { r.Scope = runScope("") }},
		{"an unnamed generation", func(r *protocol.ReadEmbeddingStateRequest) {
			r.Scope = &protocol.EmbeddingStateScope{Scope: &protocol.EmbeddingStateScope_Generation{}}
		}},
		{"the runs of an unnamed generation", func(r *protocol.ReadEmbeddingStateRequest) {
			r.Scope = &protocol.EmbeddingStateScope{Scope: &protocol.EmbeddingStateScope_RunsOfGeneration{}}
		}},
		{"a pointer without a table", func(r *protocol.ReadEmbeddingStateRequest) {
			r.Scope = &protocol.EmbeddingStateScope{Scope: &protocol.EmbeddingStateScope_Pointer{Pointer: &protocol.EmbeddingPointerKey{TargetSchema: "app"}}}
		}},
		{"a pointer schema that is not text", func(r *protocol.ReadEmbeddingStateRequest) {
			r.Scope = &protocol.EmbeddingStateScope{Scope: &protocol.EmbeddingStateScope_Pointer{
				Pointer: &protocol.EmbeddingPointerKey{TargetSchema: "a\x00b", TargetTable: "documents"}}}
		}},
		{"no bound", func(r *protocol.ReadEmbeddingStateRequest) { r.MaxRecords = 0 }},
		{"a bound above the limit", func(r *protocol.ReadEmbeddingStateRequest) { r.MaxRecords = provider.MaxEmbeddingRecords + 1 }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request, _ := stateExchange()
			test.mutate(request)
			c.Assert(provider.ValidateReadEmbeddingStateRequest(request), qt.ErrorIs, provider.ErrProtocol)
		})
	}
}

func TestValidateEmbeddingState_FailurePath(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*protocol.ReadEmbeddingStateRequest, *protocol.EmbeddingState)
	}{
		{"an invalid request", func(q *protocol.ReadEmbeddingStateRequest, _ *protocol.EmbeddingState) { q.SessionId = "" }},
		{"another scope", func(_ *protocol.ReadEmbeddingStateRequest, r *protocol.EmbeddingState) { r.Scope = runScope("run-1") }},
		{"no token", func(_ *protocol.ReadEmbeddingStateRequest, r *protocol.EmbeddingState) { r.Token = "" }},
		{"a token above the limit", func(_ *protocol.ReadEmbeddingStateRequest, r *protocol.EmbeddingState) {
			r.Token = strings.Repeat("t", 4097)
		}},
		{"a token that is not text", func(_ *protocol.ReadEmbeddingStateRequest, r *protocol.EmbeddingState) { r.Token = "t\x00" }},
		{"more records than the bound", func(q *protocol.ReadEmbeddingStateRequest, _ *protocol.EmbeddingState) { q.MaxRecords = 5 }},
		{"records of a missing store", func(_ *protocol.ReadEmbeddingStateRequest, r *protocol.EmbeddingState) { r.Exists = false }},
		{"a missing generation", func(_ *protocol.ReadEmbeddingStateRequest, r *protocol.EmbeddingState) {
			r.Records.Generations[1] = nil
		}},
		{"a generation without an identity", func(_ *protocol.ReadEmbeddingStateRequest, r *protocol.EmbeddingState) {
			r.Records.Generations[0].Identity = ""
		}},
		{"a generation field that is not text", func(_ *protocol.ReadEmbeddingStateRequest, r *protocol.EmbeddingState) {
			r.Records.Generations[0].ConsistencyMode = "immutable\x00"
		}},
		{"a generation time with nanoseconds out of range", func(_ *protocol.ReadEmbeddingStateRequest, r *protocol.EmbeddingState) {
			r.Records.Generations[0].RetiredAt = &protocol.TimestampValue{Seconds: 1, Nanos: 1e9}
		}},
		{"a generation time with negative nanoseconds", func(_ *protocol.ReadEmbeddingStateRequest, r *protocol.EmbeddingState) {
			r.Records.Generations[0].VerifiedAt = &protocol.TimestampValue{Seconds: 1, Nanos: -1}
		}},
		{"a generation time before year 1", func(_ *protocol.ReadEmbeddingStateRequest, r *protocol.EmbeddingState) {
			r.Records.Generations[0].MaintainedUntil = &protocol.TimestampValue{Seconds: -62135596801}
		}},
		{"a generation time after year 9999", func(_ *protocol.ReadEmbeddingStateRequest, r *protocol.EmbeddingState) {
			r.Records.Generations[0].CreatedAt = &protocol.TimestampValue{Seconds: 253402300800}
		}},
		{"generations out of key order", func(_ *protocol.ReadEmbeddingStateRequest, r *protocol.EmbeddingState) {
			slices.Reverse(r.Records.Generations)
		}},
		{"a repeated generation", func(_ *protocol.ReadEmbeddingStateRequest, r *protocol.EmbeddingState) {
			r.Records.Generations[1] = generationRecord("gen-1")
		}},
		{"a missing run", func(_ *protocol.ReadEmbeddingStateRequest, r *protocol.EmbeddingState) { r.Records.Runs[0] = nil }},
		{"a run without an ID", func(_ *protocol.ReadEmbeddingStateRequest, r *protocol.EmbeddingState) { r.Records.Runs[0].Id = "" }},
		{"a run without a generation", func(_ *protocol.ReadEmbeddingStateRequest, r *protocol.EmbeddingState) {
			r.Records.Runs[0].GenerationIdentity = ""
		}},
		{"a run field that is not text", func(_ *protocol.ReadEmbeddingStateRequest, r *protocol.EmbeddingState) {
			r.Records.Runs[0].FailureDetail = "\xff"
		}},
		{"a run cursor that is not text", func(_ *protocol.ReadEmbeddingStateRequest, r *protocol.EmbeddingState) {
			r.Records.Runs[0].Cursor = []string{"acme", "4\x001"}
		}},
		{"a negative fencing token", func(_ *protocol.ReadEmbeddingStateRequest, r *protocol.EmbeddingState) {
			r.Records.Runs[0].FencingToken = -1
		}},
		{"a negative count", func(_ *protocol.ReadEmbeddingStateRequest, r *protocol.EmbeddingState) {
			r.Records.Runs[1].Progress.ProviderUsageBatches = -1
		}},
		{"a run time out of range", func(_ *protocol.ReadEmbeddingStateRequest, r *protocol.EmbeddingState) {
			r.Records.Runs[1].UpdatedAt = &protocol.TimestampValue{Nanos: 1e9}
		}},
		{"runs out of key order", func(_ *protocol.ReadEmbeddingStateRequest, r *protocol.EmbeddingState) {
			slices.Reverse(r.Records.Runs)
		}},
		{"a missing pointer", func(_ *protocol.ReadEmbeddingStateRequest, r *protocol.EmbeddingState) { r.Records.Pointers[1] = nil }},
		{"a pointer without a table", func(_ *protocol.ReadEmbeddingStateRequest, r *protocol.EmbeddingState) {
			r.Records.Pointers[0].TargetTable = ""
		}},
		{"a pointer without an active generation", func(_ *protocol.ReadEmbeddingStateRequest, r *protocol.EmbeddingState) {
			r.Records.Pointers[0].ActiveGeneration = ""
		}},
		{"a pointer field that is not text", func(_ *protocol.ReadEmbeddingStateRequest, r *protocol.EmbeddingState) {
			r.Records.Pointers[0].PlanDigest = "\x00"
		}},
		{"a pointer time out of range", func(_ *protocol.ReadEmbeddingStateRequest, r *protocol.EmbeddingState) {
			r.Records.Pointers[0].CutOverAt = &protocol.TimestampValue{Nanos: -1}
		}},
		{"pointers out of key order", func(_ *protocol.ReadEmbeddingStateRequest, r *protocol.EmbeddingState) {
			slices.Reverse(r.Records.Pointers)
		}},
		{"a repeated pointer", func(_ *protocol.ReadEmbeddingStateRequest, r *protocol.EmbeddingState) {
			r.Records.Pointers[1] = pointerRecord("", "documents")
		}},
		{"another run in a run scope", func(q *protocol.ReadEmbeddingStateRequest, r *protocol.EmbeddingState) {
			q.Scope, r.Scope = runScope("run-1"), runScope("run-1")
			r.Records = &protocol.EmbeddingRecords{Runs: []*protocol.EmbeddingRun{runRecord("run-2", "gen-2")}}
		}},
		{"two runs in a run scope", func(q *protocol.ReadEmbeddingStateRequest, r *protocol.EmbeddingState) {
			q.Scope, r.Scope = runScope("run-1"), runScope("run-1")
			r.Records = &protocol.EmbeddingRecords{Runs: []*protocol.EmbeddingRun{runRecord("run-1", "gen-1"), runRecord("run-2", "gen-1")}}
		}},
		{"a generation in a run scope", func(q *protocol.ReadEmbeddingStateRequest, r *protocol.EmbeddingState) {
			q.Scope, r.Scope = runScope("run-1"), runScope("run-1")
			r.Records = &protocol.EmbeddingRecords{Generations: []*protocol.EmbeddingGeneration{generationRecord("gen-1")}}
		}},
		{"a pointer in a run scope", func(q *protocol.ReadEmbeddingStateRequest, r *protocol.EmbeddingState) {
			q.Scope, r.Scope = runScope("run-1"), runScope("run-1")
			r.Records = &protocol.EmbeddingRecords{Pointers: []*protocol.EmbeddingPointer{pointerRecord("", "documents")}}
		}},
		{"another generation in a generation scope", func(q *protocol.ReadEmbeddingStateRequest, r *protocol.EmbeddingState) {
			scope := &protocol.EmbeddingStateScope{Scope: &protocol.EmbeddingStateScope_Generation{Generation: "gen-1"}}
			q.Scope, r.Scope = scope, proto.CloneOf(scope)
			r.Records = &protocol.EmbeddingRecords{Generations: []*protocol.EmbeddingGeneration{generationRecord("gen-2")}}
		}},
		{"a run in a generation scope", func(q *protocol.ReadEmbeddingStateRequest, r *protocol.EmbeddingState) {
			scope := &protocol.EmbeddingStateScope{Scope: &protocol.EmbeddingStateScope_Generation{Generation: "gen-1"}}
			q.Scope, r.Scope = scope, proto.CloneOf(scope)
			r.Records = &protocol.EmbeddingRecords{Runs: []*protocol.EmbeddingRun{runRecord("run-1", "gen-1")}}
		}},
		{"a run of another generation", func(q *protocol.ReadEmbeddingStateRequest, r *protocol.EmbeddingState) {
			scope := &protocol.EmbeddingStateScope{Scope: &protocol.EmbeddingStateScope_RunsOfGeneration{RunsOfGeneration: "gen-1"}}
			q.Scope, r.Scope = scope, proto.CloneOf(scope)
			r.Records = &protocol.EmbeddingRecords{Runs: []*protocol.EmbeddingRun{runRecord("run-1", "gen-1"), runRecord("run-2", "gen-2")}}
		}},
		{"a generation among the runs of a generation", func(q *protocol.ReadEmbeddingStateRequest, r *protocol.EmbeddingState) {
			scope := &protocol.EmbeddingStateScope{Scope: &protocol.EmbeddingStateScope_RunsOfGeneration{RunsOfGeneration: "gen-1"}}
			q.Scope, r.Scope = scope, proto.CloneOf(scope)
			r.Records = &protocol.EmbeddingRecords{Generations: []*protocol.EmbeddingGeneration{generationRecord("gen-1")}}
		}},
		{"another pointer in a pointer scope", func(q *protocol.ReadEmbeddingStateRequest, r *protocol.EmbeddingState) {
			scope := &protocol.EmbeddingStateScope{Scope: &protocol.EmbeddingStateScope_Pointer{
				Pointer: &protocol.EmbeddingPointerKey{TargetSchema: "app", TargetTable: "documents"}}}
			q.Scope, r.Scope = scope, proto.CloneOf(scope)
			r.Records = &protocol.EmbeddingRecords{Pointers: []*protocol.EmbeddingPointer{pointerRecord("", "documents")}}
		}},
		{"a pointer of another table in a pointer scope", func(q *protocol.ReadEmbeddingStateRequest, r *protocol.EmbeddingState) {
			scope := &protocol.EmbeddingStateScope{Scope: &protocol.EmbeddingStateScope_Pointer{
				Pointer: &protocol.EmbeddingPointerKey{TargetSchema: "app", TargetTable: "documents"}}}
			q.Scope, r.Scope = scope, proto.CloneOf(scope)
			r.Records = &protocol.EmbeddingRecords{Pointers: []*protocol.EmbeddingPointer{pointerRecord("app", "notes")}}
		}},
		{"a run in a pointer scope", func(q *protocol.ReadEmbeddingStateRequest, r *protocol.EmbeddingState) {
			scope := &protocol.EmbeddingStateScope{Scope: &protocol.EmbeddingStateScope_Pointer{
				Pointer: &protocol.EmbeddingPointerKey{TargetSchema: "app", TargetTable: "documents"}}}
			q.Scope, r.Scope = scope, proto.CloneOf(scope)
			r.Records = &protocol.EmbeddingRecords{Runs: []*protocol.EmbeddingRun{runRecord("run-1", "gen-1")}}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request, result := stateExchange()
			test.mutate(request, result)
			c.Assert(provider.ValidateEmbeddingState(request, result), qt.ErrorIs, provider.ErrProtocol)
		})
	}
}

func TestValidateMutatedEmbeddingState_HappyPath(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*protocol.MutateEmbeddingStateRequest, *protocol.EmbeddingWriteResult)
	}{
		{"applied", func(*protocol.MutateEmbeddingStateRequest, *protocol.EmbeddingWriteResult) {}},
		{"a conflict", func(_ *protocol.MutateEmbeddingStateRequest, r *protocol.EmbeddingWriteResult) {
			r.Outcome, r.Written = protocol.EmbeddingWriteResult_CONFLICT, nil
		}},
		{"a refusal", func(_ *protocol.MutateEmbeddingStateRequest, r *protocol.EmbeddingWriteResult) {
			r.Outcome, r.Written, r.Refusal = protocol.EmbeddingWriteResult_REFUSED, nil, refusal(protocol.EmbeddingRefusal_ABSENT)
		}},
		{"not applied", func(_ *protocol.MutateEmbeddingStateRequest, r *protocol.EmbeddingWriteResult) {
			r.Outcome, r.Written, r.Message = protocol.EmbeddingWriteResult_NOT_APPLIED, nil, "aborted twelve times"
		}},
		{"unknown with a message", func(_ *protocol.MutateEmbeddingStateRequest, r *protocol.EmbeddingWriteResult) {
			r.Outcome, r.Written, r.Message = protocol.EmbeddingWriteResult_UNKNOWN, nil, "the commit reply was lost"
		}},
		{"one run in its own scope", func(q *protocol.MutateEmbeddingStateRequest, r *protocol.EmbeddingWriteResult) {
			q.Scope = runScope("run-1")
			q.Records.Generations, r.Written.Generations = nil, nil
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request, result := mutateExchange()
			test.mutate(request, result)
			c.Assert(provider.ValidateMutatedEmbeddingState(request, result), qt.IsNil)
		})
	}
}

func TestValidateMutateEmbeddingStateRequest_FailurePath(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*protocol.MutateEmbeddingStateRequest)
	}{
		{"no session", func(r *protocol.MutateEmbeddingStateRequest) { r.SessionId = "" }},
		{"no request ID", func(r *protocol.MutateEmbeddingStateRequest) { r.RequestId = "" }},
		{"no scope", func(r *protocol.MutateEmbeddingStateRequest) { r.Scope = nil }},
		{"no expected token", func(r *protocol.MutateEmbeddingStateRequest) { r.ExpectedToken = "" }},
		{"no record", func(r *protocol.MutateEmbeddingStateRequest) { r.Records = nil }},
		{"an invalid record", func(r *protocol.MutateEmbeddingStateRequest) { r.Records.Runs[0].FencingToken = -1 }},
		{"a record outside the scope", func(r *protocol.MutateEmbeddingStateRequest) { r.Scope = runScope("run-1") }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request, _ := mutateExchange()
			test.mutate(request)
			c.Assert(provider.ValidateMutateEmbeddingStateRequest(request), qt.ErrorIs, provider.ErrProtocol)
		})
	}
}

func TestValidateMutatedEmbeddingState_FailurePath(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*protocol.EmbeddingWriteResult)
	}{
		{"another request", func(r *protocol.EmbeddingWriteResult) { r.RequestId = "mutate-2" }},
		{"an unknown outcome", func(r *protocol.EmbeddingWriteResult) { r.Outcome, r.Written = 9, nil }},
		{"a refusal without its reason", func(r *protocol.EmbeddingWriteResult) {
			r.Outcome, r.Written = protocol.EmbeddingWriteResult_REFUSED, nil
		}},
		{"a refusal with a conflict", func(r *protocol.EmbeddingWriteResult) {
			r.Outcome, r.Written, r.Refusal = protocol.EmbeddingWriteResult_CONFLICT, nil, refusal(protocol.EmbeddingRefusal_ABSENT)
		}},
		{"a refusal without a reason", func(r *protocol.EmbeddingWriteResult) {
			r.Outcome, r.Written, r.Refusal = protocol.EmbeddingWriteResult_REFUSED, nil, refusal(protocol.EmbeddingRefusal_REASON_UNSPECIFIED)
		}},
		{"a refusal with an unknown reason", func(r *protocol.EmbeddingWriteResult) {
			r.Outcome, r.Written, r.Refusal = protocol.EmbeddingWriteResult_REFUSED, nil, refusal(9)
		}},
		{"a refusal of two lines", func(r *protocol.EmbeddingWriteResult) {
			r.Outcome, r.Written = protocol.EmbeddingWriteResult_REFUSED, nil
			r.Refusal = &protocol.EmbeddingRefusal{Reason: protocol.EmbeddingRefusal_ABSENT, Message: "one\ntwo"}
		}},
		{"not applied without a message", func(r *protocol.EmbeddingWriteResult) {
			r.Outcome, r.Written = protocol.EmbeddingWriteResult_NOT_APPLIED, nil
		}},
		{"a message of two lines", func(r *protocol.EmbeddingWriteResult) {
			r.Outcome, r.Written, r.Message = protocol.EmbeddingWriteResult_CONFLICT, nil, "one\ntwo"
		}},
		{"a message over the limit", func(r *protocol.EmbeddingWriteResult) {
			r.Outcome, r.Written, r.Message = protocol.EmbeddingWriteResult_UNKNOWN, nil, strings.Repeat("m", 513)
		}},
		{"applied without the written records", func(r *protocol.EmbeddingWriteResult) { r.Written = nil }},
		{"written records with a conflict", func(r *protocol.EmbeddingWriteResult) { r.Outcome = protocol.EmbeddingWriteResult_CONFLICT }},
		{"a sequence from a mutation", func(r *protocol.EmbeddingWriteResult) { r.Sequence = 1 }},
		{"an index outcome from a mutation", func(r *protocol.EmbeddingWriteResult) {
			r.IndexOutcome = protocol.EmbeddingWriteResult_BUILT
		}},
		{"written records of another key", func(r *protocol.EmbeddingWriteResult) { r.Written.Runs[0].Id = "run-2" }},
		{"a written record missing", func(r *protocol.EmbeddingWriteResult) { r.Written.Generations = nil }},
		{"an invalid written record", func(r *protocol.EmbeddingWriteResult) { r.Written.Runs[0].FencingToken = -1 }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request, result := mutateExchange()
			test.mutate(result)
			c.Assert(provider.ValidateMutatedEmbeddingState(request, result), qt.ErrorIs, provider.ErrProtocol)
		})
	}
}

// A reply that is not there is refused, never read as an empty answer.
func TestEmbeddingStoreRepliesAreRequired(t *testing.T) {
	c := qt.New(t)
	stateRequest, _ := stateExchange()
	c.Assert(provider.ValidateEmbeddingState(stateRequest, nil), qt.ErrorIs, provider.ErrProtocol)
	mutateRequest, _ := mutateExchange()
	c.Assert(provider.ValidateMutatedEmbeddingState(mutateRequest, nil), qt.ErrorIs, provider.ErrProtocol)
	eventsRequest, _ := eventsExchange()
	c.Assert(provider.ValidateEmbeddingEventPage(eventsRequest, nil), qt.ErrorIs, provider.ErrProtocol)
}

// A reply to a request the SDK refuses is refused, however well formed the
// reply is.
func TestEmbeddingStoreRepliesToAnInvalidRequestAreRefused(t *testing.T) {
	c := qt.New(t)
	mutateRequest, mutated := mutateExchange()
	mutateRequest.SessionId = ""
	c.Assert(provider.ValidateMutatedEmbeddingState(mutateRequest, mutated), qt.ErrorIs, provider.ErrProtocol)
	appendRequest, appended := appendExchange()
	appendRequest.SessionId = ""
	c.Assert(provider.ValidateAppendedEmbeddingEvent(appendRequest, appended), qt.ErrorIs, provider.ErrProtocol)
	commitRequest, committed := commitExchange()
	commitRequest.SessionId = ""
	c.Assert(provider.ValidateCommittedEmbeddingBatch(commitRequest, committed), qt.ErrorIs, provider.ErrProtocol)
	prepared := &protocol.EmbeddingWriteResult{RequestId: "prepare-1", Outcome: protocol.EmbeddingWriteResult_APPLIED}
	c.Assert(provider.ValidatePreparedEmbeddingStore(&protocol.PrepareEmbeddingStoreRequest{RequestId: "prepare-1"}, prepared),
		qt.ErrorIs, provider.ErrProtocol)
}

// Storage preparation compares nothing, so it cannot conflict.
func TestValidatePreparedEmbeddingStore(t *testing.T) {
	c := qt.New(t)
	request := &protocol.PrepareEmbeddingStoreRequest{SessionId: "session", RequestId: "prepare-1"}
	applied := &protocol.EmbeddingWriteResult{RequestId: "prepare-1", Outcome: protocol.EmbeddingWriteResult_APPLIED}
	c.Assert(provider.ValidatePreparedEmbeddingStore(request, applied), qt.IsNil)
	refused := &protocol.EmbeddingWriteResult{RequestId: "prepare-1", Outcome: protocol.EmbeddingWriteResult_REFUSED,
		Refusal: refusal(protocol.EmbeddingRefusal_NOT_OWNED)}
	c.Assert(provider.ValidatePreparedEmbeddingStore(request, refused), qt.IsNil)
	conflict := &protocol.EmbeddingWriteResult{RequestId: "prepare-1", Outcome: protocol.EmbeddingWriteResult_CONFLICT}
	c.Assert(provider.ValidatePreparedEmbeddingStore(request, conflict), qt.ErrorIs, provider.ErrProtocol)
	c.Assert(provider.ValidatePrepareEmbeddingStoreRequest(&protocol.PrepareEmbeddingStoreRequest{SessionId: "session"}),
		qt.ErrorIs, provider.ErrProtocol)
	c.Assert(provider.ValidatePrepareEmbeddingStoreRequest(&protocol.PrepareEmbeddingStoreRequest{RequestId: "prepare-1"}),
		qt.ErrorIs, provider.ErrProtocol)
}

func eventRecord(sequence int64) *protocol.EmbeddingEvent {
	return &protocol.EmbeddingEvent{RunId: "run-1", Sequence: sequence, Kind: "checkpoint", At: at(1700000600), Actor: "worker-1",
		FencingToken: 2, ToPhase: "backfilling", Counts: &protocol.EmbeddingEventCounts{RowsScanned: 40, BatchesCommitted: 4}}
}

func appendExchange() (*protocol.AppendEmbeddingEventRequest, *protocol.EmbeddingWriteResult) {
	return &protocol.AppendEmbeddingEventRequest{SessionId: "session", RequestId: "append-1", Event: eventRecord(0)},
		&protocol.EmbeddingWriteResult{RequestId: "append-1", Outcome: protocol.EmbeddingWriteResult_APPLIED, Sequence: 5}
}

func TestValidateAppendedEmbeddingEvent_HappyPath(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*protocol.EmbeddingWriteResult)
	}{
		{"applied", func(*protocol.EmbeddingWriteResult) {}},
		{"a run that is not there", func(r *protocol.EmbeddingWriteResult) {
			r.Outcome, r.Sequence, r.Refusal = protocol.EmbeddingWriteResult_REFUSED, 0, refusal(protocol.EmbeddingRefusal_ABSENT)
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request, result := appendExchange()
			test.mutate(result)
			c.Assert(provider.ValidateAppendedEmbeddingEvent(request, result), qt.IsNil)
		})
	}
}

func TestValidateAppendedEmbeddingEvent_FailurePath(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*protocol.AppendEmbeddingEventRequest, *protocol.EmbeddingWriteResult)
	}{
		{"no session", func(q *protocol.AppendEmbeddingEventRequest, _ *protocol.EmbeddingWriteResult) { q.SessionId = "" }},
		{"no request ID", func(q *protocol.AppendEmbeddingEventRequest, _ *protocol.EmbeddingWriteResult) { q.RequestId = "" }},
		{"no event", func(q *protocol.AppendEmbeddingEventRequest, _ *protocol.EmbeddingWriteResult) { q.Event = nil }},
		{"an event without a run", func(q *protocol.AppendEmbeddingEventRequest, _ *protocol.EmbeddingWriteResult) { q.Event.RunId = "" }},
		{"an event with a sequence", func(q *protocol.AppendEmbeddingEventRequest, _ *protocol.EmbeddingWriteResult) { q.Event.Sequence = 3 }},
		{"an event with a negative sequence", func(q *protocol.AppendEmbeddingEventRequest, _ *protocol.EmbeddingWriteResult) {
			q.Event.Sequence = -1
		}},
		{"an event field that is not text", func(q *protocol.AppendEmbeddingEventRequest, _ *protocol.EmbeddingWriteResult) {
			q.Event.Detail = "a\x00b"
		}},
		{"an event with a negative count", func(q *protocol.AppendEmbeddingEventRequest, _ *protocol.EmbeddingWriteResult) {
			q.Event.Counts.RowsDeleted = -1
		}},
		{"an event with a negative fencing token", func(q *protocol.AppendEmbeddingEventRequest, _ *protocol.EmbeddingWriteResult) {
			q.Event.FencingToken = -1
		}},
		{"an event time out of range", func(q *protocol.AppendEmbeddingEventRequest, _ *protocol.EmbeddingWriteResult) {
			q.Event.At = &protocol.TimestampValue{Nanos: 1e9}
		}},
		{"applied without a sequence", func(_ *protocol.AppendEmbeddingEventRequest, r *protocol.EmbeddingWriteResult) { r.Sequence = 0 }},
		{"a negative sequence", func(_ *protocol.AppendEmbeddingEventRequest, r *protocol.EmbeddingWriteResult) { r.Sequence = -1 }},
		{"a conflict", func(_ *protocol.AppendEmbeddingEventRequest, r *protocol.EmbeddingWriteResult) {
			r.Outcome, r.Sequence = protocol.EmbeddingWriteResult_CONFLICT, 0
		}},
		{"a sequence with a refusal", func(_ *protocol.AppendEmbeddingEventRequest, r *protocol.EmbeddingWriteResult) {
			r.Outcome, r.Refusal = protocol.EmbeddingWriteResult_REFUSED, refusal(protocol.EmbeddingRefusal_ABSENT)
		}},
		{"records from an append", func(_ *protocol.AppendEmbeddingEventRequest, r *protocol.EmbeddingWriteResult) {
			r.Written = &protocol.EmbeddingRecords{}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request, result := appendExchange()
			test.mutate(request, result)
			c.Assert(provider.ValidateAppendedEmbeddingEvent(request, result), qt.ErrorIs, provider.ErrProtocol)
		})
	}
}

func eventsExchange() (*protocol.ReadEmbeddingEventsRequest, *protocol.EmbeddingEventPage) {
	return &protocol.ReadEmbeddingEventsRequest{SessionId: "session", RunId: "run-1", AfterSequence: 2, MaxEvents: 3},
		&protocol.EmbeddingEventPage{RunId: "run-1", Exists: true, Events: []*protocol.EmbeddingEvent{eventRecord(3), eventRecord(5)}, Complete: true}
}

func TestValidateEmbeddingEventPage_HappyPath(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*protocol.EmbeddingEventPage)
	}{
		{"events after the position", func(*protocol.EmbeddingEventPage) {}},
		{"a page with more to follow", func(r *protocol.EmbeddingEventPage) { r.Complete = false }},
		{"a run that is not there", func(r *protocol.EmbeddingEventPage) { r.Exists, r.Events = false, nil }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request, result := eventsExchange()
			test.mutate(result)
			c.Assert(provider.ValidateEmbeddingEventPage(request, result), qt.IsNil)
		})
	}
}

func TestValidateEmbeddingEventPage_FailurePath(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*protocol.ReadEmbeddingEventsRequest, *protocol.EmbeddingEventPage)
	}{
		{"no session", func(q *protocol.ReadEmbeddingEventsRequest, _ *protocol.EmbeddingEventPage) { q.SessionId = "" }},
		{"no run", func(q *protocol.ReadEmbeddingEventsRequest, _ *protocol.EmbeddingEventPage) { q.RunId = "" }},
		{"a negative position", func(q *protocol.ReadEmbeddingEventsRequest, _ *protocol.EmbeddingEventPage) { q.AfterSequence = -1 }},
		{"no bound", func(q *protocol.ReadEmbeddingEventsRequest, _ *protocol.EmbeddingEventPage) { q.MaxEvents = 0 }},
		{"a bound above the limit", func(q *protocol.ReadEmbeddingEventsRequest, _ *protocol.EmbeddingEventPage) {
			q.MaxEvents = provider.MaxEmbeddingEvents + 1
		}},
		{"another run", func(_ *protocol.ReadEmbeddingEventsRequest, r *protocol.EmbeddingEventPage) { r.RunId = "run-2" }},
		{"events of a run that is not there", func(_ *protocol.ReadEmbeddingEventsRequest, r *protocol.EmbeddingEventPage) {
			r.Exists = false
		}},
		{"a run that is not there with more to follow", func(_ *protocol.ReadEmbeddingEventsRequest, r *protocol.EmbeddingEventPage) {
			r.Exists, r.Events, r.Complete = false, nil, false
		}},
		{"more events than the bound", func(q *protocol.ReadEmbeddingEventsRequest, _ *protocol.EmbeddingEventPage) { q.MaxEvents = 1 }},
		{"an event without a sequence", func(_ *protocol.ReadEmbeddingEventsRequest, r *protocol.EmbeddingEventPage) {
			r.Events[0].Sequence = 0
		}},
		{"an event of another run", func(_ *protocol.ReadEmbeddingEventsRequest, r *protocol.EmbeddingEventPage) {
			r.Events[1].RunId = "run-2"
		}},
		{"an event at the position", func(_ *protocol.ReadEmbeddingEventsRequest, r *protocol.EmbeddingEventPage) {
			r.Events[0].Sequence = 2
		}},
		{"sequences out of order", func(_ *protocol.ReadEmbeddingEventsRequest, r *protocol.EmbeddingEventPage) {
			slices.Reverse(r.Events)
		}},
		{"a repeated sequence", func(_ *protocol.ReadEmbeddingEventsRequest, r *protocol.EmbeddingEventPage) {
			r.Events[1].Sequence = 3
		}},
		{"an event field that is not text", func(_ *protocol.ReadEmbeddingEventsRequest, r *protocol.EmbeddingEventPage) {
			r.Events[0].Detail = "a\x00b"
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request, result := eventsExchange()
			test.mutate(request, result)
			c.Assert(provider.ValidateEmbeddingEventPage(request, result), qt.ErrorIs, provider.ErrProtocol)
		})
	}
}

func TestValidateReadEmbeddingEventsRequest_FailurePath(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*protocol.ReadEmbeddingEventsRequest)
	}{
		{"no session", func(r *protocol.ReadEmbeddingEventsRequest) { r.SessionId = "" }},
		{"no run", func(r *protocol.ReadEmbeddingEventsRequest) { r.RunId = "" }},
		{"a negative position", func(r *protocol.ReadEmbeddingEventsRequest) { r.AfterSequence = -1 }},
		{"no bound", func(r *protocol.ReadEmbeddingEventsRequest) { r.MaxEvents = 0 }},
		{"a bound above the limit", func(r *protocol.ReadEmbeddingEventsRequest) { r.MaxEvents = provider.MaxEmbeddingEvents + 1 }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request, _ := eventsExchange()
			test.mutate(request)
			c.Assert(provider.ValidateReadEmbeddingEventsRequest(request), qt.ErrorIs, provider.ErrProtocol)
		})
	}
}

// The same records give the same token, and a change to any of them, their
// order, or an absent record in place of an empty one gives another.
func TestEmbeddingStateToken(t *testing.T) {
	c := qt.New(t)
	run := runRecord("run-1", "gen-1")
	first, err := provider.EmbeddingStateToken(run, generationRecord("gen-1"))
	c.Assert(err, qt.IsNil)
	again, err := provider.EmbeddingStateToken(proto.CloneOf(run), generationRecord("gen-1"))
	c.Assert(err, qt.IsNil)
	c.Assert(again, qt.Equals, first)
	c.Assert(first, qt.HasLen, 64)
	moved := proto.CloneOf(run)
	moved.FencingToken++
	changed, err := provider.EmbeddingStateToken(moved, generationRecord("gen-1"))
	c.Assert(err, qt.IsNil)
	reordered, err := provider.EmbeddingStateToken(generationRecord("gen-1"), run)
	c.Assert(err, qt.IsNil)
	absent, err := provider.EmbeddingStateToken((*protocol.EmbeddingRun)(nil))
	c.Assert(err, qt.IsNil)
	empty, err := provider.EmbeddingStateToken(&protocol.EmbeddingRun{})
	c.Assert(err, qt.IsNil)
	none, err := provider.EmbeddingStateToken()
	c.Assert(err, qt.IsNil)
	untyped, err := provider.EmbeddingStateToken(nil)
	c.Assert(err, qt.IsNil)
	c.Assert(untyped, qt.Equals, absent)
	tokens := []string{first, changed, reordered, absent, empty, none}
	slices.Sort(tokens)
	c.Assert(slices.Compact(tokens), qt.HasLen, 6)
	unencodable, err := provider.EmbeddingStateToken(&protocol.EmbeddingRun{Id: "\xff"})
	c.Assert(err, qt.ErrorMatches, `encode embedding state: .*`)
	c.Assert(unencodable, qt.Equals, "")
}

func TestCheckEmbeddingWrite_HappyPath(t *testing.T) {
	c := qt.New(t)
	c.Assert(provider.CheckEmbeddingWrite(&protocol.EmbeddingWriteResult{Outcome: protocol.EmbeddingWriteResult_APPLIED}), qt.IsNil)
}

func TestCheckEmbeddingWrite_FailurePath(t *testing.T) {
	c := qt.New(t)
	conflict := provider.CheckEmbeddingWrite(&protocol.EmbeddingWriteResult{Outcome: protocol.EmbeddingWriteResult_CONFLICT})
	c.Assert(conflict, qt.ErrorIs, provider.ErrEmbeddingConflict)
	c.Assert(conflict, qt.ErrorIs, provider.ErrExecution)
	c.Assert(conflict, qt.ErrorMatches, `provider embedding write outcome is CONFLICT`)
	refused := provider.CheckEmbeddingWrite(&protocol.EmbeddingWriteResult{Outcome: protocol.EmbeddingWriteResult_REFUSED,
		Refusal: refusal(protocol.EmbeddingRefusal_NOT_OWNED)})
	c.Assert(refused, qt.ErrorIs, provider.ErrExecution)
	c.Assert(refused, qt.Not(qt.ErrorIs), provider.ErrEmbeddingConflict)
	c.Assert(refused, qt.ErrorMatches, `provider embedding write was refused: NOT_OWNED`)
	var written *provider.EmbeddingWriteError
	c.Assert(refused, qt.ErrorAs, &written)
	c.Assert(written.Reason, qt.Equals, protocol.EmbeddingRefusal_NOT_OWNED)
	unknown := provider.CheckEmbeddingWrite(&protocol.EmbeddingWriteResult{Outcome: protocol.EmbeddingWriteResult_UNKNOWN})
	c.Assert(unknown, qt.ErrorIs, provider.ErrOutcomeUnknown)
	c.Assert(unknown, qt.Not(qt.ErrorIs), provider.ErrExecution)
	notApplied := provider.CheckEmbeddingWrite(&protocol.EmbeddingWriteResult{Outcome: protocol.EmbeddingWriteResult_NOT_APPLIED})
	c.Assert(notApplied, qt.ErrorIs, provider.ErrExecution)
	c.Assert(notApplied, qt.Not(qt.ErrorIs), provider.ErrEmbeddingConflict)
}
