package runtime

// White-box testing required: the loopback sends each client call through the
// server adapter without a process, the negotiated state of the adapter is set
// by Describe, and a malformed or lost reply must reach the client without
// passing the SDK server's own checks.

import (
	"context"
	"maps"
	"math"
	"slices"
	"strings"
	"sync"
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func vectorsDescription() *protocol.Description {
	return &protocol.Description{
		Revision: provider.Revision, ProviderId: "example.org/vectors", Version: "1", SemanticIdentity: "vectors-1",
		Features: []string{provider.EmbeddingStoreFeature},
		Engines: []*protocol.Engine{{Id: "vectors", Features: []string{provider.EmbeddingStoreFeature},
			Identifiers: &protocol.IdentifierRules{TableNames: protocol.IdentifierRules_EXACT, ColumnNames: protocol.IdentifierRules_EXACT,
				IndexNames: protocol.IdentifierRules_EXACT, IndexNamespace: protocol.IdentifierRules_TABLE},
			Embeddings: &protocol.EmbeddingEngine{ConsistencyModes: []string{"immutable"}}}},
	}
}

func vectorsTable(name string) *protocol.Reference {
	return &protocol.Reference{Engine: "vectors", Schema: "app", Kind: "table", Name: name}
}

// vectorsSpec is a generation over documents keyed by one column, stored in a
// table of its own as a set of chunks, with an index declared.
func vectorsSpec() *protocol.EmbeddingSpec {
	return &protocol.EmbeddingSpec{
		Generation: "gen-1", Chunked: true,
		Source: &protocol.EmbeddingSource{Table: vectorsTable("documents"), KeyFields: []string{"id"}, InputFields: []string{"body"}},
		Target: &protocol.EmbeddingTarget{Table: vectorsTable("document_chunks"), Column: "embedding", Layout: protocol.EmbeddingTarget_OWN_TABLE,
			Representation: "float", Metric: protocol.EmbeddingTarget_L2, Dimension: 2, IndexMethod: "flat"},
		Names: &protocol.EmbeddingNames{Index: "document_chunks_embedding_idx", OwnershipMark: "ptah embedding target for generation gen-1"},
	}
}

func vectorsKey(id string) *protocol.EmbeddingKey {
	return &protocol.EmbeddingKey{Components: []string{id}}
}

func textValue(value string) *protocol.DataValue {
	return &protocol.DataValue{Value: &protocol.DataValue_StringValue{StringValue: value}}
}

// storedChunk is one row of the target table.
type storedChunk struct {
	key     string
	ordinal uint32
	write   *protocol.EmbeddingTargetWrite
}

// memoryEmbeddings is an owner over in-memory run state, one source table and
// one target table. Each write is a compare-and-write under one lock, which
// is what an engine's transaction gives a real owner.
type memoryEmbeddings struct {
	mu          sync.Mutex
	generations map[string]*protocol.EmbeddingGeneration
	runs        map[string]*protocol.EmbeddingRun
	pointers    map[string]*protocol.EmbeddingPointer
	events      map[string][]*protocol.EmbeddingEvent
	source      map[string]string // id -> body
	target      []storedChunk     // in key and ordinal order
	ownsTable   bool
	indexBuilt  bool
}

func newMemoryEmbeddings() *memoryEmbeddings {
	return &memoryEmbeddings{
		generations: make(map[string]*protocol.EmbeddingGeneration), runs: make(map[string]*protocol.EmbeddingRun),
		pointers: make(map[string]*protocol.EmbeddingPointer), events: make(map[string][]*protocol.EmbeddingEvent),
		source:    map[string]string{"1": "alpha", "2": "beta", "3": "gamma", "4": "delta", "5": "epsilon"},
		ownsTable: true,
	}
}

// records lists the records a scope covers, in key order.
func (m *memoryEmbeddings) records(scope *protocol.EmbeddingStateScope) *protocol.EmbeddingRecords {
	records := &protocol.EmbeddingRecords{}
	for _, id := range slices.Sorted(maps.Keys(m.generations)) {
		generation := m.generations[id]
		if scope.GetEverything() != nil || scope.GetGeneration() == id {
			records.Generations = append(records.Generations, proto.CloneOf(generation))
		}
	}
	for _, id := range slices.Sorted(maps.Keys(m.runs)) {
		run := m.runs[id]
		if scope.GetEverything() != nil || scope.GetRun() == id || scope.GetRunsOfGeneration() == run.GetGenerationIdentity() {
			records.Runs = append(records.Runs, proto.CloneOf(run))
		}
	}
	for _, key := range slices.Sorted(maps.Keys(m.pointers)) {
		pointer := m.pointers[key]
		wanted := scope.GetPointer()
		if scope.GetEverything() != nil || wanted != nil && wanted.GetTargetSchema() == pointer.GetTargetSchema() && wanted.GetTargetTable() == pointer.GetTargetTable() {
			records.Pointers = append(records.Pointers, proto.CloneOf(pointer))
		}
	}
	return records
}

func token(messages ...proto.Message) string {
	value, _ := provider.EmbeddingStateToken(messages...)
	return value
}

func (m *memoryEmbeddings) PrepareEmbeddingStore(_ context.Context, request *protocol.PrepareEmbeddingStoreRequest) (*protocol.EmbeddingWriteResult, error) {
	return &protocol.EmbeddingWriteResult{RequestId: request.RequestId, Outcome: protocol.EmbeddingWriteResult_APPLIED}, nil
}

func (m *memoryEmbeddings) ReadEmbeddingState(_ context.Context, request *protocol.ReadEmbeddingStateRequest) (*protocol.EmbeddingState, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	records := m.records(request.Scope)
	return &protocol.EmbeddingState{Scope: request.Scope, Exists: true, Records: records, Token: token(records)}, nil
}

// MutateEmbeddingState writes every record whole when the scope still has the
// token, and stores times to the microsecond, as a YDB owner does.
func (m *memoryEmbeddings) MutateEmbeddingState(_ context.Context, request *protocol.MutateEmbeddingStateRequest) (*protocol.EmbeddingWriteResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := &protocol.EmbeddingWriteResult{RequestId: request.RequestId}
	if token(m.records(request.Scope)) != request.ExpectedToken {
		result.Outcome = protocol.EmbeddingWriteResult_CONFLICT
		return result, nil
	}
	written := proto.CloneOf(request.Records)
	for _, generation := range written.Generations {
		m.generations[generation.Identity] = proto.CloneOf(generation)
	}
	for _, run := range written.Runs {
		if run.UpdatedAt != nil {
			run.UpdatedAt.Nanos -= run.UpdatedAt.Nanos % 1000
		}
		m.runs[run.Id] = proto.CloneOf(run)
	}
	for _, pointer := range written.Pointers {
		m.pointers[pointer.TargetSchema+"\x00"+pointer.TargetTable] = proto.CloneOf(pointer)
	}
	result.Outcome, result.Written = protocol.EmbeddingWriteResult_APPLIED, written
	return result, nil
}

func (m *memoryEmbeddings) AppendEmbeddingEvent(_ context.Context, request *protocol.AppendEmbeddingEventRequest) (*protocol.EmbeddingWriteResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := &protocol.EmbeddingWriteResult{RequestId: request.RequestId}
	if m.runs[request.Event.RunId] == nil {
		result.Outcome = protocol.EmbeddingWriteResult_REFUSED
		result.Refusal = &protocol.EmbeddingRefusal{Reason: protocol.EmbeddingRefusal_ABSENT, Message: "the run is not there"}
		return result, nil
	}
	event := proto.CloneOf(request.Event)
	event.Sequence = int64(len(m.events[event.RunId]) + 1)
	m.events[event.RunId] = append(m.events[event.RunId], event)
	result.Outcome, result.Sequence = protocol.EmbeddingWriteResult_APPLIED, event.Sequence
	return result, nil
}

func (m *memoryEmbeddings) ReadEmbeddingEvents(_ context.Context, request *protocol.ReadEmbeddingEventsRequest) (*protocol.EmbeddingEventPage, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	page := &protocol.EmbeddingEventPage{RunId: request.RunId, Exists: m.runs[request.RunId] != nil, Complete: true}
	for _, event := range m.events[request.RunId] {
		if event.Sequence <= request.AfterSequence {
			continue
		}
		if len(page.Events) == int(request.MaxEvents) {
			page.Complete = false
			break
		}
		page.Events = append(page.Events, proto.CloneOf(event))
	}
	return page, nil
}

func (m *memoryEmbeddings) DescribeEmbeddingTarget(_ context.Context, request *protocol.DescribeEmbeddingTargetRequest) (*protocol.EmbeddingTargetObjects, error) {
	target := request.Spec.Target
	if target.Metric == protocol.EmbeddingTarget_INNER_PRODUCT {
		return &protocol.EmbeddingTargetObjects{Refusal: &protocol.EmbeddingRefusal{Reason: protocol.EmbeddingRefusal_UNSUPPORTED,
			Message: "this engine has no inner product"}}, nil
	}
	return &protocol.EmbeddingTargetObjects{ColumnType: "Blob", OwnsTable: target.Layout == protocol.EmbeddingTarget_OWN_TABLE,
		HasIndex: target.IndexMethod != "", IndexMethod: target.IndexMethod}, nil
}

func (m *memoryEmbeddings) ResolveEmbeddingRelations(_ context.Context, request *protocol.ResolveEmbeddingRelationsRequest) (*protocol.EmbeddingRelations, error) {
	result := &protocol.EmbeddingRelations{}
	for _, relation := range request.Relations {
		resolved := proto.CloneOf(relation)
		if resolved.Schema == "" {
			resolved.Schema = "app"
		}
		result.Relations = append(result.Relations, resolved)
	}
	return result, nil
}

func (m *memoryEmbeddings) MeasureEmbedding(context.Context, *protocol.MeasureEmbeddingRequest) (*protocol.EmbeddingMeasurement, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return &protocol.EmbeddingMeasurement{SourceExists: true, TargetTableExists: true, TargetColumnExists: true, RowsCounted: true,
		Rows: int64(len(m.source)), Capabilities: map[string]bool{"vector_type": true, "vector_index": true}, IndexBuildable: true}, nil
}

func (m *memoryEmbeddings) PrepareEmbeddingTarget(_ context.Context, request *protocol.PrepareEmbeddingTargetRequest) (*protocol.EmbeddingWriteResult, error) {
	return &protocol.EmbeddingWriteResult{RequestId: request.RequestId, Outcome: protocol.EmbeddingWriteResult_APPLIED}, nil
}

func (m *memoryEmbeddings) BuildEmbeddingIndex(_ context.Context, request *protocol.BuildEmbeddingIndexRequest) (*protocol.EmbeddingWriteResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := &protocol.EmbeddingWriteResult{RequestId: request.RequestId, Outcome: protocol.EmbeddingWriteResult_APPLIED}
	switch {
	case request.Spec.Target.IndexMethod == "":
		result.IndexOutcome = protocol.EmbeddingWriteResult_NOT_DECLARED
	case m.indexBuilt:
		result.IndexOutcome = protocol.EmbeddingWriteResult_ALREADY_VALID
	default:
		m.indexBuilt, result.IndexOutcome = true, protocol.EmbeddingWriteResult_BUILT
	}
	return result, nil
}

func (m *memoryEmbeddings) ReadEmbeddingStructure(_ context.Context, request *protocol.ReadEmbeddingStructureRequest) (*protocol.EmbeddingStructure, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	structure := &protocol.EmbeddingStructure{TableExists: true, ColumnExists: true, ColumnType: "Blob", Dimension: request.Location.Dimension,
		VectorSupport: true, SearchSettings: "probes=(server default)"}
	if m.indexBuilt {
		structure.IndexExists, structure.IndexValid, structure.IndexMethod = true, true, "flat"
	}
	if request.CountRows {
		structure.RowsCounted = true
		for _, chunk := range m.target {
			if chunk.write.Generation == request.Location.Generation {
				structure.Rows++
			}
		}
	}
	return structure, nil
}

func (m *memoryEmbeddings) DropEmbeddingGeneration(_ context.Context, request *protocol.DropEmbeddingGenerationRequest) (*protocol.EmbeddingWriteResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := &protocol.EmbeddingWriteResult{RequestId: request.RequestId, Outcome: protocol.EmbeddingWriteResult_APPLIED}
	if request.Storage == protocol.DropEmbeddingGenerationRequest_TABLE && !m.ownsTable {
		result.Outcome = protocol.EmbeddingWriteResult_REFUSED
		result.Refusal = &protocol.EmbeddingRefusal{Reason: protocol.EmbeddingRefusal_NOT_OWNED, Message: "app.document_chunks carries another mark"}
		return result, nil
	}
	m.target, m.indexBuilt = nil, false
	return result, nil
}

func (m *memoryEmbeddings) sourceRow(id string) *protocol.EmbeddingSourceRow {
	return &protocol.EmbeddingSourceRow{Key: vectorsKey(id), Fields: []*protocol.DataValue{textValue(m.source[id])}}
}

func (m *memoryEmbeddings) ScanEmbeddingSource(_ context.Context, request *protocol.ScanEmbeddingSourceRequest) (*protocol.EmbeddingSourcePage, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	page := &protocol.EmbeddingSourcePage{}
	after := ""
	if request.After != nil {
		after = request.After.Components[0]
	}
	for _, id := range slices.Sorted(maps.Keys(m.source)) {
		if id <= after {
			continue
		}
		page.Rows = append(page.Rows, m.sourceRow(id))
		page.Cursor = vectorsKey(id)
		if len(page.Rows) == int(request.MaxRows) {
			return page, nil
		}
	}
	page.Done = true
	return page, nil
}

func (m *memoryEmbeddings) ReadEmbeddingSourceRows(_ context.Context, request *protocol.ReadEmbeddingSourceRowsRequest) (*protocol.EmbeddingSourceRows, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := &protocol.EmbeddingSourceRows{}
	for _, key := range request.Keys {
		if _, found := m.source[key.Components[0]]; found {
			result.Rows = append(result.Rows, m.sourceRow(key.Components[0]))
		}
	}
	return result, nil
}

// batchState reads the run and the first stored row of each key.
func (m *memoryEmbeddings) batchState(runID string, keys []*protocol.EmbeddingKey) *protocol.EmbeddingBatchState {
	state := &protocol.EmbeddingBatchState{RunId: runID, Run: proto.CloneOf(m.runs[runID])}
	for _, key := range keys {
		for _, chunk := range m.target {
			if chunk.key == key.Components[0] && chunk.ordinal == 0 {
				write := chunk.write
				state.Held = append(state.Held, &protocol.EmbeddingHeldTarget{Key: proto.CloneOf(key), Generation: write.Generation,
					InputHash: write.InputHash, Version: write.Version, Kind: write.Kind})
			}
		}
	}
	messages := []proto.Message{state.Run}
	for _, held := range state.Held {
		messages = append(messages, held)
	}
	state.Token = token(messages...)
	return state
}

func (m *memoryEmbeddings) ReadEmbeddingBatchState(_ context.Context, request *protocol.ReadEmbeddingBatchStateRequest) (*protocol.EmbeddingBatchState, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.batchState(request.RunId, request.Keys), nil
}

// CommitEmbeddingBatch makes each written key's rows equal to its set and
// writes the run, when the run and the keys still have the token.
func (m *memoryEmbeddings) CommitEmbeddingBatch(_ context.Context, request *protocol.CommitEmbeddingBatchRequest) (*protocol.EmbeddingWriteResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := &protocol.EmbeddingWriteResult{RequestId: request.RequestId}
	if m.batchState(request.RunId, request.Keys).Token != request.ExpectedToken {
		result.Outcome = protocol.EmbeddingWriteResult_CONFLICT
		return result, nil
	}
	written := make(map[string]bool)
	for _, write := range request.Writes {
		written[write.Key.Components[0]] = true
	}
	kept := slices.DeleteFunc(slices.Clone(m.target), func(chunk storedChunk) bool { return written[chunk.key] })
	for _, write := range request.Writes {
		kept = append(kept, storedChunk{key: write.Key.Components[0], ordinal: write.Ordinal, write: proto.CloneOf(write)})
	}
	slices.SortFunc(kept, func(a, b storedChunk) int {
		return strings.Compare(a.key, b.key)*2 + int(a.ordinal) - int(b.ordinal)
	})
	m.target = kept
	m.runs[request.RunId] = proto.CloneOf(request.Run)
	result.Outcome = protocol.EmbeddingWriteResult_APPLIED
	result.Written = &protocol.EmbeddingRecords{Runs: []*protocol.EmbeddingRun{proto.CloneOf(request.Run)}}
	return result, nil
}

// ReadEmbeddingVerification pages the stored chunks by key, never splitting
// one key's chunks across pages.
func (m *memoryEmbeddings) ReadEmbeddingVerification(_ context.Context, request *protocol.ReadEmbeddingVerificationRequest) (*protocol.EmbeddingVerificationPage, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	after, _ := strings.CutPrefix(request.PageToken, "after:")
	page := &protocol.EmbeddingVerificationPage{}
	for start := 0; start < len(m.target); {
		end := start
		for end < len(m.target) && m.target[end].key == m.target[start].key {
			end++
		}
		key := m.target[start].key
		if key > after || request.PageToken == "" {
			if len(page.Rows)+end-start > int(request.MaxRows) {
				page.NextPageToken = "after:" + page.Rows[len(page.Rows)-1].Key.Components[0]
				return page, nil
			}
			for _, chunk := range m.target[start:end] {
				page.Rows = append(page.Rows, &protocol.EmbeddingVerificationRow{Key: vectorsKey(key),
					Source: &protocol.EmbeddingVerificationSource{Fields: []*protocol.DataValue{textValue(m.source[key])}},
					Stored: &protocol.EmbeddingStoredRow{Generation: chunk.write.Generation, InputHash: chunk.write.InputHash,
						Kind: chunk.write.Kind, Dimension: uint32(len(chunk.write.Vector)), Ordinal: chunk.ordinal}}) // #nosec G115 -- a test vector
			}
		}
		start = end
	}
	page.Complete = true
	return page, nil
}

// SearchEmbeddings ranks the stored chunks by their first element's distance
// to the vector's, which is enough to order a fixture.
func (m *memoryEmbeddings) SearchEmbeddings(_ context.Context, request *protocol.SearchEmbeddingsRequest) (*protocol.EmbeddingSearchResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	ranked := slices.Clone(m.target)
	slices.SortStableFunc(ranked, func(a, b storedChunk) int {
		return int(math.Abs(float64(a.write.Vector[0]-request.Vector[0]))*1000) - int(math.Abs(float64(b.write.Vector[0]-request.Vector[0]))*1000)
	})
	result := &protocol.EmbeddingSearchResult{Outcome: protocol.EmbeddingSearchResult_COMPLETED, IndexUsed: m.indexBuilt}
	seen := make(map[string]bool)
	for _, chunk := range ranked {
		if seen[chunk.key] || len(result.Exact) == int(request.Depth) {
			continue
		}
		seen[chunk.key] = true
		result.Exact = append(result.Exact, vectorsKey(chunk.key))
	}
	result.Indexed = slices.Clone(result.Exact)
	return result, nil
}

// embeddingLoopback sends a client call through the server adapter, as a
// process would. lost makes a write's reply disappear after the owner ran it.
type embeddingLoopback struct {
	protocol.ProviderClient
	server *serverAdapter
	calls  int
	lost   bool
}

func (l *embeddingLoopback) deliver(err error) error {
	l.calls++
	if l.lost && err == nil {
		return status.Error(codes.Unavailable, "connection reset")
	}
	return err
}

func loop[Q, R any](ctx context.Context, l *embeddingLoopback, serve func(context.Context, Q) (R, error), request Q) (R, error) {
	result, err := serve(ctx, request)
	if err = l.deliver(err); err != nil {
		var zero R
		return zero, err
	}
	return result, nil
}

func (l *embeddingLoopback) PrepareEmbeddingStore(ctx context.Context, request *protocol.PrepareEmbeddingStoreRequest, _ ...grpc.CallOption) (*protocol.EmbeddingWriteResult, error) {
	return loop(ctx, l, l.server.PrepareEmbeddingStore, request)
}

func (l *embeddingLoopback) ReadEmbeddingState(ctx context.Context, request *protocol.ReadEmbeddingStateRequest, _ ...grpc.CallOption) (*protocol.EmbeddingState, error) {
	return loop(ctx, l, l.server.ReadEmbeddingState, request)
}

func (l *embeddingLoopback) MutateEmbeddingState(ctx context.Context, request *protocol.MutateEmbeddingStateRequest, _ ...grpc.CallOption) (*protocol.EmbeddingWriteResult, error) {
	return loop(ctx, l, l.server.MutateEmbeddingState, request)
}

func (l *embeddingLoopback) AppendEmbeddingEvent(ctx context.Context, request *protocol.AppendEmbeddingEventRequest, _ ...grpc.CallOption) (*protocol.EmbeddingWriteResult, error) {
	return loop(ctx, l, l.server.AppendEmbeddingEvent, request)
}

func (l *embeddingLoopback) ReadEmbeddingEvents(ctx context.Context, request *protocol.ReadEmbeddingEventsRequest, _ ...grpc.CallOption) (*protocol.EmbeddingEventPage, error) {
	return loop(ctx, l, l.server.ReadEmbeddingEvents, request)
}

func (l *embeddingLoopback) DescribeEmbeddingTarget(ctx context.Context, request *protocol.DescribeEmbeddingTargetRequest, _ ...grpc.CallOption) (*protocol.EmbeddingTargetObjects, error) {
	return loop(ctx, l, l.server.DescribeEmbeddingTarget, request)
}

func (l *embeddingLoopback) ResolveEmbeddingRelations(ctx context.Context, request *protocol.ResolveEmbeddingRelationsRequest, _ ...grpc.CallOption) (*protocol.EmbeddingRelations, error) {
	return loop(ctx, l, l.server.ResolveEmbeddingRelations, request)
}

func (l *embeddingLoopback) MeasureEmbedding(ctx context.Context, request *protocol.MeasureEmbeddingRequest, _ ...grpc.CallOption) (*protocol.EmbeddingMeasurement, error) {
	return loop(ctx, l, l.server.MeasureEmbedding, request)
}

func (l *embeddingLoopback) PrepareEmbeddingTarget(ctx context.Context, request *protocol.PrepareEmbeddingTargetRequest, _ ...grpc.CallOption) (*protocol.EmbeddingWriteResult, error) {
	return loop(ctx, l, l.server.PrepareEmbeddingTarget, request)
}

func (l *embeddingLoopback) BuildEmbeddingIndex(ctx context.Context, request *protocol.BuildEmbeddingIndexRequest, _ ...grpc.CallOption) (*protocol.EmbeddingWriteResult, error) {
	return loop(ctx, l, l.server.BuildEmbeddingIndex, request)
}

func (l *embeddingLoopback) ReadEmbeddingStructure(ctx context.Context, request *protocol.ReadEmbeddingStructureRequest, _ ...grpc.CallOption) (*protocol.EmbeddingStructure, error) {
	return loop(ctx, l, l.server.ReadEmbeddingStructure, request)
}

func (l *embeddingLoopback) DropEmbeddingGeneration(ctx context.Context, request *protocol.DropEmbeddingGenerationRequest, _ ...grpc.CallOption) (*protocol.EmbeddingWriteResult, error) {
	return loop(ctx, l, l.server.DropEmbeddingGeneration, request)
}

func (l *embeddingLoopback) ScanEmbeddingSource(ctx context.Context, request *protocol.ScanEmbeddingSourceRequest, _ ...grpc.CallOption) (*protocol.EmbeddingSourcePage, error) {
	return loop(ctx, l, l.server.ScanEmbeddingSource, request)
}

func (l *embeddingLoopback) ReadEmbeddingSourceRows(ctx context.Context, request *protocol.ReadEmbeddingSourceRowsRequest, _ ...grpc.CallOption) (*protocol.EmbeddingSourceRows, error) {
	return loop(ctx, l, l.server.ReadEmbeddingSourceRows, request)
}

func (l *embeddingLoopback) ReadEmbeddingBatchState(ctx context.Context, request *protocol.ReadEmbeddingBatchStateRequest, _ ...grpc.CallOption) (*protocol.EmbeddingBatchState, error) {
	return loop(ctx, l, l.server.ReadEmbeddingBatchState, request)
}

func (l *embeddingLoopback) CommitEmbeddingBatch(ctx context.Context, request *protocol.CommitEmbeddingBatchRequest, _ ...grpc.CallOption) (*protocol.EmbeddingWriteResult, error) {
	return loop(ctx, l, l.server.CommitEmbeddingBatch, request)
}

func (l *embeddingLoopback) ReadEmbeddingVerification(ctx context.Context, request *protocol.ReadEmbeddingVerificationRequest, _ ...grpc.CallOption) (*protocol.EmbeddingVerificationPage, error) {
	return loop(ctx, l, l.server.ReadEmbeddingVerification, request)
}

func (l *embeddingLoopback) SearchEmbeddings(ctx context.Context, request *protocol.SearchEmbeddingsRequest, _ ...grpc.CallOption) (*protocol.EmbeddingSearchResult, error) {
	return loop(ctx, l, l.server.SearchEmbeddings, request)
}

// newEmbeddingLoopback is a client and a server after a host listed
// hostFeatures in Describe.
func newEmbeddingLoopback(owner provider.EmbeddingStore, hostFeatures ...string) (*Client, *embeddingLoopback) {
	server := negotiatedServer(Server{Description: vectorsDescription(), EmbeddingStore: owner}, hostFeatures...)
	peer := &embeddingLoopback{server: server}
	return &Client{description: server.negotiated(), rpc: peer}, peer
}

func generation(identity string) *protocol.EmbeddingGeneration {
	return &protocol.EmbeddingGeneration{Identity: identity, SpecDigest: "spec-1", Dimension: 2, TargetTable: "document_chunks",
		TargetColumn: "embedding", SourceTable: "documents", ConsistencyMode: "immutable", CreatedAt: &protocol.TimestampValue{Seconds: 1700000000}}
}

func run(id string, token int64) *protocol.EmbeddingRun {
	return &protocol.EmbeddingRun{Id: id, GenerationIdentity: "gen-1", Phase: "boundary_captured", Status: "running", FencingToken: token,
		Progress: &protocol.EmbeddingProgress{}, UpdatedAt: &protocol.TimestampValue{Seconds: 1700000000, Nanos: 123456789}}
}

func everythingScope() *protocol.EmbeddingStateScope {
	return &protocol.EmbeddingStateScope{Scope: &protocol.EmbeddingStateScope_Everything{Everything: &protocol.EmbeddingStateScope_AllRecords{}}}
}

func readEverything(c *qt.C, client *Client) *protocol.EmbeddingState {
	state, err := client.ReadEmbeddingState(c.Context(), &protocol.ReadEmbeddingStateRequest{SessionId: "s", Scope: everythingScope(), MaxRecords: 100})
	c.Assert(err, qt.IsNil)
	return state
}

func mutation(id, expected string, records *protocol.EmbeddingRecords) *protocol.MutateEmbeddingStateRequest {
	return &protocol.MutateEmbeddingStateRequest{SessionId: "s", RequestId: id, Scope: everythingScope(), ExpectedToken: expected, Records: records}
}

// A host that lists the store sees the engine's declaration and can call
// it. A host that does not list it gets neither, its client refuses every
// call before sending it, and the SDK server refuses one that reaches it.
func TestEmbeddingStoreIsTwoWay(t *testing.T) {
	c := qt.New(t)
	listed := newServerAdapter(Server{Description: vectorsDescription(), Sessions: &sessionOwner{}, EmbeddingStore: newMemoryEmbeddings()})
	described, err := listed.Describe(c.Context(), &protocol.DescribeRequest{Revision: provider.Revision, HostContract: provider.Contract(),
		HostFeatures: []string{provider.EmbeddingStoreFeature}})
	c.Assert(err, qt.IsNil)
	c.Assert(described.Features, qt.DeepEquals, []string{provider.EmbeddingStoreFeature})
	c.Assert(described.Engines[0].Embeddings.GetConsistencyModes(), qt.DeepEquals, []string{"immutable"})
	unlisted := newServerAdapter(Server{Description: vectorsDescription(), Sessions: &sessionOwner{}, EmbeddingStore: newMemoryEmbeddings()})
	described, err = unlisted.Describe(c.Context(), &protocol.DescribeRequest{Revision: provider.Revision, HostContract: provider.Contract()})
	c.Assert(err, qt.IsNil)
	c.Assert(described.Features, qt.HasLen, 0)
	c.Assert(described.Engines[0].Features, qt.HasLen, 0)
	c.Assert(described.Engines[0].Embeddings, qt.IsNil)

	client, peer := newEmbeddingLoopback(newMemoryEmbeddings())
	_, err = client.ReadEmbeddingState(c.Context(), &protocol.ReadEmbeddingStateRequest{SessionId: "s", Scope: everythingScope(), MaxRecords: 1})
	c.Assert(err, qt.ErrorMatches, `.*embeddings\.store was not negotiated`)
	_, err = client.DescribeEmbeddingTarget(c.Context(), &protocol.DescribeEmbeddingTargetRequest{Engine: "vectors", Spec: vectorsSpec()})
	c.Assert(err, qt.ErrorMatches, `.*embeddings\.store was not negotiated`)
	c.Assert(peer.calls, qt.Equals, 0)
	_, err = peer.server.ReadEmbeddingState(c.Context(), &protocol.ReadEmbeddingStateRequest{SessionId: "s", Scope: everythingScope(), MaxRecords: 1})
	c.Assert(status.Code(err), qt.Equals, codes.FailedPrecondition)
	_, err = peer.server.DescribeEmbeddingTarget(c.Context(), &protocol.DescribeEmbeddingTargetRequest{Engine: "vectors", Spec: vectorsSpec()})
	c.Assert(status.Code(err), qt.Equals, codes.InvalidArgument)
}

// The service and its implementation are advertised together, and a server
// without an owner answers that the store is unavailable.
func TestEmbeddingStoreAdvertisementMatchesItsImplementation(t *testing.T) {
	c := qt.New(t)
	missing := Server{Description: vectorsDescription(), Sessions: &sessionOwner{}}
	c.Assert(missing.Validate(), qt.ErrorMatches, `.*embeddings\.store advertisement differs from implementation`)
	unadvertised := Server{Description: vectorsDescription(), Sessions: &sessionOwner{}, EmbeddingStore: newMemoryEmbeddings()}
	unadvertised.Description.Features = nil
	c.Assert(unadvertised.Validate(), qt.ErrorIs, provider.ErrProtocol)
	served := Server{Description: vectorsDescription(), Sessions: &sessionOwner{}, EmbeddingStore: newMemoryEmbeddings()}
	c.Assert(served.Validate(), qt.IsNil)
	client, _ := newEmbeddingLoopback(nil, provider.EmbeddingStoreFeature)
	_, err := client.ReadEmbeddingState(c.Context(), &protocol.ReadEmbeddingStateRequest{SessionId: "s", Scope: everythingScope(), MaxRecords: 1})
	c.Assert(err, qt.ErrorMatches, `provider read embedding state failed \(Unimplemented\)`)
}

// A mutation lands only on the state it was read from: a second writer with
// the old token gets a CONFLICT, writes nothing, and succeeds once it reads
// again. An applied mutation answers each record as a later read returns it.
func TestEmbeddingStoreConformanceComparesAndWrites(t *testing.T) {
	c := qt.New(t)
	owner := newMemoryEmbeddings()
	client, _ := newEmbeddingLoopback(owner, provider.EmbeddingStoreFeature)
	read := readEverything(c, client)
	c.Assert(read.Records.GetRuns(), qt.HasLen, 0)
	created := &protocol.EmbeddingRecords{Generations: []*protocol.EmbeddingGeneration{generation("gen-1")},
		Runs: []*protocol.EmbeddingRun{run("run-1", 1)}}
	applied, err := client.MutateEmbeddingState(c.Context(), mutation("m-1", read.Token, created))
	c.Assert(err, qt.IsNil)
	c.Assert(applied.Written.Runs[0].UpdatedAt.Nanos, qt.Equals, int32(123456000))
	stale, err := client.MutateEmbeddingState(c.Context(), mutation("m-2", read.Token,
		&protocol.EmbeddingRecords{Runs: []*protocol.EmbeddingRun{run("run-1", 2)}}))
	c.Assert(err, qt.ErrorIs, provider.ErrEmbeddingConflict)
	c.Assert(stale.Outcome, qt.Equals, protocol.EmbeddingWriteResult_CONFLICT)
	again := readEverything(c, client)
	c.Assert(again.Token, qt.Not(qt.Equals), read.Token)
	c.Assert(again.Records.Runs[0].FencingToken, qt.Equals, int64(1))
	c.Assert(proto.Equal(again.Records.Runs[0], applied.Written.Runs[0]), qt.IsTrue)
	_, err = client.MutateEmbeddingState(c.Context(), mutation("m-3", again.Token,
		&protocol.EmbeddingRecords{Runs: []*protocol.EmbeddingRun{run("run-1", 2)}}))
	c.Assert(err, qt.IsNil)
	c.Assert(readEverything(c, client).Records.Runs[0].FencingToken, qt.Equals, int64(2))
}

func chunkWrite(id string, ordinal uint32, first float32) *protocol.EmbeddingTargetWrite {
	return &protocol.EmbeddingTargetWrite{Key: vectorsKey(id), Ordinal: ordinal, Generation: "gen-1", InputHash: "hash-" + id,
		Kind: protocol.EmbeddingWriteKind_EMBEDDING_WRITE_KIND_UPSERT, Vector: []float32{first, 0}}
}

func batchRead(c *qt.C, client *Client, keys ...string) *protocol.EmbeddingBatchState {
	request := &protocol.ReadEmbeddingBatchStateRequest{SessionId: "s", Spec: vectorsSpec(), RunId: "run-1"}
	for _, key := range keys {
		request.Keys = append(request.Keys, vectorsKey(key))
	}
	state, err := client.ReadEmbeddingBatchState(c.Context(), request)
	c.Assert(err, qt.IsNil)
	return state
}

func commit(id, expected string, checkpoint *protocol.EmbeddingRun, writes ...*protocol.EmbeddingTargetWrite) *protocol.CommitEmbeddingBatchRequest {
	request := &protocol.CommitEmbeddingBatchRequest{SessionId: "s", RequestId: id, Spec: vectorsSpec(), RunId: "run-1", ExpectedToken: expected,
		Run: checkpoint, Writes: writes}
	for _, write := range writes {
		if !slices.ContainsFunc(request.Keys, func(key *protocol.EmbeddingKey) bool { return proto.Equal(key, write.Key) }) {
			request.Keys = append(request.Keys, write.Key)
		}
	}
	return request
}

// A batch's sets and its checkpoint land together, and a batch decided on a
// state another batch has since changed writes neither.
func TestEmbeddingStoreConformanceCommitsABatchWithItsCheckpoint(t *testing.T) {
	c := qt.New(t)
	owner := newMemoryEmbeddings()
	owner.runs["run-1"] = run("run-1", 3)
	client, _ := newEmbeddingLoopback(owner, provider.EmbeddingStoreFeature)
	before := batchRead(c, client, "1", "2")
	c.Assert(before.Held, qt.HasLen, 0)
	checkpoint := run("run-1", 3)
	checkpoint.Cursor, checkpoint.Progress.BatchesCommitted = []string{"2"}, 1
	_, err := client.CommitEmbeddingBatch(c.Context(), commit("c-1", before.Token, checkpoint,
		chunkWrite("1", 0, 0.1), chunkWrite("1", 1, 0.2), chunkWrite("2", 0, 0.9)))
	c.Assert(err, qt.IsNil)
	after := batchRead(c, client, "1", "2")
	c.Assert(after.Held, qt.HasLen, 2)
	c.Assert(after.Run.Cursor, qt.DeepEquals, []string{"2"})
	late := run("run-1", 3)
	late.Cursor = []string{"9"}
	result, err := client.CommitEmbeddingBatch(c.Context(), commit("c-2", before.Token, late, chunkWrite("2", 0, 0.5)))
	c.Assert(err, qt.ErrorIs, provider.ErrEmbeddingConflict)
	c.Assert(result.Outcome, qt.Equals, protocol.EmbeddingWriteResult_CONFLICT)
	c.Assert(proto.Equal(batchRead(c, client, "1", "2"), after), qt.IsTrue)
	c.Assert(owner.target, qt.HasLen, 3)
}

// A write the SDK can refuse is refused before it is sent: a value that is
// not finite, and a set with a gap.
func TestEmbeddingStoreRefusesAMalformedBatchBeforeSendingIt(t *testing.T) {
	c := qt.New(t)
	client, peer := newEmbeddingLoopback(newMemoryEmbeddings(), provider.EmbeddingStoreFeature)
	tests := []*protocol.CommitEmbeddingBatchRequest{
		commit("c-1", "t", run("run-1", 1), chunkWrite("1", 0, float32(math.Inf(1)))),
		commit("c-2", "t", run("run-1", 1), chunkWrite("1", 0, 0.1), chunkWrite("1", 2, 0.2)),
	}
	for _, request := range tests {
		result, err := client.CommitEmbeddingBatch(c.Context(), request)
		c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
		c.Assert(result, qt.IsNil)
	}
	c.Assert(peer.calls, qt.Equals, 0)
}

// A write whose reply is lost is unknown, whatever the owner did, and the
// client sends it once. So is a reply the SDK server refuses.
func TestEmbeddingStoreLostWriteIsUnknownAndNotResent(t *testing.T) {
	c := qt.New(t)
	owner := newMemoryEmbeddings()
	client, peer := newEmbeddingLoopback(owner, provider.EmbeddingStoreFeature)
	read := readEverything(c, client)
	peer.lost = true
	result, err := client.MutateEmbeddingState(c.Context(), mutation("m-1", read.Token,
		&protocol.EmbeddingRecords{Generations: []*protocol.EmbeddingGeneration{generation("gen-1")}}))
	c.Assert(err, qt.ErrorIs, provider.ErrOutcomeUnknown)
	c.Assert(err, qt.Not(qt.ErrorIs), provider.ErrEmbeddingConflict)
	c.Assert(result, qt.IsNil)
	c.Assert(peer.calls, qt.Equals, 2)
	c.Assert(owner.generations, qt.HasLen, 1)
	careless, carelessPeer := newEmbeddingLoopback(carelessEmbeddings{newMemoryEmbeddings()}, provider.EmbeddingStoreFeature)
	read = readEverything(c, careless)
	result, err = careless.MutateEmbeddingState(c.Context(), mutation("m-2", read.Token,
		&protocol.EmbeddingRecords{Generations: []*protocol.EmbeddingGeneration{generation("gen-1")}}))
	c.Assert(err, qt.ErrorIs, provider.ErrOutcomeUnknown)
	c.Assert(err, qt.ErrorMatches, `(?s).*provider mutate embedding state failed \(Internal\).*`)
	c.Assert(result, qt.IsNil)
	c.Assert(carelessPeer.calls, qt.Equals, 2)
}

// carelessEmbeddings answers an applied mutation with records of another key.
type carelessEmbeddings struct{ *memoryEmbeddings }

func (o carelessEmbeddings) MutateEmbeddingState(ctx context.Context, request *protocol.MutateEmbeddingStateRequest) (*protocol.EmbeddingWriteResult, error) {
	result, err := o.memoryEmbeddings.MutateEmbeddingState(ctx, request)
	result.Written.Generations[0].Identity = "gen-other"
	return result, err
}

// Each event takes the run's next sequence, a page resumes after the last
// sequence it held, and an event of a run that is not there is refused as
// absent.
func TestEmbeddingStoreConformanceAssignsEventSequences(t *testing.T) {
	c := qt.New(t)
	owner := newMemoryEmbeddings()
	owner.runs["run-1"] = run("run-1", 1)
	client, _ := newEmbeddingLoopback(owner, provider.EmbeddingStoreFeature)
	for _, want := range []int64{1, 2, 3} {
		result, err := client.AppendEmbeddingEvent(c.Context(), &protocol.AppendEmbeddingEventRequest{SessionId: "s", RequestId: "e",
			Event: &protocol.EmbeddingEvent{RunId: "run-1", Kind: "checkpoint", Counts: &protocol.EmbeddingEventCounts{BatchesCommitted: want}}})
		c.Assert(err, qt.IsNil)
		c.Assert(result.Sequence, qt.Equals, want)
	}
	first, err := client.ReadEmbeddingEvents(c.Context(), &protocol.ReadEmbeddingEventsRequest{SessionId: "s", RunId: "run-1", MaxEvents: 2})
	c.Assert(err, qt.IsNil)
	c.Assert(first.Complete, qt.IsFalse)
	rest, err := client.ReadEmbeddingEvents(c.Context(), &protocol.ReadEmbeddingEventsRequest{SessionId: "s", RunId: "run-1",
		AfterSequence: first.Events[1].Sequence, MaxEvents: 2})
	c.Assert(err, qt.IsNil)
	c.Assert(rest.Complete, qt.IsTrue)
	c.Assert(rest.Events[0].Counts.BatchesCommitted, qt.Equals, int64(3))
	refused, err := client.AppendEmbeddingEvent(c.Context(), &protocol.AppendEmbeddingEventRequest{SessionId: "s", RequestId: "e",
		Event: &protocol.EmbeddingEvent{RunId: "run-9", Kind: "checkpoint"}})
	var written *provider.EmbeddingWriteError
	c.Assert(err, qt.ErrorAs, &written)
	c.Assert(written.Reason, qt.Equals, protocol.EmbeddingRefusal_ABSENT)
	c.Assert(refused.Outcome, qt.Equals, protocol.EmbeddingWriteResult_REFUSED)
}

// A keyset scan pages in key order and resumes after its cursor until it is
// done, and a reread answers only the rows still present.
func TestEmbeddingStoreConformancePagesTheSourceAndRereadsIt(t *testing.T) {
	c := qt.New(t)
	owner := newMemoryEmbeddings()
	client, _ := newEmbeddingLoopback(owner, provider.EmbeddingStoreFeature)
	source := vectorsSpec().Source
	var keys []string
	var after *protocol.EmbeddingKey
	for range 3 {
		page, err := client.ScanEmbeddingSource(c.Context(), &protocol.ScanEmbeddingSourceRequest{SessionId: "s", Source: source, After: after, MaxRows: 2})
		c.Assert(err, qt.IsNil)
		for _, row := range page.Rows {
			keys = append(keys, row.Key.Components[0])
		}
		after = page.Cursor
	}
	c.Assert(keys, qt.DeepEquals, []string{"1", "2", "3", "4", "5"})
	delete(owner.source, "3")
	present, err := client.ReadEmbeddingSourceRows(c.Context(), &protocol.ReadEmbeddingSourceRowsRequest{SessionId: "s", Source: source,
		Keys: []*protocol.EmbeddingKey{vectorsKey("2"), vectorsKey("3")}})
	c.Assert(err, qt.IsNil)
	c.Assert(present.Rows, qt.HasLen, 1)
	c.Assert(present.Rows[0].Fields[0].GetStringValue(), qt.Equals, "beta")
}

// The verification walk pages by key: a page that cannot hold a key's whole
// set stops before it, and the next page starts with it.
func TestEmbeddingStoreConformancePagesVerificationByKey(t *testing.T) {
	c := qt.New(t)
	owner := newMemoryEmbeddings()
	owner.target = []storedChunk{
		{key: "1", ordinal: 0, write: chunkWrite("1", 0, 0.1)}, {key: "2", ordinal: 0, write: chunkWrite("2", 0, 0.2)},
		{key: "2", ordinal: 1, write: chunkWrite("2", 1, 0.3)}, {key: "3", ordinal: 0, write: chunkWrite("3", 0, 0.4)},
	}
	client, _ := newEmbeddingLoopback(owner, provider.EmbeddingStoreFeature)
	request := &protocol.ReadEmbeddingVerificationRequest{SessionId: "s", Spec: vectorsSpec(), MaxRows: 2}
	first, err := client.ReadEmbeddingVerification(c.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(first.Rows, qt.HasLen, 1)
	request.PageToken = first.NextPageToken
	second, err := client.ReadEmbeddingVerification(c.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(second.Rows, qt.HasLen, 2)
	c.Assert(second.Rows[1].Stored.Ordinal, qt.Equals, uint32(1))
	request.PageToken = second.NextPageToken
	last, err := client.ReadEmbeddingVerification(c.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(last.Complete, qt.IsTrue)
	c.Assert(last.Rows[0].Stored.Dimension, qt.Equals, uint32(2))
}

// The storage calls answer what a lifecycle verb reads: the target objects
// offline, relations, a plan's measurement, the index build and its repeat,
// the structure with its count, a search, and a drop that refuses a table it
// cannot prove is the generation's.
func TestEmbeddingStoreConformanceAnswersTheStorageCalls(t *testing.T) {
	c := qt.New(t)
	owner := newMemoryEmbeddings()
	owner.target = []storedChunk{{key: "1", write: chunkWrite("1", 0, 0.1)}, {key: "2", write: chunkWrite("2", 0, 0.8)}}
	client, _ := newEmbeddingLoopback(owner, provider.EmbeddingStoreFeature)
	objects, err := client.DescribeEmbeddingTarget(c.Context(), &protocol.DescribeEmbeddingTargetRequest{Engine: "vectors", Spec: vectorsSpec()})
	c.Assert(err, qt.IsNil)
	c.Assert(objects.OwnsTable, qt.IsTrue)
	refusedSpec := vectorsSpec()
	refusedSpec.Target.Metric = protocol.EmbeddingTarget_INNER_PRODUCT
	refused, err := client.DescribeEmbeddingTarget(c.Context(), &protocol.DescribeEmbeddingTargetRequest{Engine: "vectors", Spec: refusedSpec})
	c.Assert(err, qt.IsNil)
	c.Assert(refused.Refusal.Reason, qt.Equals, protocol.EmbeddingRefusal_UNSUPPORTED)
	relations, err := client.ResolveEmbeddingRelations(c.Context(), &protocol.ResolveEmbeddingRelationsRequest{SessionId: "s",
		Relations: []*protocol.Reference{{Engine: "vectors", Kind: "table", Name: "documents"}}})
	c.Assert(err, qt.IsNil)
	c.Assert(relations.Relations[0].Schema, qt.Equals, "app")
	measured, err := client.MeasureEmbedding(c.Context(), &protocol.MeasureEmbeddingRequest{SessionId: "s", Spec: vectorsSpec()})
	c.Assert(err, qt.IsNil)
	c.Assert(measured.Rows, qt.Equals, int64(5))
	_, err = client.PrepareEmbeddingStore(c.Context(), &protocol.PrepareEmbeddingStoreRequest{SessionId: "s", RequestId: "p"})
	c.Assert(err, qt.IsNil)
	_, err = client.PrepareEmbeddingTarget(c.Context(), &protocol.PrepareEmbeddingTargetRequest{SessionId: "s", RequestId: "p", Spec: vectorsSpec(),
		Mode: protocol.PrepareEmbeddingTargetRequest_CREATE})
	c.Assert(err, qt.IsNil)
	var outcomes []protocol.EmbeddingWriteResult_IndexOutcome
	for range 2 {
		built, err := client.BuildEmbeddingIndex(c.Context(), &protocol.BuildEmbeddingIndexRequest{SessionId: "s", RequestId: "i", Spec: vectorsSpec()})
		c.Assert(err, qt.IsNil)
		outcomes = append(outcomes, built.IndexOutcome)
	}
	c.Assert(outcomes, qt.DeepEquals, []protocol.EmbeddingWriteResult_IndexOutcome{protocol.EmbeddingWriteResult_BUILT, protocol.EmbeddingWriteResult_ALREADY_VALID})
	location := &protocol.EmbeddingLocation{Generation: "gen-1", Table: vectorsTable("document_chunks"), Column: "embedding", Dimension: 2,
		Names: vectorsSpec().Names}
	structure, err := client.ReadEmbeddingStructure(c.Context(), &protocol.ReadEmbeddingStructureRequest{SessionId: "s", Location: location, CountRows: true})
	c.Assert(err, qt.IsNil)
	c.Assert(structure.Rows, qt.Equals, int64(2))
	c.Assert(structure.IndexValid, qt.IsTrue)
	found, err := client.SearchEmbeddings(c.Context(), &protocol.SearchEmbeddingsRequest{SessionId: "s", Spec: vectorsSpec(), Vector: []float32{0.7, 0}, Depth: 1})
	c.Assert(err, qt.IsNil)
	c.Assert(found.Exact[0].Components, qt.DeepEquals, []string{"2"})
	c.Assert(found.IndexUsed, qt.IsTrue)
	owner.ownsTable = false
	dropped, err := client.DropEmbeddingGeneration(c.Context(), &protocol.DropEmbeddingGenerationRequest{SessionId: "s", RequestId: "d",
		Location: location, Storage: protocol.DropEmbeddingGenerationRequest_TABLE})
	var written *provider.EmbeddingWriteError
	c.Assert(err, qt.ErrorAs, &written)
	c.Assert(written.Reason, qt.Equals, protocol.EmbeddingRefusal_NOT_OWNED)
	c.Assert(dropped.Refusal.Message, qt.Equals, "app.document_chunks carries another mark")
	c.Assert(owner.target, qt.HasLen, 2)
}
