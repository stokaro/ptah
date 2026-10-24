package runtime

import (
	"context"
	"time"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"
)

const (
	// EmbeddingWriteTimeout bounds a state mutation or a batch commit that has
	// no earlier deadline. The owner may run either again after an abort that
	// proved nothing committed.
	EmbeddingWriteTimeout = 2 * time.Minute
	// EmbeddingStorageTimeout bounds a call that has no earlier deadline and
	// may read or change a whole table: preparing storage, dropping it,
	// measuring, counting, scanning, verifying and searching.
	EmbeddingStorageTimeout = 10 * time.Minute
	// EmbeddingIndexTimeout bounds an index build that has no earlier
	// deadline. An index over a large corpus can take hours; a caller that
	// knows better sets an earlier deadline.
	EmbeddingIndexTimeout = 12 * time.Hour
)

// embeddingCall is a session call of the embedding store.
func embeddingCall[Q proto.Message, R proto.Message](operation string, timeout time.Duration,
	validateInput func(Q) error, validateOutput func(Q, R) error,
) sessionCall[Q, R] {
	return sessionCall[Q, R]{
		feature: provider.EmbeddingStoreFeature, operation: operation, timeout: timeout,
		validateInput: validateInput, validateOutput: validateOutput,
	}
}

// embeddingWrite is a session call of the embedding store that writes: a
// lost or rejected reply leaves its effect unknown.
func embeddingWrite[Q proto.Message](operation string, timeout time.Duration,
	validateInput func(Q) error, validateOutput func(Q, *protocol.EmbeddingWriteResult) error,
) sessionCall[Q, *protocol.EmbeddingWriteResult] {
	call := embeddingCall(operation, timeout, validateInput, validateOutput)
	call.uncertain = unknownEmbeddingWrite
	return call
}

func unknownEmbeddingWrite() error {
	return &provider.EmbeddingWriteError{Outcome: protocol.EmbeddingWriteResult_UNKNOWN}
}

var (
	embeddingPrepareStore = embeddingWrite("prepare embedding store", EmbeddingStorageTimeout,
		provider.ValidatePrepareEmbeddingStoreRequest, provider.ValidatePreparedEmbeddingStore)
	embeddingReadState = embeddingCall("read embedding state", CallTimeout,
		provider.ValidateReadEmbeddingStateRequest, provider.ValidateEmbeddingState)
	embeddingMutateState = embeddingWrite("mutate embedding state", EmbeddingWriteTimeout,
		provider.ValidateMutateEmbeddingStateRequest, provider.ValidateMutatedEmbeddingState)
	embeddingAppendEvent = embeddingWrite("append embedding event", CallTimeout,
		provider.ValidateAppendEmbeddingEventRequest, provider.ValidateAppendedEmbeddingEvent)
	embeddingReadEvents = embeddingCall("read embedding events", CallTimeout,
		provider.ValidateReadEmbeddingEventsRequest, provider.ValidateEmbeddingEventPage)
	embeddingResolveRelations = embeddingCall("resolve embedding relations", CallTimeout,
		provider.ValidateResolveEmbeddingRelationsRequest, provider.ValidateEmbeddingRelations)
	embeddingMeasure = embeddingCall("measure embedding", EmbeddingStorageTimeout,
		provider.ValidateMeasureEmbeddingRequest, provider.ValidateEmbeddingMeasurement)
	embeddingPrepareTarget = embeddingWrite("prepare embedding target", EmbeddingStorageTimeout,
		provider.ValidatePrepareEmbeddingTargetRequest, provider.ValidatePreparedEmbeddingTarget)
	embeddingBuildIndex = embeddingWrite("build embedding index", EmbeddingIndexTimeout,
		provider.ValidateBuildEmbeddingIndexRequest, provider.ValidateBuiltEmbeddingIndex)
	embeddingReadStructure = embeddingCall("read embedding structure", EmbeddingStorageTimeout,
		provider.ValidateReadEmbeddingStructureRequest, provider.ValidateEmbeddingStructure)
	embeddingDropGeneration = embeddingWrite("drop embedding generation", EmbeddingStorageTimeout,
		provider.ValidateDropEmbeddingGenerationRequest, provider.ValidateDroppedEmbeddingGeneration)
	embeddingScanSource = embeddingCall("scan embedding source", EmbeddingStorageTimeout,
		provider.ValidateScanEmbeddingSourceRequest, provider.ValidateEmbeddingSourcePage)
	embeddingReadSourceRows = embeddingCall("read embedding source rows", EmbeddingStorageTimeout,
		provider.ValidateReadEmbeddingSourceRowsRequest, provider.ValidateEmbeddingSourceRows)
	embeddingReadBatchState = embeddingCall("read embedding batch state", CallTimeout,
		provider.ValidateReadEmbeddingBatchStateRequest, provider.ValidateEmbeddingBatchState)
	embeddingCommitBatch = embeddingWrite("commit embedding batch", EmbeddingWriteTimeout,
		provider.ValidateCommitEmbeddingBatchRequest, provider.ValidateCommittedEmbeddingBatch)
	embeddingReadVerification = embeddingCall("read embedding verification", EmbeddingStorageTimeout,
		provider.ValidateReadEmbeddingVerificationRequest, provider.ValidateEmbeddingVerificationPage)
	embeddingSearch = embeddingCall("search embeddings", EmbeddingStorageTimeout,
		provider.ValidateSearchEmbeddingsRequest, provider.ValidateEmbeddingSearchResult)
)

// writeResult returns a validated write result with the error that a
// confirmed outcome other than APPLIED carries, as ApplySeed does.
func writeResult(result *protocol.EmbeddingWriteResult, err error) (*protocol.EmbeddingWriteResult, error) {
	if err != nil {
		return nil, err
	}
	return result, provider.CheckEmbeddingWrite(result)
}

// PrepareEmbeddingStore creates a missing run-state store or verifies an
// existing one. A confirmed outcome other than APPLIED comes with a
// provider.EmbeddingWriteError; a lost or invalid reply exposes no result
// and leaves the outcome unknown, and this method never retries it.
func (c *Client) PrepareEmbeddingStore(ctx context.Context, request *protocol.PrepareEmbeddingStoreRequest) (*protocol.EmbeddingWriteResult, error) {
	return writeResult(embeddingPrepareStore.call(ctx, c, request, c.rpc.PrepareEmbeddingStore))
}

// ReadEmbeddingState reads one scope of the run-state store as one snapshot
// with its token, or returns an error and no record.
func (c *Client) ReadEmbeddingState(ctx context.Context, request *protocol.ReadEmbeddingStateRequest) (*protocol.EmbeddingState, error) {
	return embeddingReadState.call(ctx, c, request, c.rpc.ReadEmbeddingState)
}

// MutateEmbeddingState writes the records a decision changed when the scope
// still has the token it was read with. A CONFLICT is returned with an error
// that matches provider.ErrEmbeddingConflict; the caller reads again and
// decides again. Other outcomes follow PrepareEmbeddingStore.
func (c *Client) MutateEmbeddingState(ctx context.Context, request *protocol.MutateEmbeddingStateRequest) (*protocol.EmbeddingWriteResult, error) {
	return writeResult(embeddingMutateState.call(ctx, c, request, c.rpc.MutateEmbeddingState))
}

// AppendEmbeddingEvent records one event of an existing run and answers the
// sequence it took. Outcomes follow PrepareEmbeddingStore.
func (c *Client) AppendEmbeddingEvent(ctx context.Context, request *protocol.AppendEmbeddingEventRequest) (*protocol.EmbeddingWriteResult, error) {
	return writeResult(embeddingAppendEvent.call(ctx, c, request, c.rpc.AppendEmbeddingEvent))
}

// ReadEmbeddingEvents reads one page of a run's events in sequence order.
func (c *Client) ReadEmbeddingEvents(ctx context.Context, request *protocol.ReadEmbeddingEventsRequest) (*protocol.EmbeddingEventPage, error) {
	return embeddingReadEvents.call(ctx, c, request, c.rpc.ReadEmbeddingEvents)
}

// DescribeEmbeddingTarget asks offline whether an engine can hold a
// specification, and what a generation needs on it. An invalid reply and
// cancellation expose nothing.
func (c *Client) DescribeEmbeddingTarget(ctx context.Context, request *protocol.DescribeEmbeddingTargetRequest) (*protocol.EmbeddingTargetObjects, error) {
	return callOffline(ctx, c.description, proto.CloneOf(request), "describe embedding target",
		provider.ValidateDescribeEmbeddingTargetRequest, provider.ValidateEmbeddingTargetObjects, c.rpc.DescribeEmbeddingTarget)
}

// ResolveEmbeddingRelations answers the relation each spelling names on the
// session's server.
func (c *Client) ResolveEmbeddingRelations(ctx context.Context, request *protocol.ResolveEmbeddingRelationsRequest) (*protocol.EmbeddingRelations, error) {
	return embeddingResolveRelations.call(ctx, c, request, c.rpc.ResolveEmbeddingRelations)
}

// MeasureEmbedding answers what a plan reports about the database.
func (c *Client) MeasureEmbedding(ctx context.Context, request *protocol.MeasureEmbeddingRequest) (*protocol.EmbeddingMeasurement, error) {
	return embeddingMeasure.call(ctx, c, request, c.rpc.MeasureEmbedding)
}

// PrepareEmbeddingTarget creates a generation's storage, or verifies that a
// prepared one is complete. Outcomes follow PrepareEmbeddingStore.
func (c *Client) PrepareEmbeddingTarget(ctx context.Context, request *protocol.PrepareEmbeddingTargetRequest) (*protocol.EmbeddingWriteResult, error) {
	return writeResult(embeddingPrepareTarget.call(ctx, c, request, c.rpc.PrepareEmbeddingTarget))
}

// BuildEmbeddingIndex builds the declared index and says what the build did.
// Outcomes follow PrepareEmbeddingStore.
func (c *Client) BuildEmbeddingIndex(ctx context.Context, request *protocol.BuildEmbeddingIndexRequest) (*protocol.EmbeddingWriteResult, error) {
	return writeResult(embeddingBuildIndex.call(ctx, c, request, c.rpc.BuildEmbeddingIndex))
}

// ReadEmbeddingStructure reads a generation's storage as the engine reports
// it.
func (c *Client) ReadEmbeddingStructure(ctx context.Context, request *protocol.ReadEmbeddingStructureRequest) (*protocol.EmbeddingStructure, error) {
	return embeddingReadStructure.call(ctx, c, request, c.rpc.ReadEmbeddingStructure)
}

// DropEmbeddingGeneration drops a retired generation's storage. A refusal
// drops nothing; an UNKNOWN drop is finished by running it again. Outcomes
// follow PrepareEmbeddingStore.
func (c *Client) DropEmbeddingGeneration(ctx context.Context, request *protocol.DropEmbeddingGenerationRequest) (*protocol.EmbeddingWriteResult, error) {
	return writeResult(embeddingDropGeneration.call(ctx, c, request, c.rpc.DropEmbeddingGeneration))
}

// ScanEmbeddingSource reads one keyset page of the source.
func (c *Client) ScanEmbeddingSource(ctx context.Context, request *protocol.ScanEmbeddingSourceRequest) (*protocol.EmbeddingSourcePage, error) {
	return embeddingScanSource.call(ctx, c, request, c.rpc.ScanEmbeddingSource)
}

// ReadEmbeddingSourceRows rereads source rows by key. A requested key missing
// from the reply is a row that is gone.
func (c *Client) ReadEmbeddingSourceRows(ctx context.Context, request *protocol.ReadEmbeddingSourceRowsRequest) (*protocol.EmbeddingSourceRows, error) {
	return embeddingReadSourceRows.call(ctx, c, request, c.rpc.ReadEmbeddingSourceRows)
}

// ReadEmbeddingBatchState reads the run and what the target holds at a
// batch's keys, with the token a commit compares.
func (c *Client) ReadEmbeddingBatchState(ctx context.Context, request *protocol.ReadEmbeddingBatchStateRequest) (*protocol.EmbeddingBatchState, error) {
	return embeddingReadBatchState.call(ctx, c, request, c.rpc.ReadEmbeddingBatchState)
}

// CommitEmbeddingBatch writes a batch's sets and its checkpointed run in one
// atomic operation when the run and the keys still have the token they were
// read with. Outcomes follow MutateEmbeddingState.
func (c *Client) CommitEmbeddingBatch(ctx context.Context, request *protocol.CommitEmbeddingBatchRequest) (*protocol.EmbeddingWriteResult, error) {
	return writeResult(embeddingCommitBatch.call(ctx, c, request, c.rpc.CommitEmbeddingBatch))
}

// ReadEmbeddingVerification reads one page of the verification walk.
func (c *Client) ReadEmbeddingVerification(ctx context.Context, request *protocol.ReadEmbeddingVerificationRequest) (*protocol.EmbeddingVerificationPage, error) {
	return embeddingReadVerification.call(ctx, c, request, c.rpc.ReadEmbeddingVerification)
}

// SearchEmbeddings returns the keys nearest to one vector, through the index
// and by exhaustive scan. A failed search is a result with a reason, not an
// error.
func (c *Client) SearchEmbeddings(ctx context.Context, request *protocol.SearchEmbeddingsRequest) (*protocol.EmbeddingSearchResult, error) {
	return embeddingSearch.call(ctx, c, request, c.rpc.SearchEmbeddings)
}

// embeddingStore returns the owner, or the refusal of a server without one.
func (s *serverAdapter) embeddingStore() (provider.EmbeddingStore, error) {
	if nilService(s.server.EmbeddingStore) {
		return nil, unavailable("the embedding store")
	}
	return s.server.EmbeddingStore, nil
}

func (s *serverAdapter) PrepareEmbeddingStore(ctx context.Context, request *protocol.PrepareEmbeddingStoreRequest) (*protocol.EmbeddingWriteResult, error) {
	owner, err := s.embeddingStore()
	if err != nil {
		return nil, err
	}
	return embeddingPrepareStore.serve(ctx, s, request, owner.PrepareEmbeddingStore)
}

func (s *serverAdapter) ReadEmbeddingState(ctx context.Context, request *protocol.ReadEmbeddingStateRequest) (*protocol.EmbeddingState, error) {
	owner, err := s.embeddingStore()
	if err != nil {
		return nil, err
	}
	return embeddingReadState.serve(ctx, s, request, owner.ReadEmbeddingState)
}

func (s *serverAdapter) MutateEmbeddingState(ctx context.Context, request *protocol.MutateEmbeddingStateRequest) (*protocol.EmbeddingWriteResult, error) {
	owner, err := s.embeddingStore()
	if err != nil {
		return nil, err
	}
	return embeddingMutateState.serve(ctx, s, request, owner.MutateEmbeddingState)
}

func (s *serverAdapter) AppendEmbeddingEvent(ctx context.Context, request *protocol.AppendEmbeddingEventRequest) (*protocol.EmbeddingWriteResult, error) {
	owner, err := s.embeddingStore()
	if err != nil {
		return nil, err
	}
	return embeddingAppendEvent.serve(ctx, s, request, owner.AppendEmbeddingEvent)
}

func (s *serverAdapter) ReadEmbeddingEvents(ctx context.Context, request *protocol.ReadEmbeddingEventsRequest) (*protocol.EmbeddingEventPage, error) {
	owner, err := s.embeddingStore()
	if err != nil {
		return nil, err
	}
	return embeddingReadEvents.serve(ctx, s, request, owner.ReadEmbeddingEvents)
}

func (s *serverAdapter) DescribeEmbeddingTarget(ctx context.Context, request *protocol.DescribeEmbeddingTargetRequest) (*protocol.EmbeddingTargetObjects, error) {
	owner, err := s.embeddingStore()
	if err != nil {
		return nil, err
	}
	return serveOffline(ctx, s, request, provider.ValidateDescribeEmbeddingTargetRequest, provider.ValidateEmbeddingTargetObjects, owner.DescribeEmbeddingTarget)
}

func (s *serverAdapter) ResolveEmbeddingRelations(ctx context.Context, request *protocol.ResolveEmbeddingRelationsRequest) (*protocol.EmbeddingRelations, error) {
	owner, err := s.embeddingStore()
	if err != nil {
		return nil, err
	}
	return embeddingResolveRelations.serve(ctx, s, request, owner.ResolveEmbeddingRelations)
}

func (s *serverAdapter) MeasureEmbedding(ctx context.Context, request *protocol.MeasureEmbeddingRequest) (*protocol.EmbeddingMeasurement, error) {
	owner, err := s.embeddingStore()
	if err != nil {
		return nil, err
	}
	return embeddingMeasure.serve(ctx, s, request, owner.MeasureEmbedding)
}

func (s *serverAdapter) PrepareEmbeddingTarget(ctx context.Context, request *protocol.PrepareEmbeddingTargetRequest) (*protocol.EmbeddingWriteResult, error) {
	owner, err := s.embeddingStore()
	if err != nil {
		return nil, err
	}
	return embeddingPrepareTarget.serve(ctx, s, request, owner.PrepareEmbeddingTarget)
}

func (s *serverAdapter) BuildEmbeddingIndex(ctx context.Context, request *protocol.BuildEmbeddingIndexRequest) (*protocol.EmbeddingWriteResult, error) {
	owner, err := s.embeddingStore()
	if err != nil {
		return nil, err
	}
	return embeddingBuildIndex.serve(ctx, s, request, owner.BuildEmbeddingIndex)
}

func (s *serverAdapter) ReadEmbeddingStructure(ctx context.Context, request *protocol.ReadEmbeddingStructureRequest) (*protocol.EmbeddingStructure, error) {
	owner, err := s.embeddingStore()
	if err != nil {
		return nil, err
	}
	return embeddingReadStructure.serve(ctx, s, request, owner.ReadEmbeddingStructure)
}

func (s *serverAdapter) DropEmbeddingGeneration(ctx context.Context, request *protocol.DropEmbeddingGenerationRequest) (*protocol.EmbeddingWriteResult, error) {
	owner, err := s.embeddingStore()
	if err != nil {
		return nil, err
	}
	return embeddingDropGeneration.serve(ctx, s, request, owner.DropEmbeddingGeneration)
}

func (s *serverAdapter) ScanEmbeddingSource(ctx context.Context, request *protocol.ScanEmbeddingSourceRequest) (*protocol.EmbeddingSourcePage, error) {
	owner, err := s.embeddingStore()
	if err != nil {
		return nil, err
	}
	return embeddingScanSource.serve(ctx, s, request, owner.ScanEmbeddingSource)
}

func (s *serverAdapter) ReadEmbeddingSourceRows(ctx context.Context, request *protocol.ReadEmbeddingSourceRowsRequest) (*protocol.EmbeddingSourceRows, error) {
	owner, err := s.embeddingStore()
	if err != nil {
		return nil, err
	}
	return embeddingReadSourceRows.serve(ctx, s, request, owner.ReadEmbeddingSourceRows)
}

func (s *serverAdapter) ReadEmbeddingBatchState(ctx context.Context, request *protocol.ReadEmbeddingBatchStateRequest) (*protocol.EmbeddingBatchState, error) {
	owner, err := s.embeddingStore()
	if err != nil {
		return nil, err
	}
	return embeddingReadBatchState.serve(ctx, s, request, owner.ReadEmbeddingBatchState)
}

func (s *serverAdapter) CommitEmbeddingBatch(ctx context.Context, request *protocol.CommitEmbeddingBatchRequest) (*protocol.EmbeddingWriteResult, error) {
	owner, err := s.embeddingStore()
	if err != nil {
		return nil, err
	}
	return embeddingCommitBatch.serve(ctx, s, request, owner.CommitEmbeddingBatch)
}

func (s *serverAdapter) ReadEmbeddingVerification(ctx context.Context, request *protocol.ReadEmbeddingVerificationRequest) (*protocol.EmbeddingVerificationPage, error) {
	owner, err := s.embeddingStore()
	if err != nil {
		return nil, err
	}
	return embeddingReadVerification.serve(ctx, s, request, owner.ReadEmbeddingVerification)
}

func (s *serverAdapter) SearchEmbeddings(ctx context.Context, request *protocol.SearchEmbeddingsRequest) (*protocol.EmbeddingSearchResult, error) {
	owner, err := s.embeddingStore()
	if err != nil {
		return nil, err
	}
	return embeddingSearch.serve(ctx, s, request, owner.SearchEmbeddings)
}
